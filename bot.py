import os
import re
import shutil
import asyncio
import zipfile
import html
import gc
import tempfile
import xml.etree.ElementTree as ET
from aiohttp import web
from docx import Document
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes

# ==========================================
# CONFIGURATION
# ==========================================
PORT = int(os.environ.get("PORT", 8080))
TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "9110472039:AAFkEYZpMZmknuGec5uhSTFExfKs1fOznfo")

DEFAULT_DOCX_CHUNK = 500
DEFAULT_EPUB_CHUNK = 1000
WORDS_PER_PAGE = 500
x = 25
document_queue = None

user_chunk_sizes = {}
pending_uploads = {}

PROMO_MESSAGE = (
    "यह उपन्यास Noveltap द्वारा बनाया गया है। यदि आप इसे किसी अन्य स्थान पर सुनते हैं, "
    "तो कृपया मूल ऑडियो मालिक को इसकी रिपोर्ट करें। हम आपको सबसे बेहतरीन नॉवेल्स प्रदान करते हैं "
    "और हमारे ग्रुप पर रोज़ाना 5 नए नॉवेल्स अपलोड किए जाते हैं। अधिक एपिसोड और दैनिक अपडेट के लिए "
    "हमारे ऑफिशियल Noveltap ग्रुप से जुड़ें। अडल्ट और एक्सक्लूसिव नॉवेल्स के लिए हमारे प्रीमियम "
    "ग्रुप की मेंबरशिप लें। धन्यवाद!"
)

# ==========================================
# TABLE OF CONTENTS DETECTION & REMOVAL
# ==========================================
CHAPTER_LINE_RE = re.compile(
    r'^\s*(chapter[\s\-:]*[\d]+|अध्याय[\s]*[\d]+|चैप्टर[\s]*[\d]+)',
    re.IGNORECASE
)
VOLUME_LINE_RE = re.compile(
    r'^\s*(volume[\s]*[\d]+|खंड[\s]*[\d]+|भाग[\s]*[\d]+|part[\s]*[\d]+)',
    re.IGNORECASE
)
CONTENT_ANCHOR_RE = re.compile(
    r'^\s*(synopsis|summary|prologue|preface|introduction|foreword|chapter\s*1\b'
    r'|सारांश|भूमिका|प्रस्तावना|परिचय|प्रारंभ|अध्याय\s*1\b|चैप्टर\s*1\b)',
    re.IGNORECASE
)

def is_toc_line(stripped):
    """Returns True if a line looks like a TOC entry."""
    return (
        not stripped or
        stripped == '---' or
        bool(CHAPTER_LINE_RE.match(stripped)) or
        bool(VOLUME_LINE_RE.match(stripped)) or
        bool(re.match(r'^:\s*.+', stripped))
    )

def is_toc_only_block(lines, threshold=0.85):
    """Returns True if the block of lines is overwhelmingly TOC/chapter-title lines."""
    non_blank = [l.strip() for l in lines if l.strip() and l.strip() != '---']
    if not non_blank:
        return True
    toc_lines = sum(
        1 for l in non_blank
        if CHAPTER_LINE_RE.match(l) or VOLUME_LINE_RE.match(l)
        or re.match(r'^:\s*.+', l)
    )
    ratio = toc_lines / len(non_blank)
    return ratio >= threshold

def find_toc_block(lines, min_chapter_hits=20):
    """Scans lines and returns (toc_start, toc_end) if a TOC block is found."""
    consecutive_chapter_lines = 0
    run_start = -1
    toc_start = -1

    for i, line in enumerate(lines):
        stripped = line.strip()
        is_chapter = bool(CHAPTER_LINE_RE.match(stripped))
        is_volume  = bool(VOLUME_LINE_RE.match(stripped))
        is_blank   = not stripped
        is_sep     = (stripped == '---')

        if is_chapter or is_volume or is_blank or is_sep:
            if is_chapter or is_volume:
                if run_start == -1:
                    run_start = i
                consecutive_chapter_lines += 1
            if consecutive_chapter_lines >= min_chapter_hits:
                toc_start = run_start
                break
        else:
            consecutive_chapter_lines = 0
            run_start = -1

    if toc_start == -1:
        return -1, -1

    toc_end = toc_start
    for i in range(toc_start, len(lines)):
        stripped = lines[i].strip()
        if is_toc_line(stripped):
            toc_end = i
        else:
            break

    return toc_start, toc_end

def remove_toc(lines, min_chapter_hits=20):
    """Robust TOC removal handling both beginning and end TOCs."""
    if is_toc_only_block(lines):
        return []

    toc_start, toc_end = find_toc_block(lines, min_chapter_hits)
    if toc_start == -1:
        return lines

    total_lines = len(lines)
    toc_position_ratio = toc_start / total_lines if total_lines else 0

    if toc_position_ratio > 0.5:
        result = lines[:toc_start]
        while result and (not result[-1].strip() or result[-1].strip() == '---'):
            result.pop()
        return result if result else []

    content_start = -1
    for i in range(toc_end + 1, len(lines)):
        stripped = lines[i].strip()
        if not stripped or stripped == '---':
            continue
        if CONTENT_ANCHOR_RE.match(stripped):
            content_start = i
            break
        if CHAPTER_LINE_RE.match(stripped):
            for j in range(i + 1, min(i + 10, len(lines))):
                nxt = lines[j].strip()
                if not nxt:
                    continue
                if not CHAPTER_LINE_RE.match(nxt) and not VOLUME_LINE_RE.match(nxt):
                    content_start = i
                    break
            if content_start != -1:
                break
        content_start = i
        break

    return lines[content_start:] if content_start != -1 else []

# ==========================================
# LIGHTNING WRITER & EXTRACTOR
# ==========================================
def fast_read_docx(input_path):
    """Clean XML text extraction for DOCX."""
    lines = []
    try:
        with zipfile.ZipFile(input_path, 'r') as docx_zip:
            xml_content = docx_zip.read('word/document.xml')
            tree = ET.fromstring(xml_content)
            ns = {'w': 'http://schemas.openxmlformats.org/wordprocessingml/2006/main'}
            for para in tree.findall('.//w:p', namespaces=ns):
                texts = [t.text for t in para.findall('.//w:t', namespaces=ns) if t.text]
                if texts:
                    lines.append("".join(texts).strip())
    except Exception as e:
        print(f"⚠️ XML Extraction Failed: {e}")
    return lines

def fast_html_to_text(raw_html):
    """Clean Regex HTML parser."""
    text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', raw_html)
    text = re.sub(r'<(script|style|head)[^>]*>.*?</\1>', '', text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r'</?(p|div|h[1-6]|br|tr|li)[^>]*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = html.unescape(text)
    return [line.strip() for line in text.split('\n') if line.strip()]

def natural_sort_key(s):
    return [int(text) if text.isdigit() else text.lower() for text in re.split(r'(\d+)', s)]

def save_chunk(lines, path, fmt):
    """Lightning Fast XML Writer for DOCX, standard writer for TXT."""
    if fmt == 'txt':
        with open(path, 'w', encoding='utf-8') as f:
            f.write('\n\n'.join(lines))
    else:
        doc = Document()
        doc.save(path)
        xml_paras = []
        for line in lines:
            safe_text = html.escape(line)
            xml_paras.append(f'<w:p><w:r><w:t>{safe_text}</w:t></w:r></w:p>')
        body_xml = "".join(xml_paras)
        temp_path = path + ".tmp"
        with zipfile.ZipFile(path, 'r') as zin:
            with zipfile.ZipFile(temp_path, 'w', compression=zipfile.ZIP_DEFLATED) as zout:
                for item in zin.infolist():
                    if item.filename == 'word/document.xml':
                        orig_xml = zin.read(item.filename).decode('utf-8')
                        new_xml = orig_xml.replace('<w:sectPr', body_xml + '<w:sectPr')
                        zout.writestr(item, new_xml.encode('utf-8'))
                    else:
                        zout.writestr(item, zin.read(item.filename))
        os.replace(temp_path, path)
    gc.collect()

def is_hindi_text(text_sample):
    return True

def clean_existing_promos(lines):
    """Removes previous promo messages so re-uploading doesn't stack them."""
    cleaned = []
    for line in lines:
        if "Noveltap" in line or "मूल ऑडियो मालिक" in line:
            continue
        cleaned.append(line)
    return cleaned

# ==========================================
# PAGE-BASED SPLITTER LOGIC (TXT & DOCX)
# ==========================================
def split_text_based_logic(input_path, output_dir, chunk_size_pages, output_format, is_txt_file=False):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    base_name = os.path.splitext(os.path.basename(input_path))[0]
    generated_files = []

    if is_txt_file:
        with open(input_path, "r", encoding="utf-8", errors="ignore") as f:
            lines = [line.strip() for line in f if line.strip()]
    else:
        lines = fast_read_docx(input_path)

    lines = remove_toc(lines)
    lines = clean_existing_promos(lines)

    if not lines:
        return []

    sample_text = " ".join(lines[:30])
    is_hindi_doc = is_hindi_text(sample_text)

    max_words_per_chunk = chunk_size_pages * WORDS_PER_PAGE
    collector = []
    current_word_count = 0
    start_page = 1

    promo_interval_words = x * WORDS_PER_PAGE  
    words_since_last_promo = 0

    for text in lines:
        collector.append(text)

        words_in_line = len(text.split())
        current_word_count += words_in_line
        words_since_last_promo += words_in_line

        if is_hindi_doc and words_since_last_promo >= promo_interval_words:
            collector.append(PROMO_MESSAGE)
            words_since_last_promo = 0  

        if current_word_count >= max_words_per_chunk:
            end_page = start_page + chunk_size_pages - 1
            ext = 'txt' if output_format == 'txt' else 'docx'
            part_name = f"Pages_{start_page}-{base_name}.{ext}"
            part_path = os.path.join(output_dir, part_name)

            save_chunk(collector, part_path, output_format)
            generated_files.append(part_path)

            collector = []
            current_word_count = 0
            words_since_last_promo = 0
            start_page = end_page + 1

    if collector:
        ext = 'txt' if output_format == 'txt' else 'docx'
        part_name = f"Pages_{start_page}_to_End-{base_name}.{ext}"
        part_path = os.path.join(output_dir, part_name)
        save_chunk(collector, part_path, output_format)
        generated_files.append(part_path)

    del lines
    gc.collect()
    return generated_files

def split_docx_logic(input_path, output_dir, chunk_size, output_format):
    return split_text_based_logic(input_path, output_dir, chunk_size, output_format, is_txt_file=False)

def split_txt_logic(input_path, output_dir, chunk_size, output_format):
    return split_text_based_logic(input_path, output_dir, chunk_size, output_format, is_txt_file=True)

# ==========================================
# EPUB SPLITTER LOGIC
# ==========================================
def split_epub_logic(input_path, output_dir, chunk_size, output_format):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    base_name = os.path.splitext(os.path.basename(input_path))[0]
    clean_name = re.sub(r'[^\w\-_]', '_', base_name)
    generated_files = []

    try:
        all_lines = []

        with zipfile.ZipFile(input_path, 'r') as epub_zip:
            html_files = [f for f in epub_zip.namelist() if f.lower().endswith(('.html', '.xhtml', '.htm'))]
            html_files.sort(key=natural_sort_key)

            for file_name in html_files:
                try:
                    content = epub_zip.read(file_name).decode('utf-8', errors='ignore')
                    lines = fast_html_to_text(content)
                    if not lines:
                        continue

                    if is_toc_only_block(lines):
                        continue

                    all_lines.extend(lines)
                    all_lines.append("---")
                except Exception:
                    continue

        if not all_lines:
            return []

        all_lines = remove_toc(all_lines)

        if not all_lines:
            return []

        text_buffer = []
        chapter_count = 0
        chunk_count = 1

        for line in all_lines:
            text_buffer.append(line)
            if line == "---":
                chapter_count += 1
            if chapter_count >= chunk_size:
                ext = 'txt' if output_format == 'txt' else 'docx'
                part_name = f"Part_{chunk_count}-{clean_name}.{ext}"
                part_path = os.path.join(output_dir, part_name)
                save_chunk(text_buffer, part_path, output_format)
                generated_files.append(part_path)
                text_buffer = []
                chapter_count = 0
                chunk_count += 1
                gc.collect()

        if text_buffer:
            ext = 'txt' if output_format == 'txt' else 'docx'
            part_name = f"Part_{chunk_count}-{clean_name}.{ext}"
            part_path = os.path.join(output_dir, part_name)
            save_chunk(text_buffer, part_path, output_format)
            generated_files.append(part_path)

        gc.collect()
        return generated_files

    except Exception:
        return []

# ==========================================
# BACKGROUND WORKER
# ==========================================
async def queue_worker():
    while True:
        job = await document_queue.get()
        context, status_msg = job['context'], job['status_msg']
        input_path, output_dir = job['input_path'], job['output_dir']
        file_name = job['file_name']

        try:
            loop = asyncio.get_running_loop()
            format_name = "TXT" if job['format'] == "txt" else "DOCX"

            if job['type'] == 'docx':
                await status_msg.edit_text(f"⚡ Processing DOCX: `{file_name}`...")
                files = await loop.run_in_executor(None, split_docx_logic, input_path, output_dir, job['chunk_size'], job['format'])
            elif job['type'] == 'txt':
                await status_msg.edit_text(f"⚡ Processing TXT: `{file_name}`...")
                files = await loop.run_in_executor(None, split_txt_logic, input_path, output_dir, job['chunk_size'], job['format'])
            elif job['type'] == 'epub':
                await status_msg.edit_text(f"⚡ Processing EPUB: `{file_name}`...")
                files = await loop.run_in_executor(None, split_epub_logic, input_path, output_dir, job['chunk_size'], job['format'])

            if not files:
                await status_msg.edit_text("⚠️ No readable data found or file is corrupted.")
                continue

            await status_msg.edit_text(f"✅ Done! Sending {len(files)} files to you...")

            for f in files:
                with open(f, 'rb') as doc:
                    try:
                        await status_msg.reply_document(document=doc, filename=os.path.basename(f))
                        await asyncio.sleep(0.5)
                    except Exception as e:
                        print(f"⚠️ Error sending {os.path.basename(f)}: {e}")

            await status_msg.reply_text("🎉 All files sent successfully!")

        except Exception as e:
            await status_msg.edit_text(f"❌ Error: {e}")
        finally:
            if os.path.exists(job['temp_dir']):
                shutil.rmtree(job['temp_dir'], ignore_errors=True)
            document_queue.task_done()
            gc.collect()

# ==========================================
# BOT HANDLERS
# ==========================================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = update.effective_user.first_name
    await update.message.reply_text(
        f"👋 Hello {name}!\n\n"
        f"Ultra-Fast Splitter Bot with Auto TOC Removal.\n\n"
        f"📁 Send me a **.docx**, **.txt**, or **.epub** file.\n"
        f"⚙️ `/set 50` — Change chunk size (Pages for DOCX/TXT, Chapters for EPUB).\n\n"
        f"📋 TOC is auto-detected & removed for English and Hindi books!"
    )

async def set_chunk_size(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        new_size = int(context.args[0])
        user_chunk_sizes[update.effective_user.id] = new_size
        await update.message.reply_text(f"✅ Chunk size set to **{new_size}**.")
    except:
        await update.message.reply_text("⚠️ Usage: `/set 50`")

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    doc = update.message.document
    file_name = doc.file_name.lower()
    msg_id = update.message.message_id

    if not (file_name.endswith('.docx') or file_name.endswith('.epub') or file_name.endswith('.txt')):
        await update.message.reply_text("❌ Only .docx, .txt, or .epub files allowed.")
        return

    pending_uploads[msg_id] = {
        'document': doc,
        'user_id': update.effective_user.id
    }

    if file_name.endswith('.docx'):
        keyboard = [[
            InlineKeyboardButton("📄 DOCX", callback_data=f"docx|docx|{msg_id}"),
            InlineKeyboardButton("📝 TXT",  callback_data=f"docx|txt|{msg_id}")
        ]]
        await update.message.reply_text("DOCX detected. Save chunks as:", reply_markup=InlineKeyboardMarkup(keyboard))

    elif file_name.endswith('.txt'):
        keyboard = [[
            InlineKeyboardButton("📄 DOCX", callback_data=f"txt|docx|{msg_id}"),
            InlineKeyboardButton("📝 TXT",  callback_data=f"txt|txt|{msg_id}")
        ]]
        await update.message.reply_text("TXT detected. Save chunks as:", reply_markup=InlineKeyboardMarkup(keyboard))

    else:
        keyboard = [
            [
                InlineKeyboardButton(f"📄 DOCX ({DEFAULT_EPUB_CHUNK} ch)", callback_data=f"epub|docx_def|{msg_id}"),
                InlineKeyboardButton(f"📝 TXT ({DEFAULT_EPUB_CHUNK} ch)",  callback_data=f"epub|txt_def|{msg_id}")
            ],
            [
                InlineKeyboardButton("⚙️ DOCX (Custom /set)", callback_data=f"epub|docx_cust|{msg_id}"),
                InlineKeyboardButton("⚙️ TXT (Custom /set)",  callback_data=f"epub|txt_cust|{msg_id}")
            ]
        ]
        await update.message.reply_text("EPUB detected. Choose format and chunk size:", reply_markup=InlineKeyboardMarkup(keyboard))

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data.split("|")
    job_type, action, msg_id = data[0], data[1], int(data[2])

    if msg_id not in pending_uploads:
        await query.edit_message_text("❌ Session expired. Please upload again.")
        return

    job_info = pending_uploads.pop(msg_id)
    document = job_info['document']
    user_id  = job_info['user_id']
    file_name = document.file_name

    temp_dir = tempfile.mkdtemp()
    input_path = os.path.join(temp_dir, file_name)
    os.makedirs(os.path.join(temp_dir, "output"), exist_ok=True)

    await query.edit_message_text(f"📥 Downloading `{file_name}`...")
    await (await document.get_file()).download_to_drive(input_path)

    job_data = {
        'type': job_type,
        'update': update, 'context': context, 'status_msg': query.message,
        'temp_dir': temp_dir, 'input_path': input_path,
        'output_dir': os.path.join(temp_dir, "output"),
        'file_name': file_name,
        'base_name': os.path.splitext(file_name)[0][:64].strip(),
    }

    if job_type in ['docx', 'txt']:
        job_data['format']    = action
        job_data['chunk_size'] = user_chunk_sizes.get(user_id, DEFAULT_DOCX_CHUNK)
    else:
        format_choice = action.split('_')[0]
        size_type     = action.split('_')[1]
        job_data['format']    = format_choice
        job_data['chunk_size'] = user_chunk_sizes.get(user_id, DEFAULT_EPUB_CHUNK) if size_type == "cust" else DEFAULT_EPUB_CHUNK

    await document_queue.put(job_data)

# ==========================================
# RENDER HEALTH CHECK SERVER
# ==========================================
async def handle_ping(request):
    return web.Response(text="Bot is running successfully!")

async def web_server():
    app_web = web.Application()
    app_web.router.add_get("/", handle_ping)
    runner = web.AppRunner(app_web)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    print(f"🌐 Web health check server started on port {PORT}")

# ==========================================
# MAIN RUNNER
# ==========================================
async def start_background_tasks(app: Application):
    global document_queue
    document_queue = asyncio.Queue()
    asyncio.create_task(queue_worker())
    asyncio.create_task(web_server())

def main():
    print("🤖 Bot Initializing...")
    app = Application.builder().token(TOKEN).post_init(start_background_tasks).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("set", set_chunk_size))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(CallbackQueryHandler(button_callback))
    print("🚀 Bot is LIVE on Render!")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
