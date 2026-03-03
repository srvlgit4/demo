import os
import re
import shutil
import asyncio
import zipfile
import threading
from bs4 import BeautifulSoup
from docx import Document
from flask import Flask
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes

# ==========================================
# DUMMY WEB SERVER (KEEPS RENDER AWAKE)
# ==========================================
app = Flask(__name__)

@app.route('/')
def home():
    return "Bot is running beautifully!"

def run_web_server():
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)

# ==========================================
# CONFIGURATION
# ==========================================
# Best practice: Use Environment Variables on Render, but fallback to strings if missing
TOKEN = os.environ.get("BOT_TOKEN", "7848041378:AAFI3rXRkZpECImNAAqULQDCCs4dN9VLBoc")
GROUP_ID = int(os.environ.get("GROUP_ID", -1003745983576))
DEFAULT_DOCX_CHUNK = 50
DEFAULT_EPUB_CHUNK = 500

# State Management
document_queue = asyncio.Queue()
user_chunk_sizes = {}
pending_uploads = {}

# ==========================================
# LOGIC 1: DOCX SPLITTER (WITH TOC BYPASS)
# ==========================================
def split_docx_logic(input_path, output_dir, chunk_size, output_format):
    if not os.path.exists(output_dir): os.makedirs(output_dir)
    doc = Document(input_path)
    base_name = os.path.splitext(os.path.basename(input_path))[0]
    collector = []
    generated_files = []

    current_start = 1
    target_chapter = None
    first_chapter_found = False
    pattern_num = r"^(?:vol(?:ume)?\s*\d+\s*)?(?:chapter|ch|c|अध्याय|चैप्टर|#|नॉवेलटैप|उपन्यासटैप|सी|पेज|पृष्ठ|000)?\s*(\d+)(?:[:\s-]|$)"

    def save_chunk(lines, start, end, format_type):
        ext = ".txt" if format_type == "txt" else ".docx"
        part_name = f"{start}_to_{end}-{base_name}{ext}" if end not in ["End", "Full"] else f"{end}-{base_name}{ext}"
        if end == "End": part_name = f"{start}_to_End-{base_name}{ext}"
        part_path = os.path.join(output_dir, part_name)

        if format_type == "txt":
            with open(part_path, "w", encoding="utf-8") as f: f.write("\n\n".join(lines))
        else:
            new_doc = Document()
            for line in lines: new_doc.add_paragraph(line)
            new_doc.save(part_path)
        generated_files.append(part_path)

    lines = [para.text.strip() for para in doc.paragraphs]

    def is_toc_entry(index):
        lines_checked = 0
        for j in range(index + 1, len(lines)):
            if not lines[j]: continue
            lines_checked += 1
            if lines_checked > 8: break
            if re.match(pattern_num, lines[j], re.IGNORECASE): return True
        return False

    for i, text in enumerate(lines):
        if text and not first_chapter_found:
            match = re.match(pattern_num, text, re.IGNORECASE)
            if match and not is_toc_entry(i):
                detected_num = int(match.group(1))
                current_start = detected_num
                target_chapter = detected_num + chunk_size
                first_chapter_found = True

        is_boundary = False
        if text and first_chapter_found:
            match = re.match(pattern_num, text, re.IGNORECASE)
            if match and int(match.group(1)) == target_chapter:
                is_boundary = True

        if is_boundary:
            if collector: save_chunk(collector, current_start, target_chapter - 1, output_format)
            collector = [text]
            current_start = target_chapter
            target_chapter += chunk_size
        else:
            collector.append(text)

    if collector:
        end_marker = "End" if first_chapter_found else "Full"
        save_chunk(collector, current_start, end_marker, output_format)

    return generated_files

# ==========================================
# LOGIC 2: EPUB TO DOCX CONVERTER (ZIP CRACKER METHOD)
# ==========================================
def clean_text_for_xml(text):
    return re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', text)

def natural_sort_key(s):
    return [int(text) if text.isdigit() else text.lower() for text in re.split(r'(\d+)', s)]

def split_epub_logic(input_path, output_dir, chunk_size):
    if not os.path.exists(output_dir): os.makedirs(output_dir)
    base_name = os.path.splitext(os.path.basename(input_path))[0]
    clean_name = re.sub(r'[^\w\-_]', '_', base_name)
    generated_files = []

    try:
        text_buffer = []
        chapter_count = 0
        chunk_count = 1

        with zipfile.ZipFile(input_path, 'r') as epub_zip:
            html_files = [f for f in epub_zip.namelist() if f.lower().endswith(('.html', '.xhtml', '.htm'))]
            html_files.sort(key=natural_sort_key)

            for file_name in html_files:
                try:
                    content = epub_zip.read(file_name).decode('utf-8', errors='ignore')
                    soup = BeautifulSoup(content, 'html.parser')

                    for s in soup(['script', 'style']): s.decompose()

                    raw_text = soup.get_text(separator='\n', strip=True)
                    lines = [clean_text_for_xml(line.strip()) for line in raw_text.split('\n') if line.strip()]

                    if lines:
                        text_buffer.extend(lines)
                        text_buffer.append("---")
                        chapter_count += 1

                    if chapter_count >= chunk_size:
                        part_path = os.path.join(output_dir, f"Part_{chunk_count}-{clean_name}.docx")
                        doc = Document()
                        for line in text_buffer:
                            doc.add_paragraph(line)
                        doc.save(part_path)
                        generated_files.append(part_path)

                        text_buffer = []
                        chapter_count = 0
                        chunk_count += 1
                except Exception as e:
                    print(f"Skipping bad EPUB section {file_name}: {e}")
                    continue

        if text_buffer:
            part_path = os.path.join(output_dir, f"Part_{chunk_count}-{clean_name}.docx")
            doc = Document()
            for line in text_buffer:
                doc.add_paragraph(line)
            doc.save(part_path)
            generated_files.append(part_path)

        return generated_files
    except Exception as e:
        print(f"Total EPUB Zip failure: {e}")
        return []

# ==========================================
# BACKGROUND WORKER (THE QUEUE PROCESSOR)
# ==========================================
async def queue_worker():
    while True:
        job = await document_queue.get()
        context, status_msg = job['context'], job['status_msg']
        input_path, output_dir = job['input_path'], job['output_dir']
        base_name, file_name = job['base_name'], job['file_name']

        try:
            loop = asyncio.get_event_loop()

            if job['type'] == 'docx':
                format_name = "TXT" if job['format'] == "txt" else "DOCX"
                await status_msg.edit_text(f"⚙️ Processing DOCX: `{file_name}` into chunks of {job['chunk_size']} as {format_name}...")
                files = await loop.run_in_executor(None, split_docx_logic, input_path, output_dir, job['chunk_size'], job['format'])
                err_msg = "⚠️ No chapters found. Is formatting correct?"
                intro_msg = f"📚 **{base_name}**\n👤 Uploaded by: {job['user_mention']}\n📄 Format: {format_name}"

            elif job['type'] == 'epub':
                await status_msg.edit_text(f"⚙️ Extracting EPUB Data: `{file_name}` into chunks of {job['chunk_size']}...")
                files = await loop.run_in_executor(None, split_epub_logic, input_path, output_dir, job['chunk_size'])
                err_msg = "⚠️ No readable text found. EPUB is heavily corrupted."
                intro_msg = f"📚 **{base_name}**\n👤 Uploaded by: {job['user_mention']}\n🧩 Split size: {job['chunk_size']} chapters"

            if not files:
                await status_msg.edit_text(err_msg)
                continue

            thread_id = None
            try:
                topic = await context.bot.create_forum_topic(chat_id=GROUP_ID, name=base_name[:128])
                thread_id = topic.message_thread_id
                await status_msg.edit_text(f"✅ Created topic: **{base_name[:64]}**\n📤 Sending files...")
                await context.bot.send_message(chat_id=GROUP_ID, message_thread_id=thread_id, text=intro_msg)

                if job['type'] == 'epub':
                    with open(input_path, 'rb') as orig:
                        await context.bot.send_document(chat_id=GROUP_ID, message_thread_id=thread_id, document=orig, caption="📁 Original EPUB File")
            except Exception as e:
                await status_msg.edit_text(f"⚠️ Topic Error: Sending to main chat.")

            for f in files:
                with open(f, 'rb') as doc:
                    msg = await status_msg.reply_document(document=doc, filename=os.path.basename(f))
                    try:
                        forward_args = {"chat_id": GROUP_ID, "from_chat_id": msg.chat.id, "message_id": msg.message_id}
                        if thread_id: forward_args["message_thread_id"] = thread_id
                        await context.bot.forward_message(**forward_args)
                    except: pass

            await status_msg.reply_text("🎉 Done! Files sent successfully.")

        except Exception as e:
            await status_msg.edit_text(f"❌ Error: {e}")
        finally:
            if os.path.exists(job['temp_dir']): shutil.rmtree(job['temp_dir'])
            document_queue.task_done()

# ==========================================
# BOT HANDLERS
# ==========================================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = update.effective_user.first_name
    await update.message.reply_text(
        f"👋 Hello {name}!\n\n"
        f"Send me a **.docx** or **.epub** file.\n"
        f"`/set 500` - Change custom chunk size."
    )

async def set_chunk_size(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        new_size = int(context.args[0])
        user_chunk_sizes[update.effective_user.id] = new_size
        await update.message.reply_text(f"✅ Custom split size set to **{new_size}** chapters.")
    except: await update.message.reply_text("⚠️ Example: `/set 100`")

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    doc = update.message.document
    file_name = doc.file_name.lower()
    msg_id = update.message.message_id

    if not (file_name.endswith('.docx') or file_name.endswith('.epub')):
        await update.message.reply_text("❌ Only .docx or .epub files allowed.")
        return

    pending_uploads[msg_id] = {
        'document': doc,
        'user_mention': f"@{update.effective_user.username}" if update.effective_user.username else update.effective_user.first_name,
        'user_id': update.effective_user.id
    }

    if file_name.endswith('.docx'):
        keyboard = [[InlineKeyboardButton("📄 DOCX", callback_data=f"docx|docx|{msg_id}"), InlineKeyboardButton("📝 TXT", callback_data=f"docx|txt|{msg_id}")]]
        await update.message.reply_text("DOCX detected. Save chunks as:", reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        keyboard = [
            [InlineKeyboardButton(f"🔪 {DEFAULT_EPUB_CHUNK} Chapters", callback_data=f"epub|{DEFAULT_EPUB_CHUNK}|{msg_id}")],
            [InlineKeyboardButton("⚙️ Use my /set size", callback_data=f"epub|custom|{msg_id}")]
        ]
        await update.message.reply_text("EPUB detected. How many chapters per file?", reply_markup=InlineKeyboardMarkup(keyboard))

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data.split("|")
    job_type, choice, msg_id = data[0], data[1], int(data[2])

    if msg_id not in pending_uploads:
        await query.edit_message_text("❌ Session expired. Please upload again.")
        return

    job_info = pending_uploads.pop(msg_id)
    document = job_info['document']
    user_id = job_info['user_id']

    file_name = document.file_name
    temp_dir = f"temp_{user_id}_{msg_id}"
    input_path = os.path.join(temp_dir, file_name)
    os.makedirs(os.path.join(temp_dir, "output"), exist_ok=True)

    await query.edit_message_text(f"📥 Downloading `{file_name}`...")
    await (await document.get_file()).download_to_drive(input_path)

    job_data = {
        'type': job_type,
        'update': update, 'context': context, 'status_msg': query.message,
        'temp_dir': temp_dir, 'input_path': input_path, 'output_dir': os.path.join(temp_dir, "output"),
        'file_name': file_name, 'base_name': os.path.splitext(file_name)[0][:64].strip(),
        'user_mention': job_info['user_mention']
    }

    if job_type == 'docx':
        job_data['format'] = choice
        job_data['chunk_size'] = user_chunk_sizes.get(user_id, DEFAULT_DOCX_CHUNK)
    else:
        job_data['chunk_size'] = user_chunk_sizes.get(user_id, DEFAULT_EPUB_CHUNK) if choice == "custom" else int(choice)

    await document_queue.put(job_data)

# ==========================================
# MAIN RUNNER
# ==========================================
async def start_background_tasks(app: Application):
    asyncio.create_task(queue_worker())

def main():
    print("🤖 Starting Web Server for Render Keep-Alive...")
    threading.Thread(target=run_web_server, daemon=True).start()

    print("🤖 Ultimate Cracker Bot Initializing...")
    app = Application.builder().token(TOKEN).post_init(start_background_tasks).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("set", set_chunk_size))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(CallbackQueryHandler(button_callback))
    
    print("🚀 Master Bot is LIVE!")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
