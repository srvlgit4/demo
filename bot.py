import os
import re
import shutil
import asyncio
import zipfile
import html
import gc
import ast
import json
import tempfile
import xml.etree.ElementTree as ET
from typing import Dict
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
user_data = {}  # Tracks replacement dictionaries for files

PROMO_MESSAGE = (
    "यह उपन्यास Noveltap द्वारा बनाया गया है। यदि आप इसे किसी अन्य स्थान पर सुनते हैं, "
    "तो कृपया मूल ऑडियो मालिक को इसकी रिपोर्ट करें। हम आपको सबसे बेहतरीन नॉवेल्स प्रदान करते हैं "
    "और हमारे ग्रुप पर रोज़ाना 5 नए नॉवेल्स अपलोड किए जाते हैं। अधिक एपिसोड और दैनिक अपडेट के लिए "
    "हमारे ऑफिशियल Noveltap ग्रुप से जुड़ें। अडल्ट और एक्सक्लूसिव नॉवेल्स के लिए हमारे प्रीमियम "
    "ग्रुप की मेंबरशिप लें। धन्यवाद!"
)

# ==========================================
# PRE-COMPILED UNWANTED SPAM PATTERNS (SPEED OPTIMIZED)
# ==========================================
RAW_UNWANTED_PATTERNS = [
    r"यह चैप्टर अभी खत्म नहीं हुआ है[\s\S]{0,100}?अगले पेज[^\n]*",
    r"मेरे प्यारे मास्टर, इस चैप्टर में और भी बहुत कुछ है[\s\S]{0,100}?अगले पेज[^\n]*",
    r"अगर आपको[\s\S]{0,200}?पढ़ने में मज़ा आता है, तो[\s\S]{0,100}?कृपया इसे बुकमार्क करें[\s\S]{0,100}?Shuhaige Novel Network[\s\S]{0,150}?अपडेट करता है।",
    r"अगर आपको[\s\S]{0,200}?I Rely on Beating Up Top Students[\s\S]{0,300}?अपडेट करता है।",
    r"कृपया[\s\S]{0,150}?को बुकमार्क करें[\s\S]{0,100}?Shuhaige Novel Network[\s\S]{0,150}?(?:तेज़ी से|अपडेट करता है)।?",
    r"हेलो दोस्तों, अगर आपको[\s\S]{0,100}?बुकमार्क करना न भूलें[\s\S]{0,150}?>\.\<\)",
    r"कृपया नॉवेल[\s\S]{0,50}?\"स्कमबैग टाइकून, आई कैन सी डिज़ायर\"[\s\S]{0,50}?\([\s\S]{0,10}?m\.shuhaige\.net[\s\S]{0,10}?\)[\s\S]{0,50}?को बुकमार्क करें।[\s\S]{0,100}?शुहागे नॉवेल नेटवर्क पूरे इंटरनेट पर सबसे तेज़ी से[\s\S]{0,50}?\"स्कमबैग टाइकून, आई कैन सी डिज़ायर\"[\s\S]{0,50}?को अपडेट करता है।"
]

# Compile once for blazing fast performance on Render
COMPILED_UNWANTED_PATTERNS = [re.compile(p, re.IGNORECASE) for p in RAW_UNWANTED_PATTERNS]

UNWANTED_PHRASES = [
    'कृपया नॉवेल "स्कमबैग टाइकून, आई कैन सी डिज़ायर" (m.shuhaige.net) को बुकमार्क करें। शुहागे नॉवेल नेटवर्क पूरे इंटरनेट पर सबसे तेज़ी से "स्कमबैग टाइकून, आई कैन सी डिज़ायर" को अपडेट करता है।',
    "फ्रेंडली रिमाइंडर: लॉग-इन यूज़र्स के लिए \"इंटरनल मैसेज\" फ़ंक्शन को ऑप्टिमाइज़ किया गया है। हम आपके मैसेज तुरंत पा सकते हैं और उनका जवाब दे सकते हैं। कृपया यूज़र सेंटर में \"इंटरनल मैसेज\" पेज देखें!",
    "फ्रेंडली रिमाइंडर: लॉग-इन यूज़र अपने बुकशेल्फ़ डेटा को सभी डिवाइस पर परमानेंटली सेव कर सकते हैं। हम इस फ़ीचर का इस्तेमाल करने के लिए लॉग-इन करने की सलाह देते हैं.",
    "ध्यान दें: लॉगिन यूज़र्स के \"साइट लेटर\" फ़ंक्शन को ऑप्टिमाइज़ कर दिया गया है, हम समय पर आपका मैसेज पा सकते हैं और उसका जवाब दे सकते हैं, कृपया देखने के लिए यूज़र सेंटर - \"साइट लेटर\" पेज पर जाएं!",
    "एक फ्रेंडली रिमाइंडर: अगर आपको टाइटल से किताब नहीं मिल रही है, तो लेखक को खोजने की कोशिश करें; हो सकता है कि उन्होंने बस अपना नाम बदल लिया हो!",
    "फ्रेंडली रिमाइंडर: पेज के ऊपर दाएं कोने में \"सिंपल/ट्रेडिशनल चाइनीज़ पर स्विच करें\", \"फॉन्ट साइज़ एडजस्ट करें\", और \"बैकग्राउंड कलर पढ़ें\" जैसे फंक्शन हैं।",
    "अगर आपको कोई गलती मिले (टूटे हुए लिंक, नॉन-स्टैंडर्ड कंटेंट, वगैरह), तो प्लीज़ हमें बताएं < रिपोर्ट चैप्टर > ताकि हम उसे जल्द से जल्द ठीक कर सकें।",
    "याद रखें: अगर आपको यह किताब पसंद है, तो कृपया इसे अपनी बुकशेल्फ़ में रख लें ताकि बाद में यह खो न जाए!",
    "लोगों की मांग को देखते हुए, हम अब ऐड-फ़्री VIP मेंबरशिप फ़ीचर लॉन्च कर रहे हैं।",
    "(बाद में एक और चैप्टर होगा)",
    "अमरता की अजीब दुनिया https://",
    "अमरता की विचित्र दुनिया के नवीनतम अध्यायों का सबसे तेज़ अपडेट!",
    "Novel.com",
    "www.wuxiaspot.com",
    "novel543",
    "biquge.tw",
    "ixdzs8.com",
    "wtr-lab",
    "69shuba",
    "webnovel",
    "www.uukanshu.com",
    "अगर आपको कोई गलती मिले (टूटे हुए लिंक, नॉन-STANDARd कंटेंट, वगैरह), तो कृपया हमें बताएं < रिपोर्ट चैप्टर > ताकि हम इसे जल्द से जल्द ठीक कर सकें।",
    "(इसके बाद एक और चैप्टर आएगा)",
    "फैंटेसी: सिस्टम शुरू में अपने आप लेवल अप हो जाता है",
    "फैंटेसी: द सिस्टम ऑटोमैटिकली लेवल्स अप एट द START",
    "फैंटेसी: द सिस्टम स्टार्ट्स WITH ऑटोमैटिक एंड फ्रेंज़ीड लेवलिंग अप",
    "फैंटेसी: सिस्टम शुरू में अपने आप लेवल अप करता है",
    "देखने के लिए क्लिक करें",
    "अनुवादक: एटलस स्टूडियो संपादक: एटलस स्टूडियो",
    "अनुवादक",
    "एटलस स्टूडियो",
    "संपादक",
    "(1/2)",
    "(2/2)",
    "(1/1)",
    "कुछ कारणों से आज अचानक बड़ी संख्या में उपयोगकर्ता इस साइट पर जाने के लिए वेब पेज नहीं खोल पा रहे हैं। कृपया घर का रास्ता खोजने के लिए इस साइट का डोमेन नाम www.wuxiaspot.com (साहित्य का पहला अक्षर + org dot com, wuxiaspot.com) याद रखने के लिए बुक करें दोस्तों!",
    "यदि आप शहरी सुदृढ़ीकरण प्रणाली के पूरे क्षेत्र पर हावी होना चाहते हैं, तो कृपया एकत्र करें: (www.wuxiaspot.com) शहरी सुदृढ़ीकरण प्रणाली में साहित्य का सबसे तेज़ अद्यतन।",
    "किसी कारण से, आज अचानक बड़ी संख्या में उपयोगकर्ता इस साइट पर जाने के लिए वेब पेज नहीं खोल सकते हैं। कृपया घर का रास्ता खोजने के लिए डोमेन नाम www.wuxiaspot.com (साहित्य का पहला अक्षर + ऑर्ग पॉइंट कॉम, wuxiaspot.com) याद रखने के लिए दोस्तों को बुक करें!",
    "TL:", "TL Note:", "Translator:", "Editor:", "Novelfull",
    "Read on Webnovel", "www.webnovel.com", "---", "***", "...",
    "यदि यह पुस्तक आपके अधिकारों का उल्लंघन करती है, तो कृपया लेखक की कॉपीराइट शिकायत के त्वरित समाधान के लिए क्लिक करें।",
    "अगर यह किताब आपके अधिकारों का उल्लंघन करती है, तो कृपया लेखक की कॉपीराइट शिकायत फास्ट ट्रैक तक पहुंचने के लिए क्लिक करें।",
    "https://18188.mitemin.net/i573778/",
    "▼रेफरेंस: उत्तरी वोइस्ट्रा को जीतने का एक मज़बूत और समय लेने वाला रास्ता",
    "रेफरेंस: उत्तरी वोइस्ट्रा को जीतने का एक मज़बूत और समय लेने वाला रास्ता",
    "https://18188.mitemin.net/i281912/",
    "▼रेफरेंस: शॉर्ट-टर्म निर्णायक लड़ाई ने शिकिनॉन शहर, वोइस्ट्रा वेस्टERN रूट से मार्च करने पर मजबूर किया",
    "https://18188.mitemin.net/i281913/",
    "https://18188.mitemin.net/",
    "मैंने अपनी एक्टिविटी रिपोर्ट अपडेट कर दी है, इसलिए अगर आप इंटरेस्टेड हैं तो प्लीज़ इसे देखें।",
    "https://xmypage.syosetu.com/mypageblog/view/xid/175378/blogkey/307369/",
    "आपके समर्थन के लिए धन्यवाद।",
    "m9( ゜धम्) ओवरऑल रेटिंग 140,000 पॉइंट्स से ज़्यादा हो गई है।",
    "https://18188.mitemin.net/i590561/",
    "mitemin.net",
    "https://18188.mitemin.net/","https://",
    "विषय सूची URL: https://novel18.syosetu.com/n9598df/",
    "novel18.syosetu.com",
    "◇◆◇ कॉमिक रूपांतरण वर्तमान में MAGCOMI पर धारावाहिक रूप से प्रसारित हो रहा है ◇◆◇",
    "कॉमिक रूपांतरण वर्तमान में MAGCOMI पर धारावाहिक रूप से प्रसारित हो रहा है",
    "MAGCOMI",
    "http://www.shuhaoxs.com/book/thxghp.html",
    "पर क्लिक करें",
    "http://www.shuhaoxs.com/list-27/ पर जाएं!",
    "चार्मेंडर ने भी नए सदस्यों का स्वागत किया। फैनफिक्शन और डेरिवेटिव काम पसंद हैं? और जानने के लिए ",
    "xiaoyaoshuwu",
    ".com",
    "yunlixs",
    "Shuhaige Novel Network पूरे इंटरनेट पर सबसे तेज़ी से",
    "52shuku ",
    "लिंक्स: रैंकिंग लिस्ट ",
    "बुक सर्च गाइड ",
    "को बुकमार्क करना न भूलें या अपने दोस्तों को भी इसकी सलाह दें!",
    "panshixsw",
    "220book.com",
    "URL: https://novel18.syosetu.com/n0922io/",
    "URL: https://novel18.syosetu.com/",
    "novel18.syosetu.com",
    "sosoxsw.net",
    "xszj520.com",
    "http://www.shuhaoxs.com/book/"
]

# ==========================================
# TABLE OF CONTENTS & CLEANING HELPERS
# ==========================================
CHAPTER_LINE_RE = re.compile(r'^\s*(chapter[\s\-:]*[\d]+|अध्याय[\s]*[\d]+|चैप्टर[\s]*[\d]+)', re.IGNORECASE)
VOLUME_LINE_RE = re.compile(r'^\s*(volume[\s]*[\d]+|खंड[\s]*[\d]+|भाग[\s]*[\d]+|part[\s]*[\d]+)', re.IGNORECASE)
TIMESTAMP_RE = re.compile(r'[\w\s]+,\s\[\d{2}-\d{2}-\d{4}\s\d{2}:\d{2}\]')

def preserve_case(original: str, replacement: str) -> str:
    if original.isupper(): return replacement.upper()
    if original.istitle(): return replacement.title()
    return replacement.lower() if original.islower() else replacement

def clean_text_content(text: str, name_replacements: Dict[str, str]) -> str:
    if not text: return text

    # 1. Remove Telegram timestamps
    text = TIMESTAMP_RE.sub("", text)

    # 2. Remove multi-line spam via pre-compiled regex patterns
    for pattern in COMPILED_UNWANTED_PATTERNS:
        text = pattern.sub("", text)

    # 3. Remove unwanted exact phrases
    for phrase in UNWANTED_PHRASES:
        text = text.replace(phrase, "")

    # 4. Name replacements
    if name_replacements:
        sorted_keys = sorted(name_replacements.keys(), key=len, reverse=True)
        for old_name in sorted_keys:
            rep = name_replacements[old_name]
            pattern = re.compile(r'\b' + re.escape(old_name) + r"(\b|'s\b)", re.IGNORECASE)

            def rep_func(match):
                matched = match.group(0)
                if matched.lower().endswith("'s"):
                    return preserve_case(matched[:-2], rep) + "'s"
                return preserve_case(matched, rep)

            text = pattern.sub(rep_func, text)

    # 5. Final spacing cleanup
    text = re.sub(r'\n\s*\n', '\n\n', text)
    return text.strip()

def is_toc_only_block(lines, threshold=0.85):
    non_blank = [l.strip() for l in lines if l.strip() and l.strip() != '---']
    if not non_blank: return True
    toc_lines = sum(
        1 for l in non_blank
        if CHAPTER_LINE_RE.match(l) or VOLUME_LINE_RE.match(l) or re.match(r'^:\s*.+', l)
    )
    return (toc_lines / len(non_blank)) >= threshold

def remove_toc(lines):
    if is_toc_only_block(lines):
        return []
    return lines

# ==========================================
# FILE READERS & WRITERS (MEMORY OPTIMIZED)
# ==========================================
def fast_read_docx(input_path):
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
    text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', raw_html)
    text = re.sub(r'<(script|style|head)[^>]*>.*?</\1>', '', text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r'</?(p|div|h[1-6]|br|tr|li)[^>]*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = html.unescape(text)
    return [line.strip() for line in text.split('\n') if line.strip()]

def natural_sort_key(s):
    return [int(text) if text.isdigit() else text.lower() for text in re.split(r'(\d+)', s)]

def save_chunk(lines, path, fmt):
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

def clean_existing_promos(lines):
    cleaned = []
    for line in lines:
        if "Noveltap" in line or "मूल ऑडियो मालिक" in line:
            continue
        cleaned.append(line)
    return cleaned

# ==========================================
# SPLITTING & CLEANING LOGIC
# ==========================================
def split_text_based_logic(input_path, output_dir, chunk_size_pages, output_format, reps, is_txt_file=False):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    base_name = os.path.splitext(os.path.basename(input_path))[0]
    generated_files = []

    if is_txt_file:
        with open(input_path, "r", encoding="utf-8", errors="ignore") as f:
            raw_content = f.read()
        cleaned_content = clean_text_content(raw_content, reps)
        lines = [line.strip() for line in cleaned_content.split('\n') if line.strip()]
    else:
        raw_lines = fast_read_docx(input_path)
        joined_text = "\n".join(raw_lines)
        cleaned_content = clean_text_content(joined_text, reps)
        lines = [line.strip() for line in cleaned_content.split('\n') if line.strip()]

    lines = remove_toc(lines)
    lines = clean_existing_promos(lines)

    if not lines:
        return []

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

        if words_since_last_promo >= promo_interval_words:
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

    gc.collect()
    return generated_files

def split_docx_logic(input_path, output_dir, chunk_size, output_format, reps):
    return split_text_based_logic(input_path, output_dir, chunk_size, output_format, reps, is_txt_file=False)

def split_txt_logic(input_path, output_dir, chunk_size, output_format, reps):
    return split_text_based_logic(input_path, output_dir, chunk_size, output_format, reps, is_txt_file=True)

def split_epub_logic(input_path, output_dir, chunk_size, output_format, reps):
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
                    if not lines: continue
                    all_lines.extend(lines)
                    all_lines.append("---")
                except Exception:
                    continue

        joined_text = "\n".join(all_lines)
        cleaned_content = clean_text_content(joined_text, reps)
        processed_lines = [l.strip() for l in cleaned_content.split('\n') if l.strip()]

        if not processed_lines: return []

        text_buffer = []
        chapter_count = 0
        chunk_count = 1

        for line in processed_lines:
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
        status_msg = job['status_msg']
        input_path, output_dir = job['input_path'], job['output_dir']
        file_name = job['file_name']
        reps = job['reps']

        try:
            loop = asyncio.get_running_loop()

            if job['type'] == 'docx':
                await status_msg.edit_text(f"⚡ Cleaning & Processing DOCX: `{file_name}`...")
                files = await loop.run_in_executor(None, split_docx_logic, input_path, output_dir, job['chunk_size'], job['format'], reps)
            elif job['type'] == 'txt':
                await status_msg.edit_text(f"⚡ Cleaning & Processing TXT: `{file_name}`...")
                files = await loop.run_in_executor(None, split_txt_logic, input_path, output_dir, job['chunk_size'], job['format'], reps)
            elif job['type'] == 'epub':
                await status_msg.edit_text(f"⚡ Cleaning & Processing EPUB: `{file_name}`...")
                files = await loop.run_in_executor(None, split_epub_logic, input_path, output_dir, job['chunk_size'], job['format'], reps)

            if not files:
                await status_msg.edit_text("⚠️ No readable data found or file is corrupted.")
                continue

            await status_msg.edit_text(f"✅ Done! Sending {len(files)} cleaned files to you...")

            for f in files:
                with open(f, 'rb') as doc:
                    try:
                        await status_msg.reply_document(document=doc, filename=os.path.basename(f))
                        await asyncio.sleep(0.5)
                    except Exception as e:
                        print(f"⚠️ Error sending {os.path.basename(f)}: {e}")

            await status_msg.reply_text("🎉 All files cleaned and sent successfully!")

        except Exception as e:
            await status_msg.edit_text(f"❌ Error: {e}")
        finally:
            if os.path.exists(job['temp_dir']):
                shutil.rmtree(job['temp_dir'], ignore_errors=True)
            document_queue.task_done()
            gc.collect()

# ==========================================
# BOT HANDLERS & WORKFLOW
# ==========================================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = update.effective_user.first_name
    await update.message.reply_text(
        f"👋 Hello {name}!\n\n"
        f"🧹 **Novel Cleaner & Splitter Bot Active!**\n\n"
        f"1️⃣ Send your `.txt`, `.docx`, or `.epub` file.\n"
        f"2️⃣ Send your replacement dictionary (e.g. `{{\"Old\": \"New\"}}`).\n"
        f"⚙️ Use `/set 50` to change chunk size."
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
    user_id = update.effective_user.id

    if not (file_name.endswith('.docx') or file_name.endswith('.epub') or file_name.endswith('.txt')):
        await update.message.reply_text("❌ Only .docx, .txt, or .epub files allowed.")
        return

    temp_dir = tempfile.mkdtemp()
    input_path = os.path.join(temp_dir, doc.file_name)
    os.makedirs(os.path.join(temp_dir, "output"), exist_ok=True)

    await update.message.reply_text(f"📥 Downloading `{doc.file_name}`... Please wait.")
    file_obj = await doc.get_file()
    await file_obj.download_to_drive(input_path)

    user_data[user_id] = {
        'document': doc,
        'temp_dir': temp_dir,
        'input_path': input_path,
        'file_name': doc.file_name,
        'state': 'AWAIT_DICT'
    }

    await update.message.reply_text(
        "✅ File received!\n"
        "Now, send your name replacement dictionary in Python or JSON format:\n"
        "`{\"OldName\": \"NewName\"}`"
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in user_data or user_data[user_id].get("state") != "AWAIT_DICT":
        return

    raw_text = update.message.text.strip()
    reps = {}

    try:
        reps = json.loads(raw_text)
    except json.JSONDecodeError:
        try:
            reps = ast.literal_eval(raw_text)
        except Exception:
            pass

    if not isinstance(reps, dict):
        return await update.message.reply_text(
            "❌ Format Error! Please send a valid dictionary:\n"
            "`{'OldName': 'NewName'}` or `{\"OldName\": \"NewName\"}`"
        )

    file_info = user_data.pop(user_id)
    file_name = file_info['file_name'].lower()
    msg_id = update.message.message_id

    if file_name.endswith('.docx'):
        keyboard = [[
            InlineKeyboardButton("📄 DOCX", callback_data=f"docx|docx|{msg_id}"),
            InlineKeyboardButton("📝 TXT",  callback_data=f"docx|txt|{msg_id}")
        ]]
        await update.message.reply_text("Dictionary accepted! Save cleaned chunks as:", reply_markup=InlineKeyboardMarkup(keyboard))
    elif file_name.endswith('.txt'):
        keyboard = [[
            InlineKeyboardButton("📄 DOCX", callback_data=f"txt|docx|{msg_id}"),
            InlineKeyboardButton("📝 TXT",  callback_data=f"txt|txt|{msg_id}")
        ]]
        await update.message.reply_text("Dictionary accepted! Save cleaned chunks as:", reply_markup=InlineKeyboardMarkup(keyboard))
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
        await update.message.reply_text("Dictionary accepted! Choose format and chunk size:", reply_markup=InlineKeyboardMarkup(keyboard))

    pending_uploads[msg_id] = {
        'file_info': file_info,
        'reps': reps,
        'user_id': user_id
    }

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data.split("|")
    job_type, action, msg_id = data[0], data[1], int(data[2])

    if msg_id not in pending_uploads:
        await query.edit_message_text("❌ Session expired. Please upload again.")
        return

    job_info = pending_uploads.pop(msg_id)
    file_info = job_info['file_info']
    reps = job_info['reps']
    user_id = job_info['user_id']
    file_name = file_info['file_name']

    chunk_size = user_chunk_sizes.get(user_id, DEFAULT_DOCX_CHUNK if job_type != 'epub' else DEFAULT_EPUB_CHUNK)
    if job_type == 'epub':
        format_choice = action.split('_')[0]
        size_type = action.split('_')[1]
        format_val = format_choice
        if size_type == "cust":
            chunk_size = user_chunk_sizes.get(user_id, DEFAULT_EPUB_CHUNK)
        else:
            chunk_size = DEFAULT_EPUB_CHUNK
    else:
        format_val = action

    job_data = {
        'type': job_type,
        'status_msg': query.message,
        'temp_dir': file_info['temp_dir'],
        'input_path': file_info['input_path'],
        'output_dir': os.path.join(file_info['temp_dir'], "output"),
        'file_name': file_name,
        'format': format_val,
        'chunk_size': chunk_size,
        'reps': reps
    }

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
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(CallbackQueryHandler(button_callback))
    print("🚀 Bot is LIVE on Render with Spam Cleaning & Name Replacement!")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
