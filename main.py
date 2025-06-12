# main.py

import os
import asyncio
import time
import math
import logging
from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.errors import MessageNotModified
import yt_dlp
import aiohttp
from config import API_ID, API_HASH, BOT_TOKEN, COOKIES_FILE_PATH

# --- إعدادات وتكوين ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# إعداد Pyrogram للعمل في الوضع المزدوج
app = Client(
    "UploaderSession_Final_DualMode",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

user_states = {}
DOWNLOAD_DIR = "downloads/"

# --- دوال مساعدة لإدارة الحالة ---
def get_user_state(chat_id, key, default=None): return user_states.get(chat_id, {}).get(key, default)
def set_user_state(chat_id, key, value):
    if chat_id not in user_states: user_states[chat_id] = {}
    user_states[chat_id][key] = value
def clear_user_state(chat_id):
    # مسح حالة العملية الحالية فقط
    keys_to_clear = ['current_task', 'is_waiting_for_cookies', 'temp_thumbnail', 'wants_to_rename', 'url_to_download', 'is_playlist', 'downloaded_thumb_path']
    if chat_id in user_states:
        for key in keys_to_clear:
            if key in user_states[chat_id]:
                del user_states[chat_id][key]

# --- دالة التقدم ---
async def progress_callback(current, total, message: Message, start_time, action="Uploading"):
    now = time.time()
    if (now - get_user_state(message.chat.id, 'last_update_time', 0)) < 2: return
    
    percentage = current * 100 / total
    elapsed_time = now - start_time
    speed = current / elapsed_time if elapsed_time > 0 else 0
    eta = (total - current) / speed if speed > 0 else 0
    
    def format_bytes(size):
        if size == 0: return "0B"; size_name = ("B", "KB", "MB", "GB", "TB"); i = int(math.floor(math.log(size, 1024))); p = math.pow(1024, i); s = round(size / p, 2); return f"{s} {size_name[i]}"
    
    bar_length = 10; filled_length = int(bar_length * current // total); bar = '●' * filled_length + '○' * (bar_length - filled_length)
    
    progress_str = (
        f"**{action}...**\n"
        f"**التقدم:** `{percentage:.1f}%`\n[{bar}]\n"
        f"**الحجم:** `{format_bytes(current)}` / `{format_bytes(total)}`\n"
        f"**السرعة:** `{format_bytes(speed)}/s` | **ETA:** `{time.strftime('%H:%M:%S', time.gmtime(eta))}`"
    )
    try:
        await message.edit_text(progress_str, reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("إلغاء ❌", callback_data="cancel_task")]
        ]))
        set_user_state(message.chat.id, 'last_update_time', now)
    except MessageNotModified: pass
    except Exception as e: logger.warning(f"Error updating progress: {e}")

# --- معالجات الأوامر والرسائل ---
@app.on_message(filters.command(["start", "help"]))
async def start_help_command(_, message: Message):
    user_states.pop(message.chat.id, None) # إعادة تعيين كامل للحالة عند البدء
    help_text = (
        "أهلاً بك في بوت التحميل الاحترافي!\n\n"
        "**الميزات:**\n"
        "✓ رفع ملفات تصل إلى **2 جيجابايت**.\n"
        "✓ دعم **قوائم التشغيل (Playlists)**.\n"
        "✓ **يستخدم الكوكيز تلقائيًا** للوصول للمحتوى الخاص إذا كان الملف موجودًا.\n\n"
        "**كيفية الاستخدام:**\n"
        "1. **(للصورة المصغرة - اختياري):** أرسل صورة.\n"
        "2. **أرسل الرابط:** أرسل رابط الفيديو أو قائمة التشغيل.\n\n"
        "**الأوامر:**\n"
        "`/start`, `/help` - بدء محادثة جديدة.\n"
        "`/cancel` - إلغاء العملية الحالية."
    )
    await message.reply_text(help_text, parse_mode=enums.ParseMode.MARKDOWN)

@app.on_message(filters.command("cancel"))
async def cancel_command(_, message: Message):
    task = get_user_state(message.chat.id, 'current_task')
    if task and not task.done():
        task.cancel()
    else: await message.reply_text("لا توجد عملية نشطة لإلغائها.")

@app.on_message(filters.photo)
async def photo_handler(_, message: Message):
    set_user_state(message.chat.id, 'temp_thumbnail', message.photo.file_id)
    await message.reply_text("هل تريد تعيين هذه الصورة كصورة مصغرة للتحميلات القادمة؟", reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ نعم، عينها", callback_data="set_thumb"),
         InlineKeyboardButton("❌ لا، تجاهل", callback_data="cancel_thumb")]
    ]))

@app.on_message(filters.text & ~filters.command(["start", "help", "cancel"]))
async def text_handler(client: Client, message: Message):
    chat_id = message.chat.id; text = message.text
    if get_user_state(chat_id, 'current_task'):
        await message.reply_text("لديك عملية نشطة بالفعل. الرجاء الانتظار أو إلغائها."); return

    if get_user_state(chat_id, 'wants_to_rename'):
        url = get_user_state(chat_id, 'url_to_download')
        task = asyncio.create_task(process_single_video(client, message, url, custom_name=text))
        set_user_state(chat_id, 'current_task', task)
        return

    if text.startswith("http"):
        if "playlist" in text:
            task = asyncio.create_task(process_playlist(client, message, text))
            set_user_state(chat_id, 'current_task', task)
        else:
            set_user_state(chat_id, 'url_to_download', text)
            await message.reply_text("اختر طريقة تسمية الملف:", reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("الاسم الأصلي", callback_data="default_name")],
                [InlineKeyboardButton("تسمية مخصصة", callback_data="rename_file")]
            ]))
    else: await message.reply_text("الرجاء إرسال رابط صالح.")

@app.on_callback_query()
async def callback_handler(client: Client, callback_query: CallbackQuery):
    chat_id = callback_query.message.chat.id; data = callback_query.data
    
    if data == "cancel_task":
        task = get_user_state(chat_id, 'current_task')
        if task and not task.done(): task.cancel()
        else: await callback_query.answer("لا توجد عملية لإلغائها.", show_alert=True)
        return

    if data == "set_thumb":
        set_user_state(chat_id, 'confirmed_thumbnail', get_user_state(chat_id, 'temp_thumbnail'))
        await callback_query.message.edit_text("✅ تم تعيين الصورة المصغرة بنجاح.")
    
    elif data == "cancel_thumb":
        set_user_state(chat_id, 'confirmed_thumbnail', None)
        await callback_query.message.edit_text("تم إلغاء تعيين الصورة المصغرة.")
    
    elif data in ["default_name", "rename_file"]:
        url = get_user_state(chat_id, 'url_to_download')
        if not url: await callback_query.answer("انتهت صلاحية الجلسة.", show_alert=True); return
        
        if data == "default_name":
            await callback_query.message.delete()
            task = asyncio.create_task(process_single_video(client, callback_query.message, url))
            set_user_state(chat_id, 'current_task', task)
        else:
            set_user_state(chat_id, 'wants_to_rename', True)
            await callback_query.message.edit_text("أرسل الآن الاسم الجديد للملف.")

# --- المنطق الرئيسي ---
async def process_playlist(client: Client, message: Message, playlist_url: str):
    chat_id = message.chat.id
    status_message = await client.send_message(chat_id, "🔍 جاري استخراج معلومات قائمة التشغيل...")
    set_user_state(chat_id, 'is_playlist', True)
    
    ydl_opts = {'extract_flat': True, 'quiet': True, 'cookiefile': COOKIES_FILE_PATH if os.path.exists(COOKIES_FILE_PATH or "") else None}

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            playlist_dict = await asyncio.to_thread(ydl.extract_info, playlist_url, download=False)
        videos = playlist_dict.get('entries', [])
        if not videos: await status_message.edit_text("لم يتم العثور على فيديوهات."); return

        await status_message.edit_text(f"✅ تم العثور على **{len(videos)}** فيديو. بدء المعالجة...")

        for i, video_info in enumerate(videos):
            if not get_user_state(chat_id, 'current_task'): break # إذا تم إلغاء المهمة الرئيسية
            video_url = video_info.get('url'); video_title = video_info.get('title', f"video_{i+1}")
            video_status_msg = await client.send_message(chat_id, f"**({i+1}/{len(videos)})** بدء معالجة: `{video_title}`")
            await process_single_video(client, video_status_msg, video_url, video_title, is_in_playlist=True)

        await client.send_message(chat_id, "✅ اكتملت معالجة جميع فيديوهات قائمة التشغيل!")

    except asyncio.CancelledError: await status_message.edit_text("✅ تم إلغاء عملية قائمة التشغيل.")
    except Exception as e:
        logger.error(f"Error processing playlist for {chat_id}: {e}", exc_info=True)
        await status_message.edit_text(f"❌ خطأ فادح في قائمة التشغيل:\n`{e}`")
    finally: clear_user_state(chat_id)

async def process_single_video(client: Client, message: Message, url: str, custom_name: str = None, is_in_playlist=False):
    status_message = message if not message.text else await client.send_message(message.chat.id, "تم استلام الطلب...")
    if message.text: await message.delete()

    chat_id = status_message.chat.id
    file_path = None
    try:
        if "youtube.com" in url or "youtu.be" in url:
            file_path = await download_with_ytdlp(status_message, url, custom_name)
        else:
            file_path = await download_direct(status_message, url, custom_name)
        
        if file_path and os.path.exists(file_path):
            await upload_to_telegram(client, status_message, file_path)
        else:
            if not get_user_state(chat_id, 'current_task').cancelled():
                await status_message.edit_text(f"❌ فشل تحميل: `{custom_name or url}`")

    except asyncio.CancelledError: await status_message.edit_text(f"✅ تم إلغاء تحميل: `{custom_name or url}`")
    except Exception as e:
        logger.error(f"Error processing video {url}: {e}", exc_info=True)
        await status_message.edit_text(f"❌ خطأ في تحميل `{custom_name or url}`:\n`{e}`")
    finally:
        if file_path and os.path.exists(file_path): os.remove(file_path)
        if not is_in_playlist: clear_user_state(chat_id)

async def download_with_ytdlp(status_message: Message, url: str, custom_name: str = None) -> str:
    chat_id = status_message.chat.id
    output_template = os.path.join(DOWNLOAD_DIR, f"{chat_id}_{custom_name or '%(title)s'}.%(ext)s")
    
    ydl_opts = {
        'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
        'outtmpl': output_template, 'noplaylist': True,
        'cookiefile': COOKIES_FILE_PATH if os.path.exists(COOKIES_FILE_PATH or "") else None,
        'progress_hooks': [lambda d: ytdlp_progress_hook(d, status_message)],
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = await asyncio.to_thread(ydl.extract_info, url, download=True)
        return ydl.prepare_filename(info)

def ytdlp_progress_hook(d, message: Message):
    if d['status'] == 'downloading':
        total = d.get('total_bytes') or d.get('total_bytes_estimate', 0)
        if total > 0:
            asyncio.run_coroutine_threadsafe(
                progress_callback(d['downloaded_bytes'], total, message, d.get('start_time', time.time()), "Downloading"),
                app.loop
            )

async def download_direct(status_message: Message, url: str, custom_name: str = None) -> str:
    file_name = custom_name or os.path.basename(url.split('?')[0])
    file_path = os.path.join(DOWNLOAD_DIR, f"{status_message.chat.id}_{file_name}")
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            response.raise_for_status()
            total_size = int(response.headers.get('content-length', 0))
            with open(file_path, 'wb') as f:
                current_size = 0; chunk_size = 1024 * 1024
                start_time = time.time()
                async for chunk in response.content.iter_chunked(chunk_size):
                    f.write(chunk); current_size += len(chunk)
                    if total_size > 0: await progress_callback(current_size, total_size, status_message, start_time, "Downloading")
    return file_path

async def upload_to_telegram(client: Client, status_message: Message, file_path: str):
    await status_message.edit_text("اكتمل التحميل، جاري الرفع...")
    chat_id = status_message.chat.id
    
    thumb_path = None; thumbnail_id = get_user_state(chat_id, 'confirmed_thumbnail')
    if thumbnail_id:
        try:
            thumb_path = await client.download_media(thumbnail_id, file_name=os.path.join(DOWNLOAD_DIR, f"thumb_{chat_id}.jpg"))
            set_user_state(chat_id, 'downloaded_thumb_path', thumb_path)
        except Exception as e: logger.warning(f"Failed to download thumbnail: {e}")

    try:
        await client.send_document(
            chat_id=chat_id, document=file_path, thumb=thumb_path,
            caption=f"✅ تم الرفع بنجاح!\n`{os.path.basename(file_path)}`",
            parse_mode=enums.ParseMode.MARKDOWN,
            progress=progress_callback,
            progress_args=(status_message, time.time(), "Uploading")
        )
        await status_message.delete()
    except Exception as e:
        logger.error(f"Upload failed: {e}", exc_info=True)
        await status_message.edit_text(f"❌ فشل الرفع: `{e}`")
    finally:
        if thumb_path and os.path.exists(thumb_path): os.remove(thumb_path)

# --- نقطة التشغيل الرئيسية ---
async def main():
    if not os.path.isdir(DOWNLOAD_DIR): os.makedirs(DOWNLOAD_DIR)
    
    logger.info("Starting bot in Dual Mode...")
    await app.start()
    me_bot = await app.get_me()
    logger.info(f"Bot started as @{me_bot.username}")
    
    
    

if __name__ == "__main__":
    try:
        app.run()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped.")
