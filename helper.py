#helper.py
#```python
import re
import pyrogram
from pyrogram.errors import FloodWait
from datetime import timedelta
import time
import os
import asyncio
import logging
import datetime

from database import digital_botz

logger = logging.getLogger(__name__) # Use __name__ for logger name

async def is_pro_user(user_id):
    return await digital_botz.has_premium_access(user_id)

def is_admin(user_id, admin_ids):
    return user_id in admin_ids

async def is_admin_or_pro(user_id, admin_ids):
    return is_admin(user_id, admin_ids) or await is_pro_user(user_id)

def get_uptime(bot_start_time):
    uptime_seconds = int(time.time() - bot_start_time)
    hours, remainder = divmod(uptime_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours}h {minutes}m {seconds}s"

def get_speed_and_eta(start_time, current_bytes, total_bytes):
    elapsed_time = time.time() - start_time
    if elapsed_time > 0:
        speed_bytes_per_sec = current_bytes / elapsed_time
        speed_mb_per_sec = speed_bytes_per_sec / (1024 * 1024)
        remaining_bytes = total_bytes - current_bytes
        if speed_bytes_per_sec > 0:
            eta_seconds = remaining_bytes / speed_bytes_per_sec
            eta = timedelta(seconds=int(eta_seconds))
        else:
            eta = "N/A"
        speed = f"{speed_mb_per_sec:.2f} MB/s"
    else:
        speed = "N/A"
        eta = "N/A"
    return speed, eta

last_edit_time = 0

# helper.py
# --- line 36 ---
async def progress(current, total, message, bot, initial_message, progress_type):
    global last_edit_time
    percentage = current * 100 / total
    completed_bar = int(20 * current // total)
    remaining_bar = 20 - completed_bar
    progress_bar = '▣' * completed_bar + '▢' * remaining_bar
    speed, eta = get_speed_and_eta(initial_message.start_time, current, total) # Use initial_message's start_time

    progress_message_text = f"""


{progress_type.upper()} STARTED....

{progress_bar}

╭━━━━❰@x_xf8 PROCESSING...❱━➣
┣⪼ 🗃️ ꜱɪᴢᴇ: {get_readable_size(current)} | {get_readable_size(total)}
┣⪼ ⏳️ ᴅᴏɴᴇ : {percentage:.2f}%
┣⪼ 🚀 ꜱᴩᴇᴇᴅ: {speed}
┣⪼ ⏰️ ᴇᴛᴀ: {eta}
╰━━━━━━━━━━━━━━━➣
"""
    current_time = time.time()
    if current_time - last_edit_time >= 4:
        try:
            await bot.edit_message_text(message.chat.id, initial_message.id, progress_message_text)
            logger.info(f"Progress updated in message: {message.id}, Progress: {progress_type}, Percentage: {percentage:.2f}%")
            last_edit_time = current_time
        except FloodWait as e:
            logger.warning(f"FloodWait encountered during progress update (EditMessage), waiting for {e.value} seconds.")
            await asyncio.sleep(e.value)
        except Exception as e:
            logger.error(f"Error updating progress message: {e}")
    else:
        logger.debug("Skipping progress update due to time throttling (4 seconds).")
    logger.info(f"Progress (log only): {progress_type}, Current: {current}, Total: {total}, Percentage: {percentage:.2f}%")
    
def get_readable_size(size_bytes):
    power = 2**10
    n = 0
    power_labels = {0 : '', 1: 'KB', 2: 'MB', 3: 'GB', 4: 'TB'}
    while size_bytes > power:
        size_bytes /= power
        n += 1
    return f"{size_bytes:.2f} {power_labels[n]}"

def clean_filename(text):
    text = re.sub(r'[\U0001F300-\U0001FAD6\U0000200D\U0000FE0F]', '', text, flags=re.UNICODE)
    text = re.sub(r'[|📌⬅️]', '', text)
    text = re.sub(r'[.#]', '', text)
    text = re.sub(r'[《》]', ' ', text)
    text = re.sub(r'\bميتيكس\b', '', text)
    text= "[@X_XF8] "+text
    text = re.sub(r'\s+', ' ', text).strip()
    return text

async def forward_message_to_log_channel(bot, message, log_channel_id, file=False):
    try:
        if file:
            await bot.copy_message(
                chat_id=log_channel_id,
                from_chat_id=message.chat.id,
                message_id=message.id
            )
        else:
            await bot.forward_messages(
                chat_id=log_channel_id,
                from_chat_id=message.chat.id,
                message_ids=message.id
            )
        logger.info(f"Message forwarded to log channel from chat ID: {message.chat.id}, message ID: {message.id}")
    except Exception as e:
        logger.error(f"Error forwarding message to log channel: {e}")

async def send_user_info_to_log_channel(bot, user, log_channel_id):
    try:
        if user is None: # Defensive check for user being None
            logger.warning("Attempted to send user info to log channel with a None user object.")
            return

        user_info = (
            f"مستخدم جديد بدأ البوت لأول مرة!\n\n"
            f"- الاسم: {user.first_name} {user.last_name if user.last_name else ''}\n"
            f"- اليوزر: @{user.username if user.username else 'لا يوجد'}\n"
            f"- الـ ID:  {user.id}  \n"
            f"- وقت وتاريخ الدخول:  {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  "
        )

        chat = await bot.get_chat(user.id)
        if chat.photo:
            profile_photo = await bot.download_media(chat.photo.big_file_id)
            await bot.send_photo(log_channel_id, profile_photo, caption=user_info)
            if profile_photo: # Check if download was successful
                os.remove(profile_photo)
        else:
            await bot.send_message(log_channel_id, user_info)

        logger.info(f"User {user.id} info sent to log channel.")

    except Exception as e:
        logger.error(f"Error sending user info to log channel: {e}")


def check_daily_task_limit(user_id, last_task_reset_day, user_daily_tasks, daily_task_limit):
    current_day = datetime.datetime.now().day

    if last_task_reset_day != current_day:
        user_daily_tasks = {}
        last_task_reset_day = current_day
        logger.info("Daily task limit reset for all users.")

    if user_id not in user_daily_tasks:
        user_daily_tasks[user_id] = daily_task_limit

    return user_daily_tasks[user_id] > 0


def decrement_daily_task_count(user_id, user_daily_tasks):
    if user_daily_tasks.get(user_id, 0) > 0: # Ensure user exists and has tasks left
        user_daily_tasks[user_id] -= 1
    return user_daily_tasks

async def is_user_subscribed(client, user_id, channel_id):
    try:
        member = await client.get_chat_member(channel_id, user_id)
        return member.status not in ("left", "kicked", "banned") # Corrected status check
    except Exception as e:
        logger.error(f"Error checking subscription status: {e}")
        return False

async def force_subscribe(bot, client, message, force_subscribe_channel_id):
    if not force_subscribe_channel_id: # Allow bypass if no channel is set
        return True

    if message.from_user is None: # Handle messages without from_user
        return True # Or handle differently, e.g., return False if subscription is mandatory for all messages

    user_id = message.from_user.id
    if not await is_user_subscribed(client, user_id, force_subscribe_channel_id):
        try:
            channel = await bot.get_chat(force_subscribe_channel_id)
            from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
            markup = InlineKeyboardMarkup(
                [[InlineKeyboardButton("Subscribe Now", url=f"https://t.me/{channel.username}")]]
            )
            await message.reply_text(
                "Please subscribe to our channel to use this bot:",
                reply_markup=markup,
                quote=True
            )
        except Exception as e_channel: # Catch exceptions related to fetching channel info
            logger.error(f"Error fetching force subscribe channel info: {e_channel}", exc_info=True)
            await message.reply_text("Subscription check failed due to an error. Please try again later.", quote=True)
        return False
    return True


async def is_banned_user(user_id):
    return await digital_botz.is_banned(user_id)

#--- Normal Mode Functions ---

def downstatus(bot, statusfile, message):
    while True:
        if os.path.exists(statusfile):
            break
        time.sleep(3)
    while os.path.exists(statusfile):
        with open(statusfile, "r") as downread:
            txt = downread.read()
        try:
            asyncio.run(bot.edit_message_text(message.chat.id, message.id, f"Downloaded : {txt}"))
            time.sleep(1)
        except:
            time.sleep(5)

def upstatus(bot, statusfile, message):
    while True:
        if os.path.exists(statusfile):
            break
        time.sleep(3)
    while os.path.exists(statusfile):
        with open(statusfile, "r") as upread:
            txt = upread.read()
        try:
            asyncio.run(bot.edit_message_text(message.chat.id, message.id, f"Uploaded : {txt}"))
            time.sleep(1)
        except:
            time.sleep(5)

def progress_normal(current, total, message, progress_type):
    with open(f'{message.id}{progress_type}status.txt', "w") as fileup:
        fileup.write(f"{current * 100 / total:.1f}%")

def get_message_type(msg: pyrogram.types.messages_and_media.message.Message):
    if msg.media:
        if msg.document: return "Document"
        if msg.video: return "Video"
        if msg.animation: return "Animation"
        if msg.sticker: return "Sticker"
        if msg.voice: return "Voice"
        if msg.audio: return "Audio"
        if msg.photo: return "Photo"
    if msg.text: return "Text"
    return "Unknown"
