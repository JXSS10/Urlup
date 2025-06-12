# command_handlers.py

import pyrogram
from pyrogram import filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from config import ADMIN_IDS
from helpers import is_admin, is_admin_or_pro, get_uptime, force_subscribe, send_user_info_to_log_channel
from language_handler import get_text, get_user_language
from database import digital_botz
import os
import sys
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

registered_users = set()
active_admins = set()
current_task = None
thumb_state = {}

# command_handlers.py

# ...

@pyrogram.Client.on_message(filters.command(["start"]))
async def start_command_handler(client: pyrogram.client.Client, message: pyrogram.types.messages_and_media.message.Message): # تم إزالة bot_start_time من هنا
    # ... (بقية الكود في الدالة) ...
    pass

# ...    user = message.from_user
    lang = await get_user_language(user.id) # Get language from database

    if not await force_subscribe(client, message, lang, client): # تمرير client إلى force_subscribe
        return

    await digital_botz.add_user(client, user)

    if lang not in ["en", "ar"]: # Language not set in DB yet, show language selection buttons
        markup = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("English", callback_data="lang_en"),
                 InlineKeyboardButton("العربية", callback_data="lang_ar")]
            ]
        )
        await message.reply_text(get_text("language_select", "en"), reply_markup=markup) # Use default 'en' for language select prompt
        return # Wait for language selection

    usage = ""
    if await is_admin_or_pro(user.id):
        if await digital_botz.has_premium_access(user.id): # استخدام digital_botz مباشرة
            usage = get_text("start_usage_pro", lang)
        else:
            usage = get_text("start_usage_admin", lang)

    start_message_text = get_text("start_message", lang,
                                   mention=user.mention,
                                   usage=usage)

    markup = InlineKeyboardMarkup([[InlineKeyboardButton(get_text("owner_button", lang), url="https://t.me/X_XF8"),
                                   InlineKeyboardButton(get_text("change_language_button", lang), callback_data="change_language")]])

    await client.send_message( # استخدام client المستلم كمعامل
        message.chat.id,
        start_message_text,
        reply_markup=markup,
        reply_to_message_id=message.id
    )

    if user.id not in registered_users:
        await send_user_info_to_log_channel(user, lang, client) # تمرير client إلى send_user_info_to_log_channel
        registered_users.add(user.id)

@pyrogram.Client.on_message(filters.command(["cancel"]))
async def cancel_command_handler(client: pyrogram.client.Client, message: pyrogram.types.messages_and_media.message.Message):
    lang = await get_user_language(message.from_user.id)
    if not await force_subscribe(client, message, lang, client): # تمرير client إلى force_subscribe
        return

    if not await is_admin_or_pro(message.from_user.id):
        await message.reply_text(get_text("not_admin_message", lang), reply_to_message_id=message.id)
        return

    if message.chat.id in active_downloads:
        active_downloads[message.chat.id] = False
        await client.send_message(message.chat.id, get_text("cancel_download", lang), reply_to_message_id=message.id) # استخدام client المستلم كمعامل
    if message.chat.id in active_uploads:
        active_uploads[message.chat.id] = False
        await client.send_message(message.chat.id, get_text("cancel_upload", lang), reply_to_message_id=message.id) # استخدام client المستلم كمعامل

@pyrogram.Client.on_message(filters.command(["restart"]))
async def restart_command_handler(client: pyrogram.client.Client, message: pyrogram.types.messages_and_media.message.Message):
    lang = await get_user_language(message.from_user.id)
    if not await force_subscribe(client, message, lang, client): # تمرير client إلى force_subscribe
        return

    if not is_admin(message.from_user.id):
        await client.send_message(message.chat.id, get_text("not_admin_command_message", lang), reply_to_message_id=message.id) # استخدام client المستلم كمعامل
        return

    await client.send_message(message.chat.id, get_text("restarting", lang), reply_to_message_id=message.id) # استخدام client المستلم كمعامل
    os.execv(sys.executable, [sys.executable, sys.argv[0]])

@pyrogram.Client.on_message(filters.command(["status"]))
async def status_command_handler(client: pyrogram.client.Client, message: pyrogram.types.messages_and_media.message.Message, bot_start_time, user_daily_tasks, last_task_reset_day, DAILY_TASK_LIMIT): # تمرير المتغيرات كمعاملات
    lang = await get_user_language(message.from_user.id)
    if not await force_subscribe(client, message, lang, client): # تمرير client إلى force_subscribe
        return

    if not await is_admin_or_pro(message.from_user.id):
        await message.reply_text(get_text("not_admin_message", lang), reply_to_message_id=message.id)
        return

    uptime = get_uptime(bot_start_time) # تمرير bot_start_time إلى get_uptime
    num_active_admins = len(active_admins)
    speed = "N/A"
    task_status = get_text("status_task_running", lang) if current_task else get_text("status_idle", lang)
    current_link = current_task if current_task else "None" # get_text("none", lang) is not defined in TEXTS
    num_pro_users = await digital_botz.total_premium_users_count()

    status_text_lines = [get_text("status_title", lang)]
    status_text_lines.append(get_text("status_uptime", lang, uptime))
    if is_admin(message.from_user.id):
        status_text_lines.append(get_text("status_active_admins", lang, num_active_admins))
        status_text_lines.append(get_text("status_pro_users", lang, num_pro_users))
    status_text_lines.append(get_text("status_bot_state", lang, task_status))
    status_text_lines.append(get_text("status_current_link", lang, current_link))
    status_text = "\n".join(status_text_lines)

    await client.send_message( # استخدام client المستلم كمعامل
        message.chat.id,
        status_text,
        reply_to_message_id=message.id
    )

@pyrogram.Client.on_message(filters.command(["add_pro"]))
async def add_pro_command_handler(client: pyrogram.client.Client, message: pyrogram.types.messages_and_media.message.Message):
    lang = await get_user_language(message.from_user.id)
    if not await force_subscribe(client, message, lang, client): # تمرير client إلى force_subscribe
        return

    if not is_admin(message.from_user.id):
        await message.reply_text(get_text("not_admin_command_message", lang), reply_to_message_id=message.id)
        return

    try:
        if len(message.command) < 3:
            await client.send_message(message.chat.id, get_text("add_pro_usage", lang), reply_to_message_id=message.id) # استخدام client المستلم كمعامل
            return

        user_id = int(message.command[1])
        duration = message.command[2]
        expiry_date = None

        if duration.endswith("hours"):
            hours = int(duration.replace("hours", ""))
            expiry_date = datetime.now() + timedelta(hours=hours)
        elif duration.endswith("days"):
            days = int(duration.replace("days", ""))
            expiry_date = datetime.now() + timedelta(days=days)
        elif duration.endswith("weeks"):
            weeks = int(duration.replace("weeks", ""))
            expiry_date = datetime.now() + timedelta(weeks=weeks)
        elif duration.endswith("months"):
            months = int(duration.replace("months", ""))
            expiry_date = datetime.now() + timedelta(days=months * 30) # Approximation! Months are not uniform
        elif duration.endswith("years"):
            years = int(duration.replace("years", ""))
            expiry_date = datetime.now() + timedelta(days=years * 365) # Approximation! Years are not uniform (leap years)
        else:
            await client.send_message(message.chat.id, get_text("invalid_duration", lang), reply_to_message_id=message.id) # استخدام client المستلم كمعامل
            return

        if expiry_date:
            await digital_botz.addpremium(user_id, expiry_date)

        try:
            await client.send_message( # استخدام client المستلم كمعامل
                user_id,
                get_text("pro_added_user", lang,
                         purchase_date=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                         expiry_date=expiry_date.strftime('%Y-%m-%d %H:%M:%S'))
            )
        except Exception as e:
            logger.error(f"Error sending message to user {user_id}: {e}")

        await client.send_message( # استخدام client المستلم كمعامل
            message.chat.id,
            get_text("pro_added_admin", lang,
                     user_id=user_id,
                     expiry_date=expiry_date.strftime('%Y-%m-%d %H:%M:%S')),
            reply_to_message_id=message.id
        )


    except Exception as e:
        await client.send_message(message.chat.id, get_text("error_occurred", lang, error=e), reply_to_message_id=message.id) # استخدام client المستلم كمعامل

@pyrogram.Client.on_message(filters.command(["remove_pro"]))
async def remove_pro_command_handler(client: pyrogram.client.Client, message: pyrogram.types.messages_and_media.message.Message):
    lang = await get_user_language(message.from_user.id)
    if not await force_subscribe(client, message, lang, client): # تمرير client إلى force_subscribe
        return

    if not is_admin(message.from_user.id):
        await message.reply_text(get_text("not_admin_command_message", lang), reply_to_message_id=message.id)
        return

    try:
        if len(message.command) < 2:
            await client.send_message(message.chat.id, get_text("remove_pro_usage", lang), reply_to_message_id=message.id) # استخدام client المستلم كمعامل
            return

        user_id = int(message.command[1])

        await digital_botz.remove_premium(user_id)
        await client.send_message( # استخدام client المستلم كمعامل
            message.chat.id,
            get_text("pro_removed_user", lang, user_id=user_id),
            reply_to_message_id=message.id
        )

    except Exception as e:
        await client.send_message(message.chat.id, get_text("error_occurred", lang, error=e), reply_to_message_id=message.id) # استخدام client المستلم كمعامل

@pyrogram.Client.on_message(filters.command(["remove_thumb"]))
async def remove_thumb_command_handler(client: pyrogram.client.Client, message: pyrogram.types.messages_and_media.message.Message):
    lang = await get_user_language(message.from_user.id)
    if not await force_subscribe(client, message, lang, client): # تمرير client إلى force_subscribe
        return

    user_id = message.from_user.id

    if not await is_admin_or_pro(user_id):
        await message.reply_text(get_text("not_admin_message", lang), reply_to_message_id=message.id)
        return

    await digital_botz.set_thumbnail(user_id, None)
    await client.send_message(message.chat.id, get_text("thumb_reset_default", lang), reply_to_message_id=message.id) # استخدام client المستلم كمعامل

@pyrogram.Client.on_message(filters.command(["set_thumb"]))
async def set_thumb_command_handler(client: pyrogram.client.Client, message: pyrogram.types.messages_and_media.message.Message):
    lang = await get_user_language(message.from_user.id)
    if not await force_subscribe(client, message, lang, client): # تمرير client إلى force_subscribe
        return

    admin_user_id = message.from_user.id

    if not is_admin(admin_user_id):
        await message.reply_text(get_text("not_admin_command_message", lang), reply_to_message_id=message.id)
        return

    if len(message.command) < 2:
        await client.send_message(message.chat.id, get_text("set_thumb_usage", lang), reply_to_message_id=message.id) # استخدام client المستلم كمعامل
        return

    try:
        target_user_id = int(message.command[1])
        thumb_state['thumb_target_user'] = target_user_id
        thumb_state['thumb_admin_user'] = admin_user_id
        await client.send_message(message.chat.id, get_text("send_thumb_prompt", lang, user_id=target_user_id), reply_to_message_id=message.id) # استخدام client المستلم كمعامل

    except ValueError:
        await client.send_message(message.chat.id, get_text("user_id_invalid", lang), reply_to_message_id=message.id) # استخدام client المستلم كمعامل
    except Exception as e:
        await client.send_message(message.chat.id, get_text("error_occurred", lang, error=e), reply_to_message_id=message.id) # استخدام client المستلم كمعامل

@pyrogram.Client.on_message(filters.command(["remove_thumb_user"]))
async def remove_thumb_user_command_handler(client: pyrogram.client.Client, message: pyrogram.types.messages_and_media.message.Message):
    lang = await get_user_language(message.from_user.id)
    if not await force_subscribe(client, message, lang, client): # تمرير client إلى force_subscribe
        return

    admin_user_id = message.from_user.id

    if not is_admin(admin_user_id):
        await message.reply_text(get_text("not_admin_command_message", lang), reply_to_message_id=message.id)
        return

    if len(message.command) < 2:
        await client.send_message(message.chat.id, get_text("set_thumb_usage", lang), reply_to_message_id=message.id) # استخدام client المستلم كمعامل
        return

    try:
        target_user_id = int(message.command[1])
        await digital_botz.set_thumbnail(target_user_id, None)
        await client.send_message(message.chat.id, get_text("thumb_user_reset_default", lang, user_id=target_user_id), reply_to_message_id=message.id) # استخدام client المستلم كمعامل
    except ValueError:
        await client.send_message(message.chat.id, get_text("user_id_invalid", lang), reply_to_message_id=message.id) # استخدام client المستلم كمعامل
    except Exception as e:
        await client.send_message(message.chat.id, get_text("error_occurred", lang, error=e), reply_to_message_id=message.id) # استخدام client المستلم كمعامل
