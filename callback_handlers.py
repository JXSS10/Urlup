# callback_handlers.py

import pyrogram
from pyrogram import filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from language_handler import get_text, get_user_language, set_user_language
from message_handlers import process_media_send_now, process_media_download_upload, pending_link_choices # استيراد الدوال من message_handlers
from command_handlers import start_command_handler # استيراد معالج أمر البدء

@pyrogram.Client.on_callback_query(filters.regex("lang_(en|ar)"))
async def language_set_callback_handler(client, callback_query):
    lang_code = callback_query.data.split("_")[1]
    await set_user_language(callback_query.from_user.id, lang_code) # Save language to database
    lang_name = "English" if lang_code == "en" else "Arabic"
    lang = await get_user_language(callback_query.from_user.id) # Get language from database again to confirm

    await callback_query.answer(get_text("language_changed", lang, lang_name=lang_name), show_alert=True) # Confirmation popup
    await start_command_handler(client, callback_query.message) # Resend start message in new language

@pyrogram.Client.on_callback_query(filters.regex("change_language"))
async def change_language_callback_handler(client, callback_query):
    markup = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("English", callback_data="lang_en"),
             InlineKeyboardButton("العربية", callback_data="lang_ar")]
        ]
    )
    await client.send_message(callback_query.message.chat.id, get_text("language_select", await get_user_language(callback_query.from_user.id)), reply_markup=markup)

@pyrogram.Client.on_callback_query(filters.regex(r"^(send_now|download_upload)_(-?\d+)_(\d+)$"))
async def public_link_choice_callback_handler(client, callback_query, user_daily_tasks): # استقبل user_daily_tasks كمعامل
    choice, chat_id, msg_id = callback_query.data.split("_")[0], int(callback_query.data.split("_")[1]), int(callback_query.data.split("_")[2])
    user_id = callback_query.from_user.id
    lang = await get_user_language(user_id)

    message_id_to_check = None
    for msg_id_pending, data in pending_link_choices.items():
        if data["user_id"] == user_id:
            message_id_to_check = msg_id_pending
            break

    if message_id_to_check and message_id_to_check in pending_link_choices:
        del pending_link_choices[message_id_to_check] # remove pending choice after user made decision

    try:
        await callback_query.message.delete() # Remove choice buttons message
    except:
        pass # Ignore if message already deleted

    if choice == "send_now":
        await process_media_send_now(callback_query.message, chat_id, msg_id, lang, client, user_daily_tasks) # تمرير user_daily_tasks
    elif choice == "download_upload":
        await process_media_download_upload(callback_query.message, chat_id, msg_id, lang, client, user_daily_tasks) # تمرير user_daily_tasks
    await callback_query.answer() # Acknowledge callback