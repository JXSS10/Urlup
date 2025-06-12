

#فهمته؟
import re
import pyrogram
from pyrogram import Client, filters
from pyrogram.enums import ChatMemberStatus, ParseMode # Added ParseMode
from pyrogram.errors import (
    UserAlreadyParticipant, InviteHashExpired, UsernameNotOccupied, FloodWait,
    MessageNotModified, MessageIdInvalid, UserNotParticipant, ChatAdminRequired
    # Removed AuthKeyError from this import list
)
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, CallbackQuery
import time
import os
import sys
from os import environ
import datetime # Import the module itself
from datetime import timedelta, timezone # Import timedelta and timezone
import asyncio
import logging
from typing import Union, Optional, Tuple, Dict, List, Any
# ... (other imports) ...
import secrets # For secure random code generation
import string # For character sets
import traceback # للتعامل مع الأخطاء بشكل أفضل (اختياري لكن مفيد)
from pyrogram import Client as UserClient # <<< استخدم اسمًا مستعارًا لتجنب التعارض مع 'bot'
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton # موجودة بالفعل غالبًا
from pyrogram.errors import (
    ApiIdInvalid, PhoneNumberInvalid, PhoneCodeInvalid, PhoneCodeExpired,
    SessionPasswordNeeded, PasswordHashInvalid, FloodWait, # تأكد من وجود FloodWait
    # أضف هذه الأخطاء المهمة لجلسات المستخدم
    AuthKeyUnregistered, UserDeactivatedBan, UserNotParticipant
)
import traceback
from asyncio.exceptions import TimeoutError
from pyrogram import Client as UserClient # Rename Client to avoid conflict if needed
from pyrogram.errors import (
    ApiIdInvalid,
    PhoneNumberInvalid,
    PhoneCodeInvalid,
    PhoneCodeExpired,
    SessionPasswordNeeded,
    PasswordHashInvalid,
    UserNotParticipant # تأكد من استيراد هذا الخطأ
)
from pyrogram.errors import ChannelInvalid, UserNotParticipant, AuthKeyUnregistered # وأي أخطاء أخرى تحتاجها
# ... (بقية الكود) ...
from pyrogram.errors import UserNotParticipant, ChannelInvalid, ChatIdInvalid # أضف ChannelInvalid و ChatIdInvalid
# Session String Size (can be adjusted, but this is a common value)
from pyrogram.errors import (
    UserNotParticipant, ChannelInvalid, ChatIdInvalid, # تأكد من وجود هذه
    UserAlreadyParticipant, InviteHashExpired, UsernameNotOccupied, FloodWait,
    MessageNotModified, MessageIdInvalid, ChatAdminRequired,
    AuthKeyUnregistered, UserDeactivatedBan # وأي أخطاء أخرى مستخدمة
)
import traceback # مفيد لتتبع الأخطاء
from pyrogram.enums import ParseMode, ChatMemberStatus # تأكد من استيراد هذه أيضًا
# ----- نهاية قسم الاستيراد -----


SESSION_STRING_SIZE = 351
# تأكد من استيراد كائن قاعدة البيانات الذي عدّلته (أو الكائن db المنفصل إذا اخترت ذلك)
# مثال: from database import digital_botz # أو from database.db import db
# ... (Bot credentials, thumbnail, etc.) ...

# --- Global State Variables ---
# ... (registered_users, thumb_state, etc.) ...
user_waiting_for_code: set[int] = set() # Stores user IDs waiting for code input

# ... (Daily Task Limit, Admin IDs, etc.) ...

# --- NEW: Coupon Code Settings ---
DEFAULT_CODE_PREFIX = "X_XF8" # Default prefix for generated codes
CODE_LENGTH = 12 # Length of the random part of the code
# قرب بداية الملف
FIXED_FILENAME_PREFIX = "[@X_XF8]"
# ... (Bot Start Time, Active Operations, etc.) ...
# --- Database Setup ---
# Assuming database.py contains the DigitalBotz class/functions
try:
    # Make sure database.py exists and has the DigitalBotz class/object
    # with all the required async methods.
    from database import digital_botz
    logging.info("Successfully imported 'digital_botz' from 'database'.")
except ImportError:
    logging.error("Could not import 'digital_botz' from 'database'. Using DummyDB.")
    # Create a dummy class/object to avoid immediate crashes if DB isn't ready
# داخل DummyDB في ملف البوت الرئيسي (إذا كنت لا تزال تستخدمه)
    class DummyDB:
        # ... (الدوال الوهمية الأخرى) ...
        async def set_session(self, user_id: int, session: Optional[str]):
            logging.warning(f"DB (Dummy): set_session called for user {user_id}. Session {'set' if session else 'cleared'}. Not persisted.")
            pass
        async def get_session(self, user_id: int) -> Optional[str]:
            logging.warning(f"DB (Dummy): get_session called for user {user_id} -> None. Not persisted.")
            return None
        async def add_user(self, b: Optional[Any], u: pyrogram.types.User): logging.warning("DB (Dummy): add_user called."); pass # Updated signature
        async def has_premium_access(self, user_id: int, *args, **kwargs): logging.warning(f"DB (Dummy): has_premium_access called for {user_id} -> False"); return False
        async def addpremium(self, user_id: int, expiry_date: datetime.datetime, *args, **kwargs): logging.warning(f"DB (Dummy): addpremium called for {user_id}."); pass
        async def remove_premium(self, user_id: int, *args, **kwargs): logging.warning(f"DB (Dummy): remove_premium called for {user_id}."); pass
        async def set_thumbnail(self, user_id: int, thumb_path: Optional[str]): logging.warning(f"DB (Dummy): set_thumbnail called for {user_id} with path {thumb_path}."); pass
        async def get_thumbnail(self, user_id: int): logging.warning(f"DB (Dummy): get_thumbnail called for {user_id} -> None"); return None
        async def ban_user(self, user_id: int, *args, **kwargs): logging.warning(f"DB (Dummy): ban_user called for {user_id}."); pass
        async def unban_user(self, user_id: int, *args, **kwargs): logging.warning(f"DB (Dummy): unban_user called for {user_id}."); pass
        async def is_banned(self, user_id: int): logging.warning(f"DB (Dummy): is_banned called for {user_id} -> False"); return False
        async def total_premium_users_count(self, *args, **kwargs): logging.warning("DB (Dummy): total_premium_users_count called -> 0"); return 0

        # !!! ADDED MISSING METHODS FOR CAPTION FILTERS !!!
        async def add_caption_filter_word(self, user_id: int, word: str):
            logging.warning(f"DB (Dummy): add_caption_filter_word called for user {user_id}, word '{word}' - Not implemented.")
            pass # Needs implementation in real DB
        async def remove_caption_filter_word(self, user_id: int, word: str):
            logging.warning(f"DB (Dummy): remove_caption_filter_word called for user {user_id}, word '{word}' - Not implemented.")
            pass # Needs implementation in real DB
        async def get_all_caption_filters(self) -> Dict[int, List[str]]:
            logging.warning("DB (Dummy): get_all_caption_filters called - Returning empty dict. Not implemented.")
            return {} # Return an empty dict to avoid errors at startup

        # !!! ADDED DUMMY METHODS FOR PREFIX/SUFFIX !!!
        async def set_prefix(self, user_id: int, prefix: Optional[str]):
             logging.warning(f"DB (Dummy): set_prefix called for user {user_id} with prefix '{prefix}' - Not implemented.")
             pass
        async def get_prefix(self, user_id: int) -> Optional[str]:
             logging.warning(f"DB (Dummy): get_prefix called for user {user_id} -> None - Not implemented.")
             return None
        async def set_suffix(self, user_id: int, suffix: Optional[str]):
             logging.warning(f"DB (Dummy): set_suffix called for user {user_id} with suffix '{suffix}' - Not implemented.")
             pass
        async def get_suffix(self, user_id: int) -> Optional[str]:
             logging.warning(f"DB (Dummy): get_suffix called for user {user_id} -> None - Not implemented.")
             return None
        async def get_all_prefixes(self) -> Dict[int, str]:
             logging.warning("DB (Dummy): get_all_prefixes called - Returning empty dict. Not implemented.")
             return {}
        async def get_all_suffixes(self) -> Dict[int, str]:
             logging.warning("DB (Dummy): get_all_suffixes called - Returning empty dict. Not implemented.")
             return {}

    digital_botz = DummyDB()
except Exception as e:
    logging.critical(f"An unexpected error occurred during database setup: {e}", exc_info=True)
    sys.exit(1) # Exit if DB setup fails critically

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO, # Keep INFO level for general ops
    # level=logging.DEBUG, # Use DEBUG for detailed tracing including progress skips
    format='%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s', # Added funcName and lineno
    handlers=[logging.StreamHandler(sys.stdout)] # Ensure logs go to stdout for container environments
)
# Filter out noisy pyrogram connection logs unless needed
logging.getLogger("pyrogram.client").setLevel(logging.WARNING)
logging.getLogger("pyrogram.session").setLevel(logging.WARNING)

logger = logging.getLogger(__name__) # Get logger for current module

# --- Bot credentials ---
# It's highly recommended to use environment variables for sensitive data
# !! IMPORTANT: Replace placeholders with your actual credentials or set Environment Variables !!
API_HASH = environ.get("API_HASH","d8c9b01c863dabacc484c2c06cdd0f6e")
API_ID = environ.get("API_ID",16501053)
BOT_TOKEN = environ.get("BOT_TOKEN","6754322722:AAF4VXotdh_o65vxz-fTvpCIDSGoY6HCXBo")
if not API_HASH or API_HASH == "YOUR_API_HASH":
    logger.critical("API_HASH environment variable not set or is placeholder. Exiting.")
    sys.exit(1)
if not API_ID or API_ID == 1234567:
    logger.critical("API_ID environment variable not set or is placeholder. Exiting.")
    sys.exit(1)
try:
    API_ID = int(API_ID)
except ValueError:
    logger.critical("API_ID environment variable must be an integer. Exiting.")
    sys.exit(1)
if not BOT_TOKEN or BOT_TOKEN == "YOUR_BOT_TOKEN":
    logger.critical("BOT_TOKEN environment variable not set or is placeholder. Exiting.")
    sys.exit(1)

# Initialize Pyrogram Client
bot = Client("mybot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# --- Default thumbnail path ---
DEFAULT_THUMBNAIL = "default_thumb.jpg"
DEFAULT_PREFIX = "[@X_XF8]" # Default prefix if user hasn't set one

# Create a dummy default thumbnail if it doesn't exist
if not os.path.exists(DEFAULT_THUMBNAIL):
    try:
        from PIL import Image
        img = Image.new('RGB', (100, 100), color=(20, 20, 20)) # Dark grey
        img.save(DEFAULT_THUMBNAIL)
        logger.info(f"Created dummy {DEFAULT_THUMBNAIL}")
    except ImportError:
        logger.warning("PIL/Pillow not installed. Cannot create dummy thumbnail. Please create default_thumb.jpg manually.")
    except Exception as e:
        logger.error(f"Error creating dummy thumbnail: {e}")

# --- Global State Variables ---
registered_users: set[int] = set() # Stores user IDs who have started the bot in the current session
thumb_state: Dict[int, Dict[str, Any]] = {} # Used for tracking /set_thumb command state {admin_id: {'target_user': target_id, 'time': timestamp}}
upload_chat_id: Dict[int, int] = {} # Store upload chat ID per user {user_id: chat_id}
skip_text_filter: Dict[int, str] = {} # Store skip text filter per user {user_id: 'on'/'off'}
skip_photo_filter: Dict[int, str] = {} # Store skip photo filter per user {user_id: 'on'/'off'}
skip_sticker_filter: Dict[int, str] = {} # Store skip sticker filter per user {user_id: 'on'/'off'}
caption_word_filters: Dict[int, List[str]] = {} # Store caption word filters per user {user_id: [word1, word2]} - Loaded from DB at startup
user_prefixes: Dict[int, str] = {} # To store custom prefixes per user
user_suffixes: Dict[int, str] = {} # To store custom suffixes per user

# --- Daily Task Limit Variables ---
# Default to 10 if not set, allow 0 for unlimited free tasks
DAILY_TASK_LIMIT = int(environ.get("DAILY_TASK_LIMIT", 10))
user_daily_tasks: Dict[int, int] = {} # {user_id: remaining_tasks}
last_task_reset_day: Optional[int] = None # Stores the day the task limit was last reset (UTC day)

# --- Admin User IDs ---
# !! IMPORTANT: Replace placeholder with your Admin ID(s) or set Environment Variable !!
ADMIN_IDS_STR = environ.get("ADMIN_IDS", "6169288210") # <-- REPLACE or SET ENV VAR, Comma-separated
if not ADMIN_IDS_STR or ADMIN_IDS_STR == "YOUR_ADMIN_ID":
    logger.warning("ADMIN_IDS environment variable not set or is placeholder. No admins configured.")
    admin_ids: set[int] = set()
else:
    try:
        admin_ids = set(int(admin_id.strip()) for admin_id in ADMIN_IDS_STR.split(',') if admin_id.strip())
        logger.info(f"Admin IDs loaded: {admin_ids}")
    except ValueError:
        logger.error("ADMIN_IDS environment variable contains non-integer values. Please check.")
        admin_ids = set()

# admin_mode_advanced tracks user preference IF they are admin/pro
# True = Advanced Mode, False = Normal Mode
admin_mode_advanced: Dict[int, bool] = {} # {user_id: True/False}

# --- Bot Start Time ---
bot_start_time = time.time()

# --- Active Downloads and Uploads Tracking ---
active_operations: Dict[int, bool] = {} # {chat_id: True} Tracks if ANY operation (dl/ul) is active for a chat
operation_status_map: Dict[int, int] = {} # {status_message_id: chat_id} Maps status message to chat for cancellation

# --- Current Task Tracking ---
current_task: Optional[str] = None # Stores description of the current bulk task

# --- Channel IDs ---
# !! IMPORTANT: Replace placeholders with your Channel IDs or set Environment Variables !!
try:
    # Required for logging and user info
    LOG_CHANNEL_ID_STR = environ.get("LOG_CHANNEL_ID","-1002228859831") # <-- REPLACE or SET ENV VAR
    if not LOG_CHANNEL_ID_STR or LOG_CHANNEL_ID_STR == "-100xxxxxxxxxx":
        logger.warning("LOG_CHANNEL_ID not set or is placeholder. User logging disabled.")
        LOG_CHANNEL_ID = None
    else:
        LOG_CHANNEL_ID = int(LOG_CHANNEL_ID_STR)
except (TypeError, ValueError):
    logger.error("LOG_CHANNEL_ID environment variable is invalid (must be integer). User logging disabled.")
    LOG_CHANNEL_ID = None # Set to None if invalid or not set

try:
    # Required for force subscribe feature
    FORCE_SUBSCRIBE_CHANNEL_ID_STR = environ.get("FORCE_SUBSCRIBE_CHANNEL_ID","-1001483534821") # <-- REPLACE or SET ENV VAR
    if not FORCE_SUBSCRIBE_CHANNEL_ID_STR or FORCE_SUBSCRIBE_CHANNEL_ID_STR == "-100yyyyyyyyyy":
         logger.warning("FORCE_SUBSCRIBE_CHANNEL_ID not set or is placeholder. Force subscribe disabled.")
         FORCE_SUBSCRIBE_CHANNEL_ID = None
    else:
        FORCE_SUBSCRIBE_CHANNEL_ID = int(FORCE_SUBSCRIBE_CHANNEL_ID_STR)
except (TypeError, ValueError):
    logger.warning("FORCE_SUBSCRIBE_CHANNEL_ID environment variable invalid (must be integer). Force subscribe feature will be disabled.")
    FORCE_SUBSCRIBE_CHANNEL_ID = None # Set to None if invalid or not set

# --- Usage Strings ---

# Basic usage for normal users
USAGE_NORMAL_AR = """
**✨ وضع الاستخدام العادي ✨**

🔹 **للقنوات العامة:** فقط أرسل رابط المشاركة. سيتم **تمرير** المحتوى مباشرة إليك.
   *(إذا فشل التمرير بسبب القيود، سيحاول البوت تنزيله وإعادة رفعه.)*
🔹 **للمجموعات (العامة أو الخاصة):** سيتم **تنزيل وإعادة رفع** المحتوى.
🔹 **للقنوات الخاصة:** 🚫 غير مدعوم (يتطلب عضوية مميزة أو مسؤول).

🛠️ للدعم الفني، تواصل مع المطور: @X_XF8 ☠️
"""

# Advanced usage (no change needed here, handled by buttons now)
USAGE_ADVANCED_AR = """
**🚀 وضع الاستخدام المتقدم (للمسؤولين والمستخدمين المميزين) 🚀**


👋 أهلاً بك في بوت حفظ المحتوى!

أنا بوت يمكنه حفظ المحتوى المقيد من القنوات أو المجموعات.

**يمكنك ربح عضوية مجانية عن طريق مشاركة رابط البوت و القناة**
https://t.me/+EzUoGd6C0Qs2MDU0
https://t.me/+EzUoGd6C0Qs2MDU0
** للدعم:** @X_XF8 📞
"""


# --- NEW: Command Details for Button Interface ---
# Structure:
# "category_key": {
#    "name": "Category Name AR | EN",
#    "items": [ # List of commands/groups in this category
#        {
#            "id": "unique_id", # Used in callback_data
#            "button_text": "Button Text AR | EN", # Text shown on the button
#            "usage": "`/cmd` or `/cmd <arg>`\n`/another_cmd`", # How to use (multi-line ok)
#            "desc_ar": "Description in Arabic.",
#            "desc_en": "Description in English."
#        },
#        # ... more items
#    ]
# }, ... more categories

CMD_DETAILS = {
    "basic": {
        "name": "🛠️ الأوامر الأساسية | Basic",
        "items": [
            {
                "id": "start_cmd",
                "button_text": "/start",
                "usage": "`/start`",
                "desc_ar": "🏁 بدء استخدام البوت وعرض قائمة المساعدة الرئيسية.",
                "desc_en": "🏁 Start the bot and view the main help menu."
            },
            {
                "id": "link_cmd",
                "button_text": "/l <رابط> | Link",
                "usage": "`/l <telegram_link>`\nأو أرسل الرابط مباشرة | Or send link directly\n\n`link/123` (Single)\n`link/10-20` (Range)",
                "desc_ar": "🔗 إرسال رابط رسالة تيليجرام (فردي أو نطاق) للمعالجة. سيتم تنزيل وإعادة رفع المحتوى في الوضع المتقدم أو للقنوات/المجموعات الخاصة. سيتم محاولة التمرير للقنوات العامة في الوضع العادي.",
                "desc_en": "🔗 Send a Telegram message link (single or range) for processing. Downloads & re-uploads in Advanced Mode or for private channels/groups. Attempts forwarding for public channels in Normal Mode."
            },
            {
                "id": "cancel_cmd",
                "button_text": "/cancel",
                "usage": "`/cancel`",
                "desc_ar": "⏹️ إيقاف العملية الجماعية (تحميل/رفع) الجارية *في هذه الدردشة*. لا يوقف العمليات الفردية.",
                "desc_en": "⏹️ Cancel the ongoing bulk download/upload operation *in this chat*. Doesn't stop single operations."
            },
            {
                "id": "status_cmd",
                "button_text": "/status",
                "usage": "`/status`",
                "desc_ar": "📊 عرض حالة البوت الحالية (مدة التشغيل، العمليات النشطة). تفاصيل إضافية للمسؤولين/المميزين.",
                "desc_en": "📊 View the current bot status (uptime, active operations). More details for Admin/Premium."
            },
             { # NEW
                "id": "mypremium_cmd",
                "button_text": "/mypremium",
                "usage": "`/mypremium`",
                "desc_ar": "👑 عرض حالة عضويتك المميزة الحالية وتاريخ انتهائها.",
                "desc_en": "👑 View your current premium membership status and expiry date."
            },
             { # NEW (Represents the redeem process, not a single command)
                "id": "redeem_cmd",
                "button_text": "🎁 شحن الكود | Redeem Code",
                "usage": "Use the button in /start menu\n(استخدم الزر في قائمة /start)",
                "desc_ar": "🎟️ بدء عملية شحن كود للحصول على عضوية مميزة أو تمديدها.",
                "desc_en": "🎟️ Start the process to redeem a code for premium membership or extension."
            },
        ]
    },
    "customization": {
        "name": "🔧 التخصيص | Customization",
        "items": [
            {
                "id": "chat_id_cmds",
                "button_text": "وجهة التحميل | Destination",
                "usage": "`/set_chat_id <chat_id>`\n`/see_chat_id`\n`/remove_chat_id`",
                "desc_ar": "➡️ تعيين (`-100xxxxxxxxxx`) أو عرض أو إزالة دردشة الوجهة المخصصة لإرسال الملفات إليها (قناة/مجموعة يجب أن يكون البوت عضوًا فيها ولديه صلاحية الإرسال).",
                "desc_en": "➡️ Set (`-100xxxxxxxxxx`), view, or remove a custom destination chat for sending files (channel/group where the bot is a member with send permissions)."
            },
            {
                "id": "thumb_cmds",
                "button_text": "الصورة المصغرة | Thumbnail",
                "usage": "Send a photo directly\n`/see_thumb`\n`/remove_thumb`",
                "desc_ar": "🖼️ تعيين صورتك المصغرة بإرسال صورة مباشرة، أو عرض الحالية، أو إزالة المخصصة والعودة للافتراضية.",
                "desc_en": "🖼️ Set your thumbnail by sending a photo, view the current one, or remove the custom one and use the default."
            },
            {
                "id": "caption_filter_cmds",
                "button_text": "فلتر الكابشن | Caption Filter",
                "usage": "`/add_word <word/phrase>`\n`/remove_word <word/phrase>`\n`/see_words`",
                "desc_ar": "✍️ إضافة أو إزالة كلمة/عبارة سيتم حذفها تلقائيًا من تعليقات الوسائط (الكابشن) عند الرفع في الوضع المتقدم. عرض القائمة الحالية.",
                "desc_en": "✍️ Add or remove a word/phrase that will be automatically removed from media captions when uploading in Advanced Mode. View the current list."
            },
            {
                "id": "fix_cmds",
                "button_text": "البادئة/اللاحقة | Prefix/Suffix",
                "usage": "`/set_prefix <text>`\n`/remove_prefix`\n`/set_suffix <text>`\n`/remove_suffix`\n`/see_fix`",
                "desc_ar": "🏷️ تعيين/إزالة/عرض بادئة (prefix) تضاف قبل اسم الملف، ولاحقة (suffix) تضاف بعد سطرين فارغين في نهاية الكابشن (في الوضع المتقدم).",
                "desc_en": "🏷️ Set/Remove/View a prefix added before the filename, and a suffix added after two newlines at the end of the caption (in Advanced Mode)."
            },
            {
                "id": "skip_filter_cmds",
                "button_text": "فلاتر التخطي | Skip Filters",
                "usage": "`/filter_text_skip on|off`\n`/filter_photo_skip on|off`\n`/filter_sticker_skip on|off`",
                "desc_ar": "⏩ تفعيل/تعطيل تخطي أنواع معينة من الرسائل (نص، صورة، ملصق) أثناء المعالجة الجماعية في الوضع المتقدم.",
                "desc_en": "⏩ Enable/Disable skipping of specific message types (text, photo, sticker) during bulk processing in Advanced Mode."
            },
        ]
    },
    "admin_users": {
        "name": "👑 إدارة المستخدمين | User Mgmt (Admin)",
        "items": [
             { # NEW
                "id": "gen_code_cmd",
                "button_text": "/gen_code <أيام> [بادئة]",
                "usage": "`/gen_code <days>`\n`/gen_code <days> [PREFIX]`",
                "desc_ar": "🔑 توليد كود شحن مميز جديد بالمدة المحددة (بالأيام) وبادئة اختيارية.",
                "desc_en": "🔑 Generate a new premium redeem code with specified duration (days) and optional prefix."
            },
            {
                "id": "pro_cmds",
                "button_text": "بريميوم | Premium",
                "usage": "`/add_pro <user_id> <duration>`\n`/remove_pro <user_id>`",
                "desc_ar": "⭐ إضافة (مع مدة مثل `7days`, `1month`) أو إزالة عضوية مميزة للمستخدمين.",
                "desc_en": "⭐ Add (with duration like `7days`, `1month`) or remove premium membership for users."
            },
            {
                "id": "ban_cmds",
                "button_text": "الحظر | Ban",
                "usage": "`/ban <user_id>`\n`/unban <user_id>`",
                "desc_ar": "🚫 حظر أو فك حظر المستخدمين من استخدام البوت.",
                "desc_en": "🚫 Ban or unban users from using the bot."
            },
            {
                "id": "restart_cmd",
                "button_text": "/restart",
                "usage": "`/restart`",
                "desc_ar": "🔄 إعادة تشغيل البوت (قد يستغرق بعض الوقت للعودة للعمل). استخدم بحذر.",
                "desc_en": "🔄 Restart the bot (may take time to come back online). Use with caution."
            },
        ]
    },
    "admin_thumb": {
         "name": "🖼️ إدارة الصور المصغرة | Thumb Mgmt (Admin)",
         "items": [
            {
                "id": "user_thumb_cmds",
                "button_text": "صور المستخدمين | User Thumbs",
                "usage": "`/set_thumb <user_id>` (+ Reply Photo)\n`/remove_thumb_user <user_id>`",
                "desc_ar": "🖼️ تعيين (عبر الرد بالصورة بعد الأمر) أو إزالة الصورة المصغرة المخصصة لمستخدم آخر.",
                "desc_en": "🖼️ Set (by replying with photo after command) or remove the custom thumbnail for another user."
            },
         ]
    }
}

# ==============================================================================
# --- Helper Functions (Keep existing helpers) ---
# ==============================================================================
# --- Helper Functions ---
# ... (الدوال المساعدة الموجودة) ...

# ... (بقية الدوال المساعدة) ...

async def log_outgoing_message(client: Client, sent_message: Message, log_channel_id: Optional[int]):
    """
    Logs a message successfully sent by the bot to a user/destination chat
    by copying/sending it to the log channel.

    Args:
        client: The Pyrogram client instance (usually 'bot').
        sent_message: The Message object that was successfully sent by the bot.
        log_channel_id: The ID of the log channel.
    """
    if not log_channel_id or not sent_message:
        return # Do nothing if log channel is not set or message is invalid

    log_prefix = f"📤 **[BOT OUTGOING]** -> User/Chat: `{sent_message.chat.id}`\n\n" # Prefix for context

    try:
        if sent_message.media and not sent_message.service:
            # Copy media messages to preserve format and add prefix to caption
            new_caption = log_prefix + (sent_message.caption if sent_message.caption else "")
            # Truncate caption if too long for log copy
            if len(new_caption.encode('utf-8')) > 1024:
                 limit = 1020; encoded_cap = new_caption.encode('utf-8')
                 try: new_caption = encoded_cap[:limit].decode('utf-8', errors='ignore') + "..."
                 except: new_caption = new_caption[:300] + "..." # Fallback
                 logger.warning(f"Log outgoing: Caption truncated for message {sent_message.id}")

            await client.copy_message(
                chat_id=log_channel_id,
                from_chat_id=sent_message.chat.id, # From the chat where bot sent it
                message_id=sent_message.id,
                caption=new_caption # Use the modified caption with prefix
            )
        elif sent_message.text:
            # Send text messages directly with the prefix
            text_to_log = log_prefix + sent_message.text
            # Truncate text if too long for log message
            if len(text_to_log.encode('utf-8')) > 4096:
                 limit = 4090; encoded_text = text_to_log.encode('utf-8')
                 try: text_to_log = encoded_text[:limit].decode('utf-8', errors='ignore') + "..."
                 except: text_to_log = text_to_log[:1500] + "..." # Fallback
                 logger.warning(f"Log outgoing: Text truncated for message {sent_message.id}")

            await client.send_message(
                log_channel_id,
                text_to_log,
                # Note: Sending entities from the original message might cause issues
                # if the text was truncated or modified significantly by the prefix.
                # It's safer to send without entities for the log copy.
                # entities=sent_message.entities,
                disable_web_page_preview=True
            )
        else:
            # For other types like stickers, polls etc., attempt to copy
            try:
                 await client.copy_message(
                    chat_id=log_channel_id,
                    from_chat_id=sent_message.chat.id,
                    message_id=sent_message.id
                 )
                 # Optionally add prefix for stickers if needed, but copy is simpler
            except Exception as e_copy_other:
                 logger.warning(f"Could not copy non-media/non-text message {sent_message.id} to log: {e_copy_other}. Attempting forward.")
                 # Fallback to forwarding if copy fails for some types
                 try:
                    await client.forward_messages(
                        chat_id=log_channel_id,
                        from_chat_id=sent_message.chat.id,
                        message_ids=sent_message.id
                    )
                    # Add prefix manually if forwarding text/caption? Maybe too complex. Keep simple.
                 except Exception as e_fwd:
                     logger.error(f"Failed to forward message {sent_message.id} to log channel {log_channel_id} after copy failed: {e_fwd}")


        # logger.debug(f"Logged outgoing message {sent_message.id} (to user/chat {sent_message.chat.id}) to log channel {log_channel_id}")

    except FloodWait as fw:
        logger.warning(f"FloodWait logging outgoing message: {fw.value}s. Will retry later implicitly.")
        await asyncio.sleep(fw.value + 0.5) # Wait locally
    except Exception as e:
        logger.error(f"Error logging outgoing message {sent_message.id} to log channel {log_channel_id}: {type(e).__name__} - {e}", exc_info=False)

# ... (بقية الدوال المساعدة) ...
# ... (بقية الدوال المساعدة) ...

# async def waiting_for_code_filter(_, __, message: Message) -> bool: # <<< السطر القديم
async def waiting_for_code_filter(client: Client, message: Message) -> bool: # <<< السطر الجديد
    """
    Custom filter: Checks if the user is in the waiting state,
    and the message is a private text message that is not a command or service msg.
    """
    # لا حاجة لاستخدام client هنا، لكننا نحافظ على الصيغة القياسية
    if not message.from_user: return False # Ignore messages without user
    user_id = message.from_user.id
    is_waiting = user_id in user_waiting_for_code
    is_valid_message = bool(
        message.text and                                  # Has text
        not message.text.startswith('/') and              # Does not start with /
        message.chat.type == pyrogram.enums.ChatType.PRIVATE and # Is private
        not message.service                               # Is not service message
    )
    # if is_waiting: logger.debug(f"waiting_for_code_filter: User {user_id} waiting? {is_waiting}. Valid msg? {is_valid_message}. Text: '{message.text[:20]}'")
    result = is_waiting and is_valid_message
    # logger.debug(f"waiting_for_code_filter check for {user_id}: result={result}") # يمكنك إبقاء هذه للتحقق
    return result

# تأكد من أن الديكوريتور الذي يستخدم هذا المرشح لا يزال كما هو:

    # ... (الكود داخل المعالج يبقى كما هو) ...
async def is_pro_user(user_id: int) -> bool:
    """Checks if a user has premium access via database."""
    return await digital_botz.has_premium_access(user_id)

def is_admin(user_id: int, admin_id_list: set[int]) -> bool:
    """Checks if a user ID is in the admin list."""
    return user_id in admin_id_list

async def is_admin_or_pro(user_id: int, admin_id_list: set[int]) -> bool:
    """Checks if a user is an admin or has premium access."""
    return is_admin(user_id, admin_id_list) or await is_pro_user(user_id)

def get_uptime(start_time: float) -> str:
    """Calculates the bot's uptime."""
    uptime_seconds = int(time.time() - start_time)
    days, remainder = divmod(uptime_seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)
    uptime_str = ""
    if days > 0: uptime_str += f"{days} يوم "
    if hours > 0: uptime_str += f"{hours} ساعة "
    if minutes > 0: uptime_str += f"{minutes} دقيقة "
    uptime_str += f"{seconds} ثانية"
    return uptime_str.strip()

def get_readable_size(size_bytes: Optional[int]) -> str:
    """Converts bytes to a readable format (KB, MB, GB, TB)."""
    if size_bytes is None or size_bytes < 0: return "N/A"
    if size_bytes == 0: return "0 B"
    power = 1024 # Use 1024 for binary prefixes (KiB, MiB, etc.)
    n = 0
    power_labels = {0 : 'B', 1: 'KB', 2: 'MB', 3: 'GB', 4: 'TB'}
    while size_bytes >= power and n < len(power_labels) - 1:
        size_bytes /= power
        n += 1
    return f"{size_bytes:.2f} {power_labels[n]}"

def get_speed_and_eta(start_time: float, current_bytes: int, total_bytes: Optional[int]) -> tuple[str, str]:
    """Calculates download/upload speed and estimated time remaining."""
    elapsed_time = time.time() - start_time
    speed = "N/A"
    eta = "N/A"

    if elapsed_time > 0.1 and current_bytes > 0: # Avoid division by zero or tiny times
        speed_bytes_per_sec = current_bytes / elapsed_time
        speed = f"{get_readable_size(int(speed_bytes_per_sec))}/s"

        if total_bytes and total_bytes > current_bytes and speed_bytes_per_sec > 0:
            remaining_bytes = total_bytes - current_bytes
            eta_seconds = remaining_bytes / speed_bytes_per_sec
            # Use try-except for timedelta conversion robustness
            try:
                eta = str(timedelta(seconds=int(eta_seconds)))
            except OverflowError:
                eta = "∞" # Handle very large ETAs
        elif total_bytes and total_bytes == current_bytes:
             eta = "0s" # Completed

    return speed, eta

# --- Progress Callback ---
last_edit_times: Dict[int, float] = {} # Dictionary to store last edit time per message ID
PROGRESS_UPDATE_INTERVAL = 3 # Seconds (slightly faster updates)

async def progress(current, total, user_message, type, start_time, status_message: Optional[Message]):
    """Pyrogram progress callback function."""
    global last_edit_times, operation_status_map
    # Only proceed if status_message is provided and valid
    if not status_message or not isinstance(status_message, Message) or status_message.id not in operation_status_map:
        # logger.debug("Progress callback skipped: No valid status message provided or not mapped.") # Can be noisy
        return

    message_id = status_message.id
    chat_id = status_message.chat.id # Get chat ID from the status message itself
    user_chat_id = operation_status_map.get(message_id) # Get the user's chat ID from the map

    if user_chat_id is None:
        logger.warning(f"Progress callback for msg {message_id}: User chat ID not found in operation_status_map. Skipping.")
        return

    # Check if operation was cancelled for the USER'S CHAT
    if not active_operations.get(user_chat_id, True):
        logger.debug(f"Progress update skipped for msg {message_id}: Operation cancelled in user chat {user_chat_id}")
        # Attempt to stop the underlying Pyrogram transfer
        try:
            await bot.stop_transmission()
            # logger.info(f"Requested stop_transmission for cancelled operation in chat {user_chat_id}") # Can be noisy
        except Exception as e_stop:
            logger.error(f"Error calling stop_transmission: {e_stop}")
        return

    # Create cancel button - Use the user_chat_id where the user initiated the command
    cancel_button = InlineKeyboardButton("⏹️ إلغاء | Cancel", callback_data=f"cancel_op_{user_chat_id}")
    keyboard = InlineKeyboardMarkup([[cancel_button]])

    if not total: # Can happen for some streams
        total = current # Avoid division by zero, show 100% if total unknown but progress called

    if total == 0: # If total is genuinely 0, avoid division error
        percentage = 100.0
    else:
        percentage = current * 100 / total

    completed_bar = int(20 * current // total) if total > 0 else 20
    remaining_bar = 20 - completed_bar
    progress_bar = '▣' * completed_bar + '▢' * remaining_bar # More modern bar

    speed, eta = get_speed_and_eta(start_time, current, total)

    op_type_ar = "التحميل" if type.upper() == "DOWN" else "الرفع"
    progress_message_text = f"""
**🔄 جارٍ {op_type_ar} | {type.upper()}ING...**

`{progress_bar}`

╭━━━━❰@x_xf8 PROCESSING...❱━➣
┣⪼ 🗃️ **الحجم | Size:** {get_readable_size(current)} / {get_readable_size(total)}
┣⪼ ⏳️ **اكتمل | Done:** {percentage:.1f}%
┣⪼ 🚀 **السرعة | Speed:** {speed}
┣⪼ ⏰️ **الوقت المتبقي | ETA:** {eta}
╰━━━━━━━━━━━━━━━➣
"""
    current_time = time.time()
    last_edit_time = last_edit_times.get(message_id, 0)

    # Edit message only if enough time has passed or if it's the final update
    if current_time - last_edit_time >= PROGRESS_UPDATE_INTERVAL or current == total:
        try:
            # Edit the status message
            await bot.edit_message_text(
                chat_id=chat_id, # Edit in the chat where the status message is
                message_id=message_id,
                text=progress_message_text,
                reply_markup=keyboard # Add cancel button
            )
            last_edit_times[message_id] = current_time
        except FloodWait as e:
            # Log flood wait but do not sleep here; let the main operation handle it
            logger.warning(f"FloodWait encountered during progress update (MsgID: {message_id}), waiting for {e.value} seconds in main loop.")
            # We update the last edit time to prevent immediate retry after the wait
            last_edit_times[message_id] = current_time + e.value
        except MessageNotModified:
             # logger.debug(f"Progress message {message_id} not modified.") # Can be noisy
             last_edit_times[message_id] = current_time # Still update time
        except MessageIdInvalid:
             logger.warning(f"Progress message {message_id} is invalid or deleted. Cannot update.")
             # Stop trying to update this message
             last_edit_times.pop(message_id, None)
             operation_status_map.pop(message_id, None)
        except Exception as e:
            # Log error but don't crash the download/upload
            logger.error(f"Error updating progress message (ID: {message_id}): {type(e).__name__} - {e}", exc_info=False)

    # Cleanup last_edit_times entry only when fully completed (total > 0)
    # The final message edit happens outside this progress callback now.
    if current == total and total > 0: # Ensure it was a real completion
        last_edit_times.pop(message_id, None)
        # Keep operation_status_map until the final status is set by the caller

# --- Sanitize Filename ---
def sanitize_filename(text: Optional[str]) -> str:
    """Cleans text to be used as a filename base (without adding prefix)."""
    if not text:
        return "Untitled_File" # Default name

    # Limit length early to avoid excessive processing
    text = text[:200] # Limit initial text length

    # Keep first two lines max for filename base
    lines = text.splitlines()
    text = ' '.join(lines[:2]).strip() if lines else "Untitled_File"

    # Remove emojis and specific symbols more broadly
    # This regex is broad and might remove desired characters. Fine-tune if needed.
    emoji_pattern = re.compile(
        "["
        "\U0001F1E0-\U0001F1FF"  # flags (iOS)
        "\U0001F300-\U0001F5FF"  # symbols & pictographs
        "\U0001F600-\U0001F64F"  # emoticons
        "\U0001F680-\U0001F6FF"  # transport & map symbols
        "\U0001F700-\U0001F77F"  # alchemical symbols
        "\U0001F780-\U0001F7FF"  # Geometric Shapes Extended
        "\U0001F800-\U0001F8FF"  # Supplemental Arrows-C
        "\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
        "\U0001FA70-\U0001FAFF"  # Symbols and Pictographs Extended-A
        "\U00002702-\U000027B0"  # Dingbats
        "\U000024C2-\U0001F251"
        "\U00002000-\U0000206F"  # General Punctuation (includes zero-width spaces)
        "\U00002B50" # Star
        "\U0000FE00-\U0000FE0F"  # variation selectors
        "]+", flags=re.UNICODE)
    text = emoji_pattern.sub('', text)

    # Remove specific Arabic punctuation maybe: ، ؛ ؟
    text = re.sub(r'[؛،؟]', '', text) # Remove specific Arabic punctuation

    # Remove common problematic symbols manually
    text = re.sub(r'[|📌⬅️✅❌✨⭐🔴⚪⚫🔘📅🔗📎🎼🎵🎶🎤🎧🎬🎭🎮🎯🎰🎲🎳🚨ﷺ]', '', text, flags=re.UNICODE)
    # Characters invalid in most file systems (allow . and _ and -)
    text = re.sub(r'[<>:"/\\|?*!$%^&;={}`~()\'\[\]]', '', text)

    # Remove specific unwanted words (case-insensitive) - Added \b for whole words
    unwanted_words = [r'\bميتيكس\b', r'#\w+', r'@[a-zA-Z0-9_]+', r'https?://\S+']
    for word in unwanted_words:
         text = re.sub(word, '', text, flags=re.IGNORECASE | re.UNICODE)

    # Normalize whitespace and replace with underscore for filename safety
    text = re.sub(r'\s+', '_', text).strip('_') # Replace spaces with underscore

    # Truncate if too long (OS limits filenames, common limit ~255 bytes/chars)
    max_len = 180 # Be conservative
    if len(text.encode('utf-8')) > max_len: # Check byte length for safety
        # Try cutting at the last underscore
        safe_cut = text[:max_len].rfind('_')
        if safe_cut > max_len // 2 : # Ensure cut is not too near the beginning
            text = text[:safe_cut]
        else: # Fallback to heuristic cut if safe cut fails or no underscore
            # Need a better way for UTF-8 truncation if simple slicing breaks chars
             encoded = text.encode('utf-8')[:max_len]
             try:
                 text = encoded.decode('utf-8', errors='ignore').strip('_')
             except: # Fallback to very simple slice
                 text = text[:max_len // 2].strip('_') # Heuristic cut

    # Handle empty filename after cleaning
    if not text:
        return "Untitled_File"

    return text


# ==============================================================================
# --- Helper Functions ---
# ==============================================================================

# ... (is_pro_user, is_admin, get_uptime, etc.) ...

def generate_random_code(length: int) -> str:
    """Generates a cryptographically secure random alphanumeric code."""
    # Use uppercase letters and digits
    alphabet = string.ascii_uppercase + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(length))

async def generate_unique_code(prefix: str, length: int) -> str:
    """Generates a unique code with a prefix, checking the database."""
    max_attempts = 10 # Prevent infinite loops in rare cases
    for _ in range(max_attempts):
        random_part = generate_random_code(length)
        code = f"{prefix}_{random_part}" if prefix else random_part # Add underscore separator
        # Check if code already exists (using get_coupon_details)
        existing_code = await digital_botz.get_coupon_details(code)
        if not existing_code: # If get_coupon_details returns None (not found or used is ok for generation check)
            return code
    # If we reach here, failed to generate unique code after max attempts
    raise Exception(f"Failed to generate a unique code with prefix '{prefix}' after {max_attempts} attempts.")

# ... (Rest of helper functions: get_readable_size, progress, sanitize_filename, etc.) ...



async def forward_message_to_log_channel(client: Client, message: Message, log_channel_id: Optional[int]):
    """Forwards or copies a message to the log channel."""
    if not log_channel_id: return # Do nothing if log channel is not set
    try:
        # Copy media to preserve original sender, forward text/other
        if message.media and not message.service: # Check if it's actual media
            await client.copy_message(
                chat_id=log_channel_id,
                from_chat_id=message.chat.id,
                message_id=message.id
            )
        else: # Forward text, service messages, etc.
            await client.forward_messages(
                chat_id=log_channel_id,
                from_chat_id=message.chat.id,
                message_ids=message.id
            )
        # logger.debug(f"Message {message.id} from chat {message.chat.id} forwarded/copied to log channel {log_channel_id}") # Can be noisy
    except FloodWait as fw:
        logger.warning(f"FloodWait sending log message: {fw.value}s")
        await asyncio.sleep(fw.value + 0.5) # Wait a bit longer
        # Optionally retry here, but might cause infinite loop if channel is restricted
    except Exception as e:
        logger.error(f"Error forwarding/copying message {message.id} to log channel {log_channel_id}: {type(e).__name__} - {e}")

async def send_user_info_to_log_channel(client: Client, user: pyrogram.types.User, log_channel_id: Optional[int]):
    """Sends new user information (including profile photo) to the log channel."""
    if not log_channel_id: return
    try:
        user_info = (
            f"**🆕 مستخدم جديد بدأ البوت! | New User Started!**\n\n"
            f"👤 **الاسم | Name:** {user.first_name} {user.last_name or ''}\n"
            f"🔗 **اسم المستخدم | Username:** @{user.username or 'N/A'}\n"
            f"🆔 **المعرف | ID:** `{user.id}`\n"
            # Use datetime.datetime for now() with timezone.utc
            f"🗓️ **الطابع الزمني | Timestamp:** `{datetime.datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}`"
        )

        profile_photo: Optional[str] = None
        photo_path: Optional[str] = None
        try:
            # Use get_chat to fetch user details including photo
            # Get the first profile photo
            async for photo in client.get_chat_photos(user.id, limit=1):
                # Use user ID and timestamp for unique temp filename
                timestamp = int(time.time())
                temp_filename = f"temp_pp_{user.id}_{timestamp}.jpg"
                photo_path = await client.download_media(photo.file_id, file_name=temp_filename, in_memory=False)
                profile_photo = photo_path # Assign path
                break # Only need one
        except Exception as e:
            logger.warning(f"Could not get profile photo for user {user.id}: {e}")

        if profile_photo and os.path.exists(profile_photo):
            await client.send_photo(log_channel_id, profile_photo, caption=user_info)
        else:
            await client.send_message(log_channel_id, user_info)

        logger.info(f"User {user.id} info sent to log channel {log_channel_id}.")

    except FloodWait as fw:
        logger.warning(f"FloodWait sending user info log: {fw.value}s")
        await asyncio.sleep(fw.value + 0.5) # Wait longer
    except Exception as e:
        logger.error(f"Error sending user info for {user.id} to log channel {log_channel_id}: {type(e).__name__} - {e}")
    finally:
        # Clean up downloaded photo file
        if photo_path and os.path.exists(photo_path):
            try:
                os.remove(photo_path)
            except OSError as e:
                logger.error(f"Error removing temporary profile picture {photo_path}: {e}")

def check_daily_task_limit(user_id: int, last_reset_day_global: Optional[int], user_tasks: Dict[int, int], limit: int) -> Tuple[bool, Optional[int], Dict[int, int]]:
    """Checks if the user is within their daily task limit. Handles reset."""
    global last_task_reset_day # Allow modification of the global reset day tracker
    # FIX: Use datetime.datetime.now() with datetime.timezone.utc
    current_day = datetime.datetime.now(timezone.utc).day # Use UTC day

    if last_reset_day_global != current_day:
        user_tasks.clear() # Reset tasks for all users
        last_task_reset_day = current_day # Update the global reset day
        logger.info(f"Daily task limit reset for all users (UTC Day: {current_day}).")
        last_reset_day_global = current_day # Reflect change for return value

    if user_id not in user_tasks:
        user_tasks[user_id] = limit # Assign full limit to new user for the day

    can_perform_task = user_tasks.get(user_id, 0) > 0
    # Return status, the (potentially updated) reset day, and the (potentially updated) tasks dict
    return can_perform_task, last_reset_day_global, user_tasks

def decrement_daily_task_count(user_id: int, user_tasks: Dict[int, int]) -> Dict[int, int]:
    """Decrements the task count for a user."""
    if user_id in user_tasks and user_tasks[user_id] > 0:
        user_tasks[user_id] -= 1
    # No need to return dict if modifying in-place, but can be useful
    return user_tasks

async def is_user_subscribed(client: Client, user_id: int, channel_id: Optional[int]) -> bool:
    """Checks if a user is subscribed to the force subscribe channel."""
    if not channel_id or channel_id == 0:
        # logger.debug("FORCE_SUBSCRIBE_CHANNEL_ID not set or 0. Skipping check.")
        return True # Skip check if not configured

    # logger.debug(f"Checking subscription status for user {user_id} in channel {channel_id}") # Can be noisy
    try:
        member = await client.get_chat_member(channel_id, user_id)
        status = member.status
        # logger.debug(f"[Subscription Check] User: {user_id}, Channel: {channel_id}, RESULT STATUS: {status}") # Changed to debug

        # --- Check against valid statuses ---
        VALID_STATUSES = {
            ChatMemberStatus.OWNER,
            ChatMemberStatus.ADMINISTRATOR,
            ChatMemberStatus.MEMBER
        }
        # Statuses like LEFT, KICKED, BANNED, RESTRICTED are not subscribed
        is_subscribed = status in VALID_STATUSES
        # --- END CHECK ---

        # logger.debug(f"[Subscription Check] User: {user_id}, Evaluation Result: {is_subscribed}") # Changed to debug
        return is_subscribed

    except UserNotParticipant:
        # logger.info(f"[Subscription Check] User: {user_id}, Channel: {channel_id}, RESULT: UserNotParticipant") # Can be noisy
        return False
    except ChatAdminRequired:
        logger.error(f"[Subscription Check] User: {user_id}, Channel: {channel_id}, FAILED: CHAT_ADMIN_REQUIRED. Bot is NOT admin in the channel!")
        return True # IMPORTANT: Allow usage if bot isn't admin, can't enforce sub
    except FloodWait as fw:
        logger.warning(f"[Subscription Check] FloodWait for user {user_id}, channel {channel_id}: {fw.value}s. Assuming subscribed temporarily.")
        await asyncio.sleep(fw.value + 0.5) # Add slight delay
        return True # Assume true during flood wait to prevent blocking user
    except Exception as e:
        # Log other unexpected errors
        logger.error(f"[Subscription Check] User: {user_id}, Channel: {channel_id}, FAILED: Unexpected error - {type(e).__name__}: {e}", exc_info=False) # Don't need full traceback here
        return True # Assume true on other errors to avoid blocking user

async def force_subscribe(bot_client: Client, message: Message, force_sub_channel_id: Optional[int]):
    """Checks subscription and replies if user is not subscribed."""
    if not force_sub_channel_id:
        return True # Feature disabled

    user_id = message.from_user.id
    # Pass bot client instance
    if await is_user_subscribed(bot_client, user_id, force_sub_channel_id):
        return True
    else:
        # If bot is not admin, is_user_subscribed returns True already, so this part won't execute
        # If user is genuinely not subscribed (and bot IS admin):
        try:
            # Get channel invite link or username using bot_client
            channel = await bot_client.get_chat(force_sub_channel_id)
            channel_name = channel.title or f"Channel ({force_sub_channel_id})"
            invite_link = None
            if channel.username:
                invite_link = f"https://t.me/{channel.username}"
            else:
                # Attempt to create or get an invite link (requires admin rights)
                try:
                    invite_link = await bot_client.export_chat_invite_link(force_sub_channel_id)
                except Exception as e_link:
                    logger.error(f"Could not get/create invite link for {force_sub_channel_id}: {e_link}")
                    invite_link = None # Fallback if link fails

            text = f"⚠️ **عليك الاشتراك في قناة '{channel_name}' لاستخدام البوت.**\n\n" \
                   f"**Please subscribe to the channel '{channel_name}' below to use this bot:**"
            markup = None
            if invite_link:
                 markup = InlineKeyboardMarkup(
                     [[InlineKeyboardButton(f"📢 اشترك في {channel_name} | Subscribe Now 📢", url=invite_link)]]
                 )
                 text += f"\n\n➡️ {invite_link}" # Show link in text too
            else:
                 text += "\n\n(تعذر جلب رابط القناة. يرجى التواصل مع المسؤول | Could not retrieve channel link. Please contact admin.)"

            await message.reply_text(
                text,
                reply_markup=markup,
                quote=True,
                disable_web_page_preview=True
            )
        except Exception as e_reply:
             logger.error(f"Error sending force subscribe message to user {user_id}: {e_reply}")
             # Send a simpler message if getting chat info failed
             await message.reply_text(
                 "⚠️ يرجى الاشتراك في القناة المطلوبة لاستخدام البوت. (خطأ في جلب تفاصيل القناة).\n"
                 "⚠️ Please subscribe to the required channel to use this bot. (Error retrieving channel details).",
                 quote=True
             )
        return False # User is not subscribed

async def is_banned_user(user_id: int) -> bool:
    """Checks if a user is banned via database."""
    return await digital_botz.is_banned(user_id)

def get_message_type(msg: Message) -> str:
    """Determines the type of a Pyrogram message."""
    if msg.service: return "Service"
    if msg.text: return "Text"
    if msg.photo: return "Photo"
    if msg.video: return "Video"
    if msg.audio: return "Audio"
    if msg.document: return "Document"
    if msg.sticker: return "Sticker"
    if msg.animation: return "Animation"
    if msg.voice: return "Voice"
    if msg.video_note: return "VideoNote"
    if msg.contact: return "Contact"
    if msg.location: return "Location"
    if msg.venue: return "Venue"
    if msg.poll: return "Poll"
    if msg.game: return "Game"
    # Add more types if needed
    return "Unknown"

# ==============================================================================
# --- Generate Start Message Helper ---
# ==============================================================================

async def get_start_message_data(user_id: int) -> Tuple[str, Optional[InlineKeyboardMarkup]]:
    """Generates the text and markup for the /start command based on user status."""
    try:
        user = await bot.get_users(user_id) # Get user details for mention
    except Exception as e:
        logger.error(f"Could not get user details for {user_id}: {e}")
        user = None # Fallback

    is_admin_user = is_admin(user_id, admin_ids)
    is_pro = await is_pro_user(user_id)
    is_eligible_for_advanced = is_admin_user or is_pro
    # Default eligible users to Advanced mode unless they explicitly toggled it
    is_currently_advanced = is_eligible_for_advanced and admin_mode_advanced.get(user_id, True)

    mode_name = "المتقدم (Advanced)" if is_currently_advanced else "العادي (Normal)"
    # Use simpler advanced usage text now that buttons handle details
    usage_text = USAGE_ADVANCED_AR if is_eligible_for_advanced else USAGE_NORMAL_AR
    user_mention = user.mention if user else f"User (`{user_id}`)"

    welcome_text = (
        f"👋 **أهلاً {user_mention}**, \n\n"
        "أنا بوت يمكنه حفظ المحتوى المقيد من القنوات أو المجموعات.\n"
        f"أنت حاليًا في **وضع {mode_name}**.\n\n"
        f"{usage_text}"
    )

    buttons = []
    # Row 1: Mode Toggle / Premium Info / Redeem Code
    row1 = []
    if is_eligible_for_advanced:
        toggle_text = "🔄  Normal Mode" if is_currently_advanced else "🚀  Advanced Mode"
        row1.append(InlineKeyboardButton(toggle_text, callback_data="toggle_mode"))
    else:
        row1.append(InlineKeyboardButton("⭐  Upgrade to Premium ⭐", callback_data="premium_info"))

    # Add Redeem Code button for everyone (non-banned)
    row1.append(InlineKeyboardButton("🎁  Redeem Code", callback_data="redeem_code_prompt"))
    buttons.append(row1)

    # Row 2: Settings (only for eligible users) & Developer
    settings_button = None
    if is_eligible_for_advanced:
         settings_button = InlineKeyboardButton("⚙️ الإعدادات | Settings", callback_data="show_settings_cats")

    dev_button = InlineKeyboardButton("🌐 المطور | Developer", url="https://t.me/X_XF8")

    if settings_button:
        buttons.append([settings_button, dev_button])
    else:
        buttons.append([dev_button]) # Only show dev button if not eligible for settings

    markup = InlineKeyboardMarkup(buttons) if buttons else None
    return welcome_text, markup

# ==============================================================================
# --- Command Handlers ---
# ==============================================================================

@bot.on_message(filters.command(["start", "help"]) & filters.private)
async def start_command(client: Client, message: Message):
    """Handles the /start and /help commands."""
    user = message.from_user
    user_id = user.id

    # Force Subscribe Check (Pass bot client instance)
    if not await force_subscribe(client, message, FORCE_SUBSCRIBE_CHANNEL_ID):
        return

    # Add user to DB if not already present (idempotent)
    await digital_botz.add_user(None, user) # Passed None for b and user object for u

    # Check Ban Status AFTER adding user (so ban check works)
    if await is_banned_user(user_id):
        await message.reply_text("🚫 **أنت محظور** من استخدام هذا البوت.\n"
                                 "🚫 **You are banned** from using this bot.", quote=True)
        return

    start_text, start_markup = await get_start_message_data(user_id)

    await message.reply_text(
        start_text,
        reply_markup=start_markup,
        reply_to_message_id=message.id,
        disable_web_page_preview=True
    )

    # Log new user only once per session
    if user_id not in registered_users:
        await send_user_info_to_log_channel(client, user, LOG_CHANNEL_ID)
        registered_users.add(user_id)

# --- Callback Handlers for Buttons ---

@bot.on_callback_query(filters.regex("^toggle_mode$"))
async def toggle_mode_callback(client: Client, callback_query: CallbackQuery):
    """Handles the mode toggle button callback for admin/pro users."""
    user = callback_query.from_user
    user_id = user.id
    message = callback_query.message # Original message to edit

    # Check permissions eligibility
    is_admin_user = is_admin(user_id, admin_ids)
    is_pro = await is_pro_user(user_id)
    is_eligible_for_advanced = is_admin_user or is_pro

    if not is_eligible_for_advanced:
        await callback_query.answer("This option is only available for Admins and Premium users.", show_alert=True)
        return

    # Toggle mode state for the user
    # Default to True (Advanced) if not set when reading current state
    current_mode_is_advanced = admin_mode_advanced.get(user_id, True)
    new_mode_is_advanced = not current_mode_is_advanced
    admin_mode_advanced[user_id] = new_mode_is_advanced # Save the new preference

    # Regenerate the start message content with the new mode
    new_text, new_markup = await get_start_message_data(user_id)

    try:
        await callback_query.edit_message_text(
            new_text,
            reply_markup=new_markup,
            disable_web_page_preview=True
        )
        mode_name_ar = "المتقدم" if new_mode_is_advanced else "العادي"
        await callback_query.answer(f"تم التبديل إلى الوضع {mode_name_ar}")
        logger.info(f"User {user_id} toggled mode to {'Advanced' if new_mode_is_advanced else 'Normal'}")
    except MessageNotModified:
        mode_name_ar = "المتقدم" if current_mode_is_advanced else "العادي" # Use current mode for message
        await callback_query.answer(f"أنت بالفعل في الوضع {mode_name_ar}")
    except FloodWait as fw:
        logger.warning(f"FloodWait during mode toggle edit: {fw.value}s")
        await callback_query.answer("Please wait a moment before switching again.", show_alert=True)
    except Exception as e:
        logger.error(f"Error toggling mode for user {user_id}: {e}", exc_info=True)
        await callback_query.answer("Error switching mode.", show_alert=True)

@bot.on_callback_query(filters.regex("^premium_info$"))
async def premium_info_callback(client: Client, callback_query: CallbackQuery):
    """Handles the 'Upgrade to Premium' button."""
    await callback_query.answer() # Acknowledge callback
    # Use bilingual text or primarily Arabic based on user preference if known
    premium_text = """
**✨ مميزات بريميوم | Premium Features ✨**

🔹 **الوضع المتقدم:** تنزيل وإعادة رفع المحتوى من القنوات **الخاصة** والعامة.
   *(Advanced Mode: Download & re-upload from **private** & public channels.)*
🔹 **التنزيل الجماعي** باستخدام نطاقات الروابط (مثال: `link/10-20`).
   *(Bulk download using link ranges (e.g., `link/10-20`).)*
🔹 تنزيل **جميع أنواع الوسائط** القابلة للتنزيل.
   *(Download **all downloadable media types**.)*
🔹 **لا يوجد حدود يومية** للمهام.
   *(**No daily task limits**.)*
🔹 تعيين دردشة **وجهة تحميل** مخصصة.
   *(Set custom **upload destination** chat.)*
🔹 استخدام **فلاتر كلمات التعليقات**.
   *(Use **caption word filters**.)*
🔹 تعيين **صور مصغرة** مخصصة.
   *(Set custom **thumbnails**.)*
🔹 تخطي أنواع الوسائط غير المرغوب فيها (**نص، صورة، ملصق**).
   *(Skip unwanted media types (**text, photo, sticker**).)*
🔹 تعيين **بادئة (prefix)** لأسماء الملفات.
   *(Set file name **prefix**.)*
🔹 تعيين **لاحقة (suffix)** للتعليقات.
   *(Set caption **suffix**.)*

**مهتم؟ | Interested?** تواصل مع المطور | Contact Developer: @X_XF8
"""
    # Add a Back button
    back_button = [[InlineKeyboardButton("⬅️ رجوع | Back", callback_data="back_to_start")]]
    await callback_query.message.reply_text(
        premium_text,
        reply_markup=InlineKeyboardMarkup(back_button),
        disable_web_page_preview=True,
        quote=True
    )

@bot.on_message(filters.command("gen_code") & filters.private)
async def generate_code_command(client: Client, message: Message):
    """Admin command to generate a premium coupon code."""
    if not is_admin(message.from_user.id, admin_ids):
        return await message.reply_text("⛔ هذا الأمر مخصص للمسؤولين فقط.\n⛔ This command is restricted to Admins.", quote=True)

    parts = message.command
    # Usage: /gen_code <duration_days> [prefix]
    if len(parts) < 2 or not parts[1].isdigit():
        await message.reply_text("⚠️ **الاستخدام:** `/gen_code <days> [prefix]`\n"
                                 "**مثال:** `/gen_code 30` (استخدام البادئة الافتراضية)\n"
                                 "**مثال:** `/gen_code 7 MYPREFIX` (استخدام بادئة مخصصة)\n"
                                 "*سيحصل المستخدم على عضوية مميزة لعدد الأيام المحدد عند استخدام الكود.*",
                                 quote=True)
        return

    try:
        duration_days = int(parts[1])
        if duration_days <= 0:
            return await message.reply_text("❌ يجب أن تكون المدة (بالأيام) رقمًا موجبًا.", quote=True)

        prefix = parts[2].strip().upper() if len(parts) > 2 else DEFAULT_CODE_PREFIX.upper()
        # Basic validation for prefix (e.g., no spaces, reasonable length)
        if not prefix or not prefix.isalnum() or len(prefix) > 20:
             prefix = DEFAULT_CODE_PREFIX.upper() # Fallback to default if invalid
             await message.reply_text(f"⚠️ بادئة غير صالحة، تم استخدام البادئة الافتراضية: `{prefix}`", quote=True)


        status_msg = await message.reply_text("⏳ جارٍ توليد كود فريد...", quote=True)
        new_code = await generate_unique_code(prefix, CODE_LENGTH)

        # Store the code in the database
        success = await digital_botz.add_coupon_code(new_code, duration_days)

        if success:
            await status_msg.edit_text(
                f"✅ **تم إنشاء الكود بنجاح!**\n\n"
                f"🎟️ **الكود | Code:** `{new_code}`\n"
                f"⏳ **المدة | Duration:** {duration_days} يوم | day(s)\n\n"
                f"*يمكن للمستخدمين شحن هذا الكود عبر زر 'شحن الكود' في قائمة /start.*",
                parse_mode=ParseMode.MARKDOWN # Use Markdown for backticks
            )
            logger.info(f"Admin {message.from_user.id} generated code '{new_code}' for {duration_days} days with prefix '{prefix}'.")
        else:
             await status_msg.edit_text("❌ فشل في حفظ الكود في قاعدة البيانات (ربما موجود بالفعل؟).", quote=True)
             logger.error(f"Failed to store generated code '{new_code}' in DB.")

    except ValueError: # Catches int conversion error
         await message.reply_text("❌ المدة يجب أن تكون رقمًا صحيحًا.", quote=True)
    except Exception as e:
        await message.reply_text(f"❌ **حدث خطأ أثناء توليد الكود:**\n`{e}`", quote=True)
        logger.exception(f"Error generating code requested by {message.from_user.id}:")
        if 'status_msg' in locals() and status_msg:
             try: await status_msg.delete()
             except Exception: pass


# ==============================================================================
# --- NEW: Settings Menu & Command Usage Callbacks ---
# ==============================================================================

@bot.on_callback_query(filters.regex("^show_settings_cats$"))
async def show_settings_categories_callback(client: Client, callback_query: CallbackQuery):
    """Displays the main settings category buttons."""
    user_id = callback_query.from_user.id
    is_admin_user = is_admin(user_id, admin_ids)
    is_pro = await is_pro_user(user_id)

    if not (is_admin_user or is_pro):
         await callback_query.answer("الإعدادات متاحة للمستخدمين المميزين والمسؤولين فقط.\nSettings are for Premium/Admin users only.", show_alert=True)
         return

    buttons = []
    # Generate buttons for each category, checking admin status for admin categories
    categories_to_show = ["basic", "customization"] # Always show these for Pro/Admin
    if is_admin_user:
        categories_to_show.extend(["admin_users", "admin_thumb"])

    for cat_key in categories_to_show:
        if cat_key in CMD_DETAILS:
            cat_name = CMD_DETAILS[cat_key]["name"]
            buttons.append([InlineKeyboardButton(cat_name, callback_data=f"show_cat_{cat_key}")])

    buttons.append([InlineKeyboardButton("⬅️ رجوع للقائمة الرئيسية | Back to Main Menu", callback_data="back_to_start")])

    settings_text = "⚙️ **إعدادات البوت | Bot Settings** ⚙️\n\nاختر فئة لعرض أوامرها:\nChoose a category to view its commands:"
    try:
        await callback_query.edit_message_text(
            settings_text,
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        await callback_query.answer()
    except MessageNotModified:
        await callback_query.answer("أنت بالفعل في قائمة الإعدادات.\nYou are already in the settings menu.")
    except FloodWait as fw:
        logger.warning(f"FloodWait showing settings menu: {fw.value}s")
        await callback_query.answer("Too many requests, please wait.", show_alert=True)
    except Exception as e:
        logger.error(f"Error showing settings menu: {e}", exc_info=True)
        await callback_query.answer("Error displaying settings.", show_alert=True)

@bot.on_callback_query(filters.regex("^show_cat_(\w+)$"))
async def show_category_commands_callback(client: Client, callback_query: CallbackQuery):
    """Displays buttons for commands within a specific category."""
    category_key = callback_query.matches[0].group(1)
    user_id = callback_query.from_user.id

    if category_key not in CMD_DETAILS:
        await callback_query.answer("فئة غير معروفة | Unknown category.", show_alert=True)
        return

    # Permission check
    is_admin_user = is_admin(user_id, admin_ids)
    if category_key.startswith("admin") and not is_admin_user:
         await callback_query.answer("هذه الفئة للمسؤولين فقط | Admins only.", show_alert=True)
         return

    category_data = CMD_DETAILS[category_key]
    category_name = category_data["name"]
    command_items = category_data["items"]

    buttons = []
    for item in command_items:
        item_id = item["id"]
        button_text = item["button_text"]
        # Callback includes category and item ID for context
        buttons.append([InlineKeyboardButton(button_text, callback_data=f"show_cmd_{category_key}_{item_id}")])

    # Add a "Back" button to return to the category list
    buttons.append([InlineKeyboardButton("⬅️ رجوع إلى الفئات | Back to Categories", callback_data="show_settings_cats")])

    list_text = f"**{category_name}**\n\nاختر أمرًا لعرض تفاصيل استخدامه:\nSelect a command to view its usage details:"
    try:
        await callback_query.edit_message_text(
            list_text,
            reply_markup=InlineKeyboardMarkup(buttons),
            disable_web_page_preview=True
        )
        await callback_query.answer()
    except MessageNotModified:
        await callback_query.answer() # Acknowledge anyway
    except FloodWait as fw:
        logger.warning(f"FloodWait showing commands for category {category_key}: {fw.value}s")
        await callback_query.answer("Too many requests, please wait.", show_alert=True)
    except Exception as e:
        logger.error(f"Error showing commands for category {category_key}: {e}", exc_info=True)
        await callback_query.answer("Error displaying commands.", show_alert=True)


@bot.on_callback_query(filters.regex("^show_cmd_(\w+)_(\w+)$"))
async def show_command_usage_callback(client: Client, callback_query: CallbackQuery):
    """Displays the usage description for a specific command."""
    category_key = callback_query.matches[0].group(1)
    item_id = callback_query.matches[0].group(2)
    user_id = callback_query.from_user.id # For potential future checks

    if category_key not in CMD_DETAILS:
        await callback_query.answer("فئة غير معروفة | Unknown category.", show_alert=True)
        return

    category_data = CMD_DETAILS[category_key]
    command_item = next((item for item in category_data["items"] if item["id"] == item_id), None)

    if not command_item:
        await callback_query.answer("أمر غير معروف | Unknown command.", show_alert=True)
        logger.warning(f"Command item ID '{item_id}' not found in category '{category_key}'.")
        return

    # Permission check (redundant but safe)
    is_admin_user = is_admin(user_id, admin_ids)
    if category_key.startswith("admin") and not is_admin_user:
         await callback_query.answer("هذا الأمر للمسؤولين فقط | Admins only.", show_alert=True)
         return

    # Format the usage details
    usage_text = command_item["usage"]
    desc_ar = command_item["desc_ar"]
    desc_en = command_item["desc_en"]
    button_text = command_item["button_text"] # Use the command name/button text as title

    details_text = (
        f"**📜 تفاصيل الأمر | Command Details: {button_text}**\n\n"
        f"**📝 الاستخدام | Usage:**\n```\n{usage_text}\n```\n\n"
        f"**🔎 الوصف | Description:**\n"
        f"🇦🇪 {desc_ar}\n"
        f"🇬🇧 *{desc_en}*\n\n"
        "*أرسل الأمر كما هو موضح أعلاه في الدردشة.*\n"
        "*Send the command as shown above in the chat.*"
    )

    # Add a "Back" button to return to the command list for that category
    buttons = [[InlineKeyboardButton(f"⬅️ رجوع إلى أوامر {category_data['name'].split('|')[0].strip()} | Back to Category", callback_data=f"show_cat_{category_key}")]]

    try:
        # Use Markdown since usage is in backticks
        await callback_query.edit_message_text(
            details_text,
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode=ParseMode.MARKDOWN,
            disable_web_page_preview=True
        )
        await callback_query.answer()
    except MessageNotModified:
        await callback_query.answer() # Acknowledge anyway
    except FloodWait as fw:
        logger.warning(f"FloodWait showing usage for {category_key}/{item_id}: {fw.value}s")
        await callback_query.answer("Too many requests, please wait.", show_alert=True)
    except Exception as e:
        logger.error(f"Error showing usage for {category_key}/{item_id}: {e}", exc_info=True)
        await callback_query.answer("Error displaying command usage.", show_alert=True)


@bot.on_callback_query(filters.regex("^back_to_start$"))
async def back_to_start_callback(client: Client, callback_query: CallbackQuery):
    """Returns to the main start message from settings or other menus."""
    user_id = callback_query.from_user.id
    start_text, start_markup = await get_start_message_data(user_id)
    try:
        await callback_query.edit_message_text(
            start_text,
            reply_markup=start_markup,
            disable_web_page_preview=True
        )
        await callback_query.answer()
    except MessageNotModified:
        await callback_query.answer()
    except FloodWait as fw:
        logger.warning(f"FloodWait going back to start: {fw.value}s")
        await callback_query.answer("Too many requests, please wait.", show_alert=True)
    except Exception as e:
        logger.error(f"Error going back to start menu: {e}", exc_info=True)
        await callback_query.answer("Error returning to main menu.", show_alert=True)


@bot.on_callback_query(filters.regex("^redeem_code_prompt$"))
async def redeem_code_prompt_callback(client: Client, callback_query: CallbackQuery):
    """Handles the 'Redeem Code' button press and prompts the user."""
    user_id = callback_query.from_user.id

    # Check Ban Status
    if await is_banned_user(user_id):
        await callback_query.answer("🚫 أنت محظور | You are banned.", show_alert=True)
        return

    # Add user to the waiting set
    user_waiting_for_code.add(user_id)
    logger.info(f"User {user_id} pressed redeem button, waiting for code.")

    await callback_query.answer("📝 يرجى إرسال كود الشحن الآن.\nPlease send the redeem code now.", show_alert=True)
    # Optionally edit the original message or delete it
    try:
        await callback_query.message.reply_text(
            "⌨️ **أدخل كود الشحن:** يرجى إرسال الكود الذي لديك في الرسالة التالية.\n"
            "⌨️ **Enter Redeem Code:** Please send the code you have in the next message.",
             reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ إلغاء | Cancel", callback_data="cancel_redeem")]]), # Add cancel
             quote=True
        )
    except Exception as e:
         logger.error(f"Error sending redeem prompt message: {e}")
         # Try sending without reply markup if error
         try:
             await callback_query.message.reply_text(
                 "⌨️ **أدخل كود الشحن:** يرجى إرسال الكود الذي لديك في الرسالة التالية.",
                 quote=True
             )
         except Exception as e2:
              logger.error(f"Error sending fallback redeem prompt message: {e2}")

# --- New handler for cancel redeem button ---
@bot.on_callback_query(filters.regex("^cancel_redeem$"))
async def cancel_redeem_callback(client: Client, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    if user_id in user_waiting_for_code:
        user_waiting_for_code.remove(user_id)
        logger.info(f"User {user_id} cancelled redeem process.")
        await callback_query.answer("✅ تم إلغاء عملية الشحن.\n✅ Redeem process cancelled.")
        try:
             # Edit the prompt message to indicate cancellation
             await callback_query.edit_message_text("❌ تم إلغاء عملية شحن الكود.\n❌ Redeem code process cancelled.")
        except Exception:
             pass # Ignore if message deleted or other errors
    else:
        await callback_query.answer("لم تكن في عملية شحن كود.\nYou were not in a redeem process.", show_alert=True)


# --- Message Handler for receiving the code ---
# IMPORTANT: Place this handler *before* the main link handler if possible,
# or ensure it has higher priority, or that the link handler ignores text
# from users in the waiting state. For simplicity, let's add a check
# within the link handler later if needed. For now, this dedicated handler:
known_commands = [
    "start", "help", "cancel", "restart", "status", "add_pro", "remove_pro",
    "set_thumb", "remove_thumb_user", "remove_thumb", "ban", "unban",
    "set_chat_id", "see_chat_id", "remove_chat_id", "filter_text_skip",
    "filter_photo_skip", "filter_sticker_skip", "add_word", "remove_word",
    "see_words", "see_thumb", "set_prefix", "set_suffix", "remove_prefix",
    "remove_suffix", "see_fix", "gen_code", "mypremium", # Added new commands
    "l" # Exclude /l itself here
]


# ابحث عن الديكوريتور في السطر 1352
@bot.on_message(waiting_for_code_filter, group=1)
async def handle_code_input(client: Client, message: Message):
    """Handles potential code input from users waiting to redeem."""
    user_id = message.from_user.id
    # Only process if the user was prompted AND sent text
    if user_id in user_waiting_for_code and message.text:
        code_input = message.text.strip().upper() # Standardize code input
        user_waiting_for_code.remove(user_id) # Process attempt complete, remove from wait list
        logger.info(f"User {user_id} submitted code: '{code_input}'")

        # Basic code format check (optional but good)
        # Example: Prefix_ alphanumeric part
        expected_format_regex = r"^[A-Z0-9]+_[A-Z0-9]+$"
        if not re.match(expected_format_regex, code_input):
            # Allow codes without prefix too maybe? Adjust regex if needed.
            # Let's be strict for now assuming Prefix_Code format
            # await message.reply_text("⚠️ صيغة الكود غير صحيحة. يرجى التأكد من إدخاله بشكل صحيح.\n"
            #                         "⚠️ Invalid code format. Please check and enter it correctly.", quote=True)
            # logger.warning(f"Code '{code_input}' rejected due to format.")
            # Allow any format for now, DB check is primary validation
            pass # Skip format check for now

        status_msg = await message.reply_text("⏳ جارٍ التحقق من الكود...", quote=True)

        # --- Redemption Logic ---
        try:
            coupon_details = await digital_botz.get_coupon_details(code_input)

            if coupon_details:
                duration_days = coupon_details['duration_days']

                # Check if user is already premium to extend duration correctly
                current_expiry = await digital_botz.get_premium_expiry(user_id)
                now = datetime.datetime.now(timezone.utc)

                if current_expiry and current_expiry > now:
                    # User is already premium, extend from current expiry
                    new_expiry_date = current_expiry + timedelta(days=duration_days)
                    expiry_msg_part = f"تم تمديد عضويتك.\nYour membership has been extended."
                else:
                    # User is not premium or expired, start from now
                    new_expiry_date = now + timedelta(days=duration_days)
                    expiry_msg_part = f"تم تفعيل عضويتك.\nYour membership has been activated."

                # 1. Mark code as used BEFORE granting premium (to prevent race conditions)
                marked_used = await digital_botz.mark_coupon_used(code_input, user_id)
                if not marked_used:
                     # This should ideally not happen if get_coupon_details worked, but double check
                     raise Exception("Failed to mark coupon as used (maybe used concurrently?).")

                # 2. Grant premium
                await digital_botz.addpremium(user_id, new_expiry_date)

                expiry_formatted = new_expiry_date.strftime('%Y-%m-%d %H:%M:%S UTC')
                await status_msg.edit_text(
                    f"🎉 **تم شحن الكود بنجاح! | Code Redeemed Successfully!** 🎉\n\n"
                    f"{expiry_msg_part}\n"
                    f"تاريخ الانتهاء الجديد | New Expiry Date: **{expiry_formatted}**\n\n"
                    f"استمتع بالميزات! | Enjoy the features!"
                )
                logger.info(f"User {user_id} successfully redeemed code '{code_input}' for {duration_days} days. New expiry: {expiry_formatted}")

                # Optional: Notify Admin(s) about redemption
                if admin_ids:
                     try:
                        user_obj = await client.get_users(user_id)
                        admin_notification = (f"🔔 **تم شحن كود | Code Redeemed**\n"
                                              f"👤 **المستخدم:** {user_obj.mention} (`{user_id}`)\n"
                                              f"🎟️ **الكود:** `{code_input}`\n"
                                              f"⏳ **المدة:** {duration_days} days\n"
                                              f"⏰ **تاريخ الشحن:** {datetime.datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
                        # Send to all admins or a specific log channel/admin
                        for admin_id in admin_ids: # Example: Send to all admins
                           try: await client.send_message(admin_id, admin_notification)
                           except Exception: pass # Ignore if can't message an admin
                     except Exception as e_admin_notify:
                          logger.error(f"Failed to send admin notification for code redemption: {e_admin_notify}")

            else:
                # Code not found or already used
                await status_msg.edit_text(
                    "❌ **الكود غير صالح أو تم استخدامه بالفعل.**\n"
                    "يرجى التحقق من الكود أو التواصل مع الدعم.\n\n"
                    "❌ **Invalid or Already Used Code.**\n"
                    "Please check the code or contact support."
                )
                logger.warning(f"User {user_id} failed to redeem code '{code_input}' (invalid or used).")

        except Exception as e:
            await status_msg.edit_text(f"❌ **حدث خطأ أثناء محاولة شحن الكود:**\n`{e}`")
            logger.exception(f"Error redeeming code '{code_input}' for user {user_id}:")
            # Important: If error occurred AFTER marking used but BEFORE granting premium,
            # manual DB correction might be needed depending on the real DB implementation.
            # The DummyDB logic is simple here.

    # If the user wasn't waiting for a code, or sent something other than text,
    # this handler does nothing, and the message might be processed by other handlers (like the link handler).



# Helper to format timedelta
def format_timedelta(delta: timedelta) -> str:
    """Formats a timedelta into a readable string (days, hours, minutes)."""
    days = delta.days
    seconds = delta.seconds
    hours, remainder = divmod(seconds, 3600)
    minutes, _ = divmod(remainder, 60)

    parts = []
    if days > 0:
        parts.append(f"{days} {'أيام' if days != 1 else 'يوم'} | day{'s' if days != 1 else ''}")
    if hours > 0:
        parts.append(f"{hours} {'ساعات' if hours != 1 else 'ساعة'} | hour{'s' if hours != 1 else ''}")
    if minutes > 0:
        parts.append(f"{minutes} {'دقائق' if minutes != 1 else 'دقيقة'} | minute{'s' if minutes != 1 else ''}")

    if not parts:
        return "أقل من دقيقة | Less than a minute"

    return ", ".join(parts)


@bot.on_message(filters.command("mypremium") & filters.private)
async def my_premium_status_command(client: Client, message: Message):
    """Shows the user's current premium status and expiry date."""
    user_id = message.from_user.id

    if not await force_subscribe(client, message, FORCE_SUBSCRIBE_CHANNEL_ID): return
    if await is_banned_user(user_id): return await message.reply_text("🚫 Banned.", quote=True)

    expiry_date = await digital_botz.get_premium_expiry(user_id)
    now = datetime.datetime.now(timezone.utc)

    if expiry_date and expiry_date > now:
        remaining_time = expiry_date - now
        expiry_formatted = expiry_date.strftime('%Y-%m-%d %H:%M:%S UTC')
        remaining_formatted = format_timedelta(remaining_time)

        await message.reply_text(
            f"✨ **حالة العضوية المميزة | Premium Status** ✨\n\n"
            f"✅ أنت حاليًا عضو مميز!\n✅ You are currently a Premium member!\n\n"
            f"🗓️ **تاريخ الانتهاء | Expires On:** `{expiry_formatted}`\n"
            f"⏳ **الوقت المتبقي | Time Remaining:** {remaining_formatted}",
            quote=True
        )
    else:
        await message.reply_text(
            "ℹ️ **حالة العضوية المميزة | Premium Status** ℹ️\n\n"
            "❌ أنت لست عضوًا مميزًا حاليًا.\n❌ You are not currently a Premium member.\n\n"
            "اشحن كودًا أو تواصل مع المطور للترقية!\n"
            "Redeem a code or contact the developer to upgrade!",
            reply_markup=InlineKeyboardMarkup( # Provide quick buttons
                 [
                     [InlineKeyboardButton("🎁 شحن الكود | Redeem Code", callback_data="redeem_code_prompt")],
                     [InlineKeyboardButton("⭐ معلومات البريميوم | Premium Info", callback_data="premium_info")]
                 ]
             ),
            quote=True
        )

# ==============================================================================
# --- Other Command Handlers (Keep existing handlers) ---
# ==============================================================================

@bot.on_message(filters.command("cancel") & filters.private)
async def cancel_operations_command(client: Client, message: Message):
    """Cancels ongoing bulk download/upload for the current chat via command."""
    user_id = message.from_user.id
    chat_id = message.chat.id # Use the chat ID from the user's message

    if not await force_subscribe(client, message, FORCE_SUBSCRIBE_CHANNEL_ID): return
    if await is_banned_user(user_id): return await message.reply_text("🚫 Banned.", quote=True)

    cancelled = await cancel_chat_operations(chat_id, user_id)

    if cancelled:
        await message.reply_text("✅ **تم طلب الإلغاء | Cancellation Requested:**\n"
                                 "تم طلب إيقاف العملية الجماعية الجارية في هذه الدردشة. قد يستغرق الأمر لحظة حتى ينتهي الملف الحالي.\n"
                                 "*(Stopping the current bulk operation in this chat. It might take a moment for the current file to finish.)*", quote=True)
    else:
        await message.reply_text("ℹ️ لم يتم العثور على عملية تنزيل أو تحميل جماعية نشطة في هذه الدردشة لإلغائها.\n"
                                 "*(No active bulk download/upload operation found in this chat to cancel.)*", quote=True)

@bot.on_callback_query(filters.regex(r"^cancel_op_(\d+)$"))
async def cancel_operations_button(client: Client, callback_query: CallbackQuery):
    """Handles the cancel button press from the progress message."""
    user_id = callback_query.from_user.id
    # Chat ID is embedded in the callback data - this is the chat where the USER ran the command
    match = callback_query.matches[0] # Access the Match object
    try:
        # Extract chat_id using group index 1
        chat_id_str = match.group(1)
        if not chat_id_str:
             raise ValueError("Chat ID not found in callback data group.")
        chat_id_to_cancel = int(chat_id_str)
    except (IndexError, ValueError, AttributeError) as e:
         logger.error(f"Could not extract chat_id from cancel callback data: {callback_query.data}. Error: {e}")
         await callback_query.answer("Error processing cancellation request.", show_alert=True)
         return

    logger.info(f"Cancellation requested via button by user {user_id} for operations initiated in chat {chat_id_to_cancel}")

    cancelled = await cancel_chat_operations(chat_id_to_cancel, user_id)

    if cancelled:
        await callback_query.answer("تم طلب إلغاء العملية... | Cancellation requested...")
        try:
            # Edit the message to indicate cancellation requested
            # Get current text safely
            current_text = getattr(callback_query.message, 'text', '') or "" # Handle potential None
            # Prevent adding multiple cancellation messages
            cancel_notice = "-- ⏳ **جارٍ الإلغاء... | Cancelling...** --"
            if cancel_notice not in current_text and "Cancelled" not in current_text and "إلغاء" not in current_text: # Added Arabic check too
                 await callback_query.edit_message_text(
                     current_text + f"\n\n{cancel_notice}",
                     reply_markup=None # Remove button after cancel confirmed
                 )
        except MessageNotModified:
             pass # Fine if not modified
        except FloodWait as fw:
             logger.warning(f"FloodWait editing cancel status message: {fw.value}s")
             await asyncio.sleep(fw.value + 0.5)
        except MessageIdInvalid:
            logger.warning(f"Cancel button's message {callback_query.message.id} already deleted.")
        except Exception as e:
            logger.error(f"Error editing cancellation status message {callback_query.message.id}: {e}")
    else:
        await callback_query.answer("لم يتم العثور على عملية نشطة للإلغاء. | No active operation found to cancel.", show_alert=True)
        # Optionally remove the button if no operation found
        try:
            await callback_query.edit_message_reply_markup(reply_markup=None)
        except Exception:
             pass

async def cancel_chat_operations(chat_id: int, user_id: int) -> bool:
    """Central cancellation logic for a specific chat."""
    global active_operations, operation_status_map, last_edit_times # Added last_edit_times

    if active_operations.get(chat_id):
        active_operations[chat_id] = False # Set flag to false
        logger.info(f"User {user_id} requested operation cancel in chat {chat_id}. Flag set.")

        # Request Pyrogram to stop ongoing transfer if possible
        try:
             await bot.stop_transmission()
             logger.info(f"Requested stop_transmission for cancelled operation in chat {chat_id}")
        except Exception as e_stop:
             logger.error(f"Error calling stop_transmission during cancel: {e_stop}")

        # Clean up mappings associated with this chat's operations
        keys_to_remove = [msg_id for msg_id, op_chat_id in operation_status_map.items() if op_chat_id == chat_id]
        for key in keys_to_remove:
            operation_status_map.pop(key, None)
            last_edit_times.pop(key, None) # Clean up edit time as well
            logger.debug(f"Removed status map/time tracking for message {key} in cancelled chat {chat_id}")
        return True
    else:
        logger.info(f"No active operation found for chat {chat_id} when cancel requested by {user_id}.")
        return False

@bot.on_message(filters.command("restart") & filters.private)
async def restart_bot(client: Client, message: Message):
    """Restarts the bot (Admin only)."""
    if not is_admin(message.from_user.id, admin_ids):
        return await message.reply_text("⛔ هذا الأمر مخصص للمسؤولين فقط.\n⛔ This command is restricted to Admins.", quote=True)

    await message.reply_text("🔄 **إعادة تشغيل البوت... | Restarting Bot...**\n"
                             "يرجى الانتظار لحظة. | Please wait a moment.", quote=True)
    logger.info(f"Restart initiated by admin {message.from_user.id}")

    # Cleanly stop the client before restarting
    # Flush logs if needed
    logging.shutdown()
    # Stop pyrogram client
    if client.is_initialized:
        try:
             await client.stop()
        except ConnectionError:
             logger.warning("ConnectionError during stop(), proceeding with restart.")
        except Exception as e:
             logger.error(f"Error stopping client during restart: {e}")

    # Use os.execl for a clean restart in the same process space
    # This replaces the current process with a new one running the same script
    try:
        os.execl(sys.executable, sys.executable, *sys.argv)
    except Exception as e:
        logger.critical(f"Failed to execute restart: {e}")
        # Fallback: Exit and let docker/systemd handle restart
        sys.exit(1) # Exit with error code

@bot.on_message(filters.command("status") & filters.private)
async def status_command(client: Client, message: Message):
    """Shows bot status (detailed for admin/pro, basic otherwise)."""
    user_id = message.from_user.id
    if not await force_subscribe(client, message, FORCE_SUBSCRIBE_CHANNEL_ID): return
    if await is_banned_user(user_id): return await message.reply_text("🚫 Banned.", quote=True)

    uptime = get_uptime(bot_start_time)
    # Task status: count active operations
    active_ops = sum(1 for status in active_operations.values() if status)
    task_status = f"يعالج {active_ops} عملية | Processing {active_ops} operation(s)" if active_ops > 0 else "خامل | Idle"
    if current_task: # Add detail if available
        task_status += f"\n  الحالي | Current: `{current_task}`"

    # Basic status for everyone
    status_text = (
        f"**📊 حالة البوت | Bot Status**\n\n"
        f"⏳ **مدة التشغيل | Uptime:** {uptime}\n"
        f"⚙️ **الحالة الحالية | Current Status:** {task_status}\n"
    )

    # Add details for Admin/Pro
    if await is_admin_or_pro(user_id, admin_ids):
        num_pro_users = await digital_botz.total_premium_users_count()
        status_text += f"👑 **المسؤولون | Admins:** {len(admin_ids)}\n" # Show total configured admins
        status_text += f"⭐ **المستخدمون المميزون | Pro Users:** {num_pro_users}\n"

        # System stats (optional, requires psutil)
        try:
            import psutil
            cpu_usage = psutil.cpu_percent()
            ram = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            sys_info = (
                f"\n💻 **معلومات النظام | System Info**\n"
                f"  **المعالج | CPU:** {cpu_usage}%\n"
                f"  **الذاكرة | RAM:** {get_readable_size(ram.used)} / {get_readable_size(ram.total)} ({ram.percent}%)\n"
                f"  **القرص | Disk:** {get_readable_size(disk.used)} / {get_readable_size(disk.total)} ({disk.percent}%)"
            )
            status_text += sys_info
        except ImportError:
            status_text += "\n📊 System Stats: `psutil` not installed."
        except Exception as e:
            status_text += f"\n📊 System Stats: Error ({e})"

    await message.reply_text(status_text, quote=True)

# --- Admin/Pro Commands (Keep existing handlers) ---

@bot.on_message(filters.command("add_pro") & filters.private)
async def add_pro_user(client: Client, message: Message):
    if not is_admin(message.from_user.id, admin_ids):
        return await message.reply_text("⛔ Admins only.", quote=True)

    parts = message.command
    if len(parts) != 3:
        await message.reply_text("⚠️ **Usage:** `/add_pro <user_id> <duration>`\n"
                                 "**Example:** `/add_pro 123456789 7days` | `/add_pro 987654321 1month`\n"
                                 "**Units:** `hours`, `days`, `weeks`, `months`, `years`", quote=True)
        return

    try:
        target_user_id = int(parts[1])
        duration_str = parts[2].lower()
        expiry_date = None
        # Use datetime.datetime and datetime.timezone
        now = datetime.datetime.now(timezone.utc) # Use UTC

        match = re.match(r"(\d+)\s*(\w+)", duration_str)
        if not match:
            raise ValueError("Invalid duration format.")

        num = int(match.group(1))
        unit = match.group(2).rstrip('s') # Remove trailing 's'

        if unit == "hour": expiry_date = now + timedelta(hours=num)
        elif unit == "day": expiry_date = now + timedelta(days=num)
        elif unit == "week": expiry_date = now + timedelta(weeks=num)
        elif unit == "month": expiry_date = now + timedelta(days=num * 30) # Approx 30 days/month
        elif unit == "year": expiry_date = now + timedelta(days=num * 365) # Approx 365 days/year
        else:
            await message.reply_text("❌ **Invalid duration unit!** Use `hours`, `days`, `weeks`, `months`, `years`.", quote=True)
            return

        if expiry_date:
            await digital_botz.addpremium(target_user_id, expiry_date) # Add/Update premium in DB

            expiry_formatted = expiry_date.strftime('%Y-%m-%d %H:%M:%S UTC')

            # Confirm to admin
            await message.reply_text(
                f"✅ **Premium Added!**\n\n"
                f"- **User ID:** `{target_user_id}`\n"
                f"- **Expires On:** `{expiry_formatted}`",
                quote=True
            )
            logger.info(f"Admin {message.from_user.id} added premium for user {target_user_id} until {expiry_formatted}")

            # Notify the target user
            try:
                await client.send_message(
                    target_user_id,
                    f"🎉 **تهانينا! أنت الآن مستخدم مميز! | Congratulations! You are now a Premium User!** 🎉\n\n"
                    f"وصولك المميز صالح حتى | Your premium access is valid until: **{expiry_formatted}**\n\n"
                    f"استمتع بالميزات المحسنة! استخدم /start لرؤية الأوامر المتاحة.\n"
                    f"(Enjoy the enhanced features! Use /start to see available commands.)"
                )
            except Exception as e:
                logger.error(f"Failed to send premium notification to user {target_user_id}: {e}")
                await message.reply_text(f"⚠️ Could not notify user {target_user_id}, but premium was added.", quote=True)

    except ValueError:
        await message.reply_text("❌ **Invalid User ID or Duration.** Please check the format.", quote=True)
    except Exception as e:
        await message.reply_text(f"❌ **Error adding premium:**\n`{e}`", quote=True)
        logger.error(f"Error in /add_pro by {message.from_user.id}: {e}", exc_info=True)

@bot.on_message(filters.command("remove_pro") & filters.private)
async def remove_pro_user(client: Client, message: Message):
    if not is_admin(message.from_user.id, admin_ids):
        return await message.reply_text("⛔ Admins only.", quote=True)

    parts = message.command
    if len(parts) != 2:
        await message.reply_text("⚠️ **Usage:** `/remove_pro <user_id>`", quote=True)
        return

    try:
        target_user_id = int(parts[1])
        await digital_botz.remove_premium(target_user_id) # Remove premium from DB

        await message.reply_text(f"✅ Premium access removed for User ID: `{target_user_id}`.", quote=True)
        logger.info(f"Admin {message.from_user.id} removed premium for user {target_user_id}")

        # Notify the target user
        try:
            await client.send_message(target_user_id, "ℹ️ تم إزالة وصولك المميز بواسطة المسؤول.\n"
                                                      "ℹ️ Your Premium access has been removed by an administrator.")
        except Exception as e:
            logger.error(f"Failed to send premium removal notification to user {target_user_id}: {e}")
            await message.reply_text(f"⚠️ Could not notify user {target_user_id}, but premium was removed.", quote=True)

    except ValueError:
        await message.reply_text("❌ **Invalid User ID.** Please provide a valid integer ID.", quote=True)
    except Exception as e:
        await message.reply_text(f"❌ **Error removing premium:**\n`{e}`", quote=True)
        logger.error(f"Error in /remove_pro by {message.from_user.id}: {e}", exc_info=True)

@bot.on_message(filters.command("remove_thumb") & filters.private)
async def remove_thumbnail(client: Client, message: Message):
    """Removes the user's own custom thumbnail."""
    user_id = message.from_user.id
    if not await force_subscribe(client, message, FORCE_SUBSCRIBE_CHANNEL_ID): return
    if await is_banned_user(user_id): return await message.reply_text("🚫 Banned.", quote=True)

    if not await is_admin_or_pro(user_id, admin_ids):
        return await message.reply_text("⛔ This command requires Admin or Premium status.", quote=True)

    try:
        # Get current path to potentially delete file
        current_thumb = await digital_botz.get_thumbnail(user_id)
        # Remove from DB (pass None to indicate removal)
        await digital_botz.set_thumbnail(user_id, None)

        # Delete the physical file if it exists and isn't the default
        if current_thumb and current_thumb != DEFAULT_THUMBNAIL and os.path.exists(current_thumb):
            try:
                os.remove(current_thumb)
                logger.info(f"Deleted thumbnail file {current_thumb} for user {user_id}")
            except OSError as e:
                logger.error(f"Error removing user thumb file {current_thumb}: {e}")

        await message.reply_text("✅ تم إزالة صورتك المصغرة المخصصة. سيتم استخدام الافتراضية.\n"
                                 "✅ Your custom thumbnail has been removed. Using default.", quote=True)
        logger.info(f"User {user_id} removed their custom thumbnail.")
    except Exception as e:
         await message.reply_text(f"❌ **Error removing thumbnail:**\n`{e}`", quote=True)
         logger.error(f"Error in /remove_thumb for user {user_id}: {e}", exc_info=True)

@bot.on_message(filters.command("set_thumb") & filters.private)
async def set_thumbnail_admin_cmd(client: Client, message: Message):
    """Admin command to set thumb for *another* user. Initiates the process."""
    if not is_admin(message.from_user.id, admin_ids):
        # Allow admin/pro to use /set_thumb without args to indicate they want to set their own
        if await is_admin_or_pro(message.from_user.id, admin_ids) and len(message.command) == 1:
             await message.reply_text("✅ حسناً. الآن، يرجى الرد بالصورة التي تريد تعيينها كصورة مصغرة *لنفسك*.\n"
                                      "✅ OK. Now, please reply with the image you want to set as *your own* thumbnail.", quote=True)
             # No state needed for self-setting
             return
        else: # Not admin or trying to set for someone else without being admin
             return await message.reply_text("⛔ Admins only for setting others' thumbs. Use `/set_thumb` then reply photo to set your own (if eligible).", quote=True)


    # Admin setting for another user
    parts = message.command
    if len(parts) != 2:
        await message.reply_text("⚠️ **Admin Usage:** `/set_thumb <user_id>`\nThen reply with the thumbnail image.", quote=True)
        return

    try:
        target_user_id = int(parts[1])
        if target_user_id <= 0: # Basic validation
            raise ValueError("User ID must be positive.")

        # Store state: which admin is setting thumb for which target user, and timestamp
        thumb_state[message.from_user.id] = {'target_user': target_user_id, 'time': time.time()}
        await message.reply_text(f"✅ حسناً. الآن، يرجى الرد بالصورة التي تريد تعيينها كصورة مصغرة للمستخدم `{target_user_id}`.\n"
                                 f"✅ OK. Now, please reply with the image you want to set as the thumbnail for User ID `{target_user_id}`.", quote=True)
        logger.info(f"Admin {message.from_user.id} initiated thumbnail set for user {target_user_id}")
    except ValueError:
        await message.reply_text("❌ **Invalid User ID.** Please provide a valid positive integer ID.", quote=True)
    except Exception as e:
        await message.reply_text(f"❌ **Error:**\n`{e}`", quote=True)
        logger.error(f"Error in /set_thumb setup by {message.from_user.id}: {e}", exc_info=True)

@bot.on_message(filters.command("remove_thumb_user") & filters.private)
async def remove_thumbnail_admin_cmd(client: Client, message: Message):
    """Admin command to remove thumb for *another* user."""
    if not is_admin(message.from_user.id, admin_ids):
        return await message.reply_text("⛔ Admins only.", quote=True)

    parts = message.command
    if len(parts) != 2:
        await message.reply_text("⚠️ **Usage:** `/remove_thumb_user <user_id>`", quote=True)
        return

    try:
        target_user_id = int(parts[1])
        # Get current path to potentially delete file
        current_thumb = await digital_botz.get_thumbnail(target_user_id)
        # Remove from DB
        await digital_botz.set_thumbnail(target_user_id, None)

        # Delete the physical file
        if current_thumb and current_thumb != DEFAULT_THUMBNAIL and os.path.exists(current_thumb):
            try:
                os.remove(current_thumb)
                logger.info(f"Admin {message.from_user.id} deleted thumbnail file {current_thumb} for user {target_user_id}")
            except OSError as e:
                logger.error(f"Error removing user thumb file {current_thumb}: {e}")

        await message.reply_text(f"✅ تم إزالة الصورة المصغرة المخصصة للمستخدم `{target_user_id}`.\n"
                                 f"✅ Custom thumbnail removed for User ID `{target_user_id}`.", quote=True)
        logger.info(f"Admin {message.from_user.id} removed custom thumbnail for user {target_user_id}")

        # Notify the target user
        try:
            await client.send_message(target_user_id, "ℹ️ قام المسؤول بإزالة صورتك المصغرة المخصصة.\n"
                                                      "ℹ️ Your custom thumbnail has been removed by an administrator.")
        except Exception as e:
            logger.error(f"Failed to send thumb removal notification to user {target_user_id}: {e}")

    except ValueError:
        await message.reply_text("❌ **Invalid User ID.** Please provide a valid integer ID.", quote=True)
    except Exception as e:
        await message.reply_text(f"❌ **Error removing thumbnail:**\n`{e}`", quote=True)
        logger.error(f"Error in /remove_thumb_user by {message.from_user.id}: {e}", exc_info=True)

@bot.on_message(filters.command("ban") & filters.private)
async def ban_user_command(client: Client, message: Message):
    if not is_admin(message.from_user.id, admin_ids):
        return await message.reply_text("⛔ Admins only.", quote=True)

    parts = message.command
    if len(parts) != 2:
        await message.reply_text("⚠️ **Usage:** `/ban <user_id>`", quote=True)
        return

    try:
        user_id_to_ban = int(parts[1])
        if user_id_to_ban == message.from_user.id:
             return await message.reply_text("❌ لا يمكنك حظر نفسك. | You cannot ban yourself.", quote=True)
        if is_admin(user_id_to_ban, admin_ids): # Prevent banning other admins
            return await message.reply_text("❌ لا يمكنك حظر مسؤول آخر. | You cannot ban another Admin.", quote=True)

        await digital_botz.ban_user(user_id_to_ban)
        await message.reply_text(f"✅ تم **حظر** المستخدم `{user_id_to_ban}`.\n"
                                 f"✅ User ID `{user_id_to_ban}` has been **banned**.", quote=True)
        logger.info(f"Admin {message.from_user.id} banned user {user_id_to_ban}")

        # Notify the banned user
        try:
            await client.send_message(user_id_to_ban, "🚫 لقد تم **حظرك** من استخدام هذا البوت بواسطة المسؤول.\n"
                                                      "🚫 You have been **banned** from using this bot by an administrator.")
        except Exception as e:
            logger.error(f"Failed to send ban notification to user {user_id_to_ban}: {e}")

    except ValueError:
        await message.reply_text("❌ **Invalid User ID.** Please provide a valid integer ID.", quote=True)
    except Exception as e:
        await message.reply_text(f"❌ **Error banning user:**\n`{e}`", quote=True)
        logger.error(f"Error in /ban by {message.from_user.id}: {e}", exc_info=True)

@bot.on_message(filters.command("unban") & filters.private)
async def unban_user_command(client: Client, message: Message):
    if not is_admin(message.from_user.id, admin_ids):
        return await message.reply_text("⛔ Admins only.", quote=True)

    parts = message.command
    if len(parts) != 2:
        await message.reply_text("⚠️ **Usage:** `/unban <user_id>`", quote=True)
        return

    try:
        user_id_to_unban = int(parts[1])
        await digital_botz.unban_user(user_id_to_unban)
        await message.reply_text(f"✅ تم **فك حظر** المستخدم `{user_id_to_unban}`.\n"
                                 f"✅ User ID `{user_id_to_unban}` has been **unbanned**.", quote=True)
        logger.info(f"Admin {message.from_user.id} unbanned user {user_id_to_unban}")

        # Notify the unbanned user
        try:
            await client.send_message(user_id_to_unban, "🎉 تم **فك حظرك** بواسطة المسؤول. يمكنك استخدام البوت مرة أخرى.\n"
                                                        "🎉 You have been **unbanned** by an administrator. You can use the bot again.")
        except Exception as e:
            logger.error(f"Failed to send unban notification to user {user_id_to_unban}: {e}")

    except ValueError:
        await message.reply_text("❌ **Invalid User ID.** Please provide a valid integer ID.", quote=True)
    except Exception as e:
        await message.reply_text(f"❌ **Error unbanning user:**\n`{e}`", quote=True)
        logger.error(f"Error in /unban by {message.from_user.id}: {e}", exc_info=True)

@bot.on_message(filters.command("set_chat_id") & filters.private)
async def set_upload_chat_id_cmd(client: Client, message: Message):
    """Sets a custom chat ID for uploads for the user."""
    user_id = message.from_user.id
    if not await force_subscribe(client, message, FORCE_SUBSCRIBE_CHANNEL_ID): return
    if await is_banned_user(user_id): return await message.reply_text("🚫 Banned.", quote=True)

    if not await is_admin_or_pro(user_id, admin_ids):
        return await message.reply_text("⛔ This command requires Admin or Premium status.", quote=True)

    parts = message.command
    if len(parts) != 2:
        await message.reply_text("⚠️ **Usage:** `/set_chat_id <chat_id>`\n"
                                 "(Get ID from group/channel, e.g., via @userinfobot. Must start with `-100...` for channels/supergroups).", quote=True)
        return

    try:
        chat_id_str = parts[1]
        # Basic validation: check if it looks like a group/channel ID
        if not chat_id_str.startswith("-") or not chat_id_str.replace('-', '').isdigit():
            await message.reply_text("❌ **Invalid Chat ID:** Public group/channel IDs must be negative integers (usually start with `-100`).", quote=True)
            return
        chat_id = int(chat_id_str)

        # Optional: Check if bot is actually in the chat
        try:
            chat_info = await client.get_chat(chat_id)
            logger.info(f"Verified bot is in target chat: {chat_info.title} ({chat_id})")
            # Could add check for send message permissions here if needed
            # Check if bot can send messages
            member = await client.get_chat_member(chat_id, "me") # Check own permissions
            can_send = False
            if member.status == ChatMemberStatus.ADMINISTRATOR:
                 # Check both post_messages (channels) and send_messages (groups)
                 can_send = getattr(member.privileges, 'can_post_messages', False) or \
                            getattr(member.privileges, 'can_send_messages', False)
            elif member.status == ChatMemberStatus.MEMBER:
                 # Check chat default permissions if bot is just a member
                 if hasattr(chat_info, 'permissions') and chat_info.permissions:
                      can_send = chat_info.permissions.can_send_messages

            if not can_send:
                 raise ChatAdminRequired("Bot lacks permission to send/post messages in the target chat.")

        except ChatAdminRequired as e:
             await message.reply_text(f"❌ **Permission Denied:** The bot needs permission to send/post messages in the target chat `{chat_id}`. Please grant permission and try again.\nError: `{e}`", quote=True)
             return # Stop if permission is missing
        except UserNotParticipant:
             await message.reply_text(f"❌ **Bot Not Found:** The bot is not a member of the target chat `{chat_id}`. Please add the bot first.", quote=True)
             return
        except Exception as e:
             # Warn but allow setting it anyway for other errors (e.g., chat not found yet)
             await message.reply_text(f"⚠️ **Warning:** Could not fully verify chat `{chat_id}` or permissions. Ensure the bot is a member with send/post message permissions.\nError: `{e}`", quote=True)

        upload_chat_id[user_id] = chat_id # Store chat ID for the user
        await message.reply_text(f"✅ تم تعيين وجهة التحميل إلى `{chat_id}`.\n"
                                 f"✅ Upload destination set to Chat ID: `{chat_id}`.", quote=True)
        logger.info(f"User {user_id} set upload chat ID to {chat_id}")

    except ValueError:
        await message.reply_text("❌ **Invalid Chat ID.** Must be an integer (usually negative).", quote=True)
    except Exception as e:
         await message.reply_text(f"❌ **Error setting chat ID:**\n`{e}`", quote=True)
         logger.error(f"Error in /set_chat_id by {user_id}: {e}", exc_info=True)

@bot.on_message(filters.command("see_chat_id") & filters.private)
async def see_upload_chat_id_cmd(client: Client, message: Message):
    """Shows the user's current custom upload chat ID."""
    user_id = message.from_user.id
    if not await force_subscribe(client, message, FORCE_SUBSCRIBE_CHANNEL_ID): return
    if await is_banned_user(user_id): return await message.reply_text("🚫 Banned.", quote=True)
    if not await is_admin_or_pro(user_id, admin_ids): # Check permission
        return await message.reply_text("⛔ This command requires Admin or Premium status.", quote=True)

    chat_id = upload_chat_id.get(user_id)
    if chat_id:
        await message.reply_text(f"ℹ️ وجهة التحميل المخصصة الحالية هي: `{chat_id}`.\n"
                                 f"ℹ️ Your current custom upload destination is Chat ID: `{chat_id}`.", quote=True)
    else:
        await message.reply_text("ℹ️ لم تقم بتعيين وجهة تحميل مخصصة. سيتم إرسال الملفات إلى هذه الدردشة.\n"
                                 "ℹ️ You haven't set a custom upload destination. Files will be sent back to this chat.", quote=True)

@bot.on_message(filters.command("remove_chat_id") & filters.private)
async def remove_upload_chat_id_cmd(client: Client, message: Message):
    """Removes the user's custom upload chat ID."""
    user_id = message.from_user.id
    if not await force_subscribe(client, message, FORCE_SUBSCRIBE_CHANNEL_ID): return
    if await is_banned_user(user_id): return await message.reply_text("🚫 Banned.", quote=True)
    if not await is_admin_or_pro(user_id, admin_ids): # Check permission
        return await message.reply_text("⛔ This command requires Admin or Premium status.", quote=True)

    if upload_chat_id.pop(user_id, None): # Remove if exists, default None avoids KeyError
        await message.reply_text("✅ تم إزالة وجهة التحميل المخصصة. سيتم إرسال الملفات الآن إلى هذه الدردشة.\n"
                                 "✅ Custom upload destination removed. Files will now be sent back to this chat.", quote=True)
        logger.info(f"User {user_id} removed their custom upload chat ID.")
    else:
        await message.reply_text("ℹ️ ليس لديك وجهة تحميل مخصصة لإزالتها.\n"
                                 "ℹ️ You don't have a custom upload destination set.", quote=True)

@bot.on_message(filters.command(["filter_text_skip", "filter_photo_skip", "filter_sticker_skip"]) & filters.private)
async def filter_skip_cmd(client: Client, message: Message):
    """Handles text, photo, and sticker skip filter commands."""
    user_id = message.from_user.id
    if not await force_subscribe(client, message, FORCE_SUBSCRIBE_CHANNEL_ID): return
    if await is_banned_user(user_id): return await message.reply_text("🚫 Banned.", quote=True)

    if not await is_admin_or_pro(user_id, admin_ids):
        return await message.reply_text("⛔ This command requires Admin or Premium status.", quote=True)

    command = message.command[0]
    filter_dict = None
    filter_name_ar = ""
    filter_name_en = ""

    if command == "filter_text_skip":
        filter_dict = skip_text_filter
        filter_name_ar = "الرسائل النصية"
        filter_name_en = "Text Message"
    elif command == "filter_photo_skip":
        filter_dict = skip_photo_filter
        filter_name_ar = "رسائل الصور"
        filter_name_en = "Photo Message"
    elif command == "filter_sticker_skip":
        filter_dict = skip_sticker_filter
        filter_name_ar = "رسائل الملصقات"
        filter_name_en = "Sticker Message"
    else: # Should not happen due to filters.command
        return

    parts = message.command
    if len(parts) != 2 or parts[1].lower() not in ['on', 'off']:
        current_state = filter_dict.get(user_id, 'off') # Default to off if not set
        state_ar = "مُفعّل" if current_state == 'on' else "مُعطّل"
        await message.reply_text(f"⚠️ **Usage:** `/{command} on|off`\n"
                                 f"الحالة الحالية لتخطي {filter_name_ar} هي: **{state_ar.upper()}**\n"
                                 f"Current state for {filter_name_en} skipping is: **{current_state.upper()}**", quote=True)
        return

    state = parts[1].lower()
    filter_dict[user_id] = state
    state_ar = "مُفعّل" if state == 'on' else "مُعطّل"
    await message.reply_text(f"✅ **تخطي {filter_name_ar}** في المعالجة الجماعية الآن **{state_ar.upper()}** لك.\n"
                             f"✅ **{filter_name_en} Skipping** in bulk processing is now **{state.upper()}** for you.", quote=True)
    logger.info(f"User {user_id} set {filter_name_en.lower()} skip filter to {state}")

@bot.on_message(filters.command("add_word") & filters.private)
async def add_word_filter_cmd(client: Client, message: Message):
    """Adds a word/phrase to the user's caption filter list."""
    user_id = message.from_user.id
    if not await force_subscribe(client, message, FORCE_SUBSCRIBE_CHANNEL_ID): return # Corrected call
    if await is_banned_user(user_id): return await message.reply_text("🚫 Banned.", quote=True)

    if not await is_admin_or_pro(user_id, admin_ids):
        return await message.reply_text("⛔ This command requires Admin or Premium status.", quote=True)

    if len(message.command) < 2:
        await message.reply_text("⚠️ **Usage:** `/add_word <word_or_phrase_to_remove>`\n(This word/phrase will be removed from captions in advanced mode).", quote=True)
        return

    word_to_add = message.text.split(None, 1)[1].strip() # Get everything after /add_word

    if not word_to_add:
        return await message.reply_text("⚠️ Please provide a word or phrase to add.", quote=True)

    # Initialize user's filter list if not present in memory
    if user_id not in caption_word_filters:
        caption_word_filters[user_id] = []

    # Check if already present (case-sensitive check, maybe make insensitive?)
    if word_to_add not in caption_word_filters[user_id]:
        caption_word_filters[user_id].append(word_to_add)
        # --- Persist to Database ---
        try:
            # Ensure digital_botz object actually has this method implemented in database.py
            await digital_botz.add_caption_filter_word(user_id, word_to_add)
            await message.reply_text(f"✅ تمت إضافة الكلمة/العبارة '`{word_to_add}`' إلى فلتر التعليقات.\n"
                                     f"✅ Word/Phrase '`{word_to_add}`' added to your caption filter.", quote=True)
            logger.info(f"User {user_id} added caption filter word: '{word_to_add}'")
        except AttributeError as e:
            # Handle case where method is missing in the database object
            caption_word_filters[user_id].remove(word_to_add) # Rollback in-memory change
            logger.error(f"Database Error: Method 'add_caption_filter_word' not found. Filter not saved. Error: {e}", exc_info=True)
            await message.reply_text(f"❌ Failed to save filter word to database. Error: `Database object missing method 'add_caption_filter_word'`", quote=True)
        except Exception as e:
            caption_word_filters[user_id].remove(word_to_add) # Rollback in-memory change on other DB error
            logger.error(f"Failed to add caption filter word '{word_to_add}' to DB for user {user_id}: {e}", exc_info=True)
            await message.reply_text(f"❌ Failed to save filter word to database. Error: `{e}`", quote=True)
    else:
        await message.reply_text(f"ℹ️ الكلمة/العبارة '`{word_to_add}`' موجودة بالفعل في فلتر التعليقات.\n"
                                 f"ℹ️ Word/Phrase '`{word_to_add}`' is already in your caption filter.", quote=True)

@bot.on_message(filters.command("remove_word") & filters.private)
async def remove_word_filter_cmd(client: Client, message: Message):
    """Removes a word/phrase from the user's caption filter list."""
    user_id = message.from_user.id
    if not await force_subscribe(client, message, FORCE_SUBSCRIBE_CHANNEL_ID): return
    if await is_banned_user(user_id): return await message.reply_text("🚫 Banned.", quote=True)

    if not await is_admin_or_pro(user_id, admin_ids):
        return await message.reply_text("⛔ This command requires Admin or Premium status.", quote=True)

    if len(message.command) < 2:
        await message.reply_text("⚠️ **Usage:** `/remove_word <word_or_phrase_to_remove>`", quote=True)
        return

    word_to_remove = message.text.split(None, 1)[1].strip() # Get text after command

    if not word_to_remove:
         return await message.reply_text("⚠️ Please provide a word or phrase to remove.", quote=True)

    user_filters = caption_word_filters.get(user_id, [])

    if word_to_remove in user_filters:
        original_filters = list(user_filters) # Copy for potential rollback
        user_filters.remove(word_to_remove)
        caption_word_filters[user_id] = user_filters # Update in-memory list
        # --- Persist to Database ---
        try:
            # Ensure digital_botz object actually has this method implemented in database.py
            await digital_botz.remove_caption_filter_word(user_id, word_to_remove)
            await message.reply_text(f"✅ تم إزالة الكلمة/العبارة '`{word_to_remove}`' من فلتر التعليقات.\n"
                                     f"✅ Word/Phrase '`{word_to_remove}`' removed from your caption filter.", quote=True)
            logger.info(f"User {user_id} removed caption filter word: '{word_to_remove}'")
        except AttributeError as e:
             # Handle case where method is missing in the database object
             caption_word_filters[user_id] = original_filters # Rollback in-memory change
             logger.error(f"Database Error: Method 'remove_caption_filter_word' not found. Filter not removed from DB. Error: {e}", exc_info=True)
             await message.reply_text(f"❌ Failed to remove filter word from database. Error: `Database object missing method 'remove_caption_filter_word'`", quote=True)
        except Exception as e:
             caption_word_filters[user_id] = original_filters # Rollback in-memory change
             logger.error(f"Failed to remove caption filter word '{word_to_remove}' from DB for user {user_id}: {e}", exc_info=True)
             await message.reply_text(f"❌ Failed to remove filter word from database. Error: `{e}`", quote=True)
    else:
        await message.reply_text(f"ℹ️ الكلمة/العبارة '`{word_to_remove}`' غير موجودة في فلتر التعليقات.\n"
                                 f"ℹ️ Word/Phrase '`{word_to_remove}`' is not found in your caption filter.", quote=True)

@bot.on_message(filters.command("see_words") & filters.private)
async def see_words_filter_cmd(client: Client, message: Message):
    """Displays the user's current caption filter words."""
    user_id = message.from_user.id
    if not await force_subscribe(client, message, FORCE_SUBSCRIBE_CHANNEL_ID): return
    if await is_banned_user(user_id): return await message.reply_text("🚫 Banned.", quote=True)

    # Allow anyone (admin/pro) to see their own filters if they have access to the feature
    if not await is_admin_or_pro(user_id, admin_ids):
        return await message.reply_text("ℹ️ Caption filtering is an Admin/Premium feature.", quote=True)

    # Use the in-memory dictionary (loaded from DB at start)
    user_filters = caption_word_filters.get(user_id, [])
    if user_filters:
        # Format list nicely, handle potential long lists
        words_text = "\n".join([f"- `{word}`" for word in user_filters])
        response = f"**📝 قائمة كلمات فلتر التعليقات الحالية:**\n**📝 Your Current Caption Filter Words/Phrases:**\n{words_text}"
        if len(response) > 4000: # Telegram message length limit
             response = response[:4000] + "\n\n[List truncated]"
        await message.reply_text(response, quote=True)
    else:
        await message.reply_text("ℹ️ فلتر كلمات التعليقات الخاص بك فارغ حاليًا. استخدم `/add_word` لإضافة فلاتر.\n"
                                 "ℹ️ Your caption word filter is currently empty. Use `/add_word` to add filters.", quote=True)

# --- Prefix/Suffix Commands ---

@bot.on_message(filters.command("set_prefix") & filters.private)
async def set_prefix_cmd(client: Client, message: Message):
    """Sets a custom prefix for filenames for the user."""
    user_id = message.from_user.id
    if not await force_subscribe(client, message, FORCE_SUBSCRIBE_CHANNEL_ID): return
    if await is_banned_user(user_id): return await message.reply_text("🚫 Banned.", quote=True)
    if not await is_admin_or_pro(user_id, admin_ids):
        return await message.reply_text("⛔ This command requires Admin or Premium status.", quote=True)

    if len(message.command) < 2:
        await message.reply_text("⚠️ **Usage:** `/set_prefix <prefix_text>`\n(Multi-line allowed. Appears before filename in advanced mode).", quote=True)
        return

    prefix_text = message.text.split(None, 1)[1]
    if not prefix_text:
        return await message.reply_text("⚠️ Please provide text for the prefix.", quote=True)

    # Check length (to avoid overly long filenames)
    if len(prefix_text) > 50:
         return await message.reply_text("❌ **Prefix too long!** Max 50 characters.", quote=True)

    user_prefixes[user_id] = prefix_text
    try:
        await digital_botz.set_prefix(user_id, prefix_text)
        await message.reply_text(f"✅ تم تعيين بادئة اسم الملف:\n✅ Filename prefix set:\n```\n{prefix_text}\n```", quote=True)
        logger.info(f"User {user_id} set prefix: '{prefix_text}'")
    except Exception as e:
        user_prefixes.pop(user_id, None) # Rollback in-memory
        logger.error(f"Failed to save prefix for user {user_id}: {e}", exc_info=True)
        await message.reply_text(f"❌ Failed to save prefix to database. Error: `{e}`", quote=True)

@bot.on_message(filters.command("set_suffix") & filters.private)
async def set_suffix_cmd(client: Client, message: Message):
    """Sets a custom suffix for captions for the user."""
    user_id = message.from_user.id
    if not await force_subscribe(client, message, FORCE_SUBSCRIBE_CHANNEL_ID): return
    if await is_banned_user(user_id): return await message.reply_text("🚫 Banned.", quote=True)
    if not await is_admin_or_pro(user_id, admin_ids):
        return await message.reply_text("⛔ This command requires Admin or Premium status.", quote=True)

    if len(message.command) < 2:
        await message.reply_text("⚠️ **Usage:** `/set_suffix <suffix_text>`\n(Multi-line allowed. Appears after two newlines at the end of the caption in advanced mode).", quote=True)
        return

    suffix_text = message.text.split(None, 1)[1]
    if not suffix_text:
        return await message.reply_text("⚠️ Please provide text for the suffix.", quote=True)

    # Check length (to avoid overly long captions)
    # Telegram caption limit is 1024 for media, 4096 for text.
    if len(suffix_text) > 400: # Reasonable limit for suffix
         return await message.reply_text("❌ **Suffix too long!** Max 400 characters recommended.", quote=True)

    user_suffixes[user_id] = suffix_text
    try:
        await digital_botz.set_suffix(user_id, suffix_text)
        await message.reply_text(f"✅ تم تعيين لاحقة التعليق:\n✅ Caption suffix set:\n```\n{suffix_text}\n```", quote=True)
        logger.info(f"User {user_id} set suffix: '{suffix_text}'")
    except Exception as e:
        user_suffixes.pop(user_id, None) # Rollback in-memory
        logger.error(f"Failed to save suffix for user {user_id}: {e}", exc_info=True)
        await message.reply_text(f"❌ Failed to save suffix to database. Error: `{e}`", quote=True)

@bot.on_message(filters.command("remove_prefix") & filters.private)
async def remove_prefix_cmd(client: Client, message: Message):
    """Removes the user's custom prefix."""
    user_id = message.from_user.id
    if not await force_subscribe(client, message, FORCE_SUBSCRIBE_CHANNEL_ID): return
    if await is_banned_user(user_id): return await message.reply_text("🚫 Banned.", quote=True)
    if not await is_admin_or_pro(user_id, admin_ids):
        return await message.reply_text("⛔ This command requires Admin or Premium status.", quote=True)

    if user_id in user_prefixes:
        prefix_to_remove = user_prefixes.pop(user_id)
        try:
            await digital_botz.set_prefix(user_id, None) # None indicates removal
            await message.reply_text("✅ تم إزالة بادئة اسم الملف المخصصة. سيتم استخدام الافتراضية.\n"
                                     "✅ Custom filename prefix removed. Default will be used.", quote=True)
            logger.info(f"User {user_id} removed prefix.")
        except Exception as e:
            user_prefixes[user_id] = prefix_to_remove # Restore in-memory on failure
            logger.error(f"Failed to remove prefix from DB for user {user_id}: {e}", exc_info=True)
            await message.reply_text(f"❌ Failed to remove prefix from database. Error: `{e}`", quote=True)
    else:
        await message.reply_text("ℹ️ ليس لديك بادئة مخصصة لإزالتها.\n"
                                 "ℹ️ You don't have a custom prefix set to remove.", quote=True)

@bot.on_message(filters.command("remove_suffix") & filters.private)
async def remove_suffix_cmd(client: Client, message: Message):
    """Removes the user's custom suffix."""
    user_id = message.from_user.id
    if not await force_subscribe(client, message, FORCE_SUBSCRIBE_CHANNEL_ID): return
    if await is_banned_user(user_id): return await message.reply_text("🚫 Banned.", quote=True)
    if not await is_admin_or_pro(user_id, admin_ids):
        return await message.reply_text("⛔ This command requires Admin or Premium status.", quote=True)

    if user_id in user_suffixes:
        suffix_to_remove = user_suffixes.pop(user_id)
        try:
            await digital_botz.set_suffix(user_id, None) # None indicates removal
            await message.reply_text("✅ تم إزالة لاحقة التعليق المخصصة.\n"
                                     "✅ Custom caption suffix removed.", quote=True)
            logger.info(f"User {user_id} removed suffix.")
        except Exception as e:
            user_suffixes[user_id] = suffix_to_remove # Restore in-memory on failure
            logger.error(f"Failed to remove suffix from DB for user {user_id}: {e}", exc_info=True)
            await message.reply_text(f"❌ Failed to remove suffix from database. Error: `{e}`", quote=True)
    else:
        await message.reply_text("ℹ️ ليس لديك لاحقة مخصصة لإزالتها.\n"
                                 "ℹ️ You don't have a custom suffix set to remove.", quote=True)

@bot.on_message(filters.command("see_fix") & filters.private)
async def see_fix_cmd(client: Client, message: Message):
    """Shows the user's current prefix and suffix."""
    user_id = message.from_user.id
    if not await force_subscribe(client, message, FORCE_SUBSCRIBE_CHANNEL_ID): return
    if await is_banned_user(user_id): return await message.reply_text("🚫 Banned.", quote=True)
    if not await is_admin_or_pro(user_id, admin_ids):
        return await message.reply_text("⛔ This feature requires Admin or Premium status.", quote=True)

    # Use in-memory values which should be loaded from DB at start
    prefix_value = user_prefixes.get(user_id)
    suffix_value = user_suffixes.get(user_id)

    prefix_display = f"`{prefix_value}`" if prefix_value else f"*Default: `{DEFAULT_PREFIX}`*"
    suffix_display = f"`{suffix_value}`" if suffix_value else "*None*"

    response_text = "**⚙️ إعدادات البادئة واللاحقة الحالية | Current Prefix & Suffix Settings:**\n\n"
    response_text += f"**🏷️ البادئة (اسم الملف) | Prefix (Filename):**\n{prefix_display}\n\n"
    response_text += f"**🏷️ اللاحقة (الكابشن) | Suffix (Caption):**\n{suffix_display}"

    await message.reply_text(response_text, quote=True, parse_mode=ParseMode.MARKDOWN)


@bot.on_message(filters.command("see_thumb") & filters.private)
async def see_thumb_cmd(client: Client, message: Message):
    """Shows the user's current thumbnail (custom or default)."""
    user_id = message.from_user.id
    if not await force_subscribe(client, message, FORCE_SUBSCRIBE_CHANNEL_ID): return
    if await is_banned_user(user_id): return await message.reply_text("🚫 Banned.", quote=True)
    if not await is_admin_or_pro(user_id, admin_ids): # Check permission
        return await message.reply_text("⛔ This command requires Admin or Premium status.", quote=True)

    thumb_path = await digital_botz.get_thumbnail(user_id)
    caption_text = ""
    final_thumb_to_send = None
    is_custom = False

    if thumb_path and os.path.exists(thumb_path):
        final_thumb_to_send = thumb_path
        caption_text = "ℹ️ هذه هي صورتك المصغرة المخصصة الحالية.\nℹ️ This is your current custom thumbnail."
        is_custom = True
    elif os.path.exists(DEFAULT_THUMBNAIL):
        final_thumb_to_send = DEFAULT_THUMBNAIL
        caption_text = "ℹ️ أنت تستخدم الصورة المصغرة الافتراضية.\nℹ️ You are using the default thumbnail."
    else:
        # No custom and no default found
        await message.reply_text("ℹ️ لم تقم بتعيين صورة مصغرة مخصصة، والصورة المصغرة الافتراضية مفقودة.\n"
                                 "ℹ️ You don't have a custom thumbnail set, and the default thumbnail is missing.", quote=True)
        return

    # Try sending the determined thumbnail
    try:
        await message.reply_photo(photo=final_thumb_to_send, caption=caption_text, quote=True)
    except Exception as e:
        logger.error(f"Error sending {'custom' if is_custom else 'default'} thumb ({final_thumb_to_send}) for user {user_id}: {e}", exc_info=True)
        # Fallback message if sending photo fails
        await message.reply_text(
            f"{caption_text}\n(تعذر إرسال ملف الصورة | Could not send the image file: `{e}`)",
            quote=True
        )

# ==============================================================================
# --- Photo Handler (for setting thumbnails) ---
# ==============================================================================

@bot.on_message(filters.photo & filters.private)
async def handle_thumbnail_photo(client: Client, message: Message):
    """Handles photos sent to set thumbnails (for self or others via state)."""
    user_id = message.from_user.id
    admin_id = user_id # The person sending the photo

    if not await force_subscribe(client, message, FORCE_SUBSCRIBE_CHANNEL_ID): return
    if await is_banned_user(user_id): return # Don't process photos from banned users

    target_user_id = user_id # Default: user setting their own thumb
    is_admin_setting_other = False
    admin_state_info = None

    # Scenario 1: Admin potentially setting thumb for another user via /set_thumb state
    if is_admin(admin_id, admin_ids) and admin_id in thumb_state:
        admin_state_info = thumb_state[admin_id]
        set_time = admin_state_info.get('time', 0)

        # Check if state is recent (e.g., within 5 minutes)
        if time.time() - set_time < 300:
            potential_target = admin_state_info.get('target_user')
            if potential_target: # Make sure target user is actually set
                target_user_id = potential_target
                is_admin_setting_other = True
                logger.info(f"Processing thumbnail photo from admin {admin_id} for target user {target_user_id}")
                # Clean up state immediately once photo is received
                thumb_state.pop(admin_id, None)
            else:
                 logger.warning(f"Ignoring photo from admin {admin_id}: target_user missing in state.")
                 thumb_state.pop(admin_id, None) # Clean up invalid state
                 # Fall through to scenario 2 (admin might be setting their own now)
        else:
             logger.warning(f"Ignoring stale thumbnail photo from admin {admin_id}. State timed out.")
             thumb_state.pop(admin_id, None) # Clean up old state
             # Fall through to scenario 2

    # Scenario 2: User (Admin/Pro) setting their own thumbnail
    # Check permissions for the FINAL target_user_id
    can_set_thumbnail = False
    if is_admin_setting_other:
        can_set_thumbnail = True # Admin confirmed above setting for someone else
    elif await is_admin_or_pro(user_id, admin_ids) and target_user_id == user_id:
        # User is eligible AND setting their own thumbnail
        can_set_thumbnail = True
        logger.info(f"Processing thumbnail photo from eligible user {user_id} for themselves.")
    else:
        # Not admin setting other, AND not eligible setting self
        # Do nothing unless it was a timed-out admin state
        if admin_state_info: # State existed but timed out or invalid
             await message.reply_text("⏰ انتهت مهلة تعيين الصورة المصغرة للمستخدم الآخر. يرجى استخدام `/set_thumb <user_id>` مرة أخرى.\n"
                                      "⏰ Thumbnail setting for the other user timed out. Please use `/set_thumb <user_id>` again.", quote=True)
        else:
             logger.debug(f"Ignoring photo from user {user_id} as they are not eligible to set a thumbnail or not in a valid state.")
        return

    # --- Proceed with setting the thumbnail for target_user_id ---
    photo_path = None
    thumb_dir = f"thumbnails/{target_user_id}"
    dl_status = None # Initialize dl_status
    try:
        os.makedirs(thumb_dir, exist_ok=True)
        # Define filename consistently
        photo_filename = f"{thumb_dir}/custom_thumb_{target_user_id}.jpg"

        # Download photo (use the main file_id, pyrogram handles quality)
        photo_to_download = message.photo.file_id

        # Add status message for download
        dl_status = await message.reply_text("📥 جارٍ تنزيل الصورة... | Downloading photo...", quote=True)

        photo_path = await client.download_media(photo_to_download, file_name=photo_filename)

        if not photo_path or not os.path.exists(photo_path):
            raise Exception("Download failed or file not found.")

        # --- Delete Old Thumbnail File (if exists and not default) ---
        old_thumb_path = await digital_botz.get_thumbnail(target_user_id)
        if old_thumb_path and old_thumb_path != photo_path and old_thumb_path != DEFAULT_THUMBNAIL and os.path.exists(old_thumb_path):
            try:
                os.remove(old_thumb_path)
                logger.info(f"Deleted previous thumbnail file {old_thumb_path} for user {target_user_id}")
            except OSError as e_rem_old:
                 logger.error(f"Error removing previous thumbnail file {old_thumb_path}: {e_rem_old}")

        # --- Update Database ---
        await digital_botz.set_thumbnail(target_user_id, photo_path)
        if dl_status: await dl_status.delete() # Delete download status

        # Confirm to the user who initiated
        confirmation_text = ""
        if is_admin_setting_other:
            confirmation_text = (f"✅ تم تعيين الصورة المصغرة بنجاح للمستخدم `{target_user_id}`.\n"
                                 f"✅ Thumbnail successfully set for User ID: `{target_user_id}`.")
            # Reply with the photo to the admin who sent the command
            await message.reply_photo(photo=photo_path, caption=confirmation_text, quote=True)
        else:
            # Send confirmation back by sending the photo (replying to original photo message)
            confirmation_text = ("✅ تم تعيين هذه الصورة كصورة مصغرة مخصصة لك.\n"
                                 "✅ This image has been set as your custom thumbnail.")
            await client.send_photo(user_id, photo_path, caption=confirmation_text, reply_to_message_id=message.id)

        logger.info(f"Thumbnail set for user {target_user_id} by {'admin ' + str(admin_id) if is_admin_setting_other else 'user ' + str(user_id)}. Path: {photo_path}")

        # Notify the target user if set by admin and it's not the admin themselves
        if is_admin_setting_other and target_user_id != admin_id:
            try:
                 notify_caption = ("ℹ️ قام المسؤول بتعيين هذه الصورة كصورة مصغرة مخصصة لك.\n"
                                  "ℹ️ An administrator has set this as your new custom thumbnail.")
                 await client.send_photo(target_user_id, photo_path, caption=notify_caption)
            except Exception as e_notify:
                 logger.error(f"Failed to send thumb set notification to target user {target_user_id}: {e_notify}")

    except Exception as e:
        error_message = f"❌ **خطأ في تعيين الصورة المصغرة | Error setting thumbnail:**\n`{e}`"
        if dl_status:
             try: await dl_status.delete() # Delete status on error
             except Exception: pass
        await message.reply_text(error_message, quote=True)
        logger.error(f"Error setting thumbnail for user {target_user_id} initiated by {admin_id}: {e}", exc_info=True)
        # Clean up potentially downloaded file on error
        if photo_path and os.path.exists(photo_path):
            try: os.remove(photo_path)
            except OSError: pass
    finally:
        # Clear state just in case it wasn't cleared before (e.g., on error)
        if admin_id in thumb_state:
            thumb_state.pop(admin_id, None)

# ==============================================================================
# --- Main Message Handler (Links & /l command) ---
# ==============================================================================

# Define filters
link_pattern = r'https?://t\.me/(\w+|c/\d+)/\d+'
# Updated to accept / or - for range, and also / between IDs
# Group 5 is separator, Group 6 is end_id
link_range_pattern = r'https?://t\.me/(c/(\d+)|(\w+))/(\d+)(?:([/-])(\d+))?'
join_link_pattern = r'https?://t\.me/(\+|joinchat/)\S+'

# Combined regex for any relevant Telegram link
# Make range matching non-greedy? No, range should be greedy.
# Ensure range part `[/-](\d+)` is optional `?`
telegram_link_regex = f"({link_range_pattern}|{join_link_pattern})" # Order matters: Check specific range/msg link before generic join link

# Exclude all known command triggers from the link handler
known_commands = [
    "start", "help", "cancel", "restart", "status", "add_pro",
    "remove_pro", "set_thumb", "remove_thumb_user", "remove_thumb",
    "ban", "unban", "set_chat_id", "see_chat_id", "remove_chat_id",
    "filter_text_skip", "filter_photo_skip", "filter_sticker_skip",
    "add_word", "remove_word", "see_words", "see_thumb",
    "set_prefix", "set_suffix", "remove_prefix", "remove_suffix", "see_fix", # Added new commands
    "l" # Exclude /l itself here
]
# Filter for messages that are either the /l command OR contain a TG link, AND are private, AND are NOT other known commands.
link_handler_filter = (
    (filters.command("l") | filters.regex(telegram_link_regex)) &
    filters.private &
    ~filters.command(known_commands) &
    ~filters.service # Ignore service messages
)


@bot.on_message(filters.private & ~filters.forwarded & filters.command(["logout"]))
async def logout(client: Client, message: Message):
    """Logs out the user by removing their session string."""
    user_id = message.from_user.id
    if await is_banned_user(user_id): return # Check ban

    try:
        # Assuming digital_botz has get_session and set_session methods
        user_session = await digital_botz.get_session(user_id)
        if user_session is None:
            return await message.reply_text("⚠️ لم تقم بتسجيل الدخول.\n⚠️ You are not logged in.")

        await digital_botz.set_session(user_id, session=None) # Pass None to remove
        await message.reply_text("✅ **تم تسجيل الخروج بنجاح.**\n✅ **Logout Successful.**")
        logger.info(f"User {user_id} logged out.")
    except AttributeError:
         await message.reply_text("❌ خطأ: وظائف الجلسة غير مدعومة في قاعدة البيانات الحالية.\n❌ Error: Session functions not supported in the current database.", quote=True)
         logger.error("Logout failed: digital_botz object missing session methods.")
    except Exception as e:
        await message.reply_text(f"❌ حدث خطأ أثناء تسجيل الخروج:\n`{e}`", quote=True)
        logger.error(f"Error during logout for user {user_id}: {e}", exc_info=True)

@bot.on_message(filters.private & ~filters.forwarded & filters.command(["login"]))
async def login_command(client: Client, message: Message):
    """Handles the user login process to get a session string."""
    user_id = message.from_user.id
    if await is_banned_user(user_id): return await message.reply_text("🚫 محظور | Banned.", quote=True)

    # Check if already logged in
    try:
        existing_session = await digital_botz.get_session(user_id)
        if existing_session is not None:
            await message.reply_text("⚠️ **أنت مسجل الدخول بالفعل.**\nاستخدم /logout أولاً إذا كنت تريد تسجيل الدخول بحساب آخر.\n\n⚠️ **You Are Already Logged In.**\nUse /logout first if you want to log in with a different account.")
            return
    except AttributeError:
         await message.reply_text("❌ خطأ: وظائف الجلسة غير مدعومة في قاعدة البيانات الحالية.\n❌ Error: Session functions not supported in the current database.", quote=True)
         logger.error("Login check failed: digital_botz object missing session methods.")
         return
    except Exception as e:
        await message.reply_text(f"❌ حدث خطأ أثناء التحقق من الجلسة:\n`{e}`", quote=True)
        logger.error(f"Error checking session for user {user_id} before login: {e}", exc_info=True)
        return

    # --- Login Process ---
    temp_user_client = None # Initialize
    try:
        ask_reply = None # To store the message asking for input
        # Ask for Phone Number
        ask_reply = await client.ask(
            chat_id=user_id,
            text=("**تسجيل الدخول بحساب المستخدم | User Account Login**\n\n"
                  "يرجى إرسال رقم هاتفك **مع رمز الدولة**.\n"
                  "مثال: `+1xxxxxxxxxx`, `+20xxxxxxxxxx`\n\n"
                  "*سيتم استخدام هذا لتنزيل المحتوى الذي لا يستطيع البوت الوصول إليه مباشرة.*\n"
                  "*اضغط /cancel للإلغاء.*"),
            filters=filters.text,
            timeout=300 # 5 minutes timeout
        )

        if not ask_reply or not ask_reply.text: return # Timeout or no reply
        if ask_reply.text.lower() == '/cancel':
            return await ask_reply.reply('❌ تم إلغاء العملية.\n❌ Process cancelled.')

        phone_number = ask_reply.text.strip()
        await ask_reply.reply("⏳ جارٍ إنشاء عميل مؤقت والاتصال...")

        # Use UserClient to avoid conflict with main 'bot' client variable name
        temp_user_client = UserClient(":memory:", API_ID, API_HASH)
        await temp_user_client.connect()
        await ask_reply.reply("📲 جارٍ إرسال رمز التحقق (OTP) إلى حساب تيليجرام الخاص بك...")

        # Send Code
        try:
            code_info = await temp_user_client.send_code(phone_number)
        except ApiIdInvalid:
             await ask_reply.reply('❌ **فشل:** `API_ID` أو `API_HASH` غير صالح. يرجى مراجعة المسؤول.\n❌ **Failed:** Invalid `API_ID` or `API_HASH`. Please contact the admin.')
             await temp_user_client.disconnect()
             return
        except PhoneNumberInvalid:
            await ask_reply.reply('❌ **فشل:** رقم الهاتف غير صالح. تأكد من تضمين رمز الدولة.\n❌ **Failed:** Invalid phone number. Ensure you included the country code.')
            await temp_user_client.disconnect()
            return
        except FloodWait as fw:
             await ask_reply.reply(f"⏳ **انتظار بسبب الضغط:** {fw.value} ثانية. حاول مرة أخرى لاحقًا.\n⏳ **Flood Wait:** {fw.value} seconds. Try again later.")
             await temp_user_client.disconnect()
             return


        # Ask for OTP
        ask_reply = await client.ask(
            user_id,
            ("يرجى التحقق من حساب تيليجرام الرسمي الخاص بك للحصول على رمز التحقق.\n"
             "إذا حصلت عليه، أرسل الرمز هنا.\n\n"
             "مثال: إذا كان الرمز `12345`, أرسله مع المسافات `1 2 3 4 5` (اضف مسافات).\n\n"
             "*اضغط /cancel للإلغاء.*"),
            filters=filters.text,
            timeout=600 # 10 minutes timeout
        )

        if not ask_reply or not ask_reply.text: return # Timeout or no reply
        if ask_reply.text.lower() == '/cancel':
            await temp_user_client.disconnect()
            return await ask_reply.reply('❌ تم إلغاء العملية.\n❌ Process cancelled.')

        phone_code = ask_reply.text.strip()

        # Sign In
        try:
            await temp_user_client.sign_in(phone_number, code_info.phone_code_hash, phone_code)
        except PhoneCodeInvalid:
            await ask_reply.reply('❌ **فشل:** رمز التحقق (OTP) غير صالح.\n❌ **Failed:** Invalid OTP.')
            await temp_user_client.disconnect()
            return
        except PhoneCodeExpired:
            await ask_reply.reply('❌ **فشل:** انتهت صلاحية رمز التحقق (OTP).\n❌ **Failed:** OTP Expired.')
            await temp_user_client.disconnect()
            return
        except SessionPasswordNeeded:
            # Ask for 2FA Password
            ask_reply = await client.ask(
                user_id,
                ("**يتطلب حسابك كلمة مرور تحقق بخطوتين (2FA).**\n"
                 "الرجاء إدخال كلمة المرور.\n\n"
                 "*اضغط /cancel للإلغاء.*"),
                filters=filters.text,
                timeout=300 # 5 minutes timeout
            )

            if not ask_reply or not ask_reply.text: return # Timeout or no reply
            if ask_reply.text.lower() == '/cancel':
                await temp_user_client.disconnect()
                return await ask_reply.reply('❌ تم إلغاء العملية.\n❌ Process cancelled.')

            password = ask_reply.text.strip()
            try:
                await temp_user_client.check_password(password=password)
            except PasswordHashInvalid:
                await ask_reply.reply('❌ **فشل:** كلمة المرور غير صحيحة.\n❌ **Failed:** Invalid Password.')
                await temp_user_client.disconnect()
                return
            except FloodWait as fw:
                 await ask_reply.reply(f"⏳ **انتظار بسبب الضغط:** {fw.value} ثانية. حاول مرة أخرى لاحقًا.\n⏳ **Flood Wait:** {fw.value} seconds. Try again later.")
                 await temp_user_client.disconnect()
                 return

        except FloodWait as fw:
             await ask_reply.reply(f"⏳ **انتظار بسبب الضغط:** {fw.value} ثانية. حاول مرة أخرى لاحقًا.\n⏳ **Flood Wait:** {fw.value} seconds. Try again later.")
             await temp_user_client.disconnect()
             return


        # Export Session String
        string_session = await temp_user_client.export_session_string()
        await temp_user_client.disconnect() # Disconnect temporary client

        if not string_session or len(string_session) < SESSION_STRING_SIZE: # Basic validation
            await message.reply_text("❌ **فشل في إنشاء سلسلة الجلسة.** قد تكون هناك مشكلة مؤقتة. حاول مرة أخرى.\n❌ **Failed to generate session string.** There might be a temporary issue. Please try again.", quote=True)
            logger.error(f"Failed to generate a valid session string for user {user_id}.")
            return

        # Save Session String to Database
        try:
            # Ensure digital_botz object has the set_session method
            await digital_botz.set_session(user_id, session=string_session)
            await message.reply_text("✅ **تم تسجيل الدخول بنجاح!**\nيمكن للبوت الآن محاولة استخدام حسابك للوصول إلى المحتوى المقيد عند الضرورة.\n\n✅ **Login Successful!**\nThe bot can now attempt to use your account to access restricted content when needed.", quote=True)
            logger.info(f"User {user_id} logged in successfully.")
        except AttributeError:
            await message.reply_text("❌ خطأ: وظائف الجلسة غير مدعومة في قاعدة البيانات الحالية. لم يتم حفظ الجلسة.\n❌ Error: Session functions not supported in the current database. Session not saved.", quote=True)
            logger.error("Login failed: digital_botz object missing set_session method.")
        except Exception as e_db:
            await message.reply_text(f"❌ حدث خطأ أثناء حفظ الجلسة في قاعدة البيانات:\n`{e_db}`", quote=True)
            logger.error(f"Error saving session to DB for user {user_id}: {e_db}", exc_info=True)


    except TimeoutError:
        await message.reply_text('⏰ انتهت المهلة. يرجى المحاولة مرة أخرى.\n⏰ Timeout reached. Please try again.', quote=True)
        if temp_user_client and temp_user_client.is_connected:
            await temp_user_client.disconnect()
    except FloodWait as fw: # Catch flood wait during the process
        await message.reply_text(f"⏳ **انتظار بسبب الضغط:** {fw.value} ثانية. يرجى المحاولة مرة أخرى لاحقًا.\n⏳ **Flood Wait:** {fw.value} seconds. Please try again later.", quote=True)
        if temp_user_client and temp_user_client.is_connected:
            await temp_user_client.disconnect()
    except Exception as e:
        traceback.print_exc() # Print full traceback to console/logs
        await message.reply_text(f"❌ **حدث خطأ غير متوقع أثناء تسجيل الدخول:**\n`{type(e).__name__}: {e}`\n\nيرجى مراجعة سجلات البوت أو التواصل مع المسؤول.\n(Please check bot logs or contact admin).", quote=True)
        logger.error(f"Unexpected error during login for user {user_id}: {e}", exc_info=True)
        if temp_user_client and temp_user_client.is_connected:
            await temp_user_client.disconnect()
            
            
            
@bot.on_message(link_handler_filter)
async def save_link(client: Client, message: Message):
    """Main handler for processing Telegram links and the /l command."""
    global user_daily_tasks, last_task_reset_day, current_task, active_operations # Allow modification

    user = message.from_user
    user_id = user.id
    chat_id = message.chat.id # Chat ID where the command is run

    # --- Initial Checks ---
    if not await force_subscribe(client, message, FORCE_SUBSCRIBE_CHANNEL_ID): return
    if await is_banned_user(user_id): return await message.reply_text("🚫 Banned.", quote=True)

    # Check if an operation is already active for this user in this chat
    if active_operations.get(chat_id):
        return await message.reply_text("⏳ **عملية قيد التقدم | Operation in Progress:**\n"
                                        "توجد بالفعل عملية نشطة في هذه الدردشة. يرجى الانتظار حتى تكتمل أو استخدام /cancel.\n"
                                        "*(There is already an active operation in this chat. Please wait for it to complete or use /cancel.)*", quote=True)

    # Determine text to process (from /l command argument or message text)
    text_to_process = ""
    link_found_in_text = None

    if message.command and message.command[0] == "l":
        if len(message.command) > 1:
            # Join arguments after /l in case link contains spaces (though unlikely)
            text_to_process = " ".join(message.command[1:])
        else:
            return await message.reply_text("⚠️ **Usage:** `/l <telegram_link>`", quote=True)
    # Extract link from text/caption if not using /l command
    elif message.text or message.caption:
        text_content = message.text or message.caption
        # Use re.search to find the *first* matching link
        match = re.search(telegram_link_regex, text_content)
        if match:
            text_to_process = match.group(0).strip() # Use the matched link
            link_found_in_text = text_to_process
            logger.debug(f"Extracted link: '{text_to_process}' from message {message.id}")
        else:
             # If the filter matched via regex but we can't find it now, log debug.
             logger.debug(f"Filter triggered but no link pattern found in message {message.id} by user {user_id}. Text: {text_content[:100]}")
             return # Should not happen if link_handler_filter is correct

    if not text_to_process:
        logger.warning(f"Could not extract link/text to process from message {message.id} by user {user_id}")
        # Reply only if it was the command /l explicitly without a link
        if message.command and message.command[0] == "l":
            await message.reply_text("❌ Please provide a valid Telegram link after `/l`.", quote=True)
        return

    # --- User Permissions and Mode Determination ---
    is_admin_user = is_admin(user_id, admin_ids)
    is_pro = await is_pro_user(user_id)
    is_eligible_for_advanced = is_admin_user or is_pro
    # Get user's preferred mode if they are eligible, otherwise default to Normal (False)
    is_currently_advanced = is_eligible_for_advanced and admin_mode_advanced.get(user_id, True) # Default eligible users to Advanced

    logger.info(f"Processing request from User: {user_id} (Admin: {is_admin_user}, Pro: {is_pro}, Mode: {'Advanced' if is_currently_advanced else 'Normal'}) - Link: {text_to_process}")

    # --- Log Request ---
    log_message_text = (
        f"**🔗 طلب رابط | Link Request**\n\n"
        f"👤 **المستخدم | User:** {user.mention} (`{user_id}`)\n"
        f"⚙️ **الوضع | Mode:** {'Advanced (D/U)' if is_currently_advanced else 'Normal (Forward/D&U)'}\n" # Clarified Normal mode
        f"🔗 **الرابط | Link:** `{text_to_process}`"
    )
    if link_found_in_text: # Add original message context if link was extracted
        log_message_text += f"\n\n📝 **النص الأصلي | Original Text:**\n{message.text or message.caption}"
    if LOG_CHANNEL_ID:
        try:
            await client.send_message(LOG_CHANNEL_ID, log_message_text, disable_web_page_preview=True)
        except Exception as e_log:
            logger.error(f"Error sending link request log to channel {LOG_CHANNEL_ID}: {e_log}")

    # ========================================================================
    # === PARSING LOGIC FOR MESSAGE LINKS (SINGLE/RANGE) =====================
    # ========================================================================
    msg_link_match = re.match(link_range_pattern, text_to_process.strip())
    # ========================================================================
    # === PARSING LOGIC FOR JOIN LINKS =======================================
    # ========================================================================
    join_link_match = re.match(join_link_pattern, text_to_process.strip())
    # ========================================================================

    status_message = None # Initialize status message variable outside try blocks

    if msg_link_match:
        # --- Start Processing Message Link(s) ---
        active_operations[chat_id] = True # Set operation active flag for this chat
        status_message = await message.reply_text(f"⏳ جارٍ تحليل الرابط... | Analyzing link...", quote=True)
        operation_status_map[status_message.id] = chat_id # Map this message for cancellation

        try:
            # Extract parts using Regex groups
            private_chat_prefix = msg_link_match.group(1) # Group 1: c/ID or username
            chat_id_part = msg_link_match.group(2)        # Group 2: ID part of c/ID
            username_part = msg_link_match.group(3)       # Group 3: username part
            from_msg_id_str = msg_link_match.group(4)     # Group 4: First message ID
            # separator = msg_link_match.group(5)           # Group 5: Separator (/-) or None
            to_msg_id_str = msg_link_match.group(6)       # Group 6: Second message ID (if range) or None

            chat_identifier: Union[int, str]
            is_private_chat: bool = False # Default to False
            is_public_group: bool = False # Flag for public groups

            if private_chat_prefix and private_chat_prefix.startswith('c/') and chat_id_part:
                # It's a private link like t.me/c/123.../567
                chat_identifier = int("-100" + chat_id_part)
                is_private_chat = True
                # Check private chat permissions (needed for both Normal and Advanced)
                if not is_eligible_for_advanced: # Only admin/pro can access private links
                     active_operations.pop(chat_id, None) # Clear flag
                     if status_message: await status_message.delete() # Delete status msg
                     return await message.reply_text("⛔ الوصول للقنوات/المجموعات الخاصة يتطلب عضوية مميزة أو مسؤول.\n"
                                                     "⛔ Accessing private channels/groups requires Premium/Admin status.", quote=True)
            elif username_part:
                # It's a public link like t.me/username/567
                chat_identifier = username_part
                is_private_chat = False
                # Check if it's a public group (might need D/U even in normal mode)
                try:
                    chat_info = await client.get_chat(chat_identifier)
                    if chat_info.type in [pyrogram.enums.ChatType.GROUP, pyrogram.enums.ChatType.SUPERGROUP]:
                        is_public_group = True
                        logger.info(f"Identified '{chat_identifier}' as a public group.")
                except Exception as e_get_chat:
                    logger.warning(f"Could not determine chat type for public '{chat_identifier}': {e_get_chat}. Assuming channel behavior.")

            else:
                 # This case should ideally not be reached if the main regex matches
                 raise ValueError("Could not determine chat identifier from link structure.")

            # Parse message IDs
            from_msg_id = int(from_msg_id_str)
            # If to_msg_id_str is None (no range specified), set to_msg_id same as from_msg_id
            to_msg_id = int(to_msg_id_str) if to_msg_id_str else from_msg_id
            # Determine if it's bulk based on whether the second ID was captured
            is_bulk = (to_msg_id_str is not None)

            # Validate message IDs
            if from_msg_id <= 0: raise ValueError("Start message ID must be positive.")
            if is_bulk and to_msg_id <= 0: raise ValueError("End message ID must be positive for a range.")
            if is_bulk and from_msg_id > to_msg_id:
                 # Swap if range is backward
                 from_msg_id, to_msg_id = to_msg_id, from_msg_id
                 logger.warning(f"Message range was backward, swapped to {from_msg_id}-{to_msg_id}")
                 is_bulk = True # Ensure bulk flag is true if range was specified, even if swapped

            # Bulk requires download/upload logic anyway, so permissions are checked implicitly by mode.
            # But explicit check for non-eligible users trying bulk might be clearer.
            if is_bulk and not is_eligible_for_advanced:
                 active_operations.pop(chat_id, None) # Clear flag
                 if status_message: await status_message.delete()
                 return await message.reply_text("⛔ المعالجة الجماعية (باستخدام نطاقات الروابط) تتطلب عضوية مميزة أو مسؤول (الوضع المتقدم).\n"
                                                 "⛔ Bulk processing (using link ranges) requires Premium/Admin status (Advanced Mode).", quote=True)

            # Apply bulk limit
            if is_bulk:
                max_bulk_limit = 500 # Example limit
                if (to_msg_id - from_msg_id + 1) > max_bulk_limit:
                    if status_message:
                       try:
                          await status_message.edit_text(f"{status_message.text}\n\n"
                                                    f"⚠️ **نطاق كبير جداً! | Range too large!**\n"
                                                    f"الحد الأقصى {max_bulk_limit} رسالة. سيتم معالجة أول {max_bulk_limit}.\n"
                                                    f"(Maximum {max_bulk_limit} messages allowed. Processing first {max_bulk_limit}.)")
                          await asyncio.sleep(2) # Give user time to read
                       except MessageIdInvalid: status_message = None # Handle deletion
                       except Exception as e_edit_warn: logger.warning(f"Could not edit bulk limit warning: {e_edit_warn}")
                    to_msg_id = from_msg_id + max_bulk_limit - 1

            logger.info(f"Parsed Link -> Chat: {chat_identifier}, Private: {is_private_chat}, Public Group: {is_public_group}, From: {from_msg_id}, To: {to_msg_id}, Bulk: {is_bulk}, Mode: {'Advanced' if is_currently_advanced else 'Normal'}")

            # --- Daily Limit Check (Place AFTER successful parsing & permission checks) ---
            if not is_eligible_for_advanced: # Only apply to normal users
                 can_task, updated_day, updated_tasks = check_daily_task_limit(
                     user_id, last_task_reset_day, user_daily_tasks, DAILY_TASK_LIMIT
                 )
                 last_task_reset_day = updated_day # Update global state
                 user_daily_tasks = updated_tasks   # Update global state

                 if not can_task:
                     active_operations.pop(chat_id, None) # Clear flag
                     if status_message: await status_message.delete()
                     return await message.reply_text(
                         f"🚫 **تم الوصول للحد اليومي! | Daily Limit Reached!** ({DAILY_TASK_LIMIT} tasks)\n"
                         f"قم بالترقية للوصول غير المحدود أو انتظر حتى الغد (بتوقيت UTC).\n"
                         f"(Upgrade for unlimited access or wait until tomorrow (UTC).)\n"
                         f"للاستفسارات | For inquiries: @X_XF8",
                         quote=True
                     )
                 else:
                     # Decrement ONCE per request, not per message in bulk
                     user_daily_tasks = decrement_daily_task_count(user_id, user_daily_tasks)
                     logger.info(f"Daily task count for user {user_id} decremented (1 per request). Remaining: {user_daily_tasks.get(user_id)}")
            # --- End Daily Limit Check ---

            # --- Route to correct handler ---
            if status_message: # Check if status message still exists
                try:
                    # Keep cancel button if present
                    current_markup = status_message.reply_markup if hasattr(status_message, 'reply_markup') else None
                    await status_message.edit_text("⏳ جارٍ بدء المعالجة... | Starting processing...", reply_markup=current_markup)
                except MessageIdInvalid: status_message = None
                except Exception as e_edit_start: logger.warning(f"Could not edit 'Starting processing' status: {e_edit_start}")

            if not is_bulk:
                 # Single message
                 current_task = f"Single: {chat_identifier}/{from_msg_id} (Mode: {'Adv' if is_currently_advanced else 'Norm'})"
                 logger.info(f"Handling single message: {current_task}")
                 processed = await handle_message_bulk(
                     message, # User's original message for context/reply
                     chat_identifier, # Source chat ID or username
                     from_msg_id, # Message ID to process
                     is_private_chat, # Is the source private?
                     is_public_group, # Is it a public group?
                     status_message, # Message to update progress on
                     is_currently_advanced # Is the user in advanced mode?
                 )
                 # Edit status based on result
                 if processed is True: final_text = "✅ تمت المعالجة بنجاح.\n✅ Processed successfully."
                 elif processed is False: final_text = "ℹ️ تم التخطي (فلتر، نوع، إلغاء، أو خطأ وصول).\nℹ️ Skipped (filter, type, cancel, or access error)."
                 else: final_text = "❌ حدث خطأ أثناء المعالجة.\n❌ Error during processing."

                 if status_message:
                     try:
                         # Remove keyboard when done
                         await status_message.edit_text(final_text, reply_markup=None)
                     except (MessageNotModified, MessageIdInvalid): pass # Ignore if already deleted/edited
                     except FloodWait as fw_final:
                         logger.warning(f"FloodWait editing final single status: {fw_final.value}s")
                         await asyncio.sleep(fw_final.value + 0.5)
                     except Exception as e_edit_final:
                         logger.warning(f"Could not edit final status message {status_message.id}: {e_edit_final}")
                         # Maybe send as new message
                         try: await client.send_message(chat_id, final_text)
                         except Exception: pass
                 else: # If status message was deleted, send result as new message
                      try: await client.send_message(chat_id, final_text)
                      except Exception as e_send_new: logger.error(f"Failed to send final status as new message: {e_send_new}")

            else:
                # Bulk message range
                logger.info(f"Routing bulk messages {from_msg_id}-{to_msg_id} from {chat_identifier} (Mode: {'Advanced' if is_currently_advanced else 'Normal'})")
                mode_ar = "المتقدم (تنزيل/رفع)" if is_currently_advanced else "العادي (تمرير/تنزيل)"
                mode_en = "Advanced (Download/Upload)" if is_currently_advanced else "Normal (Forward/Download)"
                source_type_ar = "خاصة" if is_private_chat else ("مجموعة عامة" if is_public_group else "قناة عامة")
                source_type_en = "Private" if is_private_chat else ("Public Group" if is_public_group else "Public Channel")
                status_text = (
                     f"⏳ **بدء المعالجة الجماعية | Bulk Processing Initiated**\n"
                     f"**الوضع | Mode:** {mode_ar} | {mode_en}\n"
                     f"**النطاق | Range:** `{from_msg_id}` to `{to_msg_id}`\n"
                     f"**المصدر | Source:** `{chat_identifier}` (`{source_type_ar} | {source_type_en}`)\n\n"
                     "جارٍ البدء... | Starting..."
                )

                # Add cancel button to bulk start message
                cancel_button = InlineKeyboardButton("⏹️ إلغاء الكل | Cancel All", callback_data=f"cancel_op_{chat_id}")
                keyboard = InlineKeyboardMarkup([[cancel_button]])
                if status_message: # Check again if it exists
                    try:
                        await status_message.edit_text(status_text, reply_markup=keyboard)
                    except MessageIdInvalid:
                         logger.warning(f"Bulk status message {status_message.id} was deleted before processing started.")
                         active_operations.pop(chat_id, None) # Clear flag
                         return # Stop if status msg gone
                    except Exception as e_edit_kb:
                         logger.error(f"Failed to edit/add cancel button to bulk status message {status_message.id}: {e_edit_kb}")
                         # Continue anyway, maybe? Or stop? Let's continue for now.
                else:
                    # If original status msg deleted, create a new one for bulk status
                    try:
                        status_message = await client.send_message(chat_id, status_text, reply_markup=keyboard)
                        operation_status_map[status_message.id] = chat_id # Map the new message
                        logger.info(f"Created new status message {status_message.id} for bulk operation.")
                    except Exception as e_send_bulk_status:
                         logger.error(f"Failed to send new bulk status message: {e_send_bulk_status}")
                         active_operations.pop(chat_id, None) # Clear flag
                         return # Cannot proceed without status msg

                total_messages_in_range = to_msg_id - from_msg_id + 1
                processed_count = 0
                skipped_count = 0
                error_count = 0
                start_bulk_time = time.time()
                last_status_update_time = time.time()

                # --- Bulk Processing Loop ---
                for current_msgid in range(from_msg_id, to_msg_id + 1):
                     # Check cancellation flag BEFORE processing each message
                     if not active_operations.get(chat_id, True):
                         logger.info(f"Bulk operation cancelled by user {user_id} before processing msg {current_msgid}")
                         if status_message:
                             try:
                                await status_message.edit_text("❌ **تم إلغاء العملية الجماعية بواسطة المستخدم.**\n"
                                                               "❌ **Bulk operation cancelled by user.**", reply_markup=None)
                             except Exception: pass # Ignore edit errors on cancel
                         break # Exit loop

                     current_task = f"Bulk: {chat_identifier}/{current_msgid} (Mode: {'Adv' if is_currently_advanced else 'Norm'})"
                     logger.debug(f"Processing {current_task}")

                     # Update status message periodically
                     now = time.time()
                     # Update more frequently if needed, e.g., every 5 seconds or 10 messages
                     processed_now = processed_count + skipped_count + error_count
                     if status_message and (now - last_status_update_time >= 5 or processed_now % 10 == 0 or current_msgid == to_msg_id):
                         elapsed_time = now - start_bulk_time
                         time_str = str(timedelta(seconds=int(elapsed_time)))
                         progress_percent = processed_now * 100 / total_messages_in_range if total_messages_in_range > 0 else 0

                         status_text_update = (
                             f"⏳ **تقدم المعالجة الجماعية | Bulk Progress** ({progress_percent:.1f}%)\n"
                             f"**الوضع:** {mode_ar}\n"
                             f"**النطاق:** `{from_msg_id}`-`{to_msg_id}` | **المصدر:** `{chat_identifier}`\n"
                             f"**تمت:** {processed_count} | **تخطي:** {skipped_count} | **أخطاء:** {error_count}\n"
                             f"**الوقت المنقضي:** {time_str}\n\n"
                             f"جارٍ العمل على الرسالة `{current_msgid}`..."
                          )
                         try:
                            # Keep the cancel button while updating status
                            await status_message.edit_text(status_text_update, reply_markup=keyboard)
                            last_status_update_time = now
                         except MessageNotModified: pass
                         except MessageIdInvalid:
                             logger.warning(f"Bulk status message {status_message.id} deleted during processing.")
                             status_message = None # Stop trying to edit deleted msg
                         except FloodWait as fw:
                             logger.warning(f"FloodWait updating bulk status: {fw.value}s. Sleeping in main loop.")
                             await asyncio.sleep(fw.value + 1) # Wait here
                             last_status_update_time = time.time() # Reset timer after sleep
                         except Exception as e_edit:
                             logger.warning(f"Error updating bulk status: {e_edit}")
                             # Don't nullify status_message, maybe it's temporary

                     result = None # Initialize result for this message
                     try:
                         # *** CRITICAL: Pass the potentially updated status_message to the handler ***
                         result = await handle_message_bulk(
                             message, chat_identifier, current_msgid, is_private_chat, is_public_group, status_message, is_currently_advanced # Pass mode
                         )
                         if result is True:
                             processed_count += 1
                             logger.debug(f"Msg {current_msgid} processed successfully.")
                         elif result is False:
                             skipped_count += 1
                             logger.debug(f"Msg {current_msgid} skipped.")
                         else: # Result is None (error)
                             error_count += 1
                             logger.warning(f"Msg {current_msgid} encountered an error during handling.")
                             # Add a small delay after an error before processing next
                             await asyncio.sleep(1)

                         # Optional small delay between messages to avoid hitting limits
                         # Delay only if processed successfully to avoid compounding delays after errors/skips
                         if result is True:
                              # Shorter delay if forwarded (Normal, Public Channel, Success)
                              forwarded_in_normal = not is_currently_advanced and not is_private_chat and not is_public_group
                              delay = 0.2 if forwarded_in_normal else 0.8
                              await asyncio.sleep(delay)

                     except FloodWait as fw_loop:
                          error_count += 1 # Count FloodWait as error for this message
                          wait_time = fw_loop.value
                          logger.warning(f"FloodWait during bulk loop for message {current_msgid}: {wait_time}s. Sleeping.")
                          if status_message:
                             try:
                                current_text = await bot.get_messages(status_message.chat.id, status_message.id) # Get latest text
                                status_text_flood = getattr(current_text, 'text', '') or ""
                                await status_message.edit_text(f"{status_text_flood}\n\n-- FloodWait: Pausing for {int(wait_time)}s --", reply_markup=keyboard)
                             except Exception: pass
                          await asyncio.sleep(wait_time + 1) # Wait the flood duration + buffer
                          last_status_update_time = time.time() # Reset timer after wait
                          # Continue to next message after waiting
                     except Exception as e_bulk:
                          error_count += 1
                          logger.error(f"Unhandled error processing bulk message {current_msgid} from {chat_identifier}: {e_bulk}", exc_info=True)
                          if status_message:
                              try:
                                  current_text = await bot.get_messages(status_message.chat.id, status_message.id) # Get latest text
                                  status_text_err = getattr(current_text, 'text', '') or ""
                                  await status_message.edit_text(f"{status_text_err}\n\n-- Error processing msg {current_msgid} --", reply_markup=keyboard)
                              except Exception: pass
                          await asyncio.sleep(2) # Longer pause after other errors
                     finally:
                          current_task = None # Clear task after each message attempt
                          logger.debug(f"Finished attempt for msg {current_msgid}. Result: {result}")


                # --- Bulk Completion ---
                if status_message: # Check if status message still exists
                    elapsed_time = time.time() - start_bulk_time
                    time_str = str(timedelta(seconds=int(elapsed_time)))
                    # Final status depends on whether it was cancelled
                    was_cancelled = not active_operations.get(chat_id, True) # Check flag again

                    if was_cancelled:
                         final_status_text = (
                             f"❌ **تم إلغاء العملية الجماعية! | Bulk Operation Cancelled!**\n\n"
                         )
                    else:
                         final_status_text = (
                             f"✅ **اكتملت العملية الجماعية! | Bulk Operation Complete!**\n\n"
                         )
                    final_status_text += (
                        f"**الوضع:** {mode_ar}\n"
                        f"**النطاق:** `{from_msg_id}`-`{to_msg_id}` | **المصدر:** `{chat_identifier}`\n"
                        f"**تمت:** {processed_count} | **تخطي:** {skipped_count} | **أخطاء:** {error_count}\n"
                        f"**الوقت المستغرق:** {time_str}\n\n"
                        f"*(Mode: {mode_en} | Range: `{from_msg_id}`-`{to_msg_id}` | Source: `{chat_identifier}` | Processed: {processed_count} | Skipped: {skipped_count} | Errors: {error_count} | Time: {time_str})*"
                    )
                    try:
                        # Remove cancel button from final status
                        await status_message.edit_text(final_status_text, reply_markup=None)
                    except (MessageNotModified, MessageIdInvalid): pass
                    except FloodWait as fw_f:
                        logger.warning(f"FloodWait editing final bulk status: {fw_f.value}s")
                        await asyncio.sleep(fw_f.value + 1)
                        try: await status_message.edit_text(final_status_text, reply_markup=None) # Retry edit
                        except Exception: pass # Give up if retry fails
                    except Exception as e_final:
                         logger.error(f"Error editing final bulk status message: {e_final}")
                         # Send as new if edit fails
                         try:
                             await client.send_message(chat_id, final_status_text)
                         except Exception as e_send_final:
                              logger.error(f"Failed to send final bulk status as new message: {e_send_final}")
                else:
                    logger.info("Final bulk status message was deleted or invalid, cannot update.")


                logger.info(f"Bulk operation {from_msg_id}-{to_msg_id} completed/cancelled for user {user_id}. Processed: {processed_count}, Skipped: {skipped_count}, Errors: {error_count}")

        # --- End of try block for message link parsing ---
        except ValueError as ve:
             if status_message:
                 try: await status_message.delete()
                 except Exception: pass
             await message.reply_text(f"❌ **رابط أو نطاق غير صالح | Invalid Link or Range:**\n`{ve}`", quote=True)
             logger.warning(f"ValueError parsing link '{text_to_process}' for user {user_id}: {ve}")
             active_operations.pop(chat_id, None) # Clear flag on parsing error
        except FloodWait as fw:
             wait_duration = fw.value
             if status_message:
                 try: await status_message.delete()
                 except Exception: pass
             await message.reply_text(f"⏳ حدث انتظار بسبب الضغط: {int(wait_duration)} ثانية. يرجى المحاولة لاحقًا.\n"
                                      f"⏳ Flood wait occurred: {int(wait_duration)} seconds. Please try again later.", quote=True)
             logger.warning(f"Caught FloodWait in main handler: {wait_duration}s. Sleeping.")
             active_operations.pop(chat_id, None) # Clear flag
             await asyncio.sleep(wait_duration + 1) # Wait before allowing new operations
        except Exception as e:
            # Unexpected error during parsing or processing setup
            if status_message:
                try: await status_message.delete()
                except Exception: pass
            await message.reply_text(f"❌ **حدث خطأ غير متوقع | An unexpected error occurred:**\n`{type(e).__name__}: {e}`", quote=True)
            logger.exception(f"Error processing link '{text_to_process}' for user {user_id}:") # Log full traceback
            active_operations.pop(chat_id, None) # Clear flag

        # Moved the finally block inside the main try-except for link processing
        finally:
            # Clear flags after operation finishes or fails ONLY IF IT WAS SET
            if chat_id in active_operations:
                 active_operations.pop(chat_id, None)
                 logger.info(f"Cleared active operation flag for chat {chat_id}.")
            current_task = None
            # Clean up progress message tracking if it exists and message was potentially created
            if status_message and status_message.id in operation_status_map:
                operation_status_map.pop(status_message.id, None)
                last_edit_times.pop(status_message.id, None)
                logger.debug(f"Cleared status map/time tracking for final status message {status_message.id}")

    # --- Handle Join Links ---
    elif join_link_match:
        # Set flag as this is also an operation
        active_operations[chat_id] = True
        join_status_msg = await message.reply_text("⏳ جارٍ محاولة الانضمام للدردشة... | Attempting to join chat...", quote=True)
        try:
            if is_eligible_for_advanced: # Only Admin/Pro can join via link
                logger.info(f"Attempting join via link: {text_to_process} for user {user_id}")
                chat_info = await client.join_chat(text_to_process)
                await join_status_msg.edit_text(f"✅ تم الانضمام بنجاح للدردشة | Successfully joined chat: **{chat_info.title}** (`{chat_info.id}`)")
                logger.info(f"User {user_id} requested join: {text_to_process}. Joined: {chat_info.title} ({chat_info.id})")
            else:
                 await join_status_msg.edit_text("⛔ الانضمام للدردشات الخاصة عبر الرابط يتطلب عضوية مميزة أو مسؤول.\n"
                                                 "⛔ Joining private chats via link requires Premium/Admin status.", quote=True)
        except UserAlreadyParticipant:
            await join_status_msg.edit_text("ℹ️ أنت عضو بالفعل في هذه الدردشة.\nℹ️ Already a member of this chat.")
        except (InviteHashExpired, ValueError, ChatAdminRequired, UserNotParticipant) as specific_e: # Added UserNotParticipant
             # UserNotParticipant can happen if invite requires admin approval and user isn't approved yet
             error_detail_ar = "الرابط غير صالح، منتهي الصلاحية، يتطلب موافقة، القناة ممتلئة، أو البوت ليس لديه الإذن."
             error_detail_en = "Link might be invalid, expired, require approval, channel full, or bot lacks permission."
             if isinstance(specific_e, UserNotParticipant):
                  error_detail_ar = "قد يتطلب الانضمام موافقة المسؤول أولاً."
                  error_detail_en = "Joining might require admin approval first."

             await join_status_msg.edit_text(f"❌ **فشل الانضمام للدردشة | Failed to join chat:**\n`{specific_e}`\n({error_detail_ar} | {error_detail_en})")
             logger.warning(f"Failed to join chat {text_to_process} requested by {user_id}: {specific_e}")
        except FloodWait as fw:
             wait_duration = fw.value
             await join_status_msg.edit_text(f"⏳ انتظار بسبب الضغط أثناء محاولة الانضمام. يرجى الانتظار {int(wait_duration)} ثانية والمحاولة مرة أخرى.\n"
                                             f"⏳ Flood wait while trying to join. Please wait {int(wait_duration)} seconds and try again.")
             logger.warning(f"FloodWait joining chat {text_to_process} for user {user_id}: {wait_duration}s")
             await asyncio.sleep(wait_duration + 1)
        except Exception as e:
            await join_status_msg.edit_text(f"❌ **فشل الانضمام للدردشة | Failed to join chat:**\n`{type(e).__name__}: {e}`")
            logger.error(f"Failed to join chat {text_to_process} requested by {user_id}: {e}", exc_info=True)
        finally:
             if chat_id in active_operations:
                 active_operations.pop(chat_id, None) # Clear flag after join attempt
                 logger.info(f"Cleared active operation flag for chat {chat_id} after join attempt.")

    # --- If not a message link or join link ---
    else:
        # This case should ideally not be reached if the main filter `link_handler_filter` is accurate
        # and text_to_process was populated correctly.
        logger.warning(f"Handler triggered for message {message.id} by {user_id}, but no valid link pattern matched internal regexes. Link: '{text_to_process}'")
        # Reply only if it was the /l command specifically with invalid arg
        if message.command and message.command[0] == "l":
            await message.reply_text("❌ Please provide a valid Telegram message or join link after the `/l` command.", quote=True)

# ==============================================================================
# --- Core Message Processing Logic ---
# ==============================================================================

# ... (الكود السابق فوق handle_message_bulk) ...
#
# ----- بداية الملف: تأكد من وجود هذا الاستيراد -----


# ... (الكود السابق مثل تعريف الدوال المساعدة والمتغيرات العامة) ...

#



# --- الثوابت الجديدة أو المعدلة ---
FIXED_FILENAME_PREFIX = "[@X_XF8]"
# DEFAULT_PREFIX لم يعد له استخدام رئيسي في بناء اسم الملف
# DEFAULT_PREFIX = " "

# --- بداية الدالة المعدلة ---
async def handle_message_bulk(
    user_message: Message,
    chatid_or_username: Union[int, str],
    msgid: int,
    is_private: bool,
    is_public_group: bool, # Flag indicating if the source is a public group
    status_message: Optional[Message], # Message to update progress on
    is_advanced_mode: bool # User's current mode setting
) -> Optional[bool]:
    """
    Core function to handle processing a single message.

    Attempts to fetch and download message using Bot client, falls back to User client if needed.
    Uploads are *always* performed using the Bot client.

    Logs successfully sent messages to the log channel.

    Returns:
        True: Successfully processed (sent/downloaded+uploaded).
        False: Skipped (due to filters, unsupported type, cancellation, login required, access error).
        None: An error occurred during processing (e.g., download/upload failure, unexpected exception).
    """
    # Ensure necessary objects are available
    if not bot or not digital_botz:
         logger.critical("[handle_message_bulk] 'bot' or 'digital_botz' instance not available!")
         return None

    user_id = user_message.from_user.id
    user_chat_id = user_message.chat.id # Chat where the user initiated the command
    file_path: Optional[str] = None
    download_start_time: float = time.time()
    temp_thumb_path: Optional[str] = None # For downloaded original thumbs
    processed_successfully: Optional[bool] = None # Default to None (error state)
    dl_dir: str = "downloads" # Define download directory
    msg_to_process: Optional[Message] = None # Variable to hold the fetched message
    uploaded_msg: Optional[Message] = None # Variable to hold the successfully uploaded/sent message

    # --- Client Handling Variables ---
    active_client: Client = bot # Start with the bot client for fetching/downloading
    using_user_session: bool = False # Flag to track if UserClient was needed for fetch/download
    user_client_instance: Optional[UserClient] = None # To store temporary user client connection
    log_client_type: str = "BOT" # Default log identifier for fetch/download phase

    # --- Destination Chat Determination ---
    target_chat_id_db = upload_chat_id.get(user_id)
    destination_chat: int = target_chat_id_db if target_chat_id_db else user_chat_id # Default destination is user's chat

    # === Phase 1: Get the Message (Attempt Bot, then User if needed) ===
    # ... (هذا الجزء يبقى كما هو بدون تغيير) ...
    try:
        logger.debug(f"[handle_message_bulk:{msgid}] Attempting get_messages with BOT client for chat '{chatid_or_username}'")
        msg_to_process = await bot.get_messages(chatid_or_username, int(msgid))
        if not msg_to_process:
            logger.warning(f"[handle_message_bulk:{msgid}] BOT client: Message not found or inaccessible in '{chatid_or_username}'.")
            # Fall through to try user client if bot didn't find it (or access error below)

    except (UserNotParticipant, ChannelInvalid, ChatIdInvalid) as e_get_bot:
        error_type = type(e_get_bot).__name__
        logger.warning(f"[handle_message_bulk:{msgid}] BOT client access failed ({error_type}) for chat '{chatid_or_username}'. Checking user session for user {user_id}.")
        # --- Try User Client ---
        try:
            user_session = await digital_botz.get_session(user_id)
            if user_session:
                logger.info(f"[handle_message_bulk:{msgid}] User session found for {user_id}. Attempting UserClient connection.")
                try:
                    # Assuming UserClient is pyrogram.Client aliased
                    user_client_instance = UserClient(
                        name=f":memory:_user_{user_id}", # Use in-memory session with unique name
                        session_string=user_session,
                        api_id=API_ID, # Assumed global
                        api_hash=API_HASH, # Assumed global
                        no_updates=True # Recommended for user bots used only for actions
                    )
                    await user_client_instance.connect()
                    logger.debug(f"[handle_message_bulk:{msgid}] UserClient for {user_id} connected successfully.")

                    # Retry getting the message with user client
                    msg_to_process = await user_client_instance.get_messages(chatid_or_username, int(msgid))
                    if msg_to_process:
                        active_client = user_client_instance # *** SWITCH active_client for DOWNLOAD ***
                        using_user_session = True
                        log_client_type = "USER" # Update log identifier for download phase
                        logger.info(f"[handle_message_bulk:{msgid}] UserClient successfully accessed message from '{chatid_or_username}'. Will use USER for download.")
                    else:
                        # User client connected but still couldn't get the message
                        logger.warning(f"[handle_message_bulk:{msgid}] UserClient: Message still not found or inaccessible in '{chatid_or_username}'.")
                        return False # Treat as skipped (access error)

                # Handle User Client specific errors during connection/get_messages
                except AuthKeyUnregistered:
                    logger.warning(f"[handle_message_bulk:{msgid}] User {user_id}'s session is invalid (AuthKeyUnregistered). Session removed from DB.")
                    try: await digital_botz.set_session(user_id, None)
                    except Exception as db_err: logger.error(f"Failed to remove invalid session from DB for user {user_id}: {db_err}")
                    try:
                        await bot.send_message(
                            user_chat_id,
                            "⚠️ **جلسة تسجيل دخولك غير صالحة أو تم إنهاؤها.**\n"
                            "تمت إزالة الجلسة القديمة. يرجى تسجيل الدخول مرة أخرى باستخدام /login للمتابعة.\n\n"
                            "⚠️ **Your login session is invalid or has been terminated.**\n"
                            "The old session has been removed. Please log in again using /login to continue."
                        )
                    except Exception as e_notify: logger.error(f"Failed to notify user {user_id} about invalid session: {e_notify}")
                    return False # Skipped (needs user action)

                except UserNotParticipant as e_get_user_unp:
                    logger.warning(f"[handle_message_bulk:{msgid}] UserClient access failed: User {user_id} is not a participant in chat '{chatid_or_username}'.")
                    try:
                        await bot.send_message(
                            user_chat_id,
                            f"⚠️ لا يمكنك الوصول للرسالة `{msgid}` لأن حسابك ليس عضواً في الدردشة المصدر "
                            f"(`{chatid_or_username}`). يرجى الانضمام أولاً.\n\n"
                            f"⚠️ Cannot access message `{msgid}` because your account is not a participant "
                            f"in the source chat (`{chatid_or_username}`). Please join first."
                        )
                    except Exception as e_notify_unp: logger.error(f"Failed to notify user {user_id} about non-participation: {e_notify_unp}")
                    return False # Skipped (user needs to join)

                except (ChannelInvalid, ChatIdInvalid, UsernameNotOccupied, ValueError) as e_get_user_invalid:
                    error_type_user = type(e_get_user_invalid).__name__
                    logger.warning(f"[handle_message_bulk:{msgid}] UserClient access failed: Chat/Username '{chatid_or_username}' or Msg ID invalid ({error_type_user}). Skipping.")
                    return False # Skipped (invalid source)

                except FloodWait as fw_get_user:
                    logger.warning(f"[handle_message_bulk:{msgid}] FloodWait occurred getting message with UserClient: {fw_get_user.value}s. Raising.")
                    raise fw_get_user

                except Exception as e_get_user:
                    logger.error(f"[handle_message_bulk:{msgid}] Unexpected error during UserClient get_messages from '{chatid_or_username}': {type(e_get_user).__name__}", exc_info=True)
                    return None # Indicate error

            else: # User session not found in DB
                logger.warning(f"[handle_message_bulk:{msgid}] User session not found for user {user_id}. Prompting for login as bot access failed.")
                try:
                    await bot.send_message(
                        user_chat_id,
                        f"⚠️ **تعذر الوصول للرسالة `{msgid}` من الدردشة المصدر (`{chatid_or_username}`) باستخدام البوت.**\n"
                        f"للوصول إليها، يرجى تسجيل الدخول بحسابك باستخدام الأمر /login .\n\n"
                        f"⚠️ **Could not access message `{msgid}` from source chat (`{chatid_or_username}`) using the bot.**\n"
                        f"To access it, please log in with your account using the /login command."
                    )
                except Exception as e_prompt: logger.error(f"Failed to send login prompt to user {user_id}: {e_prompt}")
                return False # Skipped (needs login)

        # Handle other errors during session check phase
        except AttributeError as e_db_session:
            logger.error(f"[handle_message_bulk:{msgid}] Database error checking session: {e_db_session}. Session functions might be missing.", exc_info=False)
            try: await bot.send_message(user_chat_id, "❌ خطأ في إعدادات البوت الداخلية (وظائف الجلسة). يرجى التواصل مع المسؤول.")
            except Exception: pass
            return None # Error state
        except Exception as e_check_session:
            logger.error(f"[handle_message_bulk:{msgid}] Error checking/using user session for {user_id}: {type(e_check_session).__name__}", exc_info=True)
            return None # Error state

    # Handle initial Bot Get Message specific errors
    except FloodWait as fw_get_bot:
        logger.warning(f"[handle_message_bulk:{msgid}] FloodWait occurred getting message with BOT client: {fw_get_bot.value}s. Raising.")
        raise fw_get_bot

    except (UsernameNotOccupied, ValueError) as e_get_bot_invalid:
        logger.warning(f"[handle_message_bulk:{msgid}] BOT client access failed: Chat/Username '{chatid_or_username}' or Msg ID invalid ({type(e_get_bot_invalid).__name__}). Skipping.")
        return False # Skipped (invalid source)

    except Exception as e_get_bot_other:
        logger.error(f"[handle_message_bulk:{msgid}] Unexpected error during BOT client get_messages from '{chatid_or_username}': {type(e_get_bot_other).__name__}", exc_info=True)
        return None # Indicate error

    # --- Verification after Get Message Attempts ---
    if not msg_to_process:
        logger.error(f"[handle_message_bulk:{msgid}] Failed to fetch message from '{chatid_or_username}' using both bot and potential user client (or it was inaccessible/not found).")
        return None # Error or inaccessible / not found

    # --- Message fetched successfully, proceed with processing ---
    logger.debug(f"[handle_message_bulk:{msgid}] Successfully fetched message using {log_client_type} client. Proceeding with processing.")
    msg_type = get_message_type(msg_to_process).lower()
    is_protected = getattr(msg_to_process, 'protected_content', False)

    # === Phase 2: Process the Message (Main try...finally for cleanup) ===
    try:
        # --- Check for cancellation before proceeding ---
        if not active_operations.get(user_chat_id, True):
            logger.info(f"[handle_message_bulk:{msgid}] Operation cancelled before processing started.")
            return False # Treat as skipped due to cancellation

        # --- Determine Action Path ---
        should_use_direct_send = (
            not is_advanced_mode and
            not is_private and
            not is_public_group and
            not using_user_session
        )
        should_use_download_upload = not should_use_direct_send

        # --- Path A: Direct Send/Copy ---
        if should_use_direct_send:
            # ... (هذا الجزء يبقى كما هو بدون تغيير) ...
            log_prefix = f"[Normal Mode - Send:{msgid}]"
            logger.info(f"{log_prefix} Processing via direct send/copy from '{chatid_or_username}' using BOT client.")
            try:
                # Check for skippable types first
                if msg_type in ["service", "poll", "game", "location", "venue", "contact", "unknown"]:
                    logger.info(f"{log_prefix} Skipping (Unsupported type '{msg_type}').")
                    processed_successfully = False # Skipped
                # --- Handle Text ---
                elif msg_type == "text":
                    text_content = msg_to_process.text.html if msg_to_process.text and msg_to_process.text.html else (msg_to_process.text.plain if msg_to_process.text else "")
                    entities_to_send = msg_to_process.entities
                    if len(text_content.encode('utf-8')) > 4096:
                         limit = 4090; encoded_text = text_content.encode('utf-8')
                         try: text_content = encoded_text[:limit].decode('utf-8', errors='ignore') + "..."
                         except: text_content = text_content[:1500] + "..." # Fallback
                         entities_to_send = None; logger.warning(f"{log_prefix} Text truncated due to length.")

                    uploaded_msg = await bot.send_message(
                        destination_chat, text_content or ".",
                        entities=entities_to_send, disable_web_page_preview=True
                    )
                    logger.info(f"{log_prefix} Sent Text to chat {destination_chat}")
                    processed_successfully = True
                    if uploaded_msg and LOG_CHANNEL_ID:
                        try: await log_outgoing_message(bot, uploaded_msg, LOG_CHANNEL_ID)
                        except Exception as log_e: logger.error(f"Error logging outgoing TEXT message: {log_e}")

                # --- Handle Media (Send by File ID) ---
                else:
                    media_obj = getattr(msg_to_process, msg_type, None)
                    if not media_obj or not hasattr(media_obj, 'file_id'):
                        logger.warning(f"{log_prefix} Media type '{msg_type}' but no media object/file_id found. Skipping.")
                        processed_successfully = False # Skipped
                    else:
                        caption_to_send = msg_to_process.caption.html if msg_to_process.caption and msg_to_process.caption.html else (msg_to_process.caption.plain if msg_to_process.caption else None)
                        caption_entities_to_send = msg_to_process.caption_entities
                        if caption_to_send and len(caption_to_send.encode('utf-8')) > 1024:
                            limit = 1020; encoded_cap = caption_to_send.encode('utf-8')
                            try: caption_to_send = encoded_cap[:limit].decode('utf-8', errors='ignore') + "..."
                            except: caption_to_send = caption_to_send[:300] + "..." # Fallback
                            caption_entities_to_send = None; logger.warning(f"{log_prefix} Caption truncated due to length.")

                        duration = getattr(media_obj, 'duration', 0)
                        width = getattr(media_obj, 'width', 0)
                        height = getattr(media_obj, 'height', 0)
                        performer = getattr(media_obj, 'performer', None)
                        title = getattr(media_obj, 'title', None)
                        length = getattr(media_obj, 'length', 0) # VideoNote
                        file_id_to_send = media_obj.file_id

                        if msg_type == "document": uploaded_msg = await bot.send_document(destination_chat, document=file_id_to_send, caption=caption_to_send, caption_entities=caption_entities_to_send)
                        elif msg_type == "video": uploaded_msg = await bot.send_video(destination_chat, video=file_id_to_send, caption=caption_to_send, caption_entities=caption_entities_to_send, duration=duration, width=width, height=height)
                        elif msg_type == "animation": uploaded_msg = await bot.send_animation(destination_chat, animation=file_id_to_send, caption=caption_to_send, caption_entities=caption_entities_to_send, duration=duration, width=width, height=height)
                        elif msg_type == "voice": uploaded_msg = await bot.send_voice(destination_chat, voice=file_id_to_send, caption=caption_to_send, caption_entities=caption_entities_to_send, duration=duration)
                        elif msg_type == "audio": uploaded_msg = await bot.send_audio(destination_chat, audio=file_id_to_send, caption=caption_to_send, caption_entities=caption_entities_to_send, duration=duration, performer=performer, title=title)
                        elif msg_type == "photo": uploaded_msg = await bot.send_photo(destination_chat, photo=file_id_to_send, caption=caption_to_send, caption_entities=caption_entities_to_send)
                        elif msg_type == "sticker": uploaded_msg = await bot.send_sticker(destination_chat, sticker=file_id_to_send)
                        elif msg_type == "videonote": uploaded_msg = await bot.send_video_note(destination_chat, video_note=file_id_to_send, duration=duration, length=length)
                        else:
                            logger.warning(f"{log_prefix} Unhandled media type '{msg_type}' for direct send. Skipping.")
                            processed_successfully = False # Skipped

                        if processed_successfully is not False:
                             logger.info(f"{log_prefix} Sent {msg_type.capitalize()} to chat {destination_chat}")
                             processed_successfully = True
                             if uploaded_msg and LOG_CHANNEL_ID:
                                 try: await log_outgoing_message(bot, uploaded_msg, LOG_CHANNEL_ID)
                                 except Exception as log_e: logger.error(f"Error logging outgoing MEDIA message ({msg_type}): {log_e}")

            except FloodWait as fw_send:
                logger.warning(f"{log_prefix} FloodWait during direct send. Raising.")
                raise fw_send
            except Exception as e_send:
                logger.error(f"{log_prefix} Failed direct send/copy: {type(e_send).__name__} - {e_send}", exc_info=True)
                processed_successfully = None # Error state

        # --- Path B: Download & Re-upload ---
        elif should_use_download_upload:
            # ** log_client_type here refers to the DOWNLOAD client **
            log_prefix = f"[{log_client_type} Client D/U:{msgid}]"
            logger.info(f"{log_prefix} Processing via Download/Upload from '{chatid_or_username}'. Mode: {'Adv' if is_advanced_mode else 'Norm'}.")

            msg = msg_to_process # Use alias for clarity

            # --- Skip Check (Specific to D/U path if needed) ---
            skip_reason = None
            if is_advanced_mode: # Apply advanced skip filters
                if skip_text_filter.get(user_id) == 'on' and msg_type == "text": skip_reason = "Text filter (Adv)"
                if skip_photo_filter.get(user_id) == 'on' and msg_type == "photo": skip_reason = "Photo filter (Adv)"
                if skip_sticker_filter.get(user_id) == 'on' and msg_type == "sticker": skip_reason = "Sticker filter (Adv)"
            if msg_type in ["service", "poll", "game", "location", "venue", "contact", "unknown"]:
                skip_reason = f"Unsupported type ({msg_type})"

            if skip_reason:
                logger.info(f"{log_prefix} Skipping ({skip_reason}).")
                return False # Return False for skipped

            # --- Handle Text Messages (D/U Path) ---
            elif msg_type == "text":
                logger.debug(f"{log_prefix} Processing Text message (Upload uses BOT).")
                try:
                    original_text = msg.text.html if msg.text else ""
                    processed_text = original_text # ابدأ بالنص الأصلي الكامل
                    text_was_modified = False
                    final_entities = msg.entities if msg.entities else []

                    # Apply advanced customizations (filters, prefix, suffix) only in Adv Mode
                    if is_advanced_mode:
                        user_prefix_val = user_prefixes.get(user_id)
                        custom_suffix = user_suffixes.get(user_id)
                        filtered_caption_words = caption_word_filters.get(user_id, []) # Use the same filter list

                        # 1. تطبيق فلاتر الكلمات (على النص)
                        if filtered_caption_words:
                            temp_text = processed_text
                            for word in filtered_caption_words: processed_text = processed_text.replace(word, "")
                            if temp_text != processed_text: text_was_modified = True; final_entities = None

                        # 2. *** تعديل: إضافة بادئة المستخدم (إن وجدت) بدون strip ***
                        if user_prefix_val:
                            # prefix_html = user_prefix_val.strip().replace('\n', '<br/>') # <-- السطر القديم المعلق
                            prefix_html = user_prefix_val.replace('\n', '<br/>') # <-- السطر الجديد (بدون strip)
                            processed_text = f"{prefix_html}<br/><br/>{processed_text.lstrip()}"
                            text_was_modified = True; final_entities = None

                        # 3. *** تعديل: تطبيق اللاحقة بدون strip ***
                        if custom_suffix:
                            # suffix_html = custom_suffix.strip().replace('\n', '<br/>') # <-- السطر القديم المعلق
                            suffix_html = custom_suffix.replace('\n', '<br/>') # <-- السطر الجديد (بدون strip)
                            processed_text = f"{processed_text.rstrip()}<br/><br/>{suffix_html}"
                            text_was_modified = True; final_entities = None

                    # Final Length check
                    if len(processed_text.encode('utf-8')) > 4096:
                         limit = 4090; encoded_text = processed_text.encode('utf-8')
                         try: processed_text = encoded_text[:limit].decode('utf-8', errors='ignore') + "..."
                         except: processed_text = processed_text[:1500] + "..." # Fallback
                         text_was_modified = True; final_entities = None
                         logger.warning(f"{log_prefix} Text message truncated due to length.")

                    # *** Upload using BOT client ***
                    uploaded_msg = await bot.send_message(
                        destination_chat, processed_text or ".",
                        entities=final_entities if not text_was_modified else None,
                        parse_mode=ParseMode.HTML if text_was_modified and final_entities is None else None,
                        disable_web_page_preview=True
                    )
                    logger.info(f"{log_prefix} BOT Sent modified Text to chat {destination_chat}")
                    processed_successfully = True
                    # --- LOG OUTGOING MESSAGE ---
                    if uploaded_msg and LOG_CHANNEL_ID:
                        try: await log_outgoing_message(bot, uploaded_msg, LOG_CHANNEL_ID)
                        except Exception as log_e: logger.error(f"Error logging outgoing D/U TEXT message: {log_e}")

                except FloodWait as fw_send_text: raise fw_send_text
                except Exception as e_send_text:
                    logger.error(f"{log_prefix} Error BOT sending modified Text to {destination_chat}: {type(e_send_text).__name__}", exc_info=True)
                    processed_successfully = None # Error

            # --- Process Media (D/U Path) ---
            elif not msg.media:
                logger.warning(f"{log_prefix} Message has no media but is not 'text' type ({msg_type}). Skipping.")
                return False # Skipped
            else: # Has Media
                media_obj = getattr(msg, msg_type, None) # Get the media object

                # ========================================
                # === *** تعديل معالجة الكابشن هنا *** ===
                # ========================================
                original_caption = msg.caption.html if msg.caption else ""
                processed_caption = original_caption # ابدأ بالكابشن الأصلي الكامل
                caption_was_modified = False
                final_caption_entities = msg.caption_entities if msg.caption_entities else []

                # Apply filters/prefix/suffix ONLY if in advanced mode
                if is_advanced_mode:
                    # الحصول على الإعدادات
                    user_prefix_val = user_prefixes.get(user_id)
                    custom_suffix = user_suffixes.get(user_id)
                    filtered_caption_words = caption_word_filters.get(user_id, [])

                    # 1. تطبيق فلاتر الكلمات أولاً
                    if filtered_caption_words:
                        temp_caption = processed_caption
                        for word in filtered_caption_words: processed_caption = processed_caption.replace(word, "")
                        if temp_caption != processed_caption: caption_was_modified = True; final_caption_entities = None

                    # 2. *** تعديل: إضافة بادئة المستخدم (إن وجدت) بدون strip ***
                    if user_prefix_val:
                        # prefix_html = user_prefix_val.strip().replace('\n', '<br/>') # <-- السطر القديم المعلق
                        prefix_html = user_prefix_val.replace('\n', '<br/>') # <-- السطر الجديد (بدون strip)
                        processed_caption = f"{prefix_html}<br/><br/>{processed_caption.lstrip()}"
                        caption_was_modified = True; final_caption_entities = None

                    # 3. *** تعديل: تطبيق اللاحقة بدون strip ***
                    if custom_suffix:
                        # suffix_html = custom_suffix.strip().replace('\n', '<br/>') # <-- السطر القديم المعلق
                        suffix_html = custom_suffix.replace('\n', '<br/>') # <-- السطر الجديد (بدون strip)
                        processed_caption = f"{processed_caption.rstrip()}<br/><br/>{suffix_html}"
                        caption_was_modified = True; final_caption_entities = None
                # ========================================
                # === *** نهاية تعديل معالجة الكابشن *** ===
                # ========================================


                # =========================================
                # === *** تعديل إنشاء اسم الملف هنا *** ===
                # =========================================
                base_filename_source = None
                if media_obj and hasattr(media_obj, 'file_name') and getattr(media_obj, 'file_name', None):
                    base_filename_source = media_obj.file_name
                elif original_caption: # Use original_caption (full text) as source for sanitize_filename
                     base_filename_source = original_caption
                else: # Absolute fallback
                    base_filename_source = f"{msg_type}_file_{msgid}"

                # Sanitize_filename سيأخذ أول سطرين من base_filename_source
                sanitized_base_name = sanitize_filename(base_filename_source)

                # *** بناء البادئة الكاملة لاسم الملف ***
                user_prefix_val_for_filename = user_prefixes.get(user_id) if is_advanced_mode else None

                filename_prefix_parts = [FIXED_FILENAME_PREFIX] # ابدأ بالبادئة الثابتة
                if user_prefix_val_for_filename:
                    # استخدم strip هنا لاسم الملف لتجنب مشاكل النظام
                    cleaned_user_prefix = user_prefix_val_for_filename.strip()
                    if cleaned_user_prefix:
                       filename_prefix_parts.append(cleaned_user_prefix)

                filename_prefix_str = "_".join(filename_prefix_parts) + "_"
                # *** نهاية تعديل بناء البادئة ***


                # Get extension more reliably
                # ... (كود الحصول على الامتداد يبقى كما هو) ...
                file_ext = ""
                if media_obj and hasattr(media_obj, 'file_name') and getattr(media_obj, 'file_name', None):
                    _, file_ext = os.path.splitext(media_obj.file_name)
                elif media_obj and hasattr(media_obj, 'mime_type'):
                    mime = media_obj.mime_type.lower()
                    if 'jpeg' in mime: file_ext = ".jpg"
                    elif 'png' in mime: file_ext = ".png"
                    elif 'webp' in mime: file_ext = ".webp"
                    elif 'heic' in mime: file_ext = ".heic"
                    elif 'mp4' in mime: file_ext = ".mp4"
                    elif 'webm' in mime: file_ext = ".webm"
                    elif 'quicktime' in mime: file_ext = ".mov"
                    elif 'mpeg' in mime: file_ext = ".mp3"
                    elif 'ogg' in mime: file_ext = ".ogg"
                    elif 'opus' in mime: file_ext = ".opus"
                    elif 'tgs' in mime: file_ext = ".tgs"
                elif msg_type == "photo": file_ext = ".jpg"
                elif msg_type == "voice": file_ext = ".ogg"
                elif msg_type == "videonote": file_ext = ".mp4"

                # Clean base name from potential existing extension
                if not sanitized_base_name: sanitized_base_name = f"{msg_type}_{msgid}" # Fallback
                base_part_match = re.match(r"^(.*?)(\.\w+)?$", sanitized_base_name)
                base_part = base_part_match.group(1) if base_part_match and base_part_match.group(1) else sanitized_base_name
                final_ext = file_ext if file_ext else (base_part_match.group(2) if base_part_match and base_part_match.group(2) else "")
                if not base_part: base_part = f"{msg_type}_{msgid}" # Fallback

                # بناء اسم الملف النهائي
                download_filename_base = f"{filename_prefix_str}{base_part}{final_ext}"
                os.makedirs(dl_dir, exist_ok=True)
                download_filename = os.path.join(dl_dir, download_filename_base)
                # =========================================
                # === *** نهاية تعديل إنشاء اسم الملف *** ===
                # =========================================


                # --- Truncate overly long paths ---
                # ... (كود تقصير المسار يبقى كما هو) ...
                try:
                    if len(download_filename.encode('utf-8')) > 230:
                        ext_len = len(final_ext.encode('utf-8')) if final_ext else 0
                        prefix_len = len(filename_prefix_str.encode('utf-8')) # استخدم البادئة الجديدة
                        base_max_bytes = 230 - len(dl_dir.encode('utf-8')) - 1 - prefix_len - ext_len - 5
                        if base_max_bytes < 10: base_max_bytes = 10
                        encoded_base = base_part.encode('utf-8')
                        if len(encoded_base) > base_max_bytes:
                            truncated_bytes = encoded_base[:base_max_bytes]
                            try: base_part = truncated_bytes.decode('utf-8', errors='ignore').rstrip('._- ')
                            except: base_part = base_part[:base_max_bytes//2].strip('._- ')
                        download_filename_base = f"{filename_prefix_str}{base_part}{final_ext}" # أعد بناء الاسم بالـ base_part المقصر
                        download_filename = os.path.join(dl_dir, download_filename_base)
                        logger.warning(f"{log_prefix} Filename truncated due to length: {download_filename}")
                except Exception as e_trunc: logger.error(f"{log_prefix} Error during filename truncation: {e_trunc}. Using potentially long name: {download_filename}")
                logger.debug(f"{log_prefix} Final download path: {download_filename}")


                # --- Download Media (Uses active_client: Bot or User) ---
                # ... (كود التنزيل يبقى كما هو) ...
                download_retries = 2
                file_path = None
                download_ok = False
                for attempt in range(download_retries):
                    if not active_operations.get(user_chat_id, True):
                        logger.info(f"{log_prefix} Operation cancelled before download attempt {attempt + 1}.")
                        processed_successfully = False
                        return False
                    try:
                        logger.debug(f"{log_prefix} Starting download (Attempt {attempt+1}/{download_retries}) using {log_client_type} client.")
                        current_status_message = status_message if status_message and status_message.id in operation_status_map else None
                        file_path = await active_client.download_media(
                            message=msg,
                            file_name=download_filename, # <-- استخدم المسار المبني حديثاً
                            progress=progress if current_status_message else None,
                            progress_args=(user_message, "DOWN", download_start_time, current_status_message),
                        )
                        if not file_path or not os.path.exists(file_path):
                             raise Exception("Download returned invalid path or file does not exist.")
                        logger.info(f"{log_prefix} Download COMPLETED (Attempt {attempt+1}) using {log_client_type}. Path: {file_path}")
                        download_ok = True
                        break
                    except FloodWait as e_flood:
                        logger.warning(f"{log_prefix} FloodWait during download (Attempt {attempt + 1}): {e_flood.value}s. Raising.")
                        raise e_flood
                    except Exception as e_down:
                        logger.error(f"{log_prefix} Error downloading using {log_client_type} (Attempt {attempt + 1}/{download_retries}): {type(e_down).__name__} - {e_down}", exc_info=False)
                        if file_path and os.path.exists(file_path):
                            try: os.remove(file_path)
                            except OSError: pass
                            file_path = None
                        if attempt >= download_retries - 1:
                            logger.error(f"{log_prefix} All download attempts failed.")
                            break
                        await asyncio.sleep(2 * (attempt + 1))


                # --- Upload Phase (Uses BOT client *always*) ---
                if not download_ok:
                    logger.error(f"{log_prefix} Skipping upload because download failed.")
                    processed_successfully = None # Error state
                elif not active_operations.get(user_chat_id, True):
                    logger.info(f"{log_prefix} Operation cancelled after download, before upload.")
                    processed_successfully = False # Mark as skipped due to cancellation
                else:
                    logger.debug(f"{log_prefix} Download successful. Preparing for upload using BOT client.")

                    # --- Thumbnail Logic ---
                    # ... (كود الصور المصغرة يبقى كما هو) ...
                    final_thumb_path = None
                    user_thumb_db = await digital_botz.get_thumbnail(user_id)
                    if is_advanced_mode and user_thumb_db and os.path.exists(user_thumb_db):
                         final_thumb_path = user_thumb_db
                         logger.debug(f"{log_prefix} Using user's custom thumbnail: {final_thumb_path}")
                    if not final_thumb_path:
                        original_thumb_media = None
                        thumbs = []
                        if media_obj and hasattr(media_obj, 'thumbs') and getattr(media_obj, 'thumbs', None):
                             thumbs = media_obj.thumbs
                        elif msg_type == "photo" and hasattr(msg, 'photo') and hasattr(msg.photo, 'thumbs'):
                            thumbs = getattr(msg.photo, 'thumbs', []) or []
                        if thumbs:
                            try:
                                thumbs.sort(key=lambda t: getattr(t, 'file_size', 0) or 0, reverse=True)
                                if thumbs: original_thumb_media = thumbs[0]
                            except Exception as e_sort: logger.warning(f"{log_prefix} Could not sort/get best thumbnail: {e_sort}")
                        if original_thumb_media and hasattr(original_thumb_media, 'file_id'):
                            try:
                                thumb_dl_filename = f"thumb_{msgid}_{int(time.time())}.jpg"
                                thumb_dl_path = os.path.join(dl_dir, thumb_dl_filename)
                                temp_thumb_path = await active_client.download_media(original_thumb_media.file_id, file_name=thumb_dl_path)
                                if temp_thumb_path and os.path.exists(temp_thumb_path):
                                    final_thumb_path = temp_thumb_path
                                    logger.debug(f"{log_prefix} Downloaded original thumbnail: {final_thumb_path}")
                                else:
                                     logger.warning(f"{log_prefix} Original thumb download failed or file missing.")
                                     temp_thumb_path = None
                            except Exception as e_thumb:
                                logger.warning(f"{log_prefix} Failed to download original thumb: {type(e_thumb).__name__}. Trying default.")
                                if temp_thumb_path and os.path.exists(temp_thumb_path):
                                    try: os.remove(temp_thumb_path)
                                    except OSError: pass
                                temp_thumb_path = None
                    if not final_thumb_path and os.path.exists(DEFAULT_THUMBNAIL):
                        final_thumb_path = DEFAULT_THUMBNAIL
                        logger.debug(f"{log_prefix} Using default thumbnail: {DEFAULT_THUMBNAIL}")
                    thumb_to_use = final_thumb_path if final_thumb_path and os.path.exists(final_thumb_path) else None


                    # --- Upload Media (Uses BOT client) ---
                    upload_retries = 2
                    # *** تحضير الكابشن للرفع ***
                    final_caption_for_upload = None
                    upload_parse_mode = None
                    if processed_caption: # استخدم الكابشن المعالج
                        encoded_caption = processed_caption.encode('utf-8')
                        if len(encoded_caption) > 1024: # حد تيليجرام للكابشن
                            limit = 1020
                            try:
                                truncated_caption = encoded_caption[:limit].decode('utf-8', errors='ignore')
                                final_caption_for_upload = truncated_caption + "..."
                            except Exception: final_caption_for_upload = processed_caption[:300] + "..."
                            logger.warning(f"{log_prefix} Caption truncated for upload.")
                            final_caption_entities = None; caption_was_modified = True
                        else:
                            final_caption_for_upload = processed_caption

                        upload_parse_mode = ParseMode.HTML if caption_was_modified and final_caption_entities is None else None
                    # *** نهاية تحضير الكابشن ***

                    upload_start_time = time.time()
                    uploaded_msg = None
                    upload_ok = False

                    # --- Upload Attempt Loop (Uses BOT) ---
                    for attempt in range(upload_retries):
                        if not active_operations.get(user_chat_id, True):
                            logger.info(f"{log_prefix} Operation cancelled before BOT upload attempt {attempt + 1}.")
                            processed_successfully = False
                            upload_ok = False
                            break

                        try:
                            logger.debug(f"{log_prefix} Starting upload (Attempt {attempt+1}/{upload_retries}) using BOT client to chat {destination_chat}.")
                            current_status_message = status_message if status_message and status_message.id in operation_status_map else None

                            # --- Prepare Common Arguments with potentially modified caption ---
                            common_args = {
                                "chat_id": destination_chat,
                                "caption": final_caption_for_upload, # <-- استخدم الكابشن النهائي هنا
                                "caption_entities": final_caption_entities if not upload_parse_mode else None,
                                "parse_mode": upload_parse_mode, # <-- استخدم وضع التنسيق المحدد
                                "progress": progress if current_status_message else None,
                                "progress_args": (user_message, "UP", upload_start_time, current_status_message)
                            }
                            duration = getattr(media_obj, 'duration', 0) if media_obj else 0
                            width = getattr(media_obj, 'width', 0) if media_obj else 0
                            height = getattr(media_obj, 'height', 0) if media_obj else 0
                            performer = getattr(media_obj, 'performer', None) if media_obj else None
                            title = getattr(media_obj, 'title', None) if media_obj else None
                            length = getattr(media_obj, 'length', 0) if media_obj else 0

                            # Use the correctly prefixed filename for upload
                            upload_file_name = os.path.basename(download_filename) # <-- الاسم بالبادئة الصحيحة

                            # --- *** Call Specific BOT Send Method *** ---
                            if msg_type == "document": uploaded_msg = await bot.send_document(document=file_path, thumb=thumb_to_use, file_name=upload_file_name, **common_args)
                            elif msg_type == "video": uploaded_msg = await bot.send_video(video=file_path, thumb=thumb_to_use, duration=duration, width=width, height=height, file_name=upload_file_name, **common_args)
                            elif msg_type == "animation": uploaded_msg = await bot.send_animation(animation=file_path, thumb=thumb_to_use, duration=duration, width=width, height=height, file_name=upload_file_name, **common_args)
                            elif msg_type == "voice": uploaded_msg = await bot.send_voice(voice=file_path, caption=common_args["caption"], caption_entities=common_args["caption_entities"], parse_mode=common_args["parse_mode"], duration=duration, progress=common_args["progress"], progress_args=common_args["progress_args"])
                            elif msg_type == "audio": uploaded_msg = await bot.send_audio(audio=file_path, thumb=thumb_to_use, duration=duration, performer=performer, title=title, file_name=upload_file_name, **common_args)
                            elif msg_type == "photo": uploaded_msg = await bot.send_photo(photo=file_path, **common_args)
                            elif msg_type == "sticker":
                                if file_path.lower().endswith(('.webp', '.tgs')):
                                     uploaded_msg = await bot.send_sticker(sticker=file_path, chat_id=destination_chat, progress=common_args["progress"], progress_args=common_args["progress_args"])
                                else:
                                     logger.warning(f"{log_prefix} Sticker file path '{file_path}' doesn't have expected extension (.webp/.tgs). BOT attempting upload anyway.")
                                     uploaded_msg = await bot.send_sticker(sticker=file_path, chat_id=destination_chat, progress=common_args["progress"], progress_args=common_args["progress_args"])
                            elif msg_type == "videonote": uploaded_msg = await bot.send_video_note(video_note=file_path, duration=duration, length=length, thumb=thumb_to_use, progress=common_args["progress"], progress_args=common_args["progress_args"])
                            else:
                                logger.warning(f"{log_prefix} Attempted BOT upload for unhandled media type '{msg_type}'. Skipping upload.")
                                upload_ok = False
                                break

                            # --- Upload Success ---
                            logger.info(f"{log_prefix} BOT Upload successful (Attempt {attempt+1}). New msg ID: {uploaded_msg.id if uploaded_msg else 'N/A'}")
                            upload_ok = True
                            # --- LOG OUTGOING MESSAGE ---
                            if uploaded_msg and LOG_CHANNEL_ID:
                                try: await log_outgoing_message(bot, uploaded_msg, LOG_CHANNEL_ID)
                                except Exception as log_e: logger.error(f"Error logging outgoing D/U MEDIA message ({msg_type}): {log_e}")
                            break

                        # --- Specific Error Handling during BOT Upload ---
                        except FloodWait as e_flood:
                            logger.warning(f"{log_prefix} FloodWait during BOT upload (Attempt {attempt + 1}): {e_flood.value}s. Raising.")
                            raise e_flood
                        except MessageNotModified:
                            logger.info(f"{log_prefix} BOT Upload resulted in MessageNotModified. Considering success.")
                            upload_ok = True
                            break
                        except AttributeError as e_attr:
                            logger.error(f"{log_prefix} Unexpected AttributeError during BOT upload (Attempt {attempt + 1}): {e_attr}", exc_info=True)
                            upload_ok = False
                            break
                        except (ChatAdminRequired, UserNotParticipant) as e_perm:
                             logger.error(f"{log_prefix} BOT Permission Error uploading to {destination_chat} (Attempt {attempt+1}): {type(e_perm).__name__}. Check BOT permissions.", exc_info=False)
                             try:
                                 perm_msg = f"تحقق من أن البوت لديه صلاحية إرسال الوسائط في الدردشة الوجهة (`{destination_chat}`)."
                                 await bot.send_message(user_chat_id, f"❌ فشل الرفع: خطأ صلاحيات للبوت في الدردشة الوجهة.\n{perm_msg}")
                             except Exception: pass
                             upload_ok = False
                             break
                        except Exception as e_upload:
                            logger.error(f"{log_prefix} BOT Generic Error uploading (Attempt {attempt + 1}/{upload_retries}): {type(e_upload).__name__} - {e_upload}", exc_info=False)
                            if attempt >= upload_retries - 1:
                                logger.error(f"{log_prefix} All BOT upload attempts failed after generic errors.")
                                upload_ok = False
                                break
                            else:
                                await asyncio.sleep(3 * (attempt + 1))

                    # --- After Upload Attempt Loop ---
                    processed_successfully = True if upload_ok else (False if processed_successfully is False else None)
                    if processed_successfully is not True:
                        if processed_successfully is None: logger.error(f"{log_prefix} BOT Upload ultimately failed for message.")

        # --- End of Media Processing (D/U Path) ---
    # --- End of D/U Path ---

    # === Handle Outer Exceptions (during phase 2) ===
    except FloodWait as fw_outer:
        logger.warning(f"[handle_message_bulk:{msgid}] FloodWait occurred during main processing: {fw_outer.value}s. Raising.")
        raise fw_outer
    except Exception as e_outer:
        logger.exception(f"[handle_message_bulk:{msgid}] Unhandled exception during main processing block:")
        processed_successfully = None # Ensure error state

    # === Phase 3: Cleanup (Always Runs) ===
    finally:
        # --- Disconnect User Client if Used ---
        if user_client_instance and user_client_instance.is_connected:
            logger.debug(f"[handle_message_bulk:{msgid}] Disconnecting temporary UserClient for {user_id} (end of function).")
            try: await user_client_instance.disconnect()
            except Exception as e_disconnect: logger.error(f"[handle_message_bulk:{msgid}] Error disconnecting UserClient: {e_disconnect}")

        # --- Cleanup Downloaded Files (if D/U path was taken) ---
        if 'should_use_download_upload' in locals() and should_use_download_upload: # Check if var exists
            if file_path and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    logger.debug(f"[handle_message_bulk:{msgid}] Cleaned up downloaded file: {file_path}")
                except OSError as e_rem: logger.error(f"[handle_message_bulk:{msgid}] Error removing downloaded file {file_path}: {e_rem}")
            if temp_thumb_path and os.path.exists(temp_thumb_path):
                try:
                    os.remove(temp_thumb_path)
                    logger.debug(f"[handle_message_bulk:{msgid}] Cleaned up temporary thumbnail: {temp_thumb_path}")
                except OSError as e_rem_thumb: logger.error(f"[handle_message_bulk:{msgid}] Error removing temporary thumbnail {temp_thumb_path}: {e_rem_thumb}")

    # === Return Final Status ===
    final_status = processed_successfully
    logger.info(f"[handle_message_bulk:{msgid}] Finished processing. Returning status: {final_status} (DownloadClient: {log_client_type}, UploadClient: BOT)")
    return final_status
# --- نهاية الدالة المعدلة ---
#
#
# ... (بقية الكود مثل main() و __main__) ...
# ... (الكود المتبقي بعد handle_message_bulk) ...

#
# ... (بقية الكود مثل main() و __main__ يبقى كما هو) ...

# ==============================================================================
# --- Bot Startup Logic ---
# ==============================================================================

async def load_data_from_db():
    """Loads caption filters, prefixes, and suffixes from the database into memory at startup."""
    global caption_word_filters, user_prefixes, user_suffixes
    logger.info("Loading persistent user data from database...")
    try:
        # Load Filters
        all_filters = await digital_botz.get_all_caption_filters()
        if isinstance(all_filters, dict):
             caption_word_filters = all_filters
             # Ensure lists are mutable
             for user_id in caption_word_filters:
                 if not isinstance(caption_word_filters[user_id], list):
                     caption_word_filters[user_id] = list(caption_word_filters[user_id])
             logger.info(f"Loaded {len(caption_word_filters)} caption filter lists from DB.")
        else:
             logger.error(f"Failed to load caption filters: DB function returned type {type(all_filters)}, expected dict.")
             caption_word_filters = {}

        # Load Prefixes
        all_prefixes = await digital_botz.get_all_prefixes()
        if isinstance(all_prefixes, dict):
             user_prefixes = all_prefixes
             logger.info(f"Loaded {len(user_prefixes)} prefixes from DB.")
        else:
             logger.error(f"Failed to load prefixes: DB function returned type {type(all_prefixes)}, expected dict.")
             user_prefixes = {}

        # Load Suffixes
        all_suffixes = await digital_botz.get_all_suffixes()
        if isinstance(all_suffixes, dict):
             user_suffixes = all_suffixes
             logger.info(f"Loaded {len(user_suffixes)} suffixes from DB.")
        else:
             logger.error(f"Failed to load suffixes: DB function returned type {type(all_suffixes)}, expected dict.")
             user_suffixes = {}

    except AttributeError as e:
         logger.warning(f"Database object might be missing methods (e.g., get_all_...): {e}. Data may not load/persist correctly.")
    except Exception as e:
        logger.exception("Error loading persistent data from database:")
        # Reset in-memory dicts on failure to ensure clean state
        caption_word_filters = {}
        user_prefixes = {}
        user_suffixes = {}

async def main():
    """Main function to start the bot and load initial data."""
    global last_task_reset_day

    print("--- Starting Bot ---")
    print(f"Python Version: {sys.version}")
    print(f"Pyrogram Version: {pyrogram.__version__}")

    # Initialize task reset day based on UTC using datetime.datetime and datetime.timezone
    last_task_reset_day = datetime.datetime.now(timezone.utc).day
    logger.info(f"Initial task reset day (UTC): {last_task_reset_day}")

    # Create necessary directories
    os.makedirs("downloads", exist_ok=True)
    os.makedirs("thumbnails", exist_ok=True)
    logger.info("Created/verified 'downloads' and 'thumbnails' directories.")

    # Load persistent data (filters, prefixes, suffixes) from DB
    await load_data_from_db()

    logger.info("Starting Pyrogram client...")
    try:
        # Start the bot client
        await bot.start()
        myself = await bot.get_me()
        logger.info(f"Bot started as {myself.first_name} (@{myself.username} / {myself.id})")

        # --- Send startup message to log channel ---
        if LOG_CHANNEL_ID:
            try:
                startup_time_str = datetime.datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
                # <<< MODIFIED Startup Message >>>
                await bot.send_message(
                    LOG_CHANNEL_ID,
                    f"✅ **البوت يعمل الآن | Bot is working now!**\n"
                    f"👤 Name: {myself.mention}\n"
                    f"🆔 ID: `{myself.id}`\n"
                    f"🕒 Timestamp: `{startup_time_str}`"
                )
                logger.info(f"Startup message sent to log channel {LOG_CHANNEL_ID}.")
            except Exception as e:
                logger.error(f"Could not send startup message to log channel {LOG_CHANNEL_ID}: {e}")
        else:
            logger.warning("LOG_CHANNEL_ID not set, cannot send startup message.")
        # --- End startup message ---

        # Keep the bot running indefinitely using an asyncio Event
        stop_event = asyncio.Event()
        logger.info("Bot is running. Press Ctrl+C to stop.")
        await stop_event.wait() # This will wait forever until stop_event.set() is called (e.g., by signal handler)

    except Exception as e:
        # Catch general exceptions during startup
        logger.critical(f"Bot failed to start or encountered a critical error: {type(e).__name__} - {e}", exc_info=True)
        if "database is locked" in str(e).lower():
             logger.critical("Database lock error detected. Ensure only one instance is running or check session file permissions.")
        elif "API key" in str(e) or "bot token" in str(e).lower() or "AUTH_KEY" in str(e): # Check common auth error messages
             logger.critical("Authentication error detected. Please check API_ID, API_HASH, and BOT_TOKEN.")
        sys.exit(1) # Exit on any critical startup error
    finally:
        logger.info("--- Stopping Bot ---")
        if 'bot' in locals() and bot.is_initialized and bot.is_connected:
            # Send shutdown message if possible
            if LOG_CHANNEL_ID:
                try:
                    shutdown_time_str = datetime.datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
                    await bot.send_message(LOG_CHANNEL_ID, f"⚠️ **Bot Stopping**\nTimestamp: `{shutdown_time_str}`")
                except Exception: pass # Ignore errors during shutdown message
            try:
                await bot.stop()
            except ConnectionError:
                 logger.warning("ConnectionError during final stop().")
            except Exception as e_stop:
                 logger.error(f"Error stopping client during shutdown: {e_stop}")
        logger.info("Bot stopped.")
        print("--- Bot Stopped ---")

if __name__ == "__main__":
    # Get the current event loop.
    loop = asyncio.get_event_loop()
    try:
        # Run the main coroutine until it completes.
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user (Ctrl+C)")
        # Trigger graceful shutdown if possible (stop_event needs to be accessible)
        # Find a way to signal the stop_event if needed, e.g., via loop.call_soon_threadsafe
    except Exception as e:
         logger.critical(f"Unhandled exception in main loop: {e}", exc_info=True)
    finally:
        logger.info("Shutting down asyncio tasks...")
        # Find all running tasks:
        try:
            all_tasks = asyncio.all_tasks(loop=loop)
        except RuntimeError: # Handle cases where loop might be closed already
            all_tasks = set()

        # Get current task safely
        try:
            current_task_obj = asyncio.current_task(loop=loop)
        except RuntimeError:
             current_task_obj = None

        # Filter out the current task if it exists
        other_tasks = [task for task in all_tasks if task is not current_task_obj and not task.done()]

        if other_tasks:
             logger.info(f"Attempting to gather {len(other_tasks)} pending tasks...")
             try:
                 # Wait for task completion, but with a timeout
                 results = loop.run_until_complete(asyncio.wait_for(asyncio.gather(*other_tasks, return_exceptions=True), timeout=5.0))
                 # Log any exceptions that occurred in tasks during shutdown
                 for i, result in enumerate(results):
                     if isinstance(result, Exception):
                         logger.error(f"Task {other_tasks[i]} raised an exception during shutdown: {result}")
             except asyncio.TimeoutError:
                  logger.warning("Timeout waiting for pending tasks to complete during shutdown.")
             except Exception as e_gather:
                  logger.error(f"Error during pending task shutdown using gather: {e_gather}")
             finally:
                 # Cancel any tasks still running after gather/timeout
                 for task in other_tasks:
                      if not task.done():
                            task.cancel()
                            logger.info(f"Cancelled task {task}")
                 # Give cancellation a moment to propagate if needed
                 if any(not task.done() for task in other_tasks):
                      loop.run_until_complete(asyncio.sleep(0.1))

        # Close the event loop.
        if loop and not loop.is_closed():
             loop.close()
             logger.info("Event loop closed.")

