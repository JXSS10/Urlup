# -*- coding: utf-8 -*-

import motor.motor_asyncio
import datetime
import os
import logging
from typing import Optional, Dict, List, Any, Union
from datetime import timezone  # استيراد timezone بشكل مباشر
# استيراد pyrogram فقط إذا كنت تستخدم أنواعه بشكل صريح هنا، وإلا لا حاجة له
# import pyrogram

# إعداد التسجيل
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s')
logger = logging.getLogger(__name__)

# <<-- يبدأ تعريف الكلاس هنا -->>
class Database:
    """
    Handles all interactions with the MongoDB database.
    Uses a single 'users' collection for most data and separate collections for bans and premium codes.
    """
    def __init__(self, uri: str, database_name: str):
        """
        Initializes the database connection and collections.

        Args:
            uri: The MongoDB connection string.
            database_name: The name of the database to use.
        """
        try:
            self._client = motor.motor_asyncio.AsyncIOMotorClient(uri)
            self.db = self._client[database_name]
            # --- Collections ---
            self.col = self.db.users  # Collection لبيانات المستخدمين العامة والبريميوم والجلسات إلخ
            self.banned_col = self.db.banned_users # Collection للمستخدمين المحظورين
            self.premium_codes_col = self.db.premium_codes # Collection لأكواد البريميوم

            logger.info(f"Database connection established to '{database_name}'")
            # يمكنك التفكير في إنشاء فهارس هنا لزيادة الكفاءة
            # asyncio.create_task(self.create_indexes())
        except Exception as e:
            logger.critical(f"CRITICAL: Failed to connect to MongoDB at '{uri}' / DB '{database_name}'. Error: {e}", exc_info=True)
            # أعد طرح الخطأ لإيقاف البوت إذا فشل الاتصال الأساسي
            raise ConnectionError(f"Failed to connect to database: {e}") from e

    # --- دوال المستخدم الأساسية ---

    def _create_new_user_doc(self, user_id: int) -> dict:
        """Creates a dictionary representing a new user document."""
        # <<-- توحيد استخدام datetime.datetime.now(timezone.utc) -->>
        now_utc = datetime.datetime.now(timezone.utc)
        return {
            '_id': int(user_id),
            'join_date': now_utc,
            'file_id': None, # Thumbnail file_id (مسار الصورة الفعلي مخزن على الخادم)
            'prefix': None,
            'suffix': None,
            'caption_filter_words': [],
            'session_string': None, # <<-- حقل لتخزين سلسلة الجلسة
            # <<-- تبسيط الحالة الأولية -->>
            'usertype': "Free", # ['Free', 'Pro']
            'uploadlimit': 1073741824, # Default limit (1 GiB for free users)
            'premium_expiry': None, # Initial premium expiry date
            'language_code': None, # Can store user's TG language
            'last_active': now_utc # Track last activity
        }

    async def add_user(self, b: Optional[Any], u: Any): # استخدم Any بدل pyrogram.types.User لتجنب الاستيراد غير الضروري
        """Adds a new user to the database if they don't exist."""
        # تأكد من أن u يحتوي على id و username (أو قم بتمرير القيم مباشرة)
        if not hasattr(u, 'id'):
            logger.error("add_user called with object 'u' missing 'id' attribute.")
            return
        user_id = int(u.id)
        username = getattr(u, 'username', None) # Get username safely

        if not await self.is_user_exist(user_id):
            user_dict = self._create_new_user_doc(user_id)
            user_dict['language_code'] = getattr(u, 'language_code', None) # Store initial language
            try:
                await self.col.insert_one(user_dict)
                logger.info(f"New user {user_id} ('{username or 'NoUsername'}') added to the database.")
            except motor.motor_asyncio.DuplicateKeyError: # استخدم الاستثناء المحدد من motor
                logger.warning(f"Attempted to add user {user_id} which already exists (DuplicateKeyError). Ignoring.")
            except Exception as e:
                logger.error(f"Failed to add user {user_id}: {e}", exc_info=True)
        else:
            # تحديث last_active للمستخدم الحالي
             now_utc = datetime.datetime.now(timezone.utc)
             await self.update_user_data(user_id, {'last_active': now_utc})

    async def is_user_exist(self, user_id: int) -> bool:
        """Checks if a user exists in the database."""
        user_id = int(user_id)
        try:
            # التحقق من الوجود باستخدام count_documents أكثر كفاءة من find_one إذا كنت تحتاج فقط للتحقق
            count = await self.col.count_documents({'_id': user_id}, limit=1)
            return count > 0
        except Exception as e:
            logger.error(f"Error checking if user {user_id} exists: {e}", exc_info=True)
            return False # افترض عدم الوجود عند حدوث خطأ

    async def total_users_count(self) -> int:
        """Returns the total number of users."""
        try:
            count = await self.col.count_documents({})
            return count
        except Exception as e:
            logger.error(f"Error counting total users: {e}", exc_info=True)
            return 0 # أرجع 0 عند الخطأ

    async def delete_user(self, user_id: int):
        """Deletes a user from all relevant collections."""
        user_id = int(user_id)
        try:
            # حذف من مجموعة المستخدمين الرئيسية
            await self.col.delete_one({'_id': user_id})
            # حذف من مجموعة المحظورين (إذا كان موجودًا)
            await self.banned_col.delete_one({'_id': user_id})
            # [اختياري] إلغاء ربط الأكواد المستخدمة
            # await self.premium_codes_col.update_many({'used_by_user_id': user_id}, {'$set': {'used_by_user_id': None}})
            logger.info(f"Deleted user {user_id} data from database.")
        except Exception as e:
            logger.error(f"Error deleting user {user_id}: {e}", exc_info=True)

    async def get_user_data(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Retrieves complete user data from the user collection."""
        user_id = int(user_id)
        try:
            user_data = await self.col.find_one({'_id': user_id})
            return user_data
        except Exception as e:
            logger.error(f"Error getting user data for {user_id}: {e}", exc_info=True)
            return None

    async def update_user_data(self, user_id: int, data_to_set: dict):
        """Updates user data in the user collection using $set."""
        user_id = int(user_id)
        try:
            # استخدم upsert=False للتحديث فقط إذا كان المستخدم موجودًا
            result = await self.col.update_one({'_id': user_id}, {'$set': data_to_set}, upsert=False)
            # logger.debug(f"Updated user data for {user_id}. Matched: {result.matched_count}, Modified: {result.modified_count}")
        except Exception as e:
            logger.error(f"Error updating user data for {user_id}: {e}", exc_info=True)

    # --- دوال الجلسات (Session) ---

    async def set_session(self, user_id: int, session: Optional[str]):
        """Stores or clears the user's session string in the user document."""
        user_id = int(user_id)
        update_operation = {'$set': {'session_string': session}} if session else {'$unset': {'session_string': ""}}
        try:
            # تأكد من وجود المستخدم قبل محاولة التحديث إذا لم تكن متأكدًا
            # if not await self.is_user_exist(user_id):
            #     logger.warning(f"Cannot set session for non-existent user {user_id}")
            #     return

            # يجب أن تستخدم upsert=False هنا لأن add_user يجب أن تتم أولاً
            # إذا حاولت تحديث مستخدم غير موجود بدون upsert=True، لن يحدث شيء
            result = await self.col.update_one({'_id': user_id}, update_operation, upsert=False)

            if result.matched_count == 0:
                 logger.warning(f"Attempted to set/clear session for non-existent user {user_id}. No update performed.")
            else:
                 log_msg = "Stored session string" if session else "Cleared session string"
                 logger.info(f"MongoDB: {log_msg} for user {user_id} (Modified: {result.modified_count})")
        except Exception as e:
            logger.error(f"MongoDB: Error setting session for user {user_id}: {e}", exc_info=True)


    async def get_session(self, user_id: int) -> Optional[str]:
        """Retrieves the user's session string from the user document."""
        user_id = int(user_id)
        try:
            # ابحث عن المستخدم واحصل على حقل session_string فقط
            user_data = await self.col.find_one(
                {'_id': user_id},
                {'session_string': 1} # الإسقاط: جلب حقل الجلسة فقط
            )
            # استخدم .get للوصول الآمن للحقل
            session = user_data.get('session_string') if user_data else None
            logger.debug(f"MongoDB: Retrieved session for user {user_id}: {'Exists' if session else 'None'}")
            return session
        except Exception as e:
            logger.error(f"MongoDB: Error getting session for user {user_id}: {e}", exc_info=True)
            return None

    # --- دوال Thumbnail ---
    async def set_thumbnail(self, user_id: int, file_id: Optional[str]):
        """Sets or removes the user's thumbnail file_id."""
        # ملف الصورة الفعلي يتم التعامل معه في main.py (الحفظ والحذف)
        await self.update_user_data(user_id, {'file_id': file_id})
        log_msg = f"Set thumbnail file_id to {file_id}" if file_id else "Removed thumbnail file_id"
        logger.info(f"{log_msg} for user {user_id}")

    async def get_thumbnail(self, user_id: int) -> Optional[str]:
        """Gets the stored thumbnail file_id for the user."""
        user_data = await self.get_user_data(user_id)
        return user_data.get('file_id') if user_data else None

    # --- دوال Premium ---

    async def get_premium_expiry(self, user_id: int) -> Optional[datetime.datetime]:
        """Gets the active premium expiry datetime object (UTC) for a user, else None."""
        user_id = int(user_id)
        try:
            # ابحث في مجموعة المستخدمين الأساسية عن تاريخ الانتهاء
            user_data = await self.col.find_one({'_id': user_id}, {'premium_expiry': 1})
            if user_data and 'premium_expiry' in user_data:
                expiry_time = user_data['premium_expiry']
                # تأكد أنه تاريخ ووقت وليس None
                if isinstance(expiry_time, datetime.datetime):
                    # تأكد أنه بتوقيت UTC (Handle naive datetimes if they somehow got stored)
                    expiry_time_aware = expiry_time.replace(tzinfo=timezone.utc) if expiry_time.tzinfo is None else expiry_time
                    # تحقق مما إذا كان التاريخ أكبر من الوقت الحالي
                    if expiry_time_aware > datetime.datetime.now(timezone.utc):
                        return expiry_time_aware # أرجع تاريخ الانتهاء الصالح
            return None # لا يوجد بريميوم نشط أو منتهي الصلاحية
        except Exception as e:
            logger.error(f"Error getting premium expiry for {user_id}: {e}", exc_info=True)
            return None

    async def addpremium(self, user_id: int, expiry_time: datetime.datetime):
        """Adds or updates premium status for a user in the main user document."""
        user_id = int(user_id)
        # Ensure expiry_time is timezone-aware (UTC)
        if expiry_time.tzinfo is None:
            expiry_time = expiry_time.replace(tzinfo=timezone.utc)

        # حدّث مجموعة المستخدمين الأساسية مباشرة
        user_update_data = {
            'usertype': "Pro",
            'uploadlimit': 0, # حد غير نهائي للبريميوم (أو أي قيمة تفضلها)
            'premium_expiry': expiry_time
        }
        try:
            # يجب استخدام upsert=True هنا لضمان إضافة المستخدم إذا لم يكن موجودًا
            # أو تحديثه إذا كان موجودًا
            await self.col.update_one({'_id': user_id}, {'$set': user_update_data}, upsert=True)
            logger.info(f"Added/Updated premium for user {user_id} until {expiry_time.isoformat()}")
        except Exception as e:
            logger.error(f"Error adding premium for user {user_id}: {e}", exc_info=True)

    async def remove_premium(self, user_id: int):
        """Removes premium status from a user, reverting to Free settings."""
        user_id = int(user_id)
        # أعد المستخدم للحالة المجانية في المجموعة الأساسية
        user_update_data = {
            'usertype': "Free",
            'uploadlimit': 1073741824, # أعد الحد الافتراضي
            'premium_expiry': None # أزل تاريخ الانتهاء
        }
        try:
            # استخدم upsert=False لأننا نريد فقط تحديث المستخدم الموجود
            await self.col.update_one({'_id': user_id}, {'$set': user_update_data}, upsert=False)
            logger.info(f"Removed premium status for user {user_id}")
        except Exception as e:
            logger.error(f"Error removing premium for user {user_id}: {e}", exc_info=True)

    async def has_premium_access(self, user_id: int) -> bool:
        """Checks if a user has active premium access based on expiry date."""
        user_id = int(user_id)
        expiry_time = await self.get_premium_expiry(user_id) # استخدم الدالة المحدثة
        has_access = expiry_time is not None # إذا أرجعت تاريخاً، فهو صالح

        # [اختياري] تصحيح نوع المستخدم إذا كان غير متطابق مع حالة الانتهاء
        # هذا يمكن أن يساعد في الحفاظ على تناسق البيانات
        try:
            user_data = await self.get_user_data(user_id)
            if user_data:
                current_usertype = user_data.get('usertype')
                needs_correction = False
                update_data = {}

                if has_access and current_usertype != "Pro":
                    logger.warning(f"User {user_id} has active premium expiry but usertype is '{current_usertype}'. Correcting to 'Pro'.")
                    update_data = {'usertype': "Pro", 'uploadlimit': 0}
                    needs_correction = True
                elif not has_access and current_usertype == "Pro":
                    logger.warning(f"User {user_id} has no active premium expiry but usertype is 'Pro'. Correcting to 'Free'.")
                    update_data = {'usertype': "Free", 'uploadlimit': 1073741824, 'premium_expiry': None}
                    needs_correction = True

                if needs_correction:
                    await self.update_user_data(user_id, update_data)

        except Exception as e:
             logger.error(f"Error during premium status consistency check for user {user_id}: {e}")

        return has_access

    async def total_premium_users_count(self) -> int:
        """Returns the total number of users with active premium."""
        try:
            now_utc = datetime.datetime.now(timezone.utc)
            # عد المستخدمين في المجموعة الأساسية الذين لديهم تاريخ انتهاء صالح
            count = await self.col.count_documents({
                'premium_expiry': {'$ne': None, '$gt': now_utc}
            })
            return count
        except Exception as e:
            logger.error(f"Error counting premium users: {e}", exc_info=True)
            return 0

    # --- دوال فلتر الكلمات ---
    async def add_caption_filter_word(self, user_id: int, word: str):
        """Adds a word to the user's caption filter list (avoids duplicates)."""
        user_id = int(user_id)
        try:
            # استخدم $addToSet لإضافة الكلمة فقط إذا لم تكن موجودة بالفعل
            await self.col.update_one({'_id': user_id}, {'$addToSet': {'caption_filter_words': word}})
            logger.info(f"Added filter word '{word}' for user {user_id}")
        except Exception as e:
            logger.error(f"Error adding filter word for {user_id}: {e}", exc_info=True)

    async def remove_caption_filter_word(self, user_id: int, word: str):
        """Removes a word from the user's caption filter list."""
        user_id = int(user_id)
        try:
            # استخدم $pull لإزالة جميع مثيلات الكلمة (عادة ما تكون واحدة بسبب $addToSet)
            await self.col.update_one({'_id': user_id}, {'$pull': {'caption_filter_words': word}})
            logger.info(f"Attempted removal of filter word '{word}' for user {user_id}")
        except Exception as e:
            logger.error(f"Error removing filter word for {user_id}: {e}", exc_info=True)

    async def get_all_caption_filters(self) -> Dict[int, List[str]]:
        """Retrieves caption filters for all users who have them."""
        filters_dict = {}
        try:
            # ابحث عن المستخدمين الذين لديهم قائمة فلاتر موجودة وغير فارغة
            cursor = self.col.find({'caption_filter_words': {'$exists': True, '$ne': []}}, {'_id': 1, 'caption_filter_words': 1})
            async for user in cursor:
                filters_dict[user['_id']] = user.get('caption_filter_words', []) # تأكد من أنها قائمة
            logger.info(f"Loaded {len(filters_dict)} caption filter lists from DB.")
        except Exception as e:
            logger.error(f"Error getting all caption filters: {e}", exc_info=True)
        return filters_dict

    # --- دوال البادئة واللاحقة (Prefix/Suffix) ---
    async def set_prefix(self, user_id: int, prefix: Optional[str]):
        """Sets or removes the filename prefix for the user."""
        await self.update_user_data(user_id, {'prefix': prefix})
        log_msg = f"Set prefix to '{prefix}'" if prefix else "Removed prefix"
        logger.info(f"{log_msg} for user {user_id}")

    async def get_prefix(self, user_id: int) -> Optional[str]:
        """Gets the filename prefix for the user."""
        user_data = await self.get_user_data(user_id)
        return user_data.get('prefix') if user_data else None

    async def get_all_prefixes(self) -> Dict[int, str]:
        """Retrieves prefixes for all users who have them set."""
        prefixes = {}
        try:
            cursor = self.col.find({'prefix': {'$exists': True, '$ne': None}}, {'_id': 1, 'prefix': 1})
            async for user in cursor:
                prefixes[user['_id']] = user.get('prefix')
            logger.info(f"Loaded {len(prefixes)} prefixes from DB.")
        except Exception as e:
            logger.error(f"Error getting all prefixes: {e}", exc_info=True)
        return prefixes

    async def set_suffix(self, user_id: int, suffix: Optional[str]):
        """Sets or removes the caption suffix for the user."""
        await self.update_user_data(user_id, {'suffix': suffix})
        log_msg = f"Set suffix to '{suffix}'" if suffix else "Removed suffix"
        logger.info(f"{log_msg} for user {user_id}")

    async def get_suffix(self, user_id: int) -> Optional[str]:
        """Gets the caption suffix for the user."""
        user_data = await self.get_user_data(user_id)
        return user_data.get('suffix') if user_data else None

    async def get_all_suffixes(self) -> Dict[int, str]:
        """Retrieves suffixes for all users who have them set."""
        suffixes = {}
        try:
            cursor = self.col.find({'suffix': {'$exists': True, '$ne': None}}, {'_id': 1, 'suffix': 1})
            async for user in cursor:
                suffixes[user['_id']] = user.get('suffix')
            logger.info(f"Loaded {len(suffixes)} suffixes from DB.")
        except Exception as e:
            logger.error(f"Error getting all suffixes: {e}", exc_info=True)
        return suffixes

    # --- دوال الحظر/فك الحظر (Ban/Unban) ---
    async def ban_user(self, user_id: int):
        """Adds a user to the banned collection."""
        user_id = int(user_id)
        ban_doc = {'_id': user_id, 'banned_at': datetime.datetime.now(timezone.utc)}
        try:
            # استخدم $setOnInsert لمنع الكتابة فوق تاريخ الحظر إذا كان موجودًا بالفعل
            await self.banned_col.update_one({'_id': user_id}, {'$setOnInsert': ban_doc}, upsert=True)
            logger.info(f"Banned user {user_id}")
        except Exception as e:
            logger.error(f"Error banning user {user_id}: {e}", exc_info=True)

    async def unban_user(self, user_id: int):
        """Removes a user from the banned collection."""
        user_id = int(user_id)
        try:
            result = await self.banned_col.delete_one({'_id': user_id})
            if result.deleted_count > 0:
                logger.info(f"Unbanned user {user_id}")
            else:
                logger.warning(f"User {user_id} was not found in the banned list (delete attempted).")
        except Exception as e:
            logger.error(f"Error unbanning user {user_id}: {e}", exc_info=True)

    async def is_banned(self, user_id: int) -> bool:
        """Checks if a user exists in the banned collection."""
        user_id = int(user_id)
        try:
            # استخدام count_documents أكثر كفاءة للتحقق من الوجود
            count = await self.banned_col.count_documents({'_id': user_id}, limit=1)
            return count > 0
        except Exception as e:
            logger.error(f"Error checking ban status for {user_id}: {e}", exc_info=True)
            return False # افترض أنه غير محظور عند حدوث خطأ

    # --- دوال أكواد البريميوم ---

    async def add_coupon_code(self, code: str, duration_days: int, generated_by: int = 0) -> bool:
        """Adds a new premium code to the database."""
        code_doc = {
            '_id': code.strip().upper(), # استخدم الكود كمعرف فريد (وتأكد من التنسيق)
            'duration_days': int(duration_days),
            'is_used': False,
            'generated_by_admin_id': int(generated_by) if generated_by else None,
            'generated_at': datetime.datetime.now(timezone.utc),
            'used_by_user_id': None,
            'redeemed_at': None
        }
        try:
            await self.premium_codes_col.insert_one(code_doc)
            logger.info(f"Added premium code '{code_doc['_id']}' ({duration_days} days) generated by {generated_by or 'Unknown'}")
            return True
        except motor.motor_asyncio.DuplicateKeyError:
            logger.error(f"Premium code '{code_doc['_id']}' already exists.")
            return False
        except Exception as e:
            logger.error(f"Error adding premium code '{code_doc['_id']}': {e}", exc_info=True)
            return False

    async def get_coupon_details(self, code: str) -> Optional[Dict[str, Any]]:
        """Retrieves details of a premium code if it exists and is *unused*."""
        code_upper = code.strip().upper()
        try:
            # ابحث باستخدام '_id' وحالة is_used
            code_data = await self.premium_codes_col.find_one({'_id': code_upper, 'is_used': False})
            if code_data:
                logger.info(f"Found unused premium code '{code_upper}'")
                # أرجع القاموس كما هو متوقع في main.py
                return {'duration_days': code_data.get('duration_days')}
            else:
                # تحقق مما إذا كان الكود موجودًا ولكنه مستخدم بالفعل
                existing_code = await self.premium_codes_col.find_one({'_id': code_upper})
                if existing_code:
                    logger.warning(f"Code '{code_upper}' found but already used by user {existing_code.get('used_by_user_id')} at {existing_code.get('redeemed_at')}.")
                else:
                    logger.warning(f"Code '{code_upper}' not found in the database.")
                return None
        except Exception as e:
            logger.error(f"Error finding premium code '{code_upper}': {e}", exc_info=True)
            return None

    async def mark_coupon_used(self, code: str, user_id: int) -> bool:
        """Marks a premium code as used by a specific user if it's unused."""
        code_upper = code.strip().upper()
        now_utc = datetime.datetime.now(timezone.utc)
        update_data = {
            '$set': {
                'is_used': True,
                'used_by_user_id': int(user_id),
                'redeemed_at': now_utc
            }
        }
        try:
            # ابحث عن الكود الذي يحمل المعرف وماركته كـ false، وقم بتحديثه
            result = await self.premium_codes_col.update_one(
                {'_id': code_upper, 'is_used': False}, # الشرط: يجب أن يكون غير مستخدم
                update_data
            )
            # التحقق من نتيجة التحديث
            if result.modified_count == 1:
                logger.info(f"Marked code '{code_upper}' used by {user_id} at {now_utc.isoformat()}")
                return True
            elif result.matched_count == 1: # تم العثور على الكود ولكن لم يتحدث (is_used كان true بالفعل)
                logger.warning(f"Code '{code_upper}' matched but not modified (likely already used).")
                return False
            else: # لم يتم العثور على الكود أصلاً بالشرط (is_used: False)
                logger.warning(f"Code '{code_upper}' not found or already used when attempting to mark.")
                return False
        except Exception as e:
            logger.error(f"Error marking code '{code_upper}' used for {user_id}: {e}", exc_info=True)
            return False

# --- Initialize Database Instance ---
# تأكد من تعيين متغيرات البيئة هذه بشكل صحيح
DB_URL = os.environ.get("MONGODB_URI" ,"mongodb+srv://mrhex86:mrhex86@cluster0.8pxiirj.mongodb.net/?retryWrites=true&w=majority")
DB_NAME = os.environ.get("MONGODB_DATABASE_NAME", "dx4") # اسم قاعدة البيانات الافتراضي

if not DB_URL:
    logger.critical("CRITICAL: MONGODB_URI environment variable not set. Cannot connect to database.")
    # يمكنك إما الخروج من البرنامج هنا أو السماح له بالمتابعة والاعتماد على معالجة الأخطاء في __init__
    # sys.exit(1)
    # أو ارفع خطأ مخصص
    raise ValueError("MONGODB_URI environment variable is required.")

# <<-- تأكد من أن الكائن يتم إنشاؤه بعد تعريف الكلاس بالكامل -->>
# وإنشاء مثيل واحد يمكن استيراده في main.py
try:
    digital_botz = Database(DB_URL, DB_NAME)
except ConnectionError as e:
     # إذا فشل الاتصال في __init__، سيتم التقاط الخطأ هنا
     logger.critical(f"Failed to initialize database object due to connection error: {e}")
     # يمكنك إعادة طرح الخطأ أو الخروج لضمان عدم تشغيل البوت بدون قاعدة بيانات
     raise e
except ValueError as e: # يلتقط خطأ MONGODB_URI المفقود
     logger.critical(str(e))
     raise e
except Exception as e:
     logger.critical(f"An unexpected error occurred during database object initialization: {e}", exc_info=True)
     raise e


# --- End of database.py ---
