import datetime
from typing import Optional, Dict, Any
from motor.motor_asyncio import AsyncIOMotorClient
from WebStreamer.vars import Var

import logging

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, uri: str, database_name: str):
        self._client: AsyncIOMotorClient = AsyncIOMotorClient(uri)
        self.db = self._client[database_name]
        self.col = self.db.users
        self.banned_users_col = self.db.banned_users

    async def ensure_indexes(self):
        """Create database indexes for better performance"""
        try:
            await self.col.create_index("id", unique=True)
            await self.banned_users_col.create_index("user_id", unique=True)
            logger.info("Database indexes ensured.")
        except Exception as e:
            logger.error(f"Error in ensure_indexes: {e}", exc_info=True)
            raise

    def new_user(self, user_id: int) -> dict:
        """Create a new user document"""
        return {
            'id': user_id,
            'join_date': datetime.datetime.utcnow()
        }

    async def add_user(self, user_id: int) -> bool:
        """Add a new user to the database"""
        try:
            if not await self.is_user_exist(user_id):
                await self.col.insert_one(self.new_user(user_id))
                logger.info(f"Added new user {user_id} to database.")
                return True
            return False
        except Exception as e:
            logger.error(f"Error in add_user for user {user_id}: {e}", exc_info=True)
            raise

    async def is_user_exist(self, user_id: int) -> bool:
        """Check if a user exists in the database"""
        try:
            user = await self.col.find_one({'id': user_id}, {'_id': 1})
            return bool(user)
        except Exception as e:
            logger.error(f"Error in is_user_exist for user {user_id}: {e}", exc_info=True)
            raise

    async def total_users_count(self) -> int:
        """Get total number of users"""
        try:
            return await self.col.count_documents({})
        except Exception as e:
            logger.error(f"Error in total_users_count: {e}", exc_info=True)
            return 0

    def get_all_users(self):
        """Get cursor for all users"""
        return self.col.find({})

    async def delete_user(self, user_id: int) -> bool:
        """Delete a user from the database"""
        try:
            result = await self.col.delete_one({'id': user_id})
            if result.deleted_count > 0:
                logger.info(f"Deleted user {user_id}.")
                return True
            return False
        except Exception as e:
            logger.error(f"Error in delete_user for user {user_id}: {e}", exc_info=True)
            raise

    async def get_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Get user document by user_id"""
        try:
            return await self.col.find_one({'id': user_id})
        except Exception as e:
            logger.error(f"Error in get_user for user {user_id}: {e}", exc_info=True)
            return None

    # Banned users methods
    async def add_banned_user(
        self, user_id: int, banned_by: Optional[int] = None,
        reason: Optional[str] = None
    ) -> None:
        """Add or update a banned user"""
        try:
            ban_data = {
                "user_id": user_id,
                "banned_at": datetime.datetime.utcnow(),
                "banned_by": banned_by,
                "reason": reason
            }
            await self.banned_users_col.update_one(
                {"user_id": user_id},
                {"$set": ban_data},
                upsert=True
            )
            logger.info(f"Added/Updated banned user {user_id}. Reason: {reason}")
        except Exception as e:
            logger.error(f"Error in add_banned_user for user {user_id}: {e}", exc_info=True)
            raise

    async def remove_banned_user(self, user_id: int) -> bool:
        """Remove a user from banned list"""
        try:
            result = await self.banned_users_col.delete_one({"user_id": user_id})
            if result.deleted_count > 0:
                logger.info(f"Removed banned user {user_id}.")
                return True
            return False
        except Exception as e:
            logger.error(f"Error in remove_banned_user for user {user_id}: {e}", exc_info=True)
            return False

    async def is_user_banned(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Check if a user is banned, returns ban info if banned"""
        try:
            return await self.banned_users_col.find_one({"user_id": user_id})
        except Exception as e:
            logger.error(f"Error in is_user_banned for user {user_id}: {e}", exc_info=True)
            return None

    async def get_all_banned_users(self):
        """Get cursor for all banned users"""
        return self.banned_users_col.find({})

    async def total_banned_users_count(self) -> int:
        """Get total number of banned users"""
        try:
            return await self.banned_users_col.count_documents({})
        except Exception as e:
            logger.error(f"Error in total_banned_users_count: {e}", exc_info=True)
            return 0

    async def close(self):
        """Close the database connection"""
        if self._client:
            self._client.close()
            logger.info("Database connection closed.")


# Initialize database instance
db = Database(Var.DATABASE_URL, Var.DATABASE_NAME)
