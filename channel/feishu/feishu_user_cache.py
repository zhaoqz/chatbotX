import json
import os
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import pymysql
from common.log import logger

class FeishuUserCache:
    def __init__(self):
        self.db_config = self._load_db_config()
        self.init_database()
    
    def _load_db_config(self):
        """从roleX插件配置中加载数据库配置"""
        config_path = os.path.join(os.path.dirname(__file__), "../../plugins/roleX/config.json")
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                return config.get('database', {})
        except FileNotFoundError:
            logger.warning(f"[FeishuUserCache] 配置文件未找到: {config_path}，使用默认配置")
            return {
                'host': 'localhost',
                'port': 3306,
                'user': 'root',
                'password': '',
                'database': 'chatbot'
            }
        except Exception as e:
            logger.error(f"[FeishuUserCache] 加载配置文件失败: {e}")
            raise e
    
    def _get_connection(self):
        """获取MySQL连接"""
        return pymysql.connect(
            host=self.db_config["host"],
            port=self.db_config["port"],
            user=self.db_config["user"],
            password=self.db_config["password"],
            database=self.db_config["database"],
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
    
    def get_connection(self):
        """获取MySQL连接（公有方法）"""
        return self._get_connection()
    
    def init_database(self):
        """初始化数据库表"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS feishu_user_cache (
                id INT AUTO_INCREMENT PRIMARY KEY,
                open_id VARCHAR(255) NOT NULL,
                union_id VARCHAR(255),
                user_id VARCHAR(255),
                tenant_key VARCHAR(255) NOT NULL,
                sender_type VARCHAR(50) NOT NULL DEFAULT 'user',
                id_type VARCHAR(50) DEFAULT 'open_id',
                name VARCHAR(255),
                en_name VARCHAR(255),
                nickname VARCHAR(255),
                email VARCHAR(255),
                mobile VARCHAR(50),
                mobile_visible TINYINT(1) DEFAULT 0,
                gender TINYINT(1),
                city VARCHAR(255),
                country VARCHAR(255),
                work_station VARCHAR(255),
                job_title VARCHAR(255),
                employee_no VARCHAR(255),
                employee_type TINYINT(1),
                geo VARCHAR(255),
                avatar_72 TEXT,
                avatar_240 TEXT,
                avatar_640 TEXT,
                avatar_origin TEXT,
                is_frozen TINYINT(1) DEFAULT 0,
                is_resigned TINYINT(1) DEFAULT 0,
                is_activated TINYINT(1) DEFAULT 1,
                is_exited TINYINT(1) DEFAULT 0,
                is_unjoin TINYINT(1) DEFAULT 0,
                is_tenant_manager TINYINT(1) DEFAULT 0,
                department_ids TEXT,
                leader_user_id VARCHAR(255),
                dotted_line_leader_user_ids TEXT,
                job_level_id VARCHAR(255),
                job_family_id VARCHAR(255),
                join_time BIGINT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                last_api_call TIMESTAMP NULL,
                cache_expire_time TIMESTAMP NULL,
                UNIQUE KEY unique_user_tenant (open_id, tenant_key)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        ''')
        
        # 同时添加群聊缓存表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS feishu_group_cache (
                id INT AUTO_INCREMENT PRIMARY KEY,
                chat_id VARCHAR(255) NOT NULL UNIQUE,
                name VARCHAR(255),
                description TEXT,
                owner_id VARCHAR(255),
                chat_mode VARCHAR(50),
                chat_type VARCHAR(50),
                chat_tag VARCHAR(50),
                member_count INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                cache_expire_time TIMESTAMP NULL,
                INDEX idx_chat_id (chat_id),
                INDEX idx_cache_expire (cache_expire_time)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        ''')
        
        conn.commit()
        cursor.close()
        conn.close()
        logger.info("[FeishuUserCache] 数据库初始化完成")
    
    def get_user_info(self, open_id: str, tenant_key: str) -> Optional[Dict[str, Any]]:
        """从缓存获取用户信息"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM feishu_user_cache 
            WHERE open_id = %s AND tenant_key = %s
            AND (cache_expire_time IS NULL OR cache_expire_time > NOW())
        ''', (open_id, tenant_key))
        
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        
        return row
    
    def save_user_info(self, user_data: Dict[str, Any], sender_info: Dict[str, Any]) -> bool:
        """保存用户信息到缓存"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # 提取用户信息
            user = user_data.get('user', {})
            avatar = user.get('avatar', {})
            status = user.get('status', {})
            
            # 计算缓存过期时间（240小时后）
            expire_time = datetime.now() + timedelta(hours=240)
            
            cursor.execute('''
                INSERT INTO feishu_user_cache (
                    open_id, union_id, user_id, tenant_key, sender_type, id_type,
                    name, en_name, nickname, email, mobile, mobile_visible,
                    gender, city, country, work_station, job_title, employee_no,
                    employee_type, geo, avatar_72, avatar_240, avatar_640, avatar_origin,
                    is_frozen, is_resigned, is_activated, is_exited, is_unjoin, is_tenant_manager,
                    department_ids, leader_user_id, dotted_line_leader_user_ids,
                    job_level_id, job_family_id, join_time, last_api_call, cache_expire_time
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON DUPLICATE KEY UPDATE
                    union_id = VALUES(union_id),
                    user_id = VALUES(user_id),
                    sender_type = VALUES(sender_type),
                    id_type = VALUES(id_type),
                    name = VALUES(name),
                    en_name = VALUES(en_name),
                    nickname = VALUES(nickname),
                    email = VALUES(email),
                    mobile = VALUES(mobile),
                    mobile_visible = VALUES(mobile_visible),
                    gender = VALUES(gender),
                    city = VALUES(city),
                    country = VALUES(country),
                    work_station = VALUES(work_station),
                    job_title = VALUES(job_title),
                    employee_no = VALUES(employee_no),
                    employee_type = VALUES(employee_type),
                    geo = VALUES(geo),
                    avatar_72 = VALUES(avatar_72),
                    avatar_240 = VALUES(avatar_240),
                    avatar_640 = VALUES(avatar_640),
                    avatar_origin = VALUES(avatar_origin),
                    is_frozen = VALUES(is_frozen),
                    is_resigned = VALUES(is_resigned),
                    is_activated = VALUES(is_activated),
                    is_exited = VALUES(is_exited),
                    is_unjoin = VALUES(is_unjoin),
                    is_tenant_manager = VALUES(is_tenant_manager),
                    department_ids = VALUES(department_ids),
                    leader_user_id = VALUES(leader_user_id),
                    dotted_line_leader_user_ids = VALUES(dotted_line_leader_user_ids),
                    job_level_id = VALUES(job_level_id),
                    job_family_id = VALUES(job_family_id),
                    join_time = VALUES(join_time),
                    last_api_call = VALUES(last_api_call),
                    cache_expire_time = VALUES(cache_expire_time)
            ''', (
                user.get('open_id'), user.get('union_id'), user.get('user_id'),
                sender_info.get('tenant_key'), sender_info.get('sender_type', 'user'),
                sender_info.get('id_type', 'open_id'),
                user.get('name'), user.get('en_name'), user.get('nickname'),
                user.get('email'), user.get('mobile'), user.get('mobile_visible', False),
                user.get('gender'), user.get('city'), user.get('country'),
                user.get('work_station'), user.get('job_title'), user.get('employee_no'),
                user.get('employee_type'), user.get('geo'),
                avatar.get('avatar_72'), avatar.get('avatar_240'),
                avatar.get('avatar_640'), avatar.get('avatar_origin'),
                status.get('is_frozen', False), status.get('is_resigned', False),
                status.get('is_activated', True), status.get('is_exited', False),
                status.get('is_unjoin', False), user.get('is_tenant_manager', False),
                json.dumps(user.get('department_ids', [])),
                user.get('leader_user_id'),
                json.dumps(user.get('dotted_line_leader_user_ids', [])),
                user.get('job_level_id'), user.get('job_family_id'),
                user.get('join_time'), datetime.now(), expire_time
            ))
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            logger.error(f"[FeishuUserCache] Error saving user info: {e}")
            return False
    
    def get_group_info(self, chat_id: str) -> Optional[Dict[str, Any]]:
        """从缓存获取群聊信息"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM feishu_group_cache 
            WHERE chat_id = %s
            AND (cache_expire_time IS NULL OR cache_expire_time > NOW())
        ''', (chat_id,))
        
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        
        return row
    
    def save_group_info(self, chat_id: str, group_data: Dict[str, Any]) -> bool:
        """保存群聊信息到缓存"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # 计算缓存过期时间（24小时后）
            expire_time = datetime.now() + timedelta(hours=24)
            
            cursor.execute('''
                INSERT INTO feishu_group_cache (
                    chat_id, name, description, owner_id, chat_mode, chat_type, chat_tag, 
                    member_count, cache_expire_time
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    name = VALUES(name),
                    description = VALUES(description),
                    owner_id = VALUES(owner_id),
                    chat_mode = VALUES(chat_mode),
                    chat_type = VALUES(chat_type),
                    chat_tag = VALUES(chat_tag),
                    member_count = VALUES(member_count),
                    cache_expire_time = VALUES(cache_expire_time)
            ''', (
                chat_id,
                group_data.get('name'),
                group_data.get('description'),
                group_data.get('owner_id'),
                group_data.get('chat_mode'),
                group_data.get('chat_type'),
                group_data.get('chat_tag'),
                group_data.get('user_count'),
                expire_time
            ))
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            logger.error(f"[FeishuUserCache] Error saving group info: {e}")
            return False
    
    def clean_expired_cache(self):
        """清理过期缓存"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # 清理用户缓存
        cursor.execute('''
            DELETE FROM feishu_user_cache 
            WHERE cache_expire_time IS NOT NULL AND cache_expire_time < NOW()
        ''')
        user_deleted = cursor.rowcount
        
        # 清理群聊缓存
        cursor.execute('''
            DELETE FROM feishu_group_cache 
            WHERE cache_expire_time IS NOT NULL AND cache_expire_time < NOW()
        ''')
        group_deleted = cursor.rowcount
        
        conn.commit()
        conn.close()
        
        if user_deleted > 0 or group_deleted > 0:
            logger.info(f"[FeishuUserCache] Cleaned {user_deleted} expired user cache entries and {group_deleted} expired group cache entries")