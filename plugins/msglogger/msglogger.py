# encoding:utf-8

import json
import os
import sqlite3
import time
from datetime import datetime

import plugins
from bridge.context import Context, ContextType
from bridge.reply import Reply, ReplyType
from channel.chat_message import ChatMessage
from common.log import logger
from plugins import *
from config import conf

# 添加MySQL依赖
import pymysql


@plugins.register(
    name="MsgLogger",
    desire_priority=0,
    hidden=False,
    desc="A plugin that logs all messages and replies to database",
    version="0.2",
    author="AI Assistant",
)
class MsgLogger(Plugin):
    def __init__(self):
        super().__init__()
        try:
            self.config = super().load_config()
            if not self.config:
                self.config = self._load_config_template()
                
            # 初始化数据库连接
            self.use_mysql = self.config.get("use_mysql", False)
            
            if self.use_mysql:
                # MySQL配置
                self.mysql_config = self.config.get("mysql", {
                    "host": "localhost",
                    "port": 3306,
                    "user": "root",
                    "password": "password",
                    "database": "chatbot_logs"
                })
                
                # 初始化MySQL数据库
                self._init_mysql_db()
            else:
                # SQLite配置
                self.db_path = self.config.get("db_path", "message_logs.db")
                if not os.path.isabs(self.db_path):
                    self.db_path = os.path.join(self.path, self.db_path)
                    
                # 初始化SQLite数据库
                self._init_sqlite_db()
            
            # 注册事件处理函数
            self.handlers[Event.ON_RECEIVE_MESSAGE] = self.on_receive_message
            self.handlers[Event.ON_SEND_REPLY] = self.on_send_reply
            
            if self.use_mysql:
                logger.info(f"[MsgLogger] 初始化完成，使用MySQL数据库: {self.mysql_config['host']}:{self.mysql_config['port']}/{self.mysql_config['database']}")
            else:
                logger.info(f"[MsgLogger] 初始化完成，使用SQLite数据库: {self.db_path}")
        except Exception as e:
            logger.error(f"[MsgLogger] 初始化异常: {e}")
            raise Exception(f"[MsgLogger] 初始化失败: {e}")
    
    def _init_sqlite_db(self):
        """初始化SQLite数据库表结构"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 创建消息表，添加话题相关字段和AI回复触发标记
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            user_id TEXT,
            user_nickname TEXT,
            content TEXT,
            msg_type TEXT,
            is_group BOOLEAN,
            group_id TEXT,
            group_name TEXT,
            timestamp INTEGER,
            time TEXT,
            needs_processing BOOLEAN DEFAULT 0,
            parent_id TEXT DEFAULT NULL,
            root_id TEXT DEFAULT NULL,
            thread_id TEXT DEFAULT NULL,
            ai_replied BOOLEAN DEFAULT 0
        )
        ''')
        
        # 为新字段添加索引
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_parent_id ON messages(parent_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_root_id ON messages(root_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_thread_id ON messages(thread_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_ai_replied ON messages(ai_replied)')
        
        # 创建回复表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS replies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            content TEXT,
            input_content TEXT,
            reply_type TEXT,
            receiver_id TEXT,
            timestamp INTEGER,
            time TEXT,
            related_msg_id INTEGER,
            FOREIGN KEY (related_msg_id) REFERENCES messages (id)
        )
        ''')
        
        conn.commit()
        conn.close()
    
    def _init_mysql_db(self):
        """初始化MySQL数据库表结构"""
        try:
            # 连接到MySQL服务器
            conn = pymysql.connect(
                host=self.mysql_config["host"],
                port=self.mysql_config["port"],
                user=self.mysql_config["user"],
                password=self.mysql_config["password"]
            )
            cursor = conn.cursor()
            
            # 创建数据库（如果不存在）
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.mysql_config['database']} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            cursor.execute(f"USE {self.mysql_config['database']}")
            
            # 创建消息表，添加话题相关字段和AI回复触发标记
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS messages (
                id INT AUTO_INCREMENT PRIMARY KEY,
                session_id VARCHAR(255) NOT NULL,
                user_id VARCHAR(255),
                user_nickname VARCHAR(255),
                content TEXT,
                msg_type VARCHAR(50),
                is_group BOOLEAN,
                group_id VARCHAR(255),
                group_name VARCHAR(255),
                timestamp INT,
                time VARCHAR(50),
                needs_processing BOOLEAN DEFAULT 0,
                parent_id VARCHAR(255) DEFAULT NULL COMMENT '父消息ID（话题回复时使用）',
                root_id VARCHAR(255) DEFAULT NULL COMMENT '根消息ID（话题回复时使用）',
                thread_id VARCHAR(255) DEFAULT NULL COMMENT '话题线程ID（话题回复时使用）',
                ai_replied BOOLEAN DEFAULT 0 COMMENT '是否触发了AI回复',
                INDEX idx_session_id (session_id),
                INDEX idx_timestamp (timestamp),
                INDEX idx_needs_processing (needs_processing),
                INDEX idx_parent_id (parent_id),
                INDEX idx_root_id (root_id),
                INDEX idx_thread_id (thread_id),
                INDEX idx_ai_replied (ai_replied)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ''')
            
            # 创建回复表
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS replies (
                id INT AUTO_INCREMENT PRIMARY KEY,
                session_id VARCHAR(255) NOT NULL,
                content TEXT,
                input_content TEXT,
                reply_type VARCHAR(50),
                receiver_id VARCHAR(255),
                timestamp INT,
                time VARCHAR(50),
                related_msg_id INT,
                INDEX idx_session_id (session_id),
                INDEX idx_timestamp (timestamp),
                FOREIGN KEY (related_msg_id) REFERENCES messages (id) ON DELETE SET NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ''')
            
            conn.commit()
            conn.close()
            logger.info(f"[MsgLogger] MySQL数据库初始化成功")
        except Exception as e:
            logger.error(f"[MsgLogger] MySQL数据库初始化失败: {e}")
            raise
    
    def _get_mysql_connection(self):
        """获取MySQL连接"""
        return pymysql.connect(
            host=self.mysql_config["host"],
            port=self.mysql_config["port"],
            user=self.mysql_config["user"],
            password=self.mysql_config["password"],
            database=self.mysql_config["database"],
            charset='utf8mb4'
        )
    
    def on_receive_message(self, e_context: EventContext):
        """记录接收到的消息"""
        context = e_context["context"]
        
        # 无论是否有 context，都记录基本信息
        timestamp = int(time.time())
        time_str = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
        
        try:
            # 默认值
            session_id = ""
            user_id = ""
            user_nickname = ""
            content = ""
            msg_type = "UNKNOWN"
            is_group = False
            group_id = ""
            group_name = ""
            needs_processing = False
            parent_id = None
            root_id = None
            thread_id = None
            
            # 如果有 context，提取详细信息
            if context:
                session_id = context.get("session_id", "")
                msg_type = str(context.type) if context.type else "UNKNOWN"
                is_group = context.get("isgroup", False)
                content = context.content if context.content else ""
                
                # 标记需要处理的消息
                needs_processing = True
                
                # 如果有 msg 对象，提取更多信息
                msg = context.get("msg")
                if msg:
                    user_id = getattr(msg, 'from_user_id', '')
                    user_nickname = getattr(msg, 'from_user_nickname', '')
                    if is_group:
                        group_id = getattr(msg, 'other_user_id', '')
                        group_name = getattr(msg, 'other_user_nickname', '')
                    
                    # 提取话题相关信息（飞书消息特有）
                    if hasattr(msg, 'raw_message') and msg.raw_message:
                        raw_msg = msg.raw_message
                        if isinstance(raw_msg, dict):
                            event_data = raw_msg.get('event', {})
                            message_data = event_data.get('message', {})
                            
                            # 提取话题相关字段
                            parent_id = message_data.get('parent_id')
                            root_id = message_data.get('root_id')
                            thread_id = message_data.get('thread_id')
            
            if self.use_mysql:
                # 使用MySQL记录
                conn = self._get_mysql_connection()
                cursor = conn.cursor()
                
                cursor.execute(
                    "INSERT INTO messages (session_id, user_id, user_nickname, content, msg_type, is_group, group_id, group_name, timestamp, time, needs_processing, parent_id, root_id, thread_id, ai_replied) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        session_id,
                        user_id,
                        user_nickname,
                        content,
                        msg_type,
                        is_group,
                        group_id,
                        group_name,
                        timestamp,
                        time_str,
                        needs_processing,
                        parent_id,
                        root_id,
                        thread_id,
                        False  # ai_replied 初始为 False
                    )
                )
                
                # 获取插入的消息ID
                cursor.execute("SELECT LAST_INSERT_ID()")
                msg_id = cursor.fetchone()[0]
                if context:
                    context["db_msg_id"] = msg_id
            else:
                # 使用SQLite记录
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                
                cursor.execute(
                    "INSERT INTO messages (session_id, user_id, user_nickname, content, msg_type, is_group, group_id, group_name, timestamp, time, needs_processing, parent_id, root_id, thread_id, ai_replied) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        session_id,
                        user_id,
                        user_nickname,
                        content,
                        msg_type,
                        is_group,
                        group_id,
                        group_name,
                        timestamp,
                        time_str,
                        needs_processing,
                        parent_id,
                        root_id,
                        thread_id,
                        False  # ai_replied 初始为 False
                    )
                )
                
                # 获取插入的消息ID
                msg_id = cursor.lastrowid
                if context:
                    context["db_msg_id"] = msg_id
            
            conn.commit()
            conn.close()
            
            logger.debug(f"[MsgLogger] 记录消息: {content}, ID: {msg_id}, 需要处理: {needs_processing}, 话题信息: parent_id={parent_id}, root_id={root_id}, thread_id={thread_id}")
        except Exception as e:
            logger.error(f"[MsgLogger] 记录消息异常: {e}")

    def on_send_reply(self, e_context: EventContext):
        """记录发送的回复"""
        context = e_context["context"]
        reply = e_context["reply"]
        
        if not context or not reply:
            return
            
        try:
            timestamp = int(time.time())
            time_str = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
            
            # 获取回复内容，根据不同类型处理
            content = ""
            if reply.type == ReplyType.TEXT:
                content = reply.content
            elif reply.type == ReplyType.ERROR or reply.type == ReplyType.INFO:
                # 去除错误前缀，只保留实际内容
                content = reply.content
                # 处理各种可能的错误前缀格式
                prefixes = ["[ERROR] ", "[INFO] ", "ERROR: ", "INFO: "]
                for prefix in prefixes:
                    # 循环处理，确保多重前缀也能被清除
                    while content.startswith(prefix):
                        content = content.replace(prefix, "", 1)
                # 去除开头可能的空白字符
                content = content.strip()
            elif reply.type == ReplyType.IMAGE_URL:
                content = f"[图片] {reply.content}"
            elif reply.type == ReplyType.VOICE:
                content = "[语音]"
            else:
                content = f"[{reply.type}]"
            
            # 获取输入内容
            input_content = ""
            if hasattr(context, 'content') and context.content:
                input_content = context.content
            elif isinstance(context, dict) and "content" in context:
                input_content = context["content"]
            
            # 更新原消息的 ai_replied 字段为 True
            msg_id = context.get("db_msg_id")
            if msg_id:
                if self.use_mysql:
                    conn = self._get_mysql_connection()
                    cursor = conn.cursor()
                    cursor.execute("UPDATE messages SET ai_replied = %s WHERE id = %s", (True, msg_id))
                else:
                    conn = sqlite3.connect(self.db_path)
                    cursor = conn.cursor()
                    cursor.execute("UPDATE messages SET ai_replied = ? WHERE id = ?", (True, msg_id))
                
                conn.commit()
                conn.close()
            
            # 记录回复
            if self.use_mysql:
                # 使用MySQL记录
                conn = self._get_mysql_connection()
                cursor = conn.cursor()
                
                cursor.execute(
                    "INSERT INTO replies (session_id, content, input_content, reply_type, receiver_id, timestamp, time, related_msg_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        context.get("session_id", ""),
                        content,
                        input_content,
                        str(reply.type),
                        context.get("receiver", ""),
                        timestamp,
                        time_str,
                        context.get("db_msg_id", None)
                    )
                )
            else:
                # 使用SQLite记录
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                
                cursor.execute(
                    "INSERT INTO replies (session_id, content, input_content, reply_type, receiver_id, timestamp, time, related_msg_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        context.get("session_id", ""),
                        content,
                        input_content,
                        str(reply.type),
                        context.get("receiver", ""),
                        timestamp,
                        time_str,
                        context.get("db_msg_id", None)
                    )
                )
            
            conn.commit()
            conn.close()
            
            logger.debug(f"[MsgLogger] 记录回复: {content}, 输入内容: {input_content}, 已标记消息ID {msg_id} 为AI已回复")
        except Exception as e:
            logger.error(f"[MsgLogger] 记录回复异常: {e}")
    
    def get_help_text(self, **kwargs):
        help_text = "消息记录插件：记录所有消息和回复到数据库中，无需用户交互。\n"
        if self.use_mysql:
            help_text += "当前使用MySQL数据库存储。"
        else:
            help_text += "当前使用SQLite数据库存储。"
        return help_text
        
    def _load_config_template(self):
        """加载配置模板"""
        logger.debug("No MsgLogger plugin config.json, use plugins/msglogger/config.json.template")
        try:
            plugin_config_path = os.path.join(self.path, "config.json.template")
            if os.path.exists(plugin_config_path):
                with open(plugin_config_path, "r", encoding="utf-8") as f:
                    plugin_conf = json.load(f)
                    return plugin_conf
            return {"db_path": "message_logs.db", "use_mysql": False}
        except Exception as e:
            logger.exception(e)
            return {"db_path": "message_logs.db", "use_mysql": False}

