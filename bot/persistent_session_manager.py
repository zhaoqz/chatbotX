import uuid
import json
from datetime import datetime
from typing import List, Dict, Optional
from bot.session_manager import Session, SessionManager
from common.log import logger
from config import conf

class PersistentSession(Session):
    def __init__(self, session_id, system_prompt=None, title=None, db_manager=None):
        super().__init__(session_id, system_prompt)
        self.title = title or "新对话"
        self.db_manager = db_manager
        self.is_loaded = False
        
    def load_from_db(self):
        """从数据库加载会话历史"""
        if self.db_manager and not self.is_loaded:
            messages = self.db_manager.get_session_messages(self.session_id)
            self.messages = messages
            self.is_loaded = True
            
    def save_to_db(self):
        """保存会话到数据库"""
        if self.db_manager:
            self.db_manager.save_session(self)
            
    def add_query(self, query):
        super().add_query(query)
        self.save_to_db()
        
    def add_reply(self, reply):
        super().add_reply(reply)
        self.save_to_db()
        
    def update_title(self, title):
        """更新会话标题"""
        self.title = title
        if self.db_manager:
            self.db_manager.update_session_title(self.session_id, title)

class DatabaseManager:
    def __init__(self, db_config: Dict):
        self.db_config = db_config
        self.db_type = 'mysql'  # 固定为MySQL
        
        import pymysql
        self.conn = pymysql.connect(**self.db_config.get('mysql', {}))
        self._create_tables_mysql()
        self.placeholder = '%s'
        
        # 添加连接状态检查
        try:
            cursor = self.conn.cursor()
            cursor.execute('SELECT 1')
            logger.info(f"[DatabaseManager] MySQL数据库连接成功")
        except Exception as e:
            logger.error(f"[DatabaseManager] MySQL数据库连接测试失败: {e}")
    
    def get_user_sessions(self, user_id: str, limit: int = 50) -> List[Dict]:
        """获取用户的会话列表"""
        cursor = self.conn.cursor()
        query = f'''
            SELECT id, title, created_at, updated_at, 
                   (SELECT COUNT(*) FROM chat_messages WHERE session_id = chat_sessions.id) as message_count
            FROM chat_sessions 
            WHERE user_id = {self.placeholder} AND is_active = TRUE
            ORDER BY updated_at DESC
            LIMIT {self.placeholder}
        '''
        
        # 添加详细的调试日志
        logger.info(f"[DatabaseManager] 查询用户会话 - user_id: {user_id}, limit: {limit}")
        logger.info(f"[DatabaseManager] 使用数据库类型: {self.db_type}, 占位符: {self.placeholder}")
        logger.debug(f"[DatabaseManager] 执行SQL: {query}")
        logger.debug(f"[DatabaseManager] 参数: ({user_id}, {limit})")
        
        try:
            cursor.execute(query, (user_id, limit))
            rows = cursor.fetchall()
            logger.info(f"[DatabaseManager] SQL执行成功，返回 {len(rows)} 行数据")
            
            sessions = []
            for i, row in enumerate(rows):
                session_data = {
                    'id': row[0],
                    'title': row[1],
                    'created_at': row[2],
                    'updated_at': row[3],
                    'message_count': row[4]
                }
                sessions.append(session_data)
                logger.debug(f"[DatabaseManager] 会话 {i+1}: {session_data}")
            
            logger.info(f"[DatabaseManager] 为用户 {user_id} 找到 {len(sessions)} 个会话")
            return sessions
            
        except Exception as e:
            logger.error(f"[DatabaseManager] 查询会话失败: {e}")
            logger.error(f"[DatabaseManager] SQL: {query}")
            logger.error(f"[DatabaseManager] 参数: ({user_id}, {limit})")
            return []
              
    def _create_tables_mysql(self):
        """创建MySQL表结构"""
        cursor = self.conn.cursor()
        
        # 创建会话表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS chat_sessions (
                id VARCHAR(36) PRIMARY KEY,
                user_id VARCHAR(255) NOT NULL,
                title VARCHAR(500) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE,
                system_prompt TEXT,
                model VARCHAR(100),
                INDEX idx_user_id (user_id),
                INDEX idx_updated_at (updated_at)
            )
        ''')
        
        # 创建消息表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS chat_messages (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                session_id VARCHAR(36) NOT NULL,
                role ENUM('user', 'assistant', 'system') NOT NULL,
                content TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                token_count INT DEFAULT 0,
                INDEX idx_session_id (session_id),
                INDEX idx_created_at (created_at),
                FOREIGN KEY (session_id) REFERENCES chat_sessions(id) ON DELETE CASCADE
            )
        ''')
        
        self.conn.commit()
        logger.info("[PersistentSessionManager] MySQL tables created successfully")
        
    def create_session(self, user_id: str, title: str = None, system_prompt: str = None, model: str = None) -> str:
        """创建新会话"""
        session_id = str(uuid.uuid4())
        
        # 如果没有提供system_prompt，使用默认配置
        if system_prompt is None:
            from config import conf
            system_prompt = conf().get("character_desc", "你是一个有用的AI助手")
        
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO chat_sessions (id, user_id, title, system_prompt, model)
            VALUES (%s, %s, %s, %s, %s)
        ''', (session_id, user_id, title or "新对话", system_prompt, model))
        self.conn.commit()
        
        logger.info(f"[DatabaseManager] 创建新会话 {session_id}，system_prompt: {system_prompt[:50]}...")
        return session_id
        
    def get_session_messages(self, session_id: str) -> List[Dict]:
        """获取会话的消息历史"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT role, content FROM chat_messages 
            WHERE session_id = %s 
            ORDER BY created_at ASC
        ''', (session_id,))
        
        messages = []
        for row in cursor.fetchall():
            messages.append({
                'role': row[0],
                'content': row[1]
            })
        
        return messages
        
    def save_session(self, session: PersistentSession):
        """保存会话消息"""
        try:
            cursor = self.conn.cursor()
            
            # 更新会话的最后修改时间
            cursor.execute('''
                UPDATE chat_sessions 
                SET updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            ''', (session.session_id,))
            
            # 删除现有消息（简单实现，可优化为增量保存）
            cursor.execute('DELETE FROM chat_messages WHERE session_id = %s', (session.session_id,))
            
            # 保存所有消息
            message_count = 0
            for msg in session.messages:
                cursor.execute('''
                    INSERT INTO chat_messages (session_id, role, content)
                    VALUES (%s, %s, %s)
                ''', (session.session_id, msg['role'], msg['content']))
                message_count += 1
                
            self.conn.commit()
            logger.info(f"[DatabaseManager] 成功保存会话 {session.session_id}，共 {message_count} 条消息")
            
        except Exception as e:
            logger.error(f"[DatabaseManager] 保存会话失败 {session.session_id}: {e}")
            self.conn.rollback()
            raise
        
    def update_session_title(self, session_id: str, title: str):
        """更新会话标题"""
        cursor = self.conn.cursor()
        cursor.execute('''
            UPDATE chat_sessions 
            SET title = %s, updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
        ''', (title, session_id))
        self.conn.commit()
        
    def delete_session(self, session_id: str, user_id: str):
        """删除会话（软删除）"""
        cursor = self.conn.cursor()
        cursor.execute('''
            UPDATE chat_sessions 
            SET is_active = FALSE
            WHERE id = %s AND user_id = %s
        ''', (session_id, user_id))
        self.conn.commit()

class PersistentSessionManager(SessionManager):
    def __init__(self, sessioncls, db_config=None, **session_args):
        # 不调用父类的__init__，因为我们要用数据库存储
        self.sessioncls = sessioncls
        self.session_args = session_args
        self.active_sessions = {}  # 内存中的活跃会话
        self.db_manager = DatabaseManager(db_config or {})
        
    def create_new_session(self, user_id: str, title: str = None, system_prompt: str = None) -> str:
        """创建新会话"""
        session_id = self.db_manager.create_session(user_id, title, system_prompt)
        return session_id
        
    def session_query(self, query, session_id):
        """重写父类方法，支持自动创建session"""
        # 对于普通用户消息，session_id通常就是user_id
        user_id = session_id
        session = self.build_session(session_id, user_id=user_id)
        session.add_query(query)
        try:
            max_tokens = conf().get("conversation_max_tokens", 1000)
            total_tokens = session.discard_exceeding(max_tokens, None)
            logger.debug("prompt tokens used={}".format(total_tokens))
        except Exception as e:
            logger.warning("Exception when counting tokens precisely for prompt: {}".format(str(e)))
        return session

    def session_reply(self, reply, session_id, total_tokens=None):
        """重写父类方法，支持自动创建session"""
        # 对于普通用户消息，session_id通常就是user_id
        user_id = session_id
        session = self.build_session(session_id, user_id=user_id)
        session.add_reply(reply)
        try:
            max_tokens = conf().get("conversation_max_tokens", 1000)
            tokens_cnt = session.discard_exceeding(max_tokens, total_tokens)
            logger.debug("raw total_tokens={}, savesession tokens={}".format(total_tokens, tokens_cnt))
        except Exception as e:
            logger.warning("Exception when counting tokens precisely for session: {}".format(str(e)))
        return session
        
    def build_session(self, session_id, system_prompt=None, user_id=None):
        """构建会话，支持从数据库加载，如果用户没有会话则自动创建"""
        if session_id in self.active_sessions:
            return self.active_sessions[session_id]
            
        # 创建新的持久化会话
        session = PersistentSession(session_id, system_prompt, db_manager=self.db_manager)
        
        # 如果是已存在的会话，从数据库加载
        if session_id and self._session_exists(session_id):
            session.load_from_db()
        else:
            # 检查是否是用户ID，如果是则为该用户自动创建默认会话
            if user_id and session_id == user_id:
                # 检查用户是否已有会话
                user_sessions = self.db_manager.get_user_sessions(user_id, limit=1)
                if not user_sessions:
                    # 用户没有任何会话，创建默认会话
                    from datetime import datetime
                    default_title = f"对话 {datetime.now().strftime('%m-%d %H:%M')}"
                    actual_session_id = self.create_new_session(user_id, default_title, system_prompt)
                    logger.info(f"[PersistentSessionManager] 为用户 {user_id} 自动创建默认会话: {actual_session_id}")
                    
                    # 重新创建session对象，使用实际的session_id
                    session = PersistentSession(actual_session_id, system_prompt, default_title, self.db_manager)
                    # 将新创建的session也存储在active_sessions中，使用原始的session_id作为key
                    self.active_sessions[session_id] = session
                    return session
                else:
                    # 用户已有会话，使用最新的会话
                    latest_session = user_sessions[0]
                    actual_session_id = latest_session['id']
                    session = PersistentSession(actual_session_id, system_prompt, latest_session.get('title'), self.db_manager)
                    session.load_from_db()
                    logger.info(f"[PersistentSessionManager] 为用户 {user_id} 使用最新会话: {actual_session_id}")
            else:
                # 新会话，初始化系统提示
                if system_prompt:
                    session.reset()
                
        self.active_sessions[session_id] = session
        return session
        
    def _session_exists(self, session_id: str) -> bool:
        """检查会话是否存在于数据库中"""
        cursor = self.db_manager.conn.cursor()
        cursor.execute('SELECT 1 FROM chat_sessions WHERE id = %s AND is_active = TRUE', (session_id,))
        return cursor.fetchone() is not None
        
    def get_user_sessions(self, user_id: str) -> List[Dict]:
        """获取用户的历史会话列表"""
        return self.db_manager.get_user_sessions(user_id)
        
    def activate_session(self, session_id: str, user_id: str):
        """激活历史会话"""
        # 验证会话属于该用户
        cursor = self.db_manager.conn.cursor()
        
        # 如果session_id长度小于完整UUID，使用LIKE查询
        if len(session_id) < 36:  # 完整UUID长度为36
            cursor.execute('SELECT id FROM chat_sessions WHERE id LIKE %s AND user_id = %s AND is_active = TRUE', 
                          (f'{session_id}%', user_id))
            result = cursor.fetchone()
            if not result:
                return None
            # 使用找到的完整ID
            full_session_id = result[0]
        else:
            cursor.execute('SELECT 1 FROM chat_sessions WHERE id = %s AND user_id = %s AND is_active = TRUE', 
                          (session_id, user_id))
            if not cursor.fetchone():
                return None
            full_session_id = session_id
            
        return self.build_session(full_session_id, user_id=user_id)
        
    def clear_session(self, session_id):
        """清理会话"""
        if session_id in self.active_sessions:
            del self.active_sessions[session_id]
            
    def clear_all_session(self):
        """清理所有会话"""
        self.active_sessions.clear()
        
    def check_connection(self):
        """检查数据库连接状态"""
        try:
            cursor = self.conn.cursor()
            cursor.execute('SELECT 1')
            return True
        except Exception as e:
            logger.error(f"[DatabaseManager] 数据库连接检查失败: {e}")
            return False
            
    def reconnect_if_needed(self):
        """如果连接断开则重新连接"""
        if not self.check_connection():
            logger.info("[DatabaseManager] 尝试重新连接MySQL数据库")
            import pymysql
            self.conn = pymysql.connect(**self.db_config.get('mysql', {}))