from datetime import datetime
from common.expired_dict import ExpiredDict
from common.log import logger
from config import conf
from plugins.event import EventContext, EventAction
from bridge.reply import Reply, ReplyType


class Session(object):
    def __init__(self, session_id, system_prompt=None):
        self.session_id = session_id
        self.messages = []
        if system_prompt is None:
            self.system_prompt = conf().get("character_desc", "")
        else:
            self.system_prompt = system_prompt

    # 重置会话
    def reset(self):
        system_item = {"role": "system", "content": self.system_prompt}
        self.messages = [system_item]

    def set_system_prompt(self, system_prompt):
        self.system_prompt = system_prompt
        self.reset()

    def add_query(self, query):
        user_item = {"role": "user", "content": query}
        self.messages.append(user_item)

    def add_reply(self, reply):
        assistant_item = {"role": "assistant", "content": reply}
        self.messages.append(assistant_item)

    def discard_exceeding(self, max_tokens=None, cur_tokens=None):
        raise NotImplementedError

    def calc_tokens(self):
        raise NotImplementedError


class SessionManager(object):
    def __init__(self, sessioncls, **session_args):
        if conf().get("expires_in_seconds"):
            sessions = ExpiredDict(conf().get("expires_in_seconds"))
        else:
            sessions = dict()
        self.sessions = sessions
        self.sessioncls = sessioncls
        self.session_args = session_args

    def build_session(self, session_id, system_prompt=None):
        """
        如果session_id不在sessions中，创建一个新的session并添加到sessions中
        如果system_prompt不会空，会更新session的system_prompt并重置session
        """
        if session_id is None:
            return self.sessioncls(session_id, system_prompt, **self.session_args)

        if session_id not in self.sessions:
            self.sessions[session_id] = self.sessioncls(session_id, system_prompt, **self.session_args)
        elif system_prompt is not None:  # 如果有新的system_prompt，更新并重置session
            self.sessions[session_id].set_system_prompt(system_prompt)
        session = self.sessions[session_id]
        return session

    def session_query(self, query, session_id):
        session = self.build_session(session_id)
        session.add_query(query)
        try:
            max_tokens = conf().get("conversation_max_tokens", 1000)
            total_tokens = session.discard_exceeding(max_tokens, None)
            logger.debug("prompt tokens used={}".format(total_tokens))
        except Exception as e:
            logger.warning("Exception when counting tokens precisely for prompt: {}".format(str(e)))
        return session

    def session_reply(self, reply, session_id, total_tokens=None):
        session = self.build_session(session_id)
        session.add_reply(reply)
        try:
            max_tokens = conf().get("conversation_max_tokens", 1000)
            tokens_cnt = session.discard_exceeding(max_tokens, total_tokens)
            logger.debug("raw total_tokens={}, savesession tokens={}".format(total_tokens, tokens_cnt))
        except Exception as e:
            logger.warning("Exception when counting tokens precisely for session: {}".format(str(e)))
        return session

    def clear_session(self, session_id):
        if session_id in self.sessions:
            del self.sessions[session_id]

    def clear_all_session(self):
        self.sessions.clear()

    def on_handle_context(self, e_context: EventContext):
        context = e_context['context']
        content = context.content
        
        # 处理 #sessions 命令
        if content.startswith('#sessions'):
            try:
                self._handle_session_command(e_context)
                e_context.action = EventAction.BREAK_PASS
            except Exception as e:
                logger.error(f"Session command error: {e}")
                reply = Reply(ReplyType.ERROR, "会话命令处理失败")
                e_context['reply'] = reply
                e_context.action = EventAction.BREAK_PASS
        else:
            # 排除 #命令类 和 $插件命令类，不进入session管理
            if content.startswith('#') or content.startswith('$'):
                # 对于系统命令和插件命令，不进行session管理
                return
                
            # 对于普通消息，检查是否需要自动创建会话
            user_id = context.kwargs.get('session_id', '')
            if user_id:
                self._ensure_user_has_session(user_id)
                
    def _ensure_user_has_session(self, user_id: str):
        """确保用户至少有一个会话记录"""
        try:
            sessions = self.persistent_manager.get_user_sessions(user_id)
            if not sessions:
                # 自动创建默认会话
                session_id = self.persistent_manager.create_new_session(
                    user_id, 
                    title=f"对话 {datetime.now().strftime('%m-%d %H:%M')}"
                )
                logger.info(f"[SessionManager] 为用户 {user_id} 自动创建会话: {session_id}")
        except Exception as e:
            logger.error(f"[SessionManager] 自动创建会话失败: {e}")

    def _list_sessions(self, user_id: str) -> str:
        """列出用户的历史会话"""
        try:
            logger.info(f"[SessionManager] 开始获取用户会话列表，user_id: {user_id}")
            sessions = self.persistent_manager.get_user_sessions(user_id)
            logger.info(f"[SessionManager] 获取到 {len(sessions)} 个会话")
            
            if not sessions:
                return "暂无历史会话。\n\n使用 '#sessions new [标题]' 创建新会话"
            
            result = "📋 历史会话列表：\n\n"
            for i, session in enumerate(sessions, 1):
                # 安全地获取字段值并转换为字符串
                title = str(session.get('title', '未命名会话'))
                session_id = str(session.get('id', ''))
                message_count = str(session.get('message_count', 0))
                updated_at = str(session.get('updated_at', ''))
                
                logger.debug(f"[SessionManager] 处理会话 {i}: title={title}, id={session_id}, count={message_count}")
                
                # 使用字符串拼接避免格式化问题
                session_line = str(i) + ". " + title + " (ID: " + session_id + ")\n"
                session_line += "   💬 " + message_count + " 条消息 | 📅 " + updated_at + "\n\n"
                result += session_line
            
            result += "💡 使用说明：\n"
            result += "• '#sessions activate <ID>' 激活会话\n"
            result += "• '#sessions new [标题]' 创建新会话\n"
            result += "• '#sessions delete <ID>' 删除会话"
            
            logger.info(f"[SessionManager] 会话列表生成完成，长度: {len(result)}")
            return result
            
        except Exception as e:
            logger.error(f"[SessionManager] 列出会话失败: {e}")
            import traceback
            logger.error(f"[SessionManager] 错误详情: {traceback.format_exc()}")
            return f"获取会话列表失败: {str(e)}"
