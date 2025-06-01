"""
channel factory
"""
from common import const
from bot.persistent_session_manager import PersistentSessionManager
from config import conf
from common.log import logger  # 添加这行导入

def create_bot(bot_type):
    """
    create a bot_type instance
    """
    # 获取数据库配置和会话持久化配置
    db_config = conf().get("database", {})
    session_config = conf().get("session_persistence", {})
    
    # 创建Bot实例
    bot = None
    if bot_type == const.BAIDU:
        from bot.baidu.baidu_wenxin import BaiduWenxinBot
        bot = BaiduWenxinBot()
    elif bot_type == const.CHATGPT:
        from bot.chatgpt.chat_gpt_bot import ChatGPTBot
        bot = ChatGPTBot()
    elif bot_type == const.OPEN_AI:
        from bot.openai.open_ai_bot import OpenAIBot
        bot = OpenAIBot()
    elif bot_type == const.CHATGPTONAZURE:
        from bot.chatgpt.chat_gpt_bot import AzureChatGPTBot
        bot = AzureChatGPTBot()
    elif bot_type == const.LINKAI:
        from bot.linkai.link_ai_bot import LinkAIBot
        bot = LinkAIBot()
    elif bot_type == const.CLAUDEAI:
        from bot.claude.claude_ai_bot import ClaudeAIBot
        bot = ClaudeAIBot()
    elif bot_type == const.QWEN:
        from bot.ali.ali_qwen_bot import AliQwenBot
        bot = AliQwenBot()
    elif bot_type == const.GEMINI:
        from bot.gemini.google_gemini_bot import GoogleGeminiBot
        bot = GoogleGeminiBot()
    elif bot_type == const.ZHIPU_AI:
        from bot.zhipuai.zhipuai_bot import ZhipuAIBot
        bot = ZhipuAIBot()
    elif bot_type == const.MOONSHOT:
        from bot.moonshot.moonshot_bot import MoonshotBot
        bot = MoonshotBot()
    elif bot_type == const.MiniMax:
        from bot.minimax.minimax_bot import MinimaxBot
        bot = MinimaxBot()
    elif bot_type == const.XUNFEI:
        from bot.xunfei.xunfei_spark_bot import XunfeiSparkBot
        bot = XunfeiSparkBot()
    elif bot_type == const.CLAUDEAPI:
        from bot.claudeapi.claude_api_bot import ClaudeAPIBot
        bot = ClaudeAPIBot()
    elif bot_type == const.DASHSCOPE:
        from bot.dashscope.dashscope_bot import DashscopeBot
        bot = DashscopeBot()
    elif bot_type == const.ModelScope:
        from bot.modelscope.modelscope_bot import ModelScopeBot
        bot = ModelScopeBot()
    else:
        raise RuntimeError
    
    # 如果启用了会话持久化且配置了数据库，替换为PersistentSessionManager
    if session_config.get("enabled", False) and db_config:
        try:
            original_sessions = bot.sessions
            # 修复：使用正确的属性名 sessioncls 而不是 session_cls
            session_class = original_sessions.sessioncls if hasattr(original_sessions, 'sessioncls') else None
            model = getattr(original_sessions, 'model', None)
            
            # 创建持久化会话管理器
            if session_class:
                bot.sessions = PersistentSessionManager(session_class, model=model, db_config=db_config)
                logger.info(f"[BotFactory] {bot_type} 已切换到持久化会话管理器")
            else:
                logger.warning(f"[BotFactory] 无法为 {bot_type} 切换到持久化会话管理器：无法获取session_class")
        except Exception as e:
            logger.error(f"[BotFactory] 为 {bot_type} 切换持久化会话管理器失败: {e}")
    else:
        # 删除这行：from common.log import logger
        logger.info(f"[BotFactory] {bot_type} 使用内存会话管理器 (持久化未启用)")
    
    return bot
