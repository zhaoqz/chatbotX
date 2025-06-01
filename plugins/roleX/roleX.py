# 修改导入部分
import os
import json
# 将 mysql.connector 替换为 pymysql
import pymysql
from pymysql import Error
import plugins
from bridge.bridge import Bridge
from bridge.context import ContextType
from bridge.reply import Reply, ReplyType
from common import const
from common.log import logger
from config import conf
from plugins import *


class RolePlayX:
    """RoleX角色扮演会话管理类"""
    def __init__(self, bot, sessionid, rule_data):
        self.bot = bot
        self.sessionid = sessionid
        self.rule_data = rule_data
        self.wrapper = rule_data.get('wrapper', '%s')
        self.prompt = rule_data['prompt']
        self.model_id = rule_data.get('model_id', '')
        self.temperature = rule_data.get('temperature', 0.7)
        self.max_tokens = rule_data.get('max_tokens', -1)
        
        # 构建系统提示词
        system_prompt = self.prompt
        self.bot.sessions.build_session(self.sessionid, system_prompt=system_prompt)
        
        logger.info(f"[RoleX] 创建角色会话: {rule_data.get('rule_code', 'unknown')}")

    def reset(self):
        """重置会话"""
        self.bot.sessions.clear_session(self.sessionid)
        logger.debug(f"[RoleX] 重置会话: {self.sessionid}")

    def action(self, user_action):
        """处理用户输入"""
        session = self.bot.sessions.build_session(self.sessionid)
        if session.system_prompt != self.prompt:
            session.set_system_prompt(self.prompt)
        
        # 应用wrapper格式化用户输入
        formatted_input = self.wrapper % user_action if self.wrapper != '%s' else user_action
        return formatted_input


@plugins.register(
    name="RoleX",
    desire_priority=0,
    namecn="角色扮演X",
    desc="基于数据库的高级角色管理插件，支持多模型、多租户",
    version="2.0",
    author="zhaoqz",
)
class RoleX(Plugin):
    """RoleX插件主类"""
    
    def __init__(self):
        super().__init__()
        self.db_config = self.load_db_config()
        self.roleplays = {}  # 存储活跃的角色扮演会话
        self.plugin_id = "FP1006"  # 插件ID
        self.biz_code = "B1006001"  # 业务编码
        
        try:
            # 测试数据库连接
            self.test_db_connection()
            self.handlers[Event.ON_HANDLE_CONTEXT] = self.on_handle_context
            logger.info("[RoleX] 插件初始化成功")
        except Exception as e:
            logger.error(f"[RoleX] 插件初始化失败: {e}")
            raise e

    def load_db_config(self):
        """加载数据库配置"""
        config_path = os.path.join(os.path.dirname(__file__), "config.json")
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                return config.get('database', {})
        except FileNotFoundError:
            logger.warning(f"[RoleX] 配置文件未找到: {config_path}，使用默认配置")
            # 返回默认配置
            return {
                'host': 'localhost',
                'port': 3306,
                'user': 'root',
                'password': '',
                'database': 'chatbot'
            }
        except Exception as e:
            logger.error(f"[RoleX] 加载配置文件失败: {e}")
            raise e

    def test_db_connection(self):
        """测试数据库连接"""
        try:
            connection = pymysql.connect(**self.db_config)
            if connection.open:
                connection.close()
                logger.info("[RoleX] 数据库连接测试成功")
        except Exception as e:
            logger.error(f"[RoleX] 数据库连接失败: {e}")
            raise e

    def get_db_connection(self):
        """获取数据库连接"""
        try:
            return pymysql.connect(
                host=self.db_config["host"],
                port=self.db_config["port"],
                user=self.db_config["user"],
                password=self.db_config["password"],
                database=self.db_config["database"],
                charset='utf8mb4'
            )
        except Exception as e:
            logger.error(f"[RoleX] 获取数据库连接失败: {e}")
            return None

    def get_role_rules(self, tenant_id=0, vip_code=''):
        """从数据库获取角色规则列表"""
        connection = self.get_db_connection()
        if not connection:
            return []
        
        try:
            cursor = connection.cursor(pymysql.cursors.DictCursor)
            
            # 构建查询条件，明确指定表别名避免字段歧义
            where_conditions = [
                "r.plugin_id = %s",
                "r.biz_code = %s", 
                "r.status = 1"
            ]
            params = [self.plugin_id, self.biz_code]
            
            if tenant_id > 0:
                where_conditions.append("(r.tenant_id = 0 OR r.tenant_id = %s)")
                params.append(tenant_id)
            else:
                where_conditions.append("r.tenant_id = 0")
            
            if vip_code:
                where_conditions.append("(r.vip_code = '' OR r.vip_code = %s)")
                params.append(vip_code)
            else:
                where_conditions.append("r.vip_code = ''")
            
            query = f"""
                SELECT r.*, m.model_name, m.model, m.provider_id
                FROM role_rule r
                LEFT JOIN ai_model m ON r.model_id = m.model_id
                WHERE {' AND '.join(where_conditions)}
                ORDER BY r.priority DESC, r.id ASC
            """
            
            cursor.execute(query, params)
            rules = cursor.fetchall()
            
            logger.debug(f"[RoleX] 查询到 {len(rules)} 个角色规则")
            return rules
            
        except Exception as e:
            logger.error(f"[RoleX] 查询角色规则失败: {e}")
            return []
        finally:
            if connection.open:
                cursor.close()
                connection.close()

    def get_role_by_code(self, rule_code, tenant_id=0, vip_code=''):
        """根据规则代码获取角色"""
        connection = self.get_db_connection()
        if not connection:
            return None
        
        try:
            cursor = connection.cursor(pymysql.cursors.DictCursor)
            
            # 明确指定表别名避免字段歧义
            where_conditions = [
                "r.plugin_id = %s",
                "r.biz_code = %s",
                "r.rule_code = %s",
                "r.status = 1"
            ]
            params = [self.plugin_id, self.biz_code, rule_code]
            
            if tenant_id > 0:
                where_conditions.append("(r.tenant_id = 0 OR r.tenant_id = %s)")
                params.append(tenant_id)
            else:
                where_conditions.append("r.tenant_id = 0")
            
            if vip_code:
                where_conditions.append("(r.vip_code = '' OR r.vip_code = %s)")
                params.append(vip_code)
            else:
                where_conditions.append("r.vip_code = ''")
            
            query = f"""
                SELECT r.*, m.model_name, m.model, m.provider_id
                FROM role_rule r
                LEFT JOIN ai_model m ON r.model_id = m.model_id
                WHERE {' AND '.join(where_conditions)}
            """
            
            cursor.execute(query, params)
            rule = cursor.fetchone()
            
            if rule:
                logger.debug(f"[RoleX] 找到角色: {rule['rule_code']}")
            else:
                logger.debug(f"[RoleX] 未找到角色: {rule_code}")
            return rule
            
        except Exception as e:
            logger.error(f"[RoleX] 查询角色失败: {e}")
            return None
        finally:
            if connection.open:
                cursor.close()
                connection.close()

    def find_similar_role(self, role_name, tenant_id=0, vip_code=''):
        """模糊匹配角色名称"""
        connection = self.get_db_connection()
        if not connection:
            return None
        
        try:
            cursor = connection.cursor(pymysql.cursors.DictCursor)
            
            # 构建查询条件，支持模糊匹配
            where_conditions = [
                "r.plugin_id = %s",
                "r.biz_code = %s", 
                "r.status = 1",
                "(r.rule_code LIKE %s OR r.description LIKE %s)"
            ]
            
            like_pattern = f"%{role_name}%"
            params = [self.plugin_id, self.biz_code, like_pattern, like_pattern]
            
            if tenant_id > 0:
                where_conditions.append("(r.tenant_id = 0 OR r.tenant_id = %s)")
                params.append(tenant_id)
            else:
                where_conditions.append("r.tenant_id = 0")
            
            if vip_code:
                where_conditions.append("(r.vip_code = '' OR r.vip_code = %s)")
                params.append(vip_code)
            else:
                where_conditions.append("r.vip_code = ''")
            
            query = f"""
                SELECT r.*, m.model_name, m.model, m.provider_id
                FROM role_rule r
                LEFT JOIN ai_model m ON r.model_id = m.model_id
                WHERE {' AND '.join(where_conditions)}
                ORDER BY r.priority DESC, r.id ASC
                LIMIT 1
            """
            
            cursor.execute(query, params)
            rule = cursor.fetchone()
            
            if rule:
                logger.debug(f"[RoleX] 模糊匹配找到角色: {rule['rule_code']}")
            else:
                logger.debug(f"[RoleX] 模糊匹配未找到角色: {role_name}")
            return rule
            
        except Exception as e:
            logger.error(f"[RoleX] 模糊匹配角色失败: {e}")
            return None
        finally:
            if connection.open:
                cursor.close()
                connection.close()

    def on_handle_context(self, e_context: EventContext):
        """处理上下文事件"""
        if e_context["context"].type != ContextType.TEXT:
            return
        
        # 检查支持的bot类型
        btype = Bridge().get_bot_type("chat")
        if btype not in [const.OPEN_AI, const.CHATGPT, const.CHATGPTONAZURE, 
                        const.QWEN_DASHSCOPE, const.XUNFEI, const.BAIDU, 
                        const.ZHIPU_AI, const.MOONSHOT, const.MiniMax, 
                        const.LINKAI, const.MODELSCOPE]:
            logger.debug(f'[RoleX] 不支持的bot类型: {btype}')
            return
        
        bot = Bridge().get_bot("chat")
        content = e_context["context"].content[:]
        clist = e_context["context"].content.split(maxsplit=1)
        sessionid = e_context["context"]["session_id"]
        trigger_prefix = conf().get("plugin_trigger_prefix", "$")
        
        # 从上下文获取租户信息（如果有的话）
        tenant_id = e_context["context"].get("tenant_id", 0)
        vip_code = e_context["context"].get("vip_code", "")
        
        # 停止扮演
        if clist[0] == f"{trigger_prefix}停止扮演X":
            if sessionid in self.roleplays:
                self.roleplays[sessionid].reset()
                del self.roleplays[sessionid]
                logger.info(f"[RoleX] 停止角色扮演: {sessionid}")
            reply = Reply(ReplyType.INFO, "角色扮演结束!")
            e_context["reply"] = reply
            e_context.action = EventAction.BREAK_PASS
            return
        
        # 角色列表
        elif clist[0] == f"{trigger_prefix}角色列表X":
            rules = self.get_role_rules(tenant_id, vip_code)
            if not rules:
                help_text = "暂无可用角色"
            else:
                help_text = "可用角色列表：\n"
                for rule in rules:
                    model_info = f" (模型: {rule.get('model_name', '默认')})" if rule.get('model_name') else ""
                    vip_info = f" [VIP专属]" if rule.get('vip_code') else ""
                    tenant_info = f" [租户专属]" if rule.get('tenant_id', 0) > 0 else ""
                    help_text += f"• {rule['rule_code']}: {rule.get('description', '无描述')}{model_info}{vip_info}{tenant_info}\n"
            reply = Reply(ReplyType.INFO, help_text)
            e_context["reply"] = reply
            e_context.action = EventAction.BREAK_PASS
            return
        
        # 设置角色
        elif clist[0] == f"{trigger_prefix}角色X":
            if len(clist) == 1 or (len(clist) > 1 and clist[1].lower() in ["help", "帮助"]):
                reply = Reply(ReplyType.INFO, self.get_help_text(verbose=True))
                e_context["reply"] = reply
                e_context.action = EventAction.BREAK_PASS
                return
            
            role_name = clist[1]
            
            # 先精确匹配
            rule = self.get_role_by_code(role_name, tenant_id, vip_code)
            
            # 如果没找到，尝试模糊匹配
            if not rule:
                rule = self.find_similar_role(role_name, tenant_id, vip_code)
            
            if not rule:
                reply = Reply(ReplyType.ERROR, f"角色 '{role_name}' 不存在")
                e_context["reply"] = reply
                e_context.action = EventAction.BREAK_PASS
                return
            
            # 创建角色扮演实例
            try:
                # 解析规则参数
                rule_param = {}
                if rule.get('rule_param'):
                    try:
                        rule_param = json.loads(rule['rule_param'])
                    except json.JSONDecodeError:
                        logger.warning(f"[RoleX] 解析规则参数失败: {rule['rule_param']}")
                
                # 合并规则数据
                role_data = {
                    'rule_code': rule['rule_code'],
                    'prompt': rule['prompt'],
                    'model_id': rule.get('model_id', ''),
                    'temperature': rule.get('temperature', 0.7),
                    'max_tokens': rule.get('max_tokens', -1),
                    'wrapper': rule_param.get('wrapper', '%s')
                }
                
                self.roleplays[sessionid] = RolePlayX(bot, sessionid, role_data)
                
                model_info = f"\n使用模型: {rule.get('model_name', '默认')}" if rule.get('model_name') else ""
                vip_info = f"\n[VIP专属角色]" if rule.get('vip_code') else ""
                reply = Reply(ReplyType.INFO, 
                            f"已设置角色: {rule['rule_code']}\n{rule.get('description', '')}{model_info}{vip_info}")
                e_context["reply"] = reply
                e_context.action = EventAction.BREAK_PASS
                
                logger.info(f"[RoleX] 设置角色成功: {rule['rule_code']} for {sessionid}")
                
            except Exception as e:
                logger.error(f"[RoleX] 创建角色失败: {e}")
                reply = Reply(ReplyType.ERROR, "角色设置失败")
                e_context["reply"] = reply
                e_context.action = EventAction.BREAK_PASS
            return
        
        # 如果当前会话有角色扮演，处理用户输入
        elif sessionid in self.roleplays:
            e_context["context"]["generate_breaked_by"] = EventAction.BREAK
            prompt = self.roleplays[sessionid].action(content)
            e_context["context"].type = ContextType.TEXT
            e_context["context"].content = prompt
            e_context.action = EventAction.BREAK
            
            logger.debug(f"[RoleX] 处理角色输入: {sessionid}")

    def get_help_text(self, verbose=False, **kwargs):
        """获取帮助文本"""
        help_text = "基于数据库的高级角色管理插件。\n"
        if not verbose:
            return help_text
        
        trigger_prefix = conf().get("plugin_trigger_prefix", "$")
        help_text = f"""使用方法:
{trigger_prefix}角色X <角色代码>: 设定角色
{trigger_prefix}角色列表X: 查看所有可用角色
{trigger_prefix}停止扮演X: 停止当前角色扮演

特性:
• 支持多种AI模型
• 支持租户隔离
• 支持VIP专属角色
• 基于数据库存储，便于管理
• 与原role插件并存

示例:
{trigger_prefix}角色X 写作助理
{trigger_prefix}角色列表X
"""
        return help_text