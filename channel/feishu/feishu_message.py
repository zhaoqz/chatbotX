from typing import Optional, Dict, Any
from bridge.context import ContextType
from channel.chat_message import ChatMessage
import json
import requests
from common.log import logger
from common.tmp_dir import TmpDir
from common import utils
from .feishu_user_cache import FeishuUserCache


class FeishuMessage(ChatMessage):
    def __init__(self, feishu_message, is_group=False, access_token=None):
        # feishu_message 实际上是 event 对象
        event = feishu_message
        msg = event.get("message")
        sender = event.get("sender")
        
        # 调用父类构造函数，传递原始消息对象
        super().__init__(event)
        
        self.access_token = access_token
        self.msg_id = msg.get("message_id")
        self.create_time = msg.get("create_time")
        self.is_group = is_group
        
        # 初始化用户缓存
        self.user_cache = FeishuUserCache()
        
        msg_type = msg.get("message_type")

        if msg_type == "text":
            self.ctype = ContextType.TEXT
            content = json.loads(msg.get('content'))
            self.content = content.get("text").strip()
        elif msg_type == "file":
            self.ctype = ContextType.FILE
            content = json.loads(msg.get("content"))
            file_key = content.get("file_key")
            file_name = content.get("file_name")

            self.content = TmpDir().path() + file_key + "." + utils.get_path_suffix(file_name)

            def _download_file():
                # 如果响应状态码是200，则将响应内容写入本地文件
                url = f"https://open.feishu.cn/open-apis/im/v1/messages/{self.msg_id}/resources/{file_key}"
                headers = {
                    "Authorization": "Bearer " + self.access_token,
                }
                params = {
                    "type": "file"
                }
                response = requests.get(url=url, headers=headers, params=params)
                if response.status_code == 200:
                    with open(self.content, "wb") as f:
                        f.write(response.content)
                else:
                    logger.info(f"[FeiShu] Failed to download file, key={file_key}, res={response.text}")
            self._prepare_fn = _download_file
        elif msg_type == "merge_forward":
            self.ctype = ContextType.TEXT
            try:
                # 对于合并转发消息，直接通过消息ID获取详细内容
                message_id = msg.get('message_id')
                logger.debug(f"[FeiShu] Processing {access_token} merge_forward message_id: {message_id}")
                
                if message_id and access_token:
                    # 调用飞书API获取合并转发消息的详细内容
                    detailed_content = self._get_merge_forward_content(message_id, access_token)
                    if detailed_content:
                        self.content = detailed_content
                    else:
                        self.content = "[合并转发消息获取失败]"
                else:
                    self.content = "[合并转发消息 - 缺少必要参数]"
                    
                logger.info(f"[FeiShu] Processed merge_forward message: {self.content}")
                
            except Exception as e:
                logger.error(f"[FeiShu] Failed to parse merge_forward message: {e}")
                self.content = "[合并转发消息解析失败]"
        else:
            raise NotImplementedError("Unsupported message type: Type:{} ".format(msg_type))

        self.from_user_id = sender.get("sender_id").get("open_id")
        self.to_user_id = event.get("app_id")
        
        # 获取并缓存用户信息
        self._load_user_info(sender, access_token)
        
        if is_group:
            # 群聊
            self.other_user_id = msg.get("chat_id")
            self.actual_user_id = self.from_user_id
            self.content = self.content.replace("@_user_1", "").strip()
            
            # 获取群聊信息
            self._load_group_info(msg.get("chat_id"), access_token)
        else:
            # 私聊
            self.other_user_id = self.from_user_id
            self.actual_user_id = self.from_user_id
    
    def _load_user_info(self, sender, access_token):
        """加载并缓存用户信息"""
        try:
            sender_id = sender.get("sender_id", {}).get("open_id")
            tenant_key = sender.get("tenant_key", "")
            
            if sender_id and access_token:
                # 先从缓存获取
                cached_user = self.user_cache.get_user_info(sender_id, tenant_key)
                if cached_user:
                    self.from_user_nickname = cached_user.get('name') or cached_user.get('nickname') or f"用户({sender_id[:8]}...)"
                    logger.debug(f"[FeiShu] Using cached user info for {sender_id}")
                else:
                    # 缓存未命中，调用API获取用户信息
                    user_info = self._get_user_info_by_open_id(sender_id, access_token, tenant_key)
                    if user_info:
                        self.from_user_nickname = user_info
                    else:
                        self.from_user_nickname = f"用户({sender_id[:8]}...)"
            else:
                self.from_user_nickname = "未知用户"
                
        except Exception as e:
            logger.error(f"[FeiShu] Failed to load user info: {e}")
            self.from_user_nickname = "未知用户"
    
    def _load_group_info(self, chat_id, access_token):
        """加载群聊信息"""
        try:
            if chat_id and access_token:
                # 先从缓存获取群聊信息
                cached_group = self.user_cache.get_group_info(chat_id)
                if cached_group:
                    self.other_user_nickname = cached_group.get('name', f"群聊({chat_id[:8]}...)")
                    logger.debug(f"[FeiShu] Using cached group info for {chat_id}")
                else:
                    # 缓存未命中，调用API获取群聊信息
                    group_info = self._get_group_info_by_chat_id(chat_id, access_token)
                    if group_info:
                        self.other_user_nickname = group_info
                    else:
                        self.other_user_nickname = f"群聊({chat_id[:8]}...)"
            else:
                self.other_user_nickname = "未知群聊"
                
        except Exception as e:
            logger.error(f"[FeiShu] Failed to load group info: {e}")
            self.other_user_nickname = "未知群聊"
    
    def _get_group_info_by_chat_id(self, chat_id: str, access_token: str) -> Optional[str]:
        """通过 chat_id 获取群聊信息"""
        try:
            url = f"https://open.feishu.cn/open-apis/im/v1/chats/{chat_id}"
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json; charset=utf-8"
            }
            
            response = requests.get(url=url, headers=headers)
            logger.debug(f"[FeiShu] Get group info response: {response.status_code} {response.text}")
            
            if response.status_code == 200:
                result = response.json()
                if result.get('code') == 0:
                    data = result.get('data', {})
                    
                    # 保存群聊信息到缓存
                    self.user_cache.save_group_info(chat_id, data)
                    
                    # 返回群聊名称
                    name = data.get('name', '')
                    if name and name.strip():
                        return name.strip()
                    
                    description = data.get('description', '')
                    if description and description.strip():
                        return description.strip()
                        
                else:
                    logger.warning(f"[FeiShu] API error when getting group info for {chat_id}: {result.get('msg')}")
            else:
                logger.warning(f"[FeiShu] HTTP error when getting group info for {chat_id}: {response.status_code}")
                
        except Exception as e:
            logger.error(f"[FeiShu] Exception when getting group info for {chat_id}: {e}")
        
        return None

    def _get_message_content(self, message_id: str, access_token: str) -> str:
        """根据飞书官方API获取指定消息的内容"""
        try:
            url = f"https://open.feishu.cn/open-apis/im/v1/messages/{message_id}"
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json; charset=utf-8"
            }
            
            response = requests.get(url=url, headers=headers)
            if response.status_code == 200:
                result = response.json()
                if result.get('code') == 0:
                    message_data = result.get('data', {})
                    message_content = message_data.get('content', '')
                    message_type = message_data.get('msg_type', '')
                    
                    if message_type == 'text' and message_content:
                        content_json = json.loads(message_content)
                        return content_json.get('text', '').strip()
                    elif message_type == 'post' and message_content:
                        # 处理富文本消息
                        content_json = json.loads(message_content)
                        return self._extract_text_from_post(content_json)
                    else:
                        return f"[{message_type}消息]"
                else:
                    logger.warning(f"[FeiShu] API error when getting message {message_id}: {result.get('msg')}")
            else:
                logger.warning(f"[FeiShu] HTTP error when getting message {message_id}: {response.status_code}")
        except Exception as e:
            logger.error(f"[FeiShu] Exception when getting message {message_id}: {e}")
        
        return "[消息获取失败]"
    
    def _extract_text_from_post(self, post_content: dict) -> str:
        """从富文本消息中提取纯文本内容"""
        try:
            texts = []
            # 飞书富文本消息结构：{"zh_cn": {"content": [[{"tag": "text", "text": "内容"}]]}}
            for lang_key, lang_content in post_content.items():
                if isinstance(lang_content, dict) and 'content' in lang_content:
                    content_blocks = lang_content['content']
                    for block in content_blocks:
                        if isinstance(block, list):
                            for item in block:
                                if isinstance(item, dict) and item.get('tag') == 'text':
                                    text = item.get('text', '')
                                    if text:
                                        texts.append(text)
            return ' '.join(texts) if texts else '[富文本消息]'
        except Exception as e:
            logger.error(f"[FeiShu] Failed to extract text from post content: {e}")
            return '[富文本消息解析失败]'
    
    def _get_merge_forward_content(self, message_id: str, access_token: str) -> str:
        """获取合并转发消息的详细内容，解析子消息"""
        try:
            # 使用正确的API端点获取消息详情（包含子消息）
            url = f"https://open.feishu.cn/open-apis/im/v1/messages/{message_id}"
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json; charset=utf-8"
            }
            
            response = requests.get(url=url, headers=headers)
            logger.debug(f"[FeiShu] Get merge_forward message detail response: {response.status_code} {response.text}")
            
            if response.status_code == 200:
                result = response.json()
                if result.get('code') == 0:
                    data = result.get('data', {})
                    
                    # 检查是否有items数组（包含子消息）
                    items = data.get('items', [])
                    if items:
                        # 提取属于当前合并转发消息的子消息
                        sub_messages = []
                        for item in items:
                            # 跳过合并转发消息本身
                            if item.get('msg_type') == 'merge_forward':
                                continue
                                
                            # 检查是否属于当前合并转发消息
                            upper_msg_id = item.get('upper_message_id')
                            if upper_msg_id == message_id:
                                body = item.get('body', {})
                                content = body.get('content', '')
                                msg_type = item.get('msg_type', '')
                                sub_message_id = item.get('message_id', '')
                                sender= item.get('sender', {})
                                
                                # 通过子消息ID获取完整的消息信息（包含发送者详情）
                                sender_info = self._get_message_sender_info(sender, access_token)
                                
                                # 解析文本消息内容
                                if msg_type == 'text' and content:
                                    try:
                                        content_json = json.loads(content)
                                        text = content_json.get('text', '').strip()
                                        if text:
                                            # 移除@_user_1等提及标记
                                            text = text.replace('@_user_1', '').strip()
                                            sub_messages.append(f"{text}\n------ {sender_info}")
                                    except json.JSONDecodeError:
                                        sub_messages.append(f"{content}\n------ {sender_info}")
                                elif content:
                                    sub_messages.append(f"[{msg_type}消息]\n------ {sender_info}")
                        
                        if sub_messages:
                            return f"[合并转发消息]\n" + "\n\n".join(sub_messages)
                    
                    # 如果没有找到子消息，返回基本信息
                    message_data = data if 'msg_type' in data else data.get('message', {})
                    content = message_data.get('content', '')
                    return f"[合并转发消息] {content}"
                    
                else:
                    logger.warning(f"[FeiShu] API error when getting merge_forward message {message_id}: {result.get('msg')}")
            else:
                logger.warning(f"[FeiShu] HTTP error when getting merge_forward message {message_id}: {response.status_code}")
                
        except Exception as e:
            logger.error(f"[FeiShu] Exception when getting merge_forward message {message_id}: {e}")
        
        return "[合并转发消息获取失败]"
    
    def _get_message_sender_info(self, sender_info: dict, access_token: str) -> str:
        """通过发送者信息获取用户详细信息（带缓存）"""
        try:
            sender_id = sender_info.get('id', '')
            id_type = sender_info.get('id_type', '')
            tenant_key = sender_info.get('tenant_key', '')
            
            if id_type == 'open_id' and sender_id:
                # 先从缓存获取
                cached_user = self.user_cache.get_user_info(sender_id, tenant_key)
                if cached_user:
                    logger.debug(f"[FeiShu] Using cached user info for {sender_id}")
                    return cached_user.get('name') or cached_user.get('nickname') or f"用户({sender_id[:8]}...)"
                
                # 缓存未命中，调用API - 传递 tenant_key
                user_info = self._get_user_info_by_open_id(sender_id, access_token, tenant_key)
                if user_info:
                    return user_info
            
            return f"用户({sender_id[:8]}...)" if sender_id else "未知用户"
            
        except Exception as e:
            logger.error(f"[FeiShu] Exception when getting sender info: {e}")
        
        return "未知用户"
    
    def _get_user_info_by_open_id(self, open_id: str, access_token: str, tenant_key: str = None) -> Optional[str]:
        """通过 open_id 获取用户信息"""
        try:
            url = f"https://open.feishu.cn/open-apis/contact/v3/users/{open_id}"
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json; charset=utf-8"
            }
            
            response = requests.get(url=url, headers=headers)
            logger.debug(f"[FeiShu] Get user info by open_id response: {response.status_code} {response.text}")
            
            if response.status_code == 200:
                result = response.json()
                if result.get('code') == 0:
                    data = result.get('data', {})
                    user = data.get('user', {})
                    
                    # 保存到缓存 - 使用传入的 tenant_key
                    sender_info = {
                        'tenant_key': tenant_key or '',  # 使用传入的 tenant_key
                        'sender_type': 'user',
                        'id_type': 'open_id'
                    }
                    self.user_cache.save_user_info(data, sender_info)
                    
                    # 返回用户名
                    name = user.get('name', '')
                    if name and name.strip():
                        return name.strip()
                    
                    nickname = user.get('nickname', '')
                    if nickname and nickname.strip():
                        return nickname.strip()
                    
                    en_name = user.get('en_name', '')
                    if en_name and en_name.strip():
                        return en_name.strip()
                        
                else:
                    logger.warning(f"[FeiShu] API error when getting user info for {open_id}: {result.get('msg')}")
            else:
                logger.warning(f"[FeiShu] HTTP error when getting user info for {open_id}: {response.status_code}")
                
        except Exception as e:
            logger.error(f"[FeiShu] Exception when getting user info for {open_id}: {e}")
        
        return None
    
