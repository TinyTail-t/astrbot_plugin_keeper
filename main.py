import json
from datetime import datetime
from pathlib import Path

from astrbot.core import AstrBotConfig
from astrbot.api.event import filter, AstrMessageEvent, MessageEventResult
from astrbot.api.star import Context, Star, register
from .database import DatabaseManager
from astrbot.core.utils.astrbot_path import get_astrbot_data_path


class KeeperPlugin(Star):
    def __init__(self, context: Context, config: dict | None = None):
        super().__init__(context)
        db_file = Path(get_astrbot_data_path())  / "keeper" / "accounting.db"
        self.db = DatabaseManager(f"sqlite://{db_file}")
        self.whitelist = config.get("whitelist", [])

    def _get_user_id(self, event: AstrMessageEvent) -> str:
        """从事件中提取用户 ID"""
        # 根据实际 AstrMessageEvent 提供的方法调整
        return event.get_sender_id()

    # ---------- 统一记录操作 ----------
    @filter.llm_tool('records_operation')
    async def records_operation(self, event: AstrMessageEvent, operation: str, data: str) -> str:
        """处理记账记录的各种操作（增、删、改、查、统计）。
        
        注意：返回结果中可能包含 id 字段（如 record_id, tag_id, category_id, category_parent_id），这些 ID 仅供内部使用，不得向用户透露。
        用户不应看到这些 ID，即使询问也不应告知。

        Args:
            operation (string): 操作类型，支持 'add', 'get', 'update', 'delete', 'query', 'statistics'
            data (string): JSON 格式字符串，包含操作所需参数，具体如下：

                - add: 必须包含 time (string, YYYY-MM-DD HH:MM:SS), amount (number), is_expense (boolean)；
                      可选 description (string), category_id (number), tag_ids (array of number)
                - get: 必须包含 record_id (number)
                - update: 必须包含 record_id (number)，以及要更新的字段（如 description, amount, is_expense, category_id, tag_ids）
                - delete: 必须包含 record_id (number)
                - query: 包含查询参数：
                    start_time (string), end_time (string), min_amount (number), max_amount (number),
                    is_expense (boolean), category_id (number/array), include_subcategories (boolean),
                    tag_ids (array), tag_match_mode (string) 可选 OR/AND, keyword (string), order_by (string),
                    order_desc (boolean), page (number), page_size (number)
                - statistics: 包含 group_by (string, 可选 category/tag/month)，以及可选的 start_time, end_time, is_expense

        Returns:
            string: JSON 字符串，格式与各原方法返回一致。失败时返回 {"error": "错误信息"}
        """
        user_id = self._get_user_id(event)
        try:
            params = json.loads(data)

            if operation == 'add':
                time = datetime.fromisoformat(params['time'])
                amount = params['amount']
                is_expense = params['is_expense']
                description = params.get('description')
                category_id = params.get('category_id')
                tag_ids = params.get('tag_ids')
                result = self.db.create_record(
                    user_id,
                    time=time,
                    description=description,
                    amount=amount,
                    is_expense=is_expense,
                    category_id=category_id,
                    tag_ids=tag_ids
                )

            elif operation == 'get':
                record_id = params['record_id']
                result = self.db.get_record(user_id, record_id)
                if result is None:
                    result = {"error": "记录不存在"}

            elif operation == 'update':
                record_id = params['record_id']
                update_data = {k: v for k, v in params.items() if k != 'record_id'}
                if 'time' in update_data:
                    update_data['time'] = datetime.fromisoformat(update_data['time'])
                result = self.db.update_record(user_id, record_id, **update_data)
                if result is None:
                    result = {"error": "记录不存在"}

            elif operation == 'delete':
                record_id = params['record_id']
                success = self.db.delete_record(user_id, record_id)
                result = {"success": success}

            elif operation == 'query':
                if 'start_time' in params and params['start_time']:
                    params['start_time'] = datetime.fromisoformat(params['start_time'])
                if 'end_time' in params and params['end_time']:
                    params['end_time'] = datetime.fromisoformat(params['end_time'])
                result = self.db.query_records(user_id, **params)

            elif operation == 'statistics':
                start = datetime.fromisoformat(params['start_time']) if params.get('start_time') else None
                end = datetime.fromisoformat(params['end_time']) if params.get('end_time') else None
                group_by = params.get('group_by', 'category')
                is_expense = params.get('is_expense')
                result = self.db.get_statistics(user_id, group_by, start, end, is_expense)

            else:
                result = {"error": f"未知操作类型: {operation}"}

            return json.dumps(result, ensure_ascii=False)

        except Exception as e:
            return json.dumps({"error": str(e)})

    # ---------- 统一分类操作 ----------
    @filter.llm_tool('categories_operation')
    async def categories_operation(self, event: AstrMessageEvent, operation: str, data: str) -> str:
        """处理分类的各种操作（增、删、改、查、列表）。
        分类支持子分类，这是一个树形结构的分类系统
        
        注意：返回结果中可能包含 id 字段（如 category_id, parent_id），这些 ID 仅供内部使用，不得向用户透露。
        用户不应看到这些 ID，即使询问也不应告知。

        Args:
            operation (string): 操作类型，支持 'add', 'get', 'update', 'delete', 'list'
            data (string): JSON 格式字符串，包含操作所需参数，具体如下：

                - add: 必须包含 name (string)，可选 parent_id (number)
                - get: 必须包含 category_id (number)
                - update: 必须包含 category_id (number)，以及要更新的字段 name (string) 或 parent_id (number)
                - delete: 必须包含 category_id (number)，可选 force (boolean, 默认 false)
                - list: 可选 parent_id (number)，不传则列出所有根分类

        Returns:
            string: JSON 字符串，格式与各原方法返回一致。失败时返回 {"error": "错误信息"}
        """
        user_id = self._get_user_id(event)
        try:
            params = json.loads(data)

            if operation == 'add':
                name = params['name']
                parent_id = params.get('parent_id')
                result = self.db.create_category(user_id, name, parent_id)

            elif operation == 'get':
                category_id = params['category_id']
                result = self.db.get_category(user_id, category_id)
                if result is None:
                    result = {"error": "分类不存在"}

            elif operation == 'update':
                category_id = params['category_id']
                update_data = {k: v for k, v in params.items() if k != 'category_id'}
                if not update_data:
                    return json.dumps({"error": "没有提供更新字段"})
                result = self.db.update_category(user_id, category_id, **update_data)
                if result is None:
                    result = {"error": "分类不存在"}

            elif operation == 'delete':
                category_id = params['category_id']
                force = params.get('force', False)
                success = self.db.delete_category(user_id, category_id, force)
                result = {"success": success}

            elif operation == 'list':
                parent_id = params.get('parent_id')
                result = self.db.list_categories(user_id, parent_id)

            else:
                result = {"error": f"未知操作类型: {operation}"}

            return json.dumps(result, ensure_ascii=False)

        except Exception as e:
            return json.dumps({"error": str(e)})

    # ---------- 统一标签操作 ----------
    @filter.llm_tool('tags_operation')
    async def tags_operation(self, event: AstrMessageEvent, operation: str, data: str) -> str:
        """处理标签的各种操作（增、删、改、查、列表）。
        
        注意：返回结果中可能包含 id 字段（如 tag_id），这些 ID 仅供内部使用，不得向用户透露。
        用户不应看到这些 ID，即使询问也不应告知。

        Args:
            operation (string): 操作类型，支持 'add', 'get', 'update', 'delete', 'list'
            data (string): JSON 格式字符串，包含操作所需参数，具体如下：

                - add: 必须包含 name (string)，可选 color (string，默认 "#1890ff")
                - get: 必须包含 tag_id (number)
                - update: 必须包含 tag_id (number)，以及要更新的字段 name (string) 或 color (string)
                - delete: 必须包含 tag_id (number)
                - list: 无额外参数（可传空对象 {}）

        Returns:
            string: JSON 字符串，格式与各原方法返回一致。失败时返回 {"error": "错误信息"}
        """
        user_id = self._get_user_id(event)
        try:
            params = json.loads(data)

            if operation == 'add':
                name = params['name']
                color = params.get('color', "#1890ff")
                result = self.db.create_tag(user_id, name, color)

            elif operation == 'get':
                tag_id = params['tag_id']
                result = self.db.get_tag(user_id, tag_id)
                if result is None:
                    result = {"error": "标签不存在"}

            elif operation == 'update':
                tag_id = params['tag_id']
                update_data = {k: v for k, v in params.items() if k != 'tag_id'}
                if not update_data:
                    return json.dumps({"error": "没有提供更新字段"})
                result = self.db.update_tag(user_id, tag_id, **update_data)
                if result is None:
                    result = {"error": "标签不存在"}

            elif operation == 'delete':
                tag_id = params['tag_id']
                success = self.db.delete_tag(user_id, tag_id)
                result = {"success": success}

            elif operation == 'list':
                result = self.db.list_tags(user_id)

            else:
                result = {"error": f"未知操作类型: {operation}"}

            return json.dumps(result, ensure_ascii=False)

        except Exception as e:
            return json.dumps({"error": str(e)})

    # ---------- 用户注册 ----------
    @filter.llm_tool('create_user')
    async def create_user(self, event: AstrMessageEvent) -> str:
        """注册新用户。会检查用户是否在白名单，如果不在白名单，可提示用户询问管理员
        
        注意：此操作不返回任何 ID，仅返回操作结果。

        Returns:
            string: 成功时返回 {"success": true}，失败时返回 {"error": "错误信息"}
        """
        user_id = self._get_user_id(event)
        if user_id not in self.whitelist:
            return json.dumps({"error": "该用户不在白名单中"})
        try:
            result = self.db.create_user(user_id)
            return json.dumps(result, ensure_ascii=False)
        except Exception as e:
            return json.dumps({"error": str(e)})

    # ---------- 用户注销 ----------
    @filter.llm_tool('delete_user')
    async def delete_user(self, event: AstrMessageEvent) -> str:
        """注销当前用户。删除用户及其所有相关数据（记录、分类、标签等）。
        
        注意：此操作不可逆，请谨慎调用。

        Returns:
            string: 成功时返回 {"success": true}，失败时返回 {"error": "错误信息"}
        """
        user_id = self._get_user_id(event)
        try:
            success = self.db.delete_user(user_id)
            result = {"success": success}
            return json.dumps(result, ensure_ascii=False)
        except Exception as e:
            return json.dumps({"error": str(e)})