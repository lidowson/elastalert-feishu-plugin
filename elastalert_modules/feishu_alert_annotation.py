#! /usr/bin/env python
# -*- coding: utf-8 -*-

import json
import requests
import time
from elastalert.alerts import Alerter
# elastalert_logger：ElastAlert 的日志对象。EAException：ElastAlert 自定义异常类
from elastalert.util import elastalert_logger, EAException
# RequestException：requests 库的网络异常基类
from requests.exceptions import RequestException

# 定义 FeishuAlert 类，继承自 Alerter
class FeishuAlert(Alerter):
    
    # 必须的配置项
    # required_options：告诉 ElastAlert 该插件在规则文件中必须包含哪些字段。frozenset 是不可变集合，比 set 稍快一些
    # feishualert_botid: 飞书机器人 ID
    # feishualert_title: 消息标题模板
    # feishualert_body: 消息内容模板
    required_options = frozenset(['feishualert_botid', "feishualert_title", "feishualert_body"])

    # 构造函数初始化
    # 从 rule 配置中提取参数
    # self.url：飞书 webhook 前缀，默认为官方 URL
    # self.bot_id：机器人 ID（Webhook 的后半段）
    # self.title、self.body：消息模板（可包含 {} 占位符）
    # self.skip：可选字段，用于定义“静默时段”（不告警）
    # 如果缺少必要字段，抛出 EAException
    def __init__(self, rule):
        super(FeishuAlert, self).__init__(rule)
        self.url = self.rule.get("feishualert_url", "https://open.feishu.cn/open-apis/bot/v2/hook/")
        self.bot_id = self.rule.get("feishualert_botid", "")
        self.title = self.rule.get("feishualert_title", "")
        self.body = self.rule.get("feishualert_body", "")
        self.skip = self.rule.get("feishualert_skip", {})
        if not all([self.bot_id, self.title, self.body]):
            raise EAException("Configure botid|title|body is invalid")

    # 获取插件信息：ElastAlert 会调用它展示插件类型
    def get_info(self):
        return {"type": "FeishuAlert"}

    # 返回当前规则配置：方便调试或日志输出时查看规则内容
    def get_rule(self):
        return self.rule

    # 辅助函数：展平字典：方便在模板中直接使用 {host.name}
    # 例如：{"host": {"name": "server01"}, "status": 502}变成：{"host.name": "server01", "status": 502}
    def _flatten_dict(self, d, parent_key='', sep='.'):
        """将嵌套字典展平为单层字典，处理 host.name 这种情况"""
        items = {}
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.update(self._flatten_dict(v, new_key, sep))
            else:
                items[new_key] = v
        return items

    # 安全格式化函数
    # 目的：格式化字符串时不因缺失字段而崩溃
    # 例如模板 "主机: {host.name}, 错误: {error}" 中如果没有 error，就手动跳过
    # 任何格式错误都写入日志
    def _safe_format(self, text, data):
        """安全的字符串格式化，处理缺失字段"""
        try:
            # 先尝试标准格式化
            return text.format(**data)
        except KeyError as e:
            # 缺失字段时进行手动替换
            for key, value in data.items():
                placeholder = "{" + key + "}"
                text = text.replace(placeholder, str(value))
            return text
        except Exception as e:
            elastalert_logger.warning(f"Format error: {str(e)}")
            return text

    # 主函数：alert()：当 ElastAlert 匹配到日志时，这个函数被调用
    def alert(self, matches):

        # 静默时段检查：
        # 如果配置了 feishualert_skip: {start: "22:00:00", end: "06:00:00"}就会在此时间段跳过发送告警
        now = time.strftime("%H:%M:%S", time.localtime())
        if "start" in self.skip and "end" in self.skip:
            if self.skip["start"] <= now <= self.skip["end"]:
                elastalert_logger.info("Skip match in silence time...")
                return

        # 准备基础数据：
        # 合并一些公共字段，例如时间、标题
        alert_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        merge_data = {
            "feishualert_title": self.title,
            "feishualert_time": alert_time,
            **self.rule
        }

        # 合并匹配内容：
        # matches 是一个列表，包含触发的日志事件
        # 展平结构后，加入规则数据中，用于后续模板渲染
        if matches:
            # 展平嵌套结构（处理 host.name 等情况）
            flat_match = self._flatten_dict(matches[0])
            
            # 特殊处理常见字段
            flat_match.setdefault('host.name', flat_match.get('host_name', 'N/A'))
            flat_match.setdefault('@timestamp', flat_match.get('timestamp', alert_time))
            
            merge_data.update(flat_match)

        # 生成消息内容：
        # 根据模板（self.body）和数据（merge_data）生成飞书告警内容。如果出错，则生成一个错误提示消息
        try:
            formatted_body = self._safe_format(self.body, merge_data)
        except Exception as e:
            formatted_body = f"?? Format error: {str(e)}\nRaw message:\n{self.body}"
            elastalert_logger.error(f"Failed to format alert: {str(e)}")

        # 构建飞书消息：
        # 这是飞书机器人 Webhook 接口要求的 JSON 格式
        message = {
            "msg_type": "text",
            "content": {
                "text": formatted_body
            }
        }

        # 发送请求：
        # 向飞书机器人 Webhook (https://open.feishu.cn/open-apis/bot/v2/hook/<botid>) 发送 POST
        # 超时时间 10 秒
        # 如果 HTTP 请求异常，抛出 EAException（让 ElastAlert 记录错误）
        try:
            response = requests.post(
                url=f"{self.url}{self.bot_id}",
                json=message,
                headers={"Content-Type": "application/json"},
                timeout=10
            )
            response.raise_for_status()
        except RequestException as e:
            raise EAException(f"Feishu request failed: {str(e)}")
