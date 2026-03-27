"""
配置文件

【如何增减股票】
在 TARGET_STOCKS 列表中直接增删字典条目即可，格式：
  {"code": "股票代码（6位）", "name": "股票名称", "market": "SH 或 SZ"}
"""

# 目标股票 —— 随时增删
TARGET_STOCKS = [
    {"code": "600389", "name": "江山股份",   "market": "SH"},
    {"code": "588190", "name": "科创100ETF", "market": "SH"},
    {"code": "588080", "name": "科创50ETF",  "market": "SH"},
]

# 推送时间（每30分钟，9:15 ~ 14:55）
PUSH_TIMES = [
    "09:15", "09:45", "10:15", "10:45",
    "11:15", "11:45", "13:15", "13:45",
    "14:15", "14:45", "14:55"
]

# A股交易时间
MARKET_OPEN_AM  = "09:15"
MARKET_CLOSE_AM = "11:30"
MARKET_OPEN_PM  = "13:00"
MARKET_CLOSE_PM = "15:00"

# 飞书群机器人 Webhook
FEISHU_WEBHOOK = "https://open.feishu.cn/open-apis/bot/v2/hook/f328f220-8828-4b83-9d1b-4b36ed70bba5"

# 飞书签名密钥（填写后启用签名验证；不需要则置空字符串）
FEISHU_SECRET = "mzNnijX3Mg8gxKBcPmu5Ld"

# 日志级别
LOG_LEVEL = "INFO"
