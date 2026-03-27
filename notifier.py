"""
推送模块 - 控制台输出 + 飞书 Webhook（带签名验证）
"""
import requests
import json
import hmac
import hashlib
import base64
import logging
import time
from datetime import datetime

from config import FEISHU_WEBHOOK, FEISHU_SECRET

logger = logging.getLogger(__name__)


def _make_feishu_sign(secret: str, timestamp: int) -> str:
    """生成飞书 Webhook 签名"""
    # 签名串 = timestamp + "\n" + secret
    string_to_sign = f"{timestamp}\n{secret}"
    hmac_code = hmac.new(
        string_to_sign.encode("utf-8"),
        digestmod=hashlib.sha256
    ).digest()
    return base64.b64encode(hmac_code).decode("utf-8")


def send_to_console(report: str):
    """打印到控制台"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n{'='*60}")
    print(f"📡 股票雷达推送  {now}")
    print("=" * 60)
    print(report)
    print("=" * 60 + "\n")


def send_to_feishu(report: str):
    """推送到飞书群 Webhook（支持签名验证）"""
    if not FEISHU_WEBHOOK:
        return

    timestamp = int(time.time())

    payload: dict = {
        "msg_type": "text",
        "content": {"text": report},
        "timestamp": str(timestamp),
    }

    if FEISHU_SECRET:
        payload["sign"] = _make_feishu_sign(FEISHU_SECRET, timestamp)

    try:
        resp = requests.post(
            FEISHU_WEBHOOK,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json; charset=utf-8"},
            timeout=10,
        )
        data = resp.json()
        if resp.status_code == 200 and data.get("code") == 0:
            logger.info("飞书推送成功")
        else:
            logger.warning(f"飞书推送失败: {resp.status_code} {data}")
    except Exception as e:
        logger.error(f"飞书推送异常: {e}")


def notify(report: str):
    """统一推送入口"""
    send_to_console(report)
    send_to_feishu(report)
