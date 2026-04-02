"""
推送模块 - 控制台输出 + 飞书 Webhook（Interactive Card + 签名验证）
"""
import requests
import json
import hmac
import hashlib
import base64
import logging
import time
import re
from datetime import datetime

from config import FEISHU_WEBHOOK, FEISHU_SECRET

logger = logging.getLogger(__name__)


def _make_feishu_sign(secret: str, timestamp: int) -> str:
    """生成飞书 Webhook 签名"""
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


# ────────────────────────────────────────────────
# 文本 → 飞书 Interactive Card 转换
# ────────────────────────────────────────────────

def _detect_header_color(report: str) -> str:
    """
    根据报告内容决定卡片 header 颜色
    - 晨报/要闻/全球市场 → turquoise
    - 强势(涨>3%) → orange
    - 整体偏涨 → red
    - 整体偏跌 → green
    - 默认 → blue
    """
    # 晨报/要闻/全球
    if any(kw in report for kw in ["晨间要闻", "早报", "全球市场", "美股", "港股收盘", "日股", "越南", "印度"]):
        return "turquoise"

    # 提取涨跌幅数字判断
    pcts = re.findall(r"涨跌[：:]\s*[▲▼]?([+-]?\d+\.?\d*)%", report)
    if not pcts:
        pcts = re.findall(r"[▲▼](\d+\.\d+)%", report)

    vals = []
    for p in pcts:
        try:
            vals.append(float(p))
        except ValueError:
            pass

    if vals:
        avg = sum(vals) / len(vals)
        if avg > 3:
            return "orange"
        elif avg > 0:
            return "red"
        else:
            return "green"

    return "blue"


def _get_header_title(report: str) -> str:
    """从报告中提取卡片标题"""
    now = datetime.now().strftime("%H:%M")
    if any(kw in report for kw in ["晨间要闻", "早报"]):
        return f"📰 晨间要闻  {now}"
    if any(kw in report for kw in ["全球市场", "美股", "港股收盘"]):
        return f"🌏 全球市场收盘  {now}"
    return f"📡 股票雷达播报  {now}"


def _text_to_lark_md(text: str) -> str:
    """将纯文本转为 lark_md 格式"""
    # 标题行（━━━ xxx ━━━）→ 粗体
    text = re.sub(r"━+\s*(.*?)\s*━+", r"**\1**", text)
    # ▲▼ 涨跌符号保留，涨跌幅加粗
    text = re.sub(r"(▲\d+\.\d+%)", r"**\1**", text)
    text = re.sub(r"(▼\d+\.\d+%)", r"**\1**", text)
    return text


def _build_card(report: str) -> dict:
    """
    将文本报告转换为飞书 Interactive Card JSON
    按 ━━━ 分段，每段一个 div element
    """
    header_color = _detect_header_color(report)
    header_title = _get_header_title(report)

    # 按分隔线分块
    # 先按 ━━━ xxx ━━━ 分段（包含标题行）
    sections = re.split(r"\n(?=━)", report)

    elements = []
    for i, section in enumerate(sections):
        section = section.strip()
        if not section:
            continue

        md_text = _text_to_lark_md(section)

        elements.append({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": md_text
            }
        })

        # 块之间加分隔线（最后一块不加）
        if i < len(sections) - 1:
            elements.append({"tag": "hr"})

    if not elements:
        elements.append({
            "tag": "div",
            "text": {"tag": "lark_md", "content": report}
        })

    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {
                "tag": "plain_text",
                "content": header_title
            },
            "template": header_color
        },
        "elements": elements
    }


def send_to_feishu(report: str):
    """推送到飞书群 Webhook（Interactive Card，失败自动降级纯文本）"""
    if not FEISHU_WEBHOOK:
        return

    timestamp = int(time.time())
    sign = _make_feishu_sign(FEISHU_SECRET, timestamp) if FEISHU_SECRET else None

    # ── 优先尝试 Interactive Card ──
    try:
        card = _build_card(report)
        payload: dict = {
            "msg_type": "interactive",
            "card": card,
            "timestamp": str(timestamp),
        }
        if sign:
            payload["sign"] = sign

        resp = requests.post(
            FEISHU_WEBHOOK,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json; charset=utf-8"},
            timeout=10,
        )
        data = resp.json()
        if resp.status_code == 200 and data.get("code") == 0:
            logger.info("飞书卡片推送成功")
            return
        else:
            logger.warning(f"飞书卡片推送失败({data})，降级为纯文本")
    except Exception as e:
        logger.warning(f"飞书卡片构建/推送异常: {e}，降级为纯文本")

    # ── 降级：纯文本 ──
    try:
        payload = {
            "msg_type": "text",
            "content": {"text": report},
            "timestamp": str(timestamp),
        }
        if sign:
            payload["sign"] = sign

        resp = requests.post(
            FEISHU_WEBHOOK,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json; charset=utf-8"},
            timeout=10,
        )
        data = resp.json()
        if resp.status_code == 200 and data.get("code") == 0:
            logger.info("飞书纯文本降级推送成功")
        else:
            logger.warning(f"飞书纯文本降级也失败: {resp.status_code} {data}")
    except Exception as e:
        logger.error(f"飞书推送全部失败: {e}")


def notify(report: str):
    """统一推送入口"""
    send_to_console(report)
    send_to_feishu(report)
