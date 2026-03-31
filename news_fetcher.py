"""
新闻与全球市场数据获取模块

1. get_morning_news() - 获取影响A股的晨间新闻
2. get_global_market_close(market) - 获取指定市场收盘行情
"""
import akshare as ak
import pandas as pd
from datetime import datetime, timezone, timedelta
import logging

# 固定东八区
_CST = timezone(timedelta(hours=8))


def _now() -> datetime:
    return datetime.now(_CST)

logger = logging.getLogger(__name__)

# ────────────────────────────────────────────────
# 全球股指配置
# ────────────────────────────────────────────────

# 各市场目标指数代码（基于 index_global_spot_em 的代码字段）
GLOBAL_MARKETS = {
    "us": {
        "label": "美股",
        "indices": [
            {"code": "SPX",  "name": "标普500"},
            {"code": "DJIA", "name": "道琼斯"},
            {"code": "NDX",  "name": "纳斯达克"},
        ],
        # 美东时间收盘16:00 = 北京时间次日05:00(冬令)/04:00(夏令)，保守用05:30
        "push_time": "05:30",
    },
    "japan": {
        "label": "日本",
        "indices": [
            {"code": "N225", "name": "日经225"},
        ],
        "push_time": "15:45",  # 日经15:15收盘
    },
    "hk": {
        "label": "港股",
        "indices": [
            {"code": "HSI",   "name": "恒生指数"},
            {"code": "HSCEI", "name": "国企指数"},
            {"code": "HSCCI", "name": "红筹指数"},
        ],
        "push_time": "16:30",  # 港股16:00收盘
    },
    "tw": {
        "label": "台湾",
        "indices": [
            {"code": "TWII", "name": "台湾加权"},
        ],
        "push_time": "14:00",  # 台股13:30收盘
    },
    "vietnam": {
        "label": "越南",
        "indices": [
            {"code": "VNINDEX", "name": "越南胡志明"},
        ],
        "push_time": "15:30",  # 越南15:00收盘
    },
    "india": {
        "label": "印度",
        "indices": [
            {"code": "SENSEX", "name": "印度孟买SENSEX"},
        ],
        "push_time": "21:00",  # 印度15:30 IST = 北京18:00，保守+30min
    },
}


# ────────────────────────────────────────────────
# 晨间新闻（每日08:00，含非交易日）
# ────────────────────────────────────────────────

def get_morning_news(max_items: int = 12) -> str:
    """
    获取影响A股走势的晨间要闻
    数据源：财联社全球资讯（stock_info_global_cls）
    兜底：东财股市新闻（stock_news_em）
    """
    news_items = []

    # 主渠道：财联社快讯（最新资讯）
    try:
        df = ak.stock_info_global_cls()
        if not df.empty:
            today = _now().strftime("%Y-%m-%d")
            # 优先取今天的，不够则取最新的
            today_df = df[df["发布日期"] == today] if "发布日期" in df.columns else df
            src = today_df if not today_df.empty else df
            for _, row in src.head(max_items).iterrows():
                title = str(row.get("标题", "")).strip()
                content = str(row.get("内容", "")).strip()
                t = str(row.get("发布时间", "")).strip()
                if title and title != "nan":
                    news_items.append({
                        "time": t,
                        "title": title,
                        "content": content[:120] if content and content != title else "",
                    })
            if news_items:
                logger.info(f"财联社快讯获取 {len(news_items)} 条")
    except Exception as e:
        logger.warning(f"财联社快讯失败: {e}")

    # 兜底：东财市场新闻（如果财联社数据不够）
    if len(news_items) < 5:
        try:
            df2 = ak.stock_news_main_cx()
            for _, row in df2.head(max_items - len(news_items)).iterrows():
                title = str(row.get("标题", row.get("title", ""))).strip()
                t = str(row.get("发布时间", row.get("time", ""))).strip()
                if title and title != "nan":
                    news_items.append({"time": t, "title": title, "content": ""})
            logger.info(f"财新兜底获取 {len(news_items)} 条")
        except Exception as e:
            logger.warning(f"财新新闻也失败: {e}")

    if not news_items:
        return "⚠️ 暂无法获取今日新闻，请稍后关注官方媒体"

    now = _now()
    lines = [
        f"🌅 晨间要闻  {now.strftime('%Y-%m-%d')}",
        "━━━ 今日可能影响A股走势的重要资讯 ━━━",
        "",
    ]
    for i, item in enumerate(news_items[:max_items], 1):
        t = item["time"]
        if t and len(t) > 8:
            t = t[-8:]  # 取时间部分
        prefix = f"[{t}] " if t else ""
        lines.append(f"{i}. {prefix}{item['title']}")
        if item.get("content"):
            # 截取内容摘要（去掉重复的标题部分）
            content = item["content"]
            if item["title"] in content:
                content = content.replace(item["title"], "").strip()
            if content and len(content) > 10:
                lines.append(f"   └ {content[:100]}...")
        lines.append("")

    lines.append("📌 以上资讯来源：财联社 · 仅供参考，不构成投资建议")
    return "\n".join(lines)


# ────────────────────────────────────────────────
# 全球市场收盘行情
# ────────────────────────────────────────────────

_global_spot_cache: tuple = (None, "")  # (df, timestamp_min)


def _get_global_spot() -> pd.DataFrame:
    """获取全球股指实时行情，带分钟级缓存"""
    global _global_spot_cache
    ts = _now().strftime("%Y-%m-%d %H:%M")
    if _global_spot_cache[1] == ts and _global_spot_cache[0] is not None:
        return _global_spot_cache[0]
    df = ak.index_global_spot_em()
    _global_spot_cache = (df, ts)
    return df


def get_global_market_close(market_key: str) -> str:
    """
    生成指定市场的收盘总结报告
    market_key: 'us' | 'japan' | 'hk' | 'tw' | 'vietnam' | 'india'
    """
    cfg = GLOBAL_MARKETS.get(market_key)
    if not cfg:
        return f"未知市场: {market_key}"

    label = cfg["label"]
    indices = cfg["indices"]

    try:
        df = _get_global_spot()
    except Exception as e:
        logger.error(f"获取全球股指失败: {e}")
        return f"⚠️ {label}行情数据获取失败: {e}"

    results = []
    for idx in indices:
        code = idx["code"]
        name = idx["name"]
        row = df[df["代码"] == code]
        if row.empty:
            # 尝试名称匹配
            row = df[df["名称"].str.contains(name[:4])]
        if row.empty:
            logger.warning(f"未找到指数 {code}({name})")
            continue
        r = row.iloc[0]
        price = float(r.get("最新价", 0) or 0)
        chg_pct = float(r.get("涨跌幅", 0) or 0)
        chg_amt = float(r.get("涨跌额", 0) or 0)
        update_time = str(r.get("最新行情时间", ""))
        results.append({
            "name": name,
            "code": code,
            "price": price,
            "chg_pct": chg_pct,
            "chg_amt": chg_amt,
            "update_time": update_time,
        })

    if not results:
        return f"⚠️ {label}市场数据暂缺"

    now = _now()
    lines = [f"🌐 {label}收盘行情  {now.strftime('%Y-%m-%d %H:%M')}"]
    lines.append("━" * 30)

    overall_chg = sum(r["chg_pct"] for r in results) / len(results)
    emoji = "📈" if overall_chg > 0 else ("📉" if overall_chg < 0 else "➡️")

    for r in results:
        sign = "▲" if r["chg_pct"] >= 0 else "▼"
        color = "🔴" if r["chg_pct"] >= 0 else "🟢"  # 红涨绿跌（中国惯例）
        lines.append(
            f"{color} {r['name']}：{r['price']:,.2f}  "
            f"{sign}{abs(r['chg_pct']):.2f}%（{sign}{abs(r['chg_amt']):.2f}）"
        )
        if r["update_time"]:
            lines.append(f"   更新时间：{r['update_time']}")

    lines.append("")

    # 简单总结
    if overall_chg > 1.5:
        summary = f"{emoji} {label}市场大幅上涨，平均涨幅 {overall_chg:.2f}%，市场情绪乐观"
    elif overall_chg > 0.3:
        summary = f"{emoji} {label}市场小幅上涨，平均涨幅 {overall_chg:.2f}%"
    elif overall_chg > -0.3:
        summary = f"➡️ {label}市场基本持平，平均涨跌 {overall_chg:.2f}%"
    elif overall_chg > -1.5:
        summary = f"{emoji} {label}市场小幅下跌，平均跌幅 {abs(overall_chg):.2f}%"
    else:
        summary = f"{emoji} {label}市场大幅下跌，平均跌幅 {abs(overall_chg):.2f}%，注意风险"

    lines.append(summary)
    lines.append("📌 数据来源：东方财富 · 仅供参考，不构成投资建议")
    return "\n".join(lines)


def get_all_global_markets_summary() -> str:
    """获取所有全球主要市场的汇总（用于测试或一次性查询）"""
    parts = []
    for key in GLOBAL_MARKETS:
        parts.append(get_global_market_close(key))
        parts.append("")
    return "\n".join(parts)
