"""
股票雷达 (Stock Radar)
A股每周交易日，9:15 ~ 14:55 每30分钟推送分析报告

新增功能：
  - 每日 08:00 晨间要闻推送（含非交易日）
  - 美股/越南/印度/日本/港澳台各市场收盘后30分钟推送

目标股票：江山股份、科创100ETF、科创50ETF

【节假日过滤】
使用 akshare 的 tool_trade_date_hist_sina 接口获取 A 股实际交易日历，
自动过滤节假日，当天非交易日时跳过A股分析推送，但晨报和全球市场推送不受影响。
"""
import schedule
import time
import logging
from datetime import datetime, timezone, timedelta

from config import TARGET_STOCKS, PUSH_TIMES
from fetcher import (
    get_stock_realtime, get_stock_realtime_with_fallback, get_stock_kline, get_stock_kline_30m,
    get_fund_flow, get_market_index, get_sector_performance, get_market_breadth
)
from analyzer import build_stock_report, build_market_report, analyze_macd_30m
from notifier import notify
from news_fetcher import get_morning_news, get_global_market_close, GLOBAL_MARKETS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# 固定东八区，不依赖系统时区或 pytz
TZ = timezone(timedelta(hours=8))


def now_cst() -> datetime:
    """返回当前东八区时间"""
    return datetime.now(TZ)

# 缓存当年交易日集合，避免重复请求
_trade_dates_cache: set[str] = set()
_trade_dates_year: int = 0


def _load_trade_dates(year: int) -> set[str]:
    """从 akshare 加载指定年份的 A 股交易日（格式 YYYY-MM-DD）"""
    global _trade_dates_cache, _trade_dates_year
    if _trade_dates_year == year and _trade_dates_cache:
        return _trade_dates_cache
    try:
        import akshare as ak
        df = ak.tool_trade_date_hist_sina()
        dates: set[str] = set()
        for val in df["trade_date"]:
            s = str(val)[:10]
            if s.startswith(str(year)):
                dates.add(s)
        _trade_dates_cache = dates
        _trade_dates_year = year
        logger.info(f"已加载 {year} 年交易日 {len(dates)} 个")
        return dates
    except Exception as e:
        logger.warning(f"获取交易日历失败（降级为只过滤周末）: {e}")
        return set()


def is_trading_day() -> bool:
    """判断今天是否为 A 股交易日"""
    now = now_cst()
    if now.weekday() >= 5:
        return False
    today = now.strftime("%Y-%m-%d")
    trade_dates = _load_trade_dates(now.year)
    if trade_dates:
        return today in trade_dates
    return True


# ────────────────────────────────────────────────
# A股分析推送（交易日专属）
# ────────────────────────────────────────────────

def run_analysis():
    """执行一次完整A股分析并推送（仅交易日）"""
    if not is_trading_day():
        logger.info("今日非交易日（含节假日），跳过A股分析")
        return

    now = now_cst()
    logger.info(f"开始分析 {now.strftime('%Y-%m-%d %H:%M')} CST")

    report_parts = []
    report_parts.append(f"🕐 {now.strftime('%Y-%m-%d %H:%M')} 股票雷达播报\n")

    # ── 个股分析 ──
    for stock in TARGET_STOCKS:
        code = stock["code"]
        market = stock["market"]
        stock_name = stock["name"]
        try:
            realtime = get_stock_realtime_with_fallback(code, market)
            # 如果接口没返回名称，用配置里的名称兜底
            if not realtime.get("name") or realtime["name"] == code:
                realtime["name"] = stock_name
            kline = get_stock_kline(code, market, periods=20)
            flow = get_fund_flow(code)
            # 30分钟MACD（仅交易时间有意义，失败不影响主流程）
            macd_30m_str = ""
            try:
                kline_30m = get_stock_kline_30m(code, market)
                macd_30m_str = analyze_macd_30m(kline_30m)
            except Exception as e:
                logger.warning(f"30分钟MACD {stock_name} 失败: {e}")
            report = build_stock_report(realtime, kline, flow, macd_30m=macd_30m_str)
            report_parts.append(report)
        except Exception as e:
            logger.error(f"分析 {stock_name} 失败: {e}")
            report_parts.append(f"⚠️ {stock_name}（{code}）数据获取失败: {e}")

    # ── 大盘分析 ──
    try:
        indices = get_market_index()
        breadth = get_market_breadth()
        sectors = get_sector_performance()
        market_report = build_market_report(indices, breadth, sectors)
        # 若大盘数据完全空，给友好提示
        if not indices and not breadth:
            market_report = "━━━ 🏛️ 大盘概况 ━━━\n⚠️ 大盘指数数据源暂时不可用，请关注官方行情"
        report_parts.append(market_report)
    except Exception as e:
        logger.error(f"大盘分析失败: {e}")
        report_parts.append(f"⚠️ 大盘数据获取失败: {e}")

    full_report = "\n\n".join(report_parts)
    notify(full_report)


# ────────────────────────────────────────────────
# 晨间要闻推送（每日08:00，含非交易日）
# ────────────────────────────────────────────────

def run_morning_news():
    """推送晨间要闻（每日，不管交易日）"""
    logger.info("开始获取晨间要闻")
    try:
        report = get_morning_news(max_items=12)
        notify(report)
        logger.info("晨间要闻推送完成")
    except Exception as e:
        logger.error(f"晨间要闻推送失败: {e}")
        notify(f"⚠️ 晨间要闻获取失败: {e}")


# ────────────────────────────────────────────────
# 全球市场收盘推送
# ────────────────────────────────────────────────

def make_global_push(market_key: str):
    """返回一个指定市场的推送函数（用于注册到 schedule）"""
    def _push():
        label = GLOBAL_MARKETS.get(market_key, {}).get("label", market_key)
        logger.info(f"开始推送 {label} 收盘行情")
        try:
            report = get_global_market_close(market_key)
            notify(report)
            logger.info(f"{label} 收盘行情推送完成")
        except Exception as e:
            logger.error(f"{label} 收盘行情推送失败: {e}")
    _push.__name__ = f"push_{market_key}"
    return _push


# ────────────────────────────────────────────────
# 定时任务注册
# ────────────────────────────────────────────────

def setup_schedule():
    """注册所有定时任务"""

    # 1. A股交易日分析（9:15 ~ 14:55 每30分钟）
    for t in PUSH_TIMES:
        schedule.every().day.at(t).do(run_analysis)
        logger.info(f"已注册A股推送时间: {t}")

    # 2. 每日晨间要闻 08:00（含非交易日）
    schedule.every().day.at("08:00").do(run_morning_news)
    logger.info("已注册晨间要闻推送: 08:00（每日）")

    # 3. 全球各市场收盘推送
    for market_key, cfg in GLOBAL_MARKETS.items():
        push_time = cfg["push_time"]
        label = cfg["label"]
        schedule.every().day.at(push_time).do(make_global_push(market_key))
        logger.info(f"已注册全球市场推送: {label} {push_time}")


def main():
    logger.info("🚀 股票雷达启动")
    logger.info(f"目标股票: {[s['name'] for s in TARGET_STOCKS]}")
    logger.info(f"A股推送时间: {PUSH_TIMES}")

    setup_schedule()

    # 启动时立即执行一次A股分析（验证功能）
    run_analysis()

    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()
