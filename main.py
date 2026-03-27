"""
股票雷达 (Stock Radar)
A股每周交易日，9:15 ~ 14:55 每30分钟推送分析报告

目标股票：江山股份、科创100ETF、科创50ETF

【节假日过滤】
使用 akshare 的 tool_trade_date_hist_sina 接口获取 A 股实际交易日历，
自动过滤节假日，当天非交易日时跳过推送。
"""
import schedule
import time
import logging
from datetime import datetime
import pytz

from config import TARGET_STOCKS, PUSH_TIMES
from fetcher import (
    get_stock_realtime, get_stock_kline, get_fund_flow,
    get_market_index, get_sector_performance, get_market_breadth
)
from analyzer import build_stock_report, build_market_report
from notifier import notify

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

TZ = pytz.timezone("Asia/Shanghai")

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
        # 列名: trade_date, 类型: object(str) 或 Timestamp
        dates: set[str] = set()
        for val in df["trade_date"]:
            s = str(val)[:10]  # 取 YYYY-MM-DD
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
    now = datetime.now(TZ)
    # 先排除周末
    if now.weekday() >= 5:
        return False
    today = now.strftime("%Y-%m-%d")
    trade_dates = _load_trade_dates(now.year)
    if trade_dates:
        return today in trade_dates
    # 降级：只过滤周末
    return True


def run_analysis():
    """执行一次完整分析并推送"""
    if not is_trading_day():
        logger.info("今日非交易日（含节假日），跳过")
        return

    now = datetime.now(TZ)
    logger.info(f"开始分析 {now.strftime('%Y-%m-%d %H:%M')}")

    report_parts = []
    report_parts.append(f"🕐 {now.strftime('%Y-%m-%d %H:%M')} 股票雷达播报\n")

    # ── 个股分析 ──
    for stock in TARGET_STOCKS:
        code = stock["code"]
        market = stock["market"]
        try:
            realtime = get_stock_realtime(code, market)
            kline = get_stock_kline(code, market, periods=20)
            flow = get_fund_flow(code)
            report = build_stock_report(realtime, kline, flow)
            report_parts.append(report)
        except Exception as e:
            logger.error(f"分析 {stock['name']} 失败: {e}")
            report_parts.append(f"⚠️ {stock['name']}（{code}）数据获取失败: {e}")

    # ── 大盘分析 ──
    try:
        indices = get_market_index()
        breadth = get_market_breadth()
        sectors = get_sector_performance()
        market_report = build_market_report(indices, breadth, sectors)
        report_parts.append(market_report)
    except Exception as e:
        logger.error(f"大盘分析失败: {e}")
        report_parts.append(f"⚠️ 大盘数据获取失败: {e}")

    full_report = "\n\n".join(report_parts)
    notify(full_report)


def setup_schedule():
    """注册定时任务"""
    for t in PUSH_TIMES:
        schedule.every().day.at(t).do(run_analysis)
        logger.info(f"已注册推送时间: {t}")


def main():
    logger.info("🚀 股票雷达启动")
    logger.info(f"目标股票: {[s['name'] for s in TARGET_STOCKS]}")
    logger.info(f"推送时间: {PUSH_TIMES}")

    setup_schedule()

    # 启动时立即执行一次（验证功能）
    run_analysis()

    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()
