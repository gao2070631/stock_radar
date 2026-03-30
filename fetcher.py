"""
数据获取模块 - 基于 akshare（主数据源）+ 新浪/腾讯（兜底备用）

兜底策略：
  - 每个接口均有 try/except，失败时自动降级到备用数据源
  - 实时行情：akshare 东财 → akshare 新浪 → 空字典（报警）
  - K线：akshare 东财 → akshare 新浪 → 空 DataFrame（报警）
  - 资金流向：akshare 东财 → 空字典（仅日内有效，无其他可靠备源）
  - 大盘指数：akshare 东财 → akshare 新浪 → 空字典
  - 板块行情：akshare 东财（字段自适应）→ 降级跳过
"""
import akshare as ak
import pandas as pd
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# ────────────────────────────────────────────────
# 内部工具
# ────────────────────────────────────────────────

def _parse_spot_row(r, code: str) -> dict:
    """统一解析行情行（兼容东财/新浪字段差异）"""
    def _f(keys, default=0.0):
        for k in keys:
            v = r.get(k)
            if v is not None and v != "" and str(v).strip() != "-":
                try:
                    return float(v)
                except (ValueError, TypeError):
                    pass
        return default

    return {
        "code": code,
        "name": r.get("名称", r.get("name", "")),
        "price": _f(["最新价", "最新"]),
        "change_pct": _f(["涨跌幅"]),
        "change_amt": _f(["涨跌额"]),
        "volume": _f(["成交量"]),
        "amount": _f(["成交额"]),
        "open": _f(["今开", "开盘价", "今日开盘价"]),
        "high": _f(["最高", "最高价", "今日最高价"]),
        "low": _f(["最低", "最低价", "今日最低价"]),
        "close_prev": _f(["昨收", "昨日收盘价"]),
        "turnover_rate": _f(["换手率"]),
        "pe": r.get("市盈率-动态", None),
        "pb": r.get("市净率", None),
        "total_mv": r.get("总市值", None),
        "float_mv": r.get("流通市值", None),
    }


def _is_etf(code: str) -> bool:
    """判断是否为 ETF（以5开头的纯数字，或含字母）"""
    return code.startswith("5") or code.startswith("15") or not code.isdigit()


# ────────────────────────────────────────────────
# 实时行情（主 + 兜底）
# ────────────────────────────────────────────────

_spot_em_cache: tuple[pd.DataFrame | None, str] = (None, "")   # (df, date_str)
_etf_spot_cache: tuple[pd.DataFrame | None, str] = (None, "")


def _get_spot_em() -> pd.DataFrame:
    """A股全市场实时行情（东财），带简单内存缓存（同分钟不重复请求）"""
    global _spot_em_cache
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    if _spot_em_cache[1] == ts and _spot_em_cache[0] is not None:
        return _spot_em_cache[0]
    df = ak.stock_zh_a_spot_em()
    _spot_em_cache = (df, ts)
    return df


def _get_etf_spot() -> pd.DataFrame:
    """ETF 全市场实时行情（东财），带简单内存缓存"""
    global _etf_spot_cache
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    if _etf_spot_cache[1] == ts and _etf_spot_cache[0] is not None:
        return _etf_spot_cache[0]
    df = ak.fund_etf_spot_em()
    _etf_spot_cache = (df, ts)
    return df


def get_stock_realtime(code: str, market: str) -> dict:
    """
    获取单只股票/ETF 实时行情
    兜底链路：
      ETF → fund_etf_spot_em(东财) → 返回空
      A股 → stock_zh_a_spot_em(东财) → stock_zh_a_spot(新浪) → 返回空
    """
    if _is_etf(code):
        # ETF 主渠道
        try:
            df = _get_etf_spot()
            row = df[df["代码"] == code]
            if not row.empty:
                return _parse_spot_row(row.iloc[0], code)
            logger.warning(f"ETF {code} 在东财行情中未找到")
        except Exception as e:
            logger.warning(f"[兜底] 东财ETF行情失败({code}): {e}")

        # ETF 兜底：尝试A股全量接口（部分ETF在里面）
        try:
            df = _get_spot_em()
            row = df[df["代码"] == code]
            if not row.empty:
                return _parse_spot_row(row.iloc[0], code)
        except Exception as e:
            logger.warning(f"[兜底] 东财A股行情中查ETF失败({code}): {e}")
        logger.error(f"❌ {code} 行情所有渠道均失败")
        return {}

    # 普通A股：东财主渠道
    try:
        df = _get_spot_em()
        row = df[df["代码"] == code]
        if not row.empty:
            return _parse_spot_row(row.iloc[0], code)
        logger.warning(f"A股 {code} 在东财行情中未找到")
    except Exception as e:
        logger.warning(f"[兜底] 东财A股行情失败({code}): {e}")

    # 兜底：新浪实时接口
    try:
        symbol = f"sh{code}" if code.startswith("6") or code.startswith("0") else f"sz{code}"
        df_sina = ak.stock_zh_a_spot()
        row = df_sina[df_sina["代码"] == code]
        if not row.empty:
            logger.info(f"[兜底] {code} 使用新浪备用行情")
            r = row.iloc[0]
            return {
                "code": code,
                "name": str(r.get("名称", "")),
                "price": float(r.get("最新价", 0) or 0),
                "change_pct": float(r.get("涨跌幅", 0) or 0),
                "change_amt": float(r.get("涨跌额", 0) or 0),
                "volume": float(r.get("成交量", 0) or 0),
                "amount": float(r.get("成交额", 0) or 0),
                "open": float(r.get("今开", 0) or 0),
                "high": float(r.get("最高", 0) or 0),
                "low": float(r.get("最低", 0) or 0),
                "close_prev": float(r.get("昨收", 0) or 0),
                "turnover_rate": 0.0,
                "pe": None, "pb": None,
                "total_mv": None, "float_mv": None,
            }
    except Exception as e:
        logger.warning(f"[兜底] 新浪A股行情也失败({code}): {e}")

    logger.error(f"❌ {code} 行情所有渠道均失败")
    return {}


def _get_etf_hist_fallback(code: str) -> pd.DataFrame:
    """
    ETF 历史数据专用兜底
    链路：新浪 fund_etf_hist_sina → 腾讯 stock_zh_a_hist_tx
    """
    # 新浪：sh + code
    try:
        symbol = f"sh{code}" if code.startswith(("5", "51", "58")) else f"sz{code}"
        df = ak.fund_etf_hist_sina(symbol=symbol)
        if not df.empty:
            logger.info(f"[ETF历史兜底] {code} 新浪接口成功，{len(df)}条")
            df = df.rename(columns={
                "date": "日期", "open": "开盘", "close": "收盘",
                "high": "最高", "low": "最低", "volume": "成交量", "amount": "成交额"
            })
            return df.tail(20)
    except Exception as e:
        logger.warning(f"[ETF历史兜底] 新浪失败({code}): {e}")

    # 腾讯兜底
    try:
        symbol_tx = f"sh{code}" if code.startswith(("5", "51", "58")) else f"sz{code}"
        start = (datetime.now() - pd.Timedelta(days=30)).strftime("%Y%m%d")
        end = datetime.now().strftime("%Y%m%d")
        df = ak.stock_zh_a_hist_tx(symbol=symbol_tx, start_date=start, end_date=end)
        if not df.empty:
            logger.info(f"[ETF历史兜底] {code} 腾讯接口成功，{len(df)}条")
            # 腾讯列名: 日期/开盘/收盘/最高/最低/成交量
            df.columns = ["日期", "开盘", "收盘", "最高", "最低", "成交量"]
            return df.tail(20)
    except Exception as e:
        logger.warning(f"[ETF历史兜底] 腾讯也失败({code}): {e}")

    return pd.DataFrame()


def get_stock_realtime_with_fallback(code: str, market: str) -> dict:
    """
    获取实时行情，若 price=0（收盘/无数据），自动用最近一个交易日历史数据兜底
    """
    data = get_stock_realtime(code, market)
    if data and float(data.get("price", 0)) > 0:
        return data

    # 实时数据为空或 price=0，用历史收盘数据兜底
    logger.info(f"{code} 实时行情为空，尝试用历史日线兜底")
    try:
        # ETF 优先用专用历史接口
        if _is_etf(code):
            df = _get_etf_hist_fallback(code)
            if df.empty:
                df = get_stock_kline(code, market, periods=3)
        else:
            df = get_stock_kline(code, market, periods=3)
        if not df.empty:
            last = df.iloc[-1]
            # 兼容东财/新浪列名
            def _col(candidates):
                for c in candidates:
                    if c in df.columns:
                        return float(last.get(c, 0) or 0)
                return 0.0

            close = _col(["收盘", "close"])
            open_ = _col(["开盘", "open"])
            high  = _col(["最高", "high"])
            low   = _col(["最低", "low"])
            vol   = _col(["成交量", "volume"])
            amt   = _col(["成交额", "amount"])
            date  = str(last.get("日期", last.get("date", "")))[:10]

            # 计算涨跌（用前一天收盘）
            prev_close = 0.0
            if len(df) >= 2:
                prev_close = float(df.iloc[-2].get("收盘", df.iloc[-2].get("close", 0)) or 0)
            change_amt = round(close - prev_close, 3) if prev_close else 0
            change_pct = round(change_amt / prev_close * 100, 2) if prev_close else 0

            logger.info(f"{code} 使用历史收盘数据兜底：{date} 收盘价 {close}")
            result = data.copy() if data else {}
            result.update({
                "code": code,
                "price": close,
                "change_pct": change_pct,
                "change_amt": change_amt,
                "volume": vol,
                "amount": amt,
                "open": open_,
                "high": high,
                "low": low,
                "_is_history": True,
                "_history_date": date,
            })
            if not result.get("name"):
                result["name"] = code
            return result
    except Exception as e:
        logger.warning(f"历史数据兜底失败({code}): {e}")

    return data


# ────────────────────────────────────────────────
# K 线（主 + 兜底）
# ────────────────────────────────────────────────

def get_stock_kline(code: str, market: str, periods: int = 20) -> pd.DataFrame:
    """
    获取近N日K线（日线，前复权）
    兜底链路：stock_zh_a_hist(东财) → stock_zh_a_daily(新浪) → 空 DataFrame
    """
    start = (datetime.now() - pd.Timedelta(days=periods * 2)).strftime("%Y%m%d")
    end   = datetime.now().strftime("%Y%m%d")

    # 主渠道：东财
    try:
        df = ak.stock_zh_a_hist(
            symbol=code, period="daily", adjust="qfq",
            start_date=start, end_date=end,
        )
        if not df.empty:
            return df.tail(periods)
    except Exception as e:
        logger.warning(f"[兜底] 东财K线失败({code}): {e}")

    # 兜底：新浪日线（仅对普通A股有效）
    if not _is_etf(code):
        try:
            symbol = f"sh{code}" if code.startswith("6") else f"sz{code}"
            df = ak.stock_zh_a_daily(symbol=symbol, adjust="qfq")
            if not df.empty:
                logger.info(f"[兜底] {code} 使用新浪日线数据")
                # 列名适配为东财格式
                df = df.rename(columns={
                    "date": "日期", "open": "开盘", "high": "最高",
                    "low": "最低", "close": "收盘", "volume": "成交量",
                })
                return df.tail(periods)
        except Exception as e:
            logger.warning(f"[兜底] 新浪日线也失败({code}): {e}")

    # ETF 专用历史接口兜底
    if _is_etf(code):
        df = _get_etf_hist_fallback(code)
        if not df.empty:
            return df

    logger.error(f"❌ {code} K线所有渠道均失败")
    return pd.DataFrame()


# ────────────────────────────────────────────────
# 资金流向
# ────────────────────────────────────────────────

def get_fund_flow(code: str) -> dict:
    """
    获取个股/ETF资金流向
    ETF → 直接读 fund_etf_spot_em 内嵌字段（已含资金流）
    A股 → stock_individual_fund_flow（东财）
    无可靠三方兜底，失败返回空字典
    """
    if _is_etf(code):
        try:
            df = _get_etf_spot()
            row = df[df["代码"] == code]
            if not row.empty:
                r = row.iloc[0]
                return {
                    "main_net_inflow": float(r.get("主力净流入-净额", 0) or 0),
                    "main_net_pct":    float(r.get("主力净流入-净占比", 0) or 0),
                    "super_large_net": float(r.get("超大单净流入-净额", 0) or 0),
                    "large_net":       float(r.get("大单净流入-净额", 0) or 0),
                    "medium_net":      float(r.get("中单净流入-净额", 0) or 0),
                    "small_net":       float(r.get("小单净流入-净额", 0) or 0),
                }
        except Exception as e:
            logger.warning(f"[兜底] ETF资金流失败({code}): {e}")
        return {}

    try:
        mkt = "sh" if code.startswith("6") else "sz"
        df = ak.stock_individual_fund_flow(stock=code, market=mkt)
        if not df.empty:
            today = df.iloc[-1]
            return {
                "main_net_inflow": float(today.get("主力净流入-净额", 0) or 0),
                "main_net_pct":    float(today.get("主力净流入-净占比", 0) or 0),
                "super_large_net": float(today.get("超大单净流入-净额", 0) or 0),
                "large_net":       float(today.get("大单净流入-净额", 0) or 0),
                "medium_net":      float(today.get("中单净流入-净额", 0) or 0),
                "small_net":       float(today.get("小单净流入-净额", 0) or 0),
            }
    except Exception as e:
        logger.warning(f"资金流向获取失败({code}): {e}")
    return {}


# ────────────────────────────────────────────────
# 大盘指数（主 + 兜底）
# ────────────────────────────────────────────────

def get_market_index() -> dict:
    """
    获取大盘主要指数
    兜底链路：stock_zh_index_spot_em(东财) → stock_zh_index_spot(新浪)
    """
    targets = {
        "上证指数": "000001", "深证成指": "399001",
        "创业板指": "399006", "科创50":  "000688",
        "沪深300":  "000300", "北证50":  "899050",
    }
    result = {}

    # 主渠道：东财
    try:
        df = ak.stock_zh_index_spot_em()
        for name, code in targets.items():
            row = df[df["代码"] == code]
            if not row.empty:
                r = row.iloc[0]
                result[name] = {
                    "code": code,
                    "price":      float(r.get("最新价", 0) or 0),
                    "change_pct": float(r.get("涨跌幅", 0) or 0),
                    "change_amt": float(r.get("涨跌额", 0) or 0),
                    "volume":     float(r.get("成交量", 0) or 0),
                    "amount":     float(r.get("成交额", 0) or 0),
                }
        if result:
            return result
    except Exception as e:
        logger.warning(f"[兜底] 东财指数行情失败: {e}")

    # 兜底：新浪指数
    try:
        df = ak.stock_zh_index_spot()
        for name, code in targets.items():
            if name in result:
                continue
            row = df[df["代码"] == code]
            if not row.empty:
                r = row.iloc[0]
                result[name] = {
                    "code": code,
                    "price":      float(r.get("最新价", r.get("当前价", 0)) or 0),
                    "change_pct": float(r.get("涨跌幅", 0) or 0),
                    "change_amt": float(r.get("涨跌额", 0) or 0),
                    "volume":     float(r.get("成交量", 0) or 0),
                    "amount":     float(r.get("成交额", 0) or 0),
                }
        if result:
            logger.info("[兜底] 使用新浪指数行情")
    except Exception as e:
        logger.warning(f"[兜底] 新浪指数也失败: {e}")

    return result


# ────────────────────────────────────────────────
# 板块行情（资金流向 + 成交量活跃）
# ────────────────────────────────────────────────

def _safe_cols(df: pd.DataFrame, preferred: list, fallback: list) -> list:
    cols = [c for c in preferred if c in df.columns]
    return cols if cols else [c for c in fallback if c in df.columns]


def get_sector_performance() -> dict:
    """
    获取板块涨跌 + 资金流向 + 成交量最活跃板块
    字段自适应（兼容东财接口不同版本）
    """
    result = {"industry": {}, "concept": {}}

    for key, fetch_fn, label in [
        ("industry", ak.stock_board_industry_name_em, "行业板块"),
        ("concept",  ak.stock_board_concept_name_em,  "概念板块"),
    ]:
        try:
            df = fetch_fn()
            if df.empty:
                continue

            # ── 字段探查 ──
            chg_col    = "涨跌幅" if "涨跌幅" in df.columns else None
            amt_col    = next((c for c in ["总市值", "成交额", "换手率"] if c in df.columns), None)
            name_col   = "板块名称" if "板块名称" in df.columns else df.columns[1]

            # 主力净流入（板块级别，字段名因版本而异）
            flow_col   = next((c for c in ["主力净流入", "主力净额", "净额", "主力净流入-净额"] if c in df.columns), None)

            if not chg_col:
                logger.warning(f"{label} 无涨跌幅字段，跳过")
                continue

            df_sorted = df.sort_values(chg_col, ascending=False)

            # 涨幅 Top3 / Bottom3
            disp_cols = _safe_cols(df, [name_col, chg_col], [name_col])
            top3 = df_sorted.head(3)[disp_cols].to_dict("records")
            bot3 = df_sorted.tail(3)[disp_cols].to_dict("records")

            # 资金流向排名（主力净流入最多 Top3）
            flow_top3 = []
            if flow_col:
                df_flow = df.copy()
                df_flow[flow_col] = pd.to_numeric(df_flow[flow_col], errors="coerce")
                df_flow_sorted = df_flow.sort_values(flow_col, ascending=False)
                flow_cols = _safe_cols(df_flow, [name_col, flow_col, chg_col], [name_col, flow_col])
                flow_top3 = df_flow_sorted.head(3)[flow_cols].to_dict("records")

            # 成交量活跃排名（成交额/总市值 最大 Top3）
            active_top3 = []
            if amt_col:
                df_amt = df.copy()
                df_amt[amt_col] = pd.to_numeric(df_amt[amt_col], errors="coerce")
                df_amt_sorted = df_amt.sort_values(amt_col, ascending=False)
                act_cols = _safe_cols(df_amt, [name_col, amt_col, chg_col], [name_col, amt_col])
                active_top3 = df_amt_sorted.head(3)[act_cols].to_dict("records")

            result[key] = {
                "top":       top3,
                "bottom":    bot3,
                "flow_top":  flow_top3,
                "active_top": active_top3,
                "flow_col":  flow_col,
                "amt_col":   amt_col,
                "name_col":  name_col,
                "chg_col":   chg_col,
            }

        except Exception as e:
            logger.error(f"获取{label}失败: {e}")

    return result


# ────────────────────────────────────────────────
# 市场情绪（涨跌家数）
# ────────────────────────────────────────────────

def get_market_breadth() -> dict:
    """
    获取市场涨跌家数
    兜底链路：stock_zh_a_spot_em → 直接跳过（已在 get_stock_realtime 缓存）
    """
    try:
        df = _get_spot_em()
        up         = int((df["涨跌幅"] > 0).sum())
        down       = int((df["涨跌幅"] < 0).sum())
        flat       = int((df["涨跌幅"] == 0).sum())
        limit_up   = int((df["涨跌幅"] >= 9.9).sum())
        limit_down = int((df["涨跌幅"] <= -9.9).sum())
        total_amt  = float(df["成交额"].sum())
        return {
            "up": up, "down": down, "flat": flat,
            "limit_up": limit_up, "limit_down": limit_down,
            "total_amount": total_amt,
        }
    except Exception as e:
        logger.error(f"获取市场宽度失败: {e}")
        return {}
