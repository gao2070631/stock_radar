"""
数据获取模块 - 基于 akshare

策略：
  - 东财接口优先（当可用时效果最好）
  - 新浪接口作为主要兜底（稳定性更高）
  - 所有接口均有 try/except，失败时自动降级
  - 实时行情：东财 → 新浪日线最新收盘（兜底）
  - K线：东财 → 新浪日线
  - ETF历史：新浪 fund_etf_hist_sina（东财不稳定时主力）
  - 大盘指数：东财 → 新浪 stock_zh_index_spot_sina
  - 资金流向：东财（无可靠备源，失败返回空）
"""
import akshare as ak
import pandas as pd
import requests
from datetime import datetime, timedelta
import logging
from datetime import timezone as _tz_module

logger = logging.getLogger(__name__)

# 固定东八区
_CST = _tz_module(timedelta(hours=8))

def _now_cst():
    return datetime.now(_CST)

# ────────────────────────────────────────────────
# 内部工具
# ────────────────────────────────────────────────

def _is_etf(code: str) -> bool:
    """判断是否为 ETF（以5或15开头的6位纯数字）"""
    return code.startswith("5") or code.startswith("15")


def _sina_prefix(code: str) -> str:
    """
    给代码加新浪前缀
    上交所（sh）：以 0、6 开头的 A 股，以 5 开头的 ETF（上交所科创/主板ETF）
    深交所（sz）：以 0、3 开头的深市A股，以 15 开头的 ETF（深市ETF）
    """
    if code.startswith("6") or code.startswith("0") or code.startswith("5") or code.startswith("58"):
        return f"sh{code}"
    return f"sz{code}"


def _parse_spot_row(r, code: str) -> dict:
    """统一解析东财行情行"""
    def _f(keys, default=0.0):
        for k in keys:
            v = r.get(k)
            if v is not None and v != "" and str(v).strip() not in ("-", "nan"):
                try:
                    return float(v)
                except (ValueError, TypeError):
                    pass
        return default

    return {
        "code": code,
        "name": str(r.get("名称", r.get("name", ""))),
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


# ────────────────────────────────────────────────
# 新浪实时报价（逐只查询，稳定可靠）
# ────────────────────────────────────────────────

_SINA_HQ_URL = "https://hq.sinajs.cn/list={symbols}"
_SINA_HQ_HEADERS = {
    "Referer": "https://finance.sina.com.cn",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
}


def _get_sina_realtime(codes: list[str]) -> dict[str, dict]:
    """
    通过新浪 hq.sinajs.cn 获取实时行情（支持A股+ETF，今日数据）
    codes: list of 带前缀代码, 如 ['sh600389', 'sh588190']
    返回: {code_without_prefix: info_dict}
    """
    symbols = ",".join(codes)
    try:
        r = requests.get(
            _SINA_HQ_URL.format(symbols=symbols),
            headers=_SINA_HQ_HEADERS,
            timeout=8,
        )
        r.encoding = "gbk"
        result = {}
        for line in r.text.strip().split("\n"):
            line = line.strip()
            if not line or '=""' in line:
                continue
            # var hq_str_sh600389="..."
            try:
                key_part, val_part = line.split("=", 1)
                prefix_code = key_part.strip().replace("var hq_str_", "")
                raw_code = prefix_code[2:]  # 去掉 sh/sz 前缀
                val = val_part.strip().strip('";')
                fields = val.split(",")
                if len(fields) < 10:
                    continue
                price = float(fields[3]) if fields[3] else 0.0
                result[raw_code] = {
                    "code": raw_code,
                    "name": fields[0],
                    "price": price,
                    "open":  float(fields[1]) if fields[1] else 0.0,
                    "close_prev": float(fields[2]) if fields[2] else 0.0,
                    "high": float(fields[4]) if fields[4] else 0.0,
                    "low":  float(fields[5]) if fields[5] else 0.0,
                    "volume": float(fields[8]) if fields[8] else 0.0,
                    "amount": float(fields[9]) if fields[9] else 0.0,
                    "change_amt": round(price - float(fields[2]), 3) if fields[2] and price else 0.0,
                    "change_pct": round((price - float(fields[2])) / float(fields[2]) * 100, 2)
                                  if fields[2] and float(fields[2]) > 0 and price else 0.0,
                    "turnover_rate": 0.0,
                    "pe": None, "pb": None,
                    "total_mv": None, "float_mv": None,
                    "_quote_time": f"{fields[30] if len(fields) > 30 else ''} {fields[31] if len(fields) > 31 else ''}".strip(),
                }
            except Exception as e:
                logger.debug(f"解析新浪行情行失败: {e} | {line[:80]}")
        return result
    except Exception as e:
        logger.warning(f"[新浪实时] 请求失败: {e}")
        return {}


# ────────────────────────────────────────────────
# 内存缓存（同分钟不重复请求）
# ────────────────────────────────────────────────

_spot_em_cache: tuple = (None, "")
_etf_spot_cache: tuple = (None, "")


def _get_spot_em() -> pd.DataFrame:
    """A股全市场实时行情（东财），分钟缓存"""
    global _spot_em_cache
    ts = _now_cst().strftime("%Y-%m-%d %H:%M")
    if _spot_em_cache[1] == ts and _spot_em_cache[0] is not None:
        return _spot_em_cache[0]
    df = ak.stock_zh_a_spot_em()
    _spot_em_cache = (df, ts)
    return df


def _get_etf_spot_em() -> pd.DataFrame:
    """ETF全市场实时行情（东财），分钟缓存"""
    global _etf_spot_cache
    ts = _now_cst().strftime("%Y-%m-%d %H:%M")
    if _etf_spot_cache[1] == ts and _etf_spot_cache[0] is not None:
        return _etf_spot_cache[0]
    df = ak.fund_etf_spot_em()
    _etf_spot_cache = (df, ts)
    return df


# ────────────────────────────────────────────────
# K线获取（统一标准化列名）
# ────────────────────────────────────────────────

def _normalize_kline(df: pd.DataFrame) -> pd.DataFrame:
    """
    将不同来源的K线列名统一为：日期/开盘/收盘/最高/最低/成交量/成交额
    支持东财（中文）和新浪（英文）两种格式
    """
    col_map = {
        "date":   "日期",
        "open":   "开盘",
        "close":  "收盘",
        "high":   "最高",
        "low":    "最低",
        "volume": "成交量",
        "amount": "成交额",
    }
    # 只重命名存在的列
    rename = {k: v for k, v in col_map.items() if k in df.columns}
    if rename:
        df = df.rename(columns=rename)
    # 确保收盘列存在（部分ETF数据列名可能不同）
    if "收盘" not in df.columns and "close" not in df.columns:
        logger.warning(f"K线数据列名异常: {df.columns.tolist()}")
    return df


def get_stock_kline(code: str, market: str, periods: int = 20) -> pd.DataFrame:
    """
    获取近N日K线（日线，前复权）
    兜底链路：
      A股：东财 stock_zh_a_hist → 新浪 stock_zh_a_daily
      ETF：东财 stock_zh_a_hist / fund_etf_hist_em → 新浪 fund_etf_hist_sina
    返回标准化列名（日期/开盘/收盘/最高/最低/成交量）
    """
    start = (_now_cst() - timedelta(days=periods * 2)).strftime("%Y%m%d")
    end = _now_cst().strftime("%Y%m%d")

    # ── 主渠道：东财（A股 + ETF 通用）──
    try:
        df = ak.stock_zh_a_hist(
            symbol=code, period="daily", adjust="qfq",
            start_date=start, end_date=end,
        )
        if not df.empty:
            df = _normalize_kline(df)
            return df.tail(periods)
    except Exception as e:
        logger.warning(f"[东财K线] {code} 失败: {e}")

    # ── ETF 专用：新浪历史接口 ──
    if _is_etf(code):
        try:
            symbol_sina = _sina_prefix(code)
            df = ak.fund_etf_hist_sina(symbol=symbol_sina)
            if not df.empty:
                df = _normalize_kline(df)
                logger.info(f"[新浪ETF历史] {code} 成功，{len(df)}条")
                return df.tail(periods)
        except Exception as e:
            logger.warning(f"[新浪ETF历史] {code} 失败: {e}")

        # ETF 东财专用历史接口
        try:
            df = ak.fund_etf_hist_em(
                symbol=code, period="daily", adjust="qfq",
                start_date=start, end_date=end,
            )
            if not df.empty:
                df = _normalize_kline(df)
                logger.info(f"[东财ETF历史] {code} 成功")
                return df.tail(periods)
        except Exception as e:
            logger.warning(f"[东财ETF历史] {code} 失败: {e}")

    # ── A股：新浪日线 ──
    if not _is_etf(code):
        try:
            symbol_sina = _sina_prefix(code)
            df = ak.stock_zh_a_daily(symbol=symbol_sina, adjust="qfq")
            if not df.empty:
                df = _normalize_kline(df)
                logger.info(f"[新浪A股日线] {code} 成功，{len(df)}条")
                return df.tail(periods)
        except Exception as e:
            logger.warning(f"[新浪A股日线] {code} 失败: {e}")

    logger.error(f"❌ {code} K线所有渠道均失败")
    return pd.DataFrame()


# ────────────────────────────────────────────────
# 实时行情
# ────────────────────────────────────────────────

def get_stock_realtime(code: str, market: str) -> dict:
    """
    获取实时行情（今日最新）
    链路：新浪 hq.sinajs.cn（首选，今日实时） → 东财（备用）
    """
    # ── 首选：新浪实时 ──
    sina_code = _sina_prefix(code)
    sina_data = _get_sina_realtime([sina_code])
    if code in sina_data and float(sina_data[code].get("price", 0)) > 0:
        logger.debug(f"[新浪实时] {code} 成功，价格={sina_data[code]['price']}")
        return sina_data[code]

    logger.warning(f"[新浪实时] {code} 数据为空，尝试东财备用")

    # ── 备用：东财（ETF）──
    if _is_etf(code):
        try:
            df = _get_etf_spot_em()
            row = df[df["代码"] == code]
            if not row.empty:
                return _parse_spot_row(row.iloc[0], code)
        except Exception as e:
            logger.warning(f"[东财ETF实时] {code} 失败: {e}")
        try:
            df = _get_spot_em()
            row = df[df["代码"] == code]
            if not row.empty:
                return _parse_spot_row(row.iloc[0], code)
        except Exception as e:
            logger.warning(f"[东财A股表查ETF] {code} 失败: {e}")
    else:
        # ── 备用：东财（A股）──
        try:
            df = _get_spot_em()
            row = df[df["代码"] == code]
            if not row.empty:
                return _parse_spot_row(row.iloc[0], code)
        except Exception as e:
            logger.warning(f"[东财A股实时] {code} 失败: {e}")

    logger.warning(f"{code} 实时行情全渠道失败，将用历史兜底")
    return {}


def get_stock_realtime_with_fallback(code: str, market: str) -> dict:
    """
    获取实时行情；若 price=0 或数据为空，自动用最近K线兜底
    """
    data = get_stock_realtime(code, market)
    if data and float(data.get("price", 0)) > 0:
        return data

    # 实时数据不可用，用历史日线最新收盘兜底
    logger.info(f"{code} 实时行情不可用，尝试历史日线兜底")
    try:
        df = get_stock_kline(code, market, periods=5)
        if not df.empty:
            last = df.iloc[-1]

            def _col(names, default=0.0):
                for n in names:
                    if n in df.columns:
                        try:
                            return float(last[n] or 0)
                        except Exception:
                            pass
                return default

            close = _col(["收盘"])
            open_ = _col(["开盘"])
            high  = _col(["最高"])
            low   = _col(["最低"])
            vol   = _col(["成交量"])
            amt   = _col(["成交额"])
            date  = str(last.get("日期", ""))[:10]

            prev_close = 0.0
            if len(df) >= 2:
                try:
                    prev_close = float(df.iloc[-2]["收盘"] or 0)
                except Exception:
                    pass

            change_amt = round(close - prev_close, 3) if prev_close else 0
            change_pct = round(change_amt / prev_close * 100, 2) if prev_close else 0

            logger.info(f"{code} 历史兜底成功：{date} 收盘价 {close}")
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
        logger.warning(f"历史兜底失败({code}): {e}")

    return data if data else {}


# ────────────────────────────────────────────────
# 资金流向
# ────────────────────────────────────────────────

def get_fund_flow(code: str) -> dict:
    """
    获取个股/ETF资金流向
    ETF → fund_etf_spot_em 内嵌字段
    A股 → stock_individual_fund_flow（东财）
    无可靠三方兜底，失败返回空字典
    """
    if _is_etf(code):
        try:
            df = _get_etf_spot_em()
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
            logger.warning(f"[ETF资金流] {code} 失败: {e}")
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
        logger.warning(f"[A股资金流] {code} 失败: {e}")
    return {}


# ────────────────────────────────────────────────
# 大盘指数
# ────────────────────────────────────────────────

# 新浪指数代码映射（带 sh/sz 前缀）
_INDEX_SINA_CODES = {
    "上证指数": "sh000001",
    "深证成指": "sz399001",
    "创业板指": "sz399006",
    "科创50":   "sh000688",
    "沪深300":  "sh000300",
    "北证50":   "bj899050",
}

_INDEX_EM_CODES = {
    "上证指数": "000001",
    "深证成指": "399001",
    "创业板指": "399006",
    "科创50":   "000688",
    "沪深300":  "000300",
    "北证50":   "899050",
}

_index_cache: tuple = (None, "")


def _get_index_sina(retries: int = 3) -> pd.DataFrame:
    """新浪指数实时数据（分钟缓存，自动重试）"""
    global _index_cache
    ts = _now_cst().strftime("%Y-%m-%d %H:%M")
    if _index_cache[1] == ts and _index_cache[0] is not None:
        return _index_cache[0]
    import time
    last_err = None
    for i in range(retries):
        try:
            df = ak.stock_zh_index_spot_sina()
            if not df.empty:
                _index_cache = (df, ts)
                return df
        except Exception as e:
            last_err = e
            if i < retries - 1:
                time.sleep(2)
    raise RuntimeError(f"新浪指数重试{retries}次均失败: {last_err}")


def get_market_index() -> dict:
    """
    获取大盘主要指数
    兜底链路：东财 stock_zh_index_spot_em → 新浪 stock_zh_index_spot_sina
    """
    result = {}

    # 主渠道：东财
    try:
        df = ak.stock_zh_index_spot_em()
        for name, code in _INDEX_EM_CODES.items():
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
        logger.warning(f"[东财指数] 失败: {e}")

    # 兜底：新浪（代码带 sh/sz 前缀）
    try:
        df = _get_index_sina()
        for name, sina_code in _INDEX_SINA_CODES.items():
            if name in result:
                continue
            row = df[df["代码"] == sina_code]
            if not row.empty:
                r = row.iloc[0]
                result[name] = {
                    "code": sina_code,
                    "price":      float(r.get("最新价", 0) or 0),
                    "change_pct": float(r.get("涨跌幅", 0) or 0),
                    "change_amt": float(r.get("涨跌额", 0) or 0),
                    "volume":     float(r.get("成交量", 0) or 0),
                    "amount":     float(r.get("成交额", 0) or 0),
                }
        if result:
            logger.info("[新浪指数] 兜底成功")
    except Exception as e:
        logger.warning(f"[新浪指数] 也失败: {e}")

    return result


# ────────────────────────────────────────────────
# 市场情绪（涨跌家数）
# ────────────────────────────────────────────────

def get_market_breadth() -> dict:
    """
    获取市场涨跌家数（来自东财全量行情）
    东财挂时直接返回空
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
        logger.warning(f"市场宽度获取失败（东财不可用）: {e}")
        return {}


# ────────────────────────────────────────────────
# 板块行情
# ────────────────────────────────────────────────

def _safe_cols(df: pd.DataFrame, preferred: list, fallback: list) -> list:
    cols = [c for c in preferred if c in df.columns]
    return cols if cols else [c for c in fallback if c in df.columns]


def get_sector_performance() -> dict:
    """
    获取板块涨跌 + 资金流向 + 成交量活跃度
    仅依赖东财（板块数据无可靠新浪备源）；东财挂时返回空
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

            chg_col  = "涨跌幅" if "涨跌幅" in df.columns else None
            amt_col  = next((c for c in ["总市值", "成交额", "换手率"] if c in df.columns), None)
            name_col = "板块名称" if "板块名称" in df.columns else df.columns[1]
            flow_col = next((c for c in ["主力净流入", "主力净额", "净额", "主力净流入-净额"] if c in df.columns), None)

            if not chg_col:
                continue

            df_sorted = df.sort_values(chg_col, ascending=False)
            disp_cols = _safe_cols(df, [name_col, chg_col], [name_col])
            top3 = df_sorted.head(3)[disp_cols].to_dict("records")
            bot3 = df_sorted.tail(3)[disp_cols].to_dict("records")

            flow_top3 = []
            if flow_col:
                df_flow = df.copy()
                df_flow[flow_col] = pd.to_numeric(df_flow[flow_col], errors="coerce")
                df_flow_sorted = df_flow.sort_values(flow_col, ascending=False)
                flow_cols = _safe_cols(df_flow, [name_col, flow_col, chg_col], [name_col, flow_col])
                flow_top3 = df_flow_sorted.head(3)[flow_cols].to_dict("records")

            active_top3 = []
            if amt_col:
                df_amt = df.copy()
                df_amt[amt_col] = pd.to_numeric(df_amt[amt_col], errors="coerce")
                df_amt_sorted = df_amt.sort_values(amt_col, ascending=False)
                act_cols = _safe_cols(df_amt, [name_col, amt_col, chg_col], [name_col, amt_col])
                active_top3 = df_amt_sorted.head(3)[act_cols].to_dict("records")

            result[key] = {
                "top": top3, "bottom": bot3,
                "flow_top": flow_top3, "active_top": active_top3,
                "flow_col": flow_col, "amt_col": amt_col,
                "name_col": name_col, "chg_col": chg_col,
            }
        except Exception as e:
            logger.warning(f"[{label}] 获取失败（东财不可用）: {e}")

    return result
