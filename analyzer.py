"""
分析模块 - 根据数据生成分析结论
"""
import pandas as pd
import numpy as np
from typing import Optional


def analyze_kline(df: pd.DataFrame) -> dict:
    """K线技术分析（MA、MACD、RSI 简单判断）"""
    if df.empty or len(df) < 10:
        return {"summary": "K线数据不足，无法分析"}

    # 兼容中英文列名（fetcher 已统一，这里作双保险）
    close_col = "收盘" if "收盘" in df.columns else "close"
    vol_col   = "成交量" if "成交量" in df.columns else "volume"
    if close_col not in df.columns:
        return {"summary": f"K线列名异常: {df.columns.tolist()}"}

    close = df[close_col].astype(float)
    volume = df[vol_col].astype(float) if vol_col in df.columns else pd.Series([0]*len(df))

    # 均线
    ma5 = close.rolling(5).mean().iloc[-1]
    ma10 = close.rolling(10).mean().iloc[-1]
    ma20 = close.rolling(20).mean().iloc[-1] if len(df) >= 20 else None

    current = close.iloc[-1]
    prev = close.iloc[-2]

    # RSI (14)
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    rs = gain / loss
    rsi = (100 - 100 / (1 + rs)).iloc[-1]

    # 成交量趋势
    vol_avg5 = volume.rolling(5).mean().iloc[-1]
    vol_today = volume.iloc[-1]
    vol_ratio = vol_today / vol_avg5 if vol_avg5 > 0 else 1

    # 趋势判断
    trend = "震荡"
    if current > ma5 > ma10:
        trend = "短期上升趋势"
    elif current < ma5 < ma10:
        trend = "短期下降趋势"

    # RSI 判断
    rsi_signal = "中性"
    if rsi > 70:
        rsi_signal = "超买区域，注意回调风险"
    elif rsi < 30:
        rsi_signal = "超卖区域，可能存在反弹机会"
    elif rsi > 50:
        rsi_signal = "偏强"
    else:
        rsi_signal = "偏弱"

    # 操作建议
    suggestion = "观望"
    if trend == "短期上升趋势" and rsi < 70 and vol_ratio > 1.2:
        suggestion = "多头信号，可考虑逢低布局"
    elif trend == "短期下降趋势" and rsi > 30:
        suggestion = "空头信号，谨慎操作，注意止损"
    elif rsi < 30 and vol_ratio > 1.5:
        suggestion = "超卖+放量，可能存在反弹，短线可关注"

    return {
        "trend": trend,
        "ma5": round(ma5, 3),
        "ma10": round(ma10, 3),
        "ma20": round(ma20, 3) if ma20 else None,
        "rsi": round(rsi, 2),
        "rsi_signal": rsi_signal,
        "vol_ratio": round(vol_ratio, 2),
        "suggestion": suggestion,
    }


def analyze_fund_flow(flow: dict) -> str:
    """资金流向简析"""
    if not flow:
        return "资金流向数据暂缺"
    net = flow.get("main_net_inflow", 0)
    pct = flow.get("main_net_pct", 0)
    unit = "万"
    val = net / 10000
    if abs(val) >= 10000:
        val /= 10000
        unit = "亿"
    direction = "流入" if net > 0 else "流出"
    strength = "大幅" if abs(pct) > 5 else ("明显" if abs(pct) > 2 else "小幅")
    return f"主力资金{strength}{direction} {abs(val):.2f}{unit}，占比 {pct:.2f}%"


def format_amount(amount: float) -> str:
    """格式化金额"""
    if amount >= 1e8:
        return f"{amount / 1e8:.2f}亿"
    elif amount >= 1e4:
        return f"{amount / 1e4:.2f}万"
    else:
        return f"{amount:.2f}"


def analyze_macd_30m(df: pd.DataFrame) -> str:
    """
    计算30分钟K线 MACD，返回简短结论
    EMA12, EMA26, 信号线9
    """
    if df.empty or len(df) < 5:
        return "30分钟数据不足"

    close_col = "收盘" if "收盘" in df.columns else "close"
    if close_col not in df.columns:
        return "30分钟数据异常"

    close = df[close_col].astype(float)

    try:
        ema12 = close.ewm(span=12, adjust=False).mean()
        ema26 = close.ewm(span=26, adjust=False).mean()
        dif = ema12 - ema26
        dea = dif.ewm(span=9, adjust=False).mean()

        dif_last = dif.iloc[-1]
        dea_last = dea.iloc[-1]
        dif_prev = dif.iloc[-2] if len(dif) >= 2 else dif_last
        dea_prev = dea.iloc[-2] if len(dea) >= 2 else dea_last

        # 金叉：DIF 从下方穿越 DEA
        if dif_prev <= dea_prev and dif_last > dea_last:
            zone = "多头区" if dif_last > 0 else "空头区"
            return f"30分钟MACD金叉（{zone}），短线多头信号"

        # 死叉：DIF 从上方穿越 DEA
        if dif_prev >= dea_prev and dif_last < dea_last:
            zone = "多头区" if dif_last > 0 else "空头区"
            return f"30分钟MACD死叉（{zone}），注意回调"

        # 未金叉/死叉，判断所处区域
        gap = abs(dif_last - dea_last)
        if dif_last > dea_last:
            if dif_last > 0:
                return "30分钟MACD多头运行，趋势偏强"
            else:
                return "30分钟MACD空头区偏强，观望为主"
        else:
            if dif_last < 0:
                return "30分钟MACD空头运行，谨慎偏空"
            else:
                return "30分钟MACD多头区回落，注意方向"

    except Exception as e:
        return f"30分钟MACD计算失败"


def build_stock_report(stock_info: dict, kline_df: pd.DataFrame, fund_flow: dict, macd_30m: str = "") -> str:
    """生成单只股票分析报告"""
    name = stock_info.get("name", "未知")
    code = stock_info.get("code", "")
    price = stock_info.get("price", 0)
    change_pct = stock_info.get("change_pct", 0)
    change_amt = stock_info.get("change_amt", 0)
    volume = stock_info.get("volume", 0)
    amount = stock_info.get("amount", 0)
    turnover = stock_info.get("turnover_rate", 0)
    pe = stock_info.get("pe", "-")
    pb = stock_info.get("pb", "-")

    sign = "▲" if change_pct >= 0 else "▼"
    color_tag = "🔴" if change_pct >= 0 else "🟢"

    # 收盘数据兜底标注
    is_history = stock_info.get("_is_history", False)
    history_date = stock_info.get("_history_date", "")
    data_label = f"📅 收盘数据（{history_date}）" if is_history else ""

    kline_analysis = analyze_kline(kline_df)
    fund_str = analyze_fund_flow(fund_flow)

    lines = [
        f"━━━ {color_tag} {name}（{code}）{' ' + data_label if data_label else ''} ━━━",
        f"现价：{price}  涨跌：{sign}{abs(change_pct):.2f}%（{sign}{abs(change_amt):.3f}）",
        f"成交量：{format_amount(volume * 100)}股  成交额：{format_amount(amount)}",
        f"换手率：{turnover:.2f}%",
        f"",
        f"📊 基本面",
        f"  市盈率(动态)：{pe}  市净率：{pb}",
        f"",
        f"💰 资金流向",
        f"  {fund_str}",
        f"",
        f"📈 K线技术面",
        f"  趋势：{kline_analysis.get('trend', '-')}",
        f"  MA5：{kline_analysis.get('ma5', '-')}  MA10：{kline_analysis.get('ma10', '-')}",
        f"  RSI(14)：{kline_analysis.get('rsi', '-')} → {kline_analysis.get('rsi_signal', '-')}",
        f"  量比：{kline_analysis.get('vol_ratio', '-')}",
        f"",
        f"💡 操作建议：{kline_analysis.get('suggestion', '观望')}",
    ]
    if macd_30m:
        lines.append(f"📉 30分钟MACD：{macd_30m}")
    return "\n".join(lines)


def build_market_report(indices: dict, breadth: dict, sectors: dict) -> str:
    """生成大盘分析报告"""
    lines = ["━━━ 🏛️ 大盘概况 ━━━"]

    for name, info in indices.items():
        sign = "▲" if info["change_pct"] >= 0 else "▼"
        lines.append(
            f"  {name}：{info['price']}  {sign}{abs(info['change_pct']):.2f}%  "
            f"成交额 {format_amount(info['amount'])}"
        )

    if breadth:
        lines += [
            "",
            f"📊 市场情绪",
            f"  上涨 {breadth.get('up', 0)} 家  下跌 {breadth.get('down', 0)} 家  "
            f"涨停 {breadth.get('limit_up', 0)} 家  跌停 {breadth.get('limit_down', 0)} 家",
            f"  全市场成交额：{format_amount(breadth.get('total_amount', 0))}",
        ]
        # 市场情绪判断
        up = breadth.get("up", 1)
        down = breadth.get("down", 1)
        ratio = up / (up + down) if (up + down) > 0 else 0.5
        if ratio > 0.65:
            mood = "偏多，市场情绪较好"
        elif ratio < 0.35:
            mood = "偏空，市场情绪较弱"
        else:
            mood = "分化，多空博弈"
        lines.append(f"  情绪判断：{mood}（上涨占比 {ratio*100:.0f}%）")

    # 板块
    for sector_key, emoji, label in [
        ("industry", "🏭", "行业板块"),
        ("concept",  "💡", "概念板块"),
    ]:
        sec = sectors.get(sector_key, {})
        if not sec:
            continue

        name_col = sec.get("name_col", "板块名称")
        chg_col  = sec.get("chg_col",  "涨跌幅")
        flow_col = sec.get("flow_col")
        amt_col  = sec.get("amt_col")

        lines += ["", f"{emoji} {label}"]

        # 涨跌幅 Top/Bottom
        top = sec.get("top", [])
        bot = sec.get("bottom", [])
        if top:
            lines.append("  涨幅最强：" + " | ".join(
                f"{r.get(name_col, '')} {float(r.get(chg_col, 0)):.2f}%" for r in top
            ))
        if bot:
            lines.append("  涨幅最弱：" + " | ".join(
                f"{r.get(name_col, '')} {float(r.get(chg_col, 0)):.2f}%" for r in bot
            ))

        # 资金流向（主力净流入 Top3）
        flow_top = sec.get("flow_top", [])
        if flow_top and flow_col:
            def _fmt_flow(v):
                try:
                    v = float(v)
                    if abs(v) >= 1e8:
                        return f"{v/1e8:+.2f}亿"
                    return f"{v/1e4:+.0f}万"
                except Exception:
                    return str(v)
            lines.append("  💰主力流入TOP：" + " | ".join(
                f"{r.get(name_col, '')} {_fmt_flow(r.get(flow_col, 0))}"
                + (f" {float(r.get(chg_col, 0)):.2f}%" if chg_col in r else "")
                for r in flow_top
            ))

        # 成交量活跃 Top3
        active_top = sec.get("active_top", [])
        if active_top and amt_col:
            def _fmt_amt(v):
                try:
                    v = float(v)
                    if v >= 1e8:
                        return f"{v/1e8:.0f}亿"
                    if v >= 1e4:
                        return f"{v/1e4:.0f}万"
                    return f"{v:.0f}"
                except Exception:
                    return str(v)
            lines.append(f"  🔥成交活跃TOP({amt_col})：" + " | ".join(
                f"{r.get(name_col, '')} {_fmt_amt(r.get(amt_col, 0))}"
                for r in active_top
            ))

    return "\n".join(lines)
