# 📈 Stock Radar — A股实时推送雷达

每个 A 股交易日（自动过滤节假日），按固定时间窗口抓取个股行情、资金流向、K 线技术面，结合大盘情绪与行业/概念板块资金动向，推送到飞书群机器人。

---

## ✨ 功能特性

| 功能 | 说明 |
|------|------|
| 📅 节假日过滤 | 接入新浪 A 股交易日历，自动跳过节假日 |
| 📊 个股分析 | 实时行情 + K 线均线/RSI/量比 + 主力资金流向 |
| 🏛️ 大盘概况 | 上证/深证/创业板/科创50/沪深300/北证50 |
| 💰 资金流向板块 | 主力净流入最多的行业/概念 Top3 |
| 🔥 成交活跃板块 | 成交金额最高的行业/概念 Top3 |
| 🛡️ 多重兜底 | 东财接口 → 新浪备用接口，任一失败自动降级 |
| ✉️ 飞书推送 | 支持签名验证的群机器人 Webhook |
| 🔧 灵活配置 | 增减股票只需编辑 `config.py` |

---

## 📁 项目结构

```
stock_radar/
├── config.py       # 配置：股票列表、推送时间、Webhook、签名密钥
├── fetcher.py      # 数据获取（akshare 东财主 + 新浪兜底）
├── analyzer.py     # 分析与报告生成
├── notifier.py     # 推送（控制台 + 飞书 Webhook）
├── main.py         # 入口：定时调度 + 节假日判断
├── run.sh          # 一键启动脚本
├── requirements.txt
└── README.md
```

---

## 🚀 快速开始

### 1. 环境要求

- Python 3.12+
- macOS / Linux

### 2. 安装依赖

```bash
cd stock_radar
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. 配置

编辑 `config.py`：

```python
# 目标股票（随时增删）
TARGET_STOCKS = [
    {"code": "600389", "name": "江山股份",   "market": "SH"},
    {"code": "588190", "name": "科创100ETF", "market": "SH"},
    {"code": "588080", "name": "科创50ETF",  "market": "SH"},
]

# 飞书群机器人 Webhook
FEISHU_WEBHOOK = "https://open.feishu.cn/open-apis/bot/v2/hook/YOUR_HOOK_ID"

# 飞书签名密钥（不需要则置空字符串）
FEISHU_SECRET = "your_secret"
```

### 4. 启动

```bash
bash run.sh
# 或
source .venv/bin/activate && python main.py
```

启动后会立即执行一次分析推送，然后按 `PUSH_TIMES` 定时运行。

---

## ⏰ 推送时间

交易日内每 30 分钟推送一次：

```
09:15  09:45  10:15  10:45
11:15  11:45  13:15  13:45
14:15  14:45  14:55
```

---

## 📋 推送内容示例

```
🕐 2026-03-27 14:45 股票雷达播报

━━━ 🔴 江山股份（600389） ━━━
现价：28.4  涨跌：▲1.76%（▲0.490）
成交量：877.60万股  成交额：2.49亿
换手率：2.04%

📊 基本面
  市盈率(动态)：21.57  市净率：2.99

💰 资金流向
  主力资金明显流出 731.66万，占比 -2.94%

📈 K线技术面
  趋势：短期下降趋势
  MA5：28.472  MA10：29.27
  RSI(14)：45.12 → 偏弱
  量比：0.79

💡 操作建议：空头信号，谨慎操作，注意止损

━━━ 🏛️ 大盘概况 ━━━
  上证指数：3913.72  ▲0.63%  成交额 7996.96亿
  ...

📊 市场情绪
  上涨 4337 家  下跌 1073 家  涨停 100 家  跌停 4 家
  全市场成交额：18638.34亿
  情绪判断：偏多，市场情绪较好（上涨占比 80%）

🏭 行业板块
  涨幅最强：锂 8.88% | 能源金属 7.36% | 医疗研发外包 5.72%
  涨幅最弱：水力发电 -1.37% | 农商行 -1.69% | 风力发电 -2.13%
  💰主力流入TOP：锂 +12.34亿 8.88% | 半导体 +8.76亿 2.15% | ...
  🔥成交活跃TOP(总市值)：银行 45000亿 | 非银金融 32000亿 | ...
```

---

## 🛡️ 兜底策略

| 数据类型 | 主渠道（东财） | 备用渠道（新浪） |
|---------|--------------|--------------|
| 个股实时行情 | `stock_zh_a_spot_em` | `stock_zh_a_spot` |
| ETF 实时行情 | `fund_etf_spot_em` | `stock_zh_a_spot_em` |
| K 线数据 | `stock_zh_a_hist` | `stock_zh_a_daily` |
| 大盘指数 | `stock_zh_index_spot_em` | `stock_zh_index_spot` |
| 资金流向 | `stock_individual_fund_flow` | ⚠️ 无备用（日内接口） |
| 板块行情 | `stock_board_*_name_em` | 字段自适应降级 |

所有接口均有 `try/except` 兜底，单点失败不影响整体推送。

---

## ➕ 增减股票

只需修改 `config.py` 中的 `TARGET_STOCKS`：

```python
TARGET_STOCKS = [
    {"code": "600389", "name": "江山股份",   "market": "SH"},
    {"code": "588190", "name": "科创100ETF", "market": "SH"},
    {"code": "588080", "name": "科创50ETF",  "market": "SH"},
    # 添加新股票 ↓
    {"code": "000001", "name": "平安银行",   "market": "SZ"},
]
```

修改后重启程序生效。

---

## 📦 依赖

```
akshare
requests
schedule
pytz
pandas
numpy
```

---

## ⚠️ 免责声明

本项目仅供学习和个人研究使用，不构成任何投资建议。市场有风险，投资需谨慎。
