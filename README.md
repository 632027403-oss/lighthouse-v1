# Lighthouse V1

手机优先的 Lighthouse 实时数据中枢验证版。

## 已包含
- FastAPI + Render Free 配置
- CCXT（MIT）统一 REST/WebSocket 数据层
- Binance / OKX / Coinbase
- BTC / ETH / SOL
- 每个数据源独立连接、自动重连、指数退避
- WebSocket 失败时 REST fallback
- 数据新鲜度与错误状态
- 手机网页仪表盘

## 设计原则
本版本先把“真实数据持续进入云端”跑稳，再启用 Lighthouse 统计判断层。
不会用假数据填充，也不会把美股实时行情写成看似实时的假数字。

## 部署
Render:
Build: `pip install -r requirements.txt`
Start: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`
Health: `/health`

无需 API Key 即可使用公开市场数据。
