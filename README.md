# Polymarket Whale Bot

盯 [polymarket.com](https://polymarket.com) 的大额成交，超过你设的阈值就推 Telegram。

参考 [predict_whale_bot](https://github.com/7CCCCC21X/predict_whale_bot) 的架构改造而来，数据源换成
`data-api.polymarket.com/trades`，链上链接走 Polygonscan。

## 功能

- 实时轮询 Polymarket 全网成交，按 USDC 阈值过滤
- 巨鲸卡片：方向 / 数量 / 价格 / 成交额 / 二元市场看涨看空信号
- 每个 chat 独立阈值、语言、暂停
- 地址别名（`/label 0x... 名字`）
- 自动摘要（按市场聚合 Top 3）
- seen 列表 + runtime state 持久化，重启续跑不重复推送
- 中英双语 (`/lang zh|en`)

## 快速开始

```bash
cp .env.example .env
# 把 TG_BOT_TOKEN / TG_CHAT_ID 填上
pip install -r requirements.txt
python polymarket_whale_bot.py
```

## Railway 部署

1. Fork → New Project → Deploy from GitHub
2. Variables 填 `TG_BOT_TOKEN` / `TG_CHAT_ID` / `THRESHOLD_USDC` / `ALLOWED_USER_IDS`
3. 挂 Volume 到 `/data`（不挂的话重启会丢 seen + 订阅者）
4. `railway.toml` 已经把 startCommand 配好了

## 命令

| 命令 | 说明 |
| --- | --- |
| `/menu` | 主菜单（预设阈值 / 自定义 / 暂停 / 语言） |
| `/set 1000` | 直接改阈值（USDC） |
| `/summary` | 立刻看一次摘要 |
| `/set_summary 60` | 自动摘要间隔（分钟），`0` 关 |
| `/pause` · `/resume` | 暂停 / 恢复推送 |
| `/lang zh\|en` | 切语言 |
| `/label 0x... 名字` | 给地址起别名 |
| `/labels` · `/unlabel 0x...` | 列表 / 删别名 |
| `/whoami` | 看自己的 chat_id / user_id / 当前设置 |
| `/subscribers` | 订阅者列表（admin） |
| `/unsubscribe` | 私聊用户退订 |

## 主告警 vs 私聊订阅

- `TG_CHAT_ID` 指向的频道是**主告警**，用 `THRESHOLD_USDC` 全局阈值，只有 `ALLOWED_USER_IDS` 里的人能改
- 任何在 bot 私聊里发命令的用户会自动注册成订阅者，每人独立阈值/语言/暂停状态
- 大单触发后：先按全局阈值推主频道，再 fanout 给每个达阈的订阅者
