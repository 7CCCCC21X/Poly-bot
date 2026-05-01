#!/usr/bin/env python3
"""
Polymarket 大额成交 Telegram 监控机器人。

数据源：https://data-api.polymarket.com/trades
链上结算：Polygon (polygonscan.com)
前端：polymarket.com

Railway 部署：startCommand = "python -u polymarket_whale_bot.py"
"""

from __future__ import annotations

import asyncio
import hashlib
import html
import json
import logging
import os
import random
import re
import signal
import sys
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx
from dotenv import load_dotenv


LOG = logging.getLogger("polymarket_whale_bot")


# ==========================================================================
#  环境变量解析
# ==========================================================================

def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_decimal(name: str, default: str) -> Decimal:
    raw = os.getenv(name, default).strip()
    try:
        return Decimal(raw)
    except InvalidOperation as exc:
        raise RuntimeError(f"环境变量 {name} 不是合法数字: {raw}") from exc


def parse_user_id_list(raw: str) -> "frozenset[int]":
    out: "set[int]" = set()
    for token in (raw or "").split(","):
        token = token.strip()
        if not token:
            continue
        try:
            out.add(int(token))
        except ValueError:
            LOG.warning("ALLOWED_USER_IDS 里有非整数项已忽略: %r", token)
    return frozenset(out)


@dataclass(frozen=True)
class Config:
    data_api_base: str
    gamma_api_base: str

    tg_bot_token: str
    tg_chat_id: str

    threshold_usdc: Decimal

    poll_interval_sec: float
    trades_page_size: int
    trades_max_pages: int
    alert_on_startup: bool
    max_seen_ids: int
    seen_state_path: str
    runtime_state_path: str

    request_timeout_sec: float
    api_max_retries: int

    market_url_template: str
    event_url_template: str
    tx_url_template: str
    user_url_template: str

    allowed_user_ids: frozenset

    default_lang: str
    summary_interval_sec: int
    display_tz: str
    max_alert_age_sec: int

    @classmethod
    def from_env(cls) -> "Config":
        load_dotenv()

        token = os.getenv("TG_BOT_TOKEN", "").strip()
        chat_id = os.getenv("TG_CHAT_ID", "").strip()

        if not token:
            raise RuntimeError("缺少 TG_BOT_TOKEN。请在 Railway Variables 里填写。")
        if not chat_id:
            raise RuntimeError("缺少 TG_CHAT_ID。请在 Railway Variables 里填写。")

        return cls(
            data_api_base=os.getenv("DATA_API_BASE", "https://data-api.polymarket.com").rstrip("/"),
            gamma_api_base=os.getenv("GAMMA_API_BASE", "https://gamma-api.polymarket.com").rstrip("/"),

            tg_bot_token=token,
            tg_chat_id=chat_id,

            threshold_usdc=env_decimal("THRESHOLD_USDC", "10000"),

            poll_interval_sec=float(os.getenv("POLL_INTERVAL_SEC", "3")),
            trades_page_size=int(os.getenv("TRADES_PAGE_SIZE", "100")),
            trades_max_pages=int(os.getenv("TRADES_MAX_PAGES", "5")),
            alert_on_startup=env_bool("ALERT_ON_STARTUP", False),
            max_seen_ids=int(os.getenv("MAX_SEEN_IDS", "10000")),
            seen_state_path=os.getenv("SEEN_STATE_PATH", "/data/polymarket_seen.json").strip(),
            runtime_state_path=os.getenv("RUNTIME_STATE_PATH", "/data/runtime_state.json").strip(),

            request_timeout_sec=float(os.getenv("REQUEST_TIMEOUT_SEC", "12")),
            api_max_retries=int(os.getenv("API_MAX_RETRIES", "5")),

            market_url_template=os.getenv(
                "MARKET_URL_TEMPLATE", "https://polymarket.com/market/{slug}"
            ).strip(),
            event_url_template=os.getenv(
                "EVENT_URL_TEMPLATE", "https://polymarket.com/event/{event_slug}"
            ).strip(),
            tx_url_template=os.getenv(
                "TX_URL_TEMPLATE", "https://polygonscan.com/tx/{hash}"
            ).strip(),
            user_url_template=os.getenv(
                "USER_URL_TEMPLATE", "https://polymarket.com/profile/{address}"
            ).strip(),

            allowed_user_ids=parse_user_id_list(os.getenv("ALLOWED_USER_IDS", "")),

            default_lang=(os.getenv("LANG_BOT") or os.getenv("LANG", "zh")).strip().lower() or "zh",
            summary_interval_sec=int(os.getenv("SUMMARY_INTERVAL_SEC", "3600")),
            display_tz=(os.getenv("DISPLAY_TZ") or "Asia/Shanghai").strip() or "Asia/Shanghai",
            max_alert_age_sec=int(os.getenv("MAX_ALERT_AGE_SEC", "300")),
        )


# ==========================================================================
#  运行期可变状态（订阅者、阈值、地址别名等，全部持久化到 RUNTIME_STATE_PATH）
# ==========================================================================

@dataclass
class RuntimeState:
    threshold_usdc: Decimal
    telegram_offset: int = 0
    lang: str = "zh"
    paused: bool = False
    summary_interval_sec: int = 3600
    summary_baseline_iso: str = ""
    main_chat_id: str = ""

    subscribers: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    address_labels: Dict[str, str] = field(default_factory=dict)

    cumulative_trades: "OrderedDict[str, int]" = field(default_factory=OrderedDict)
    recent_trades: List[Dict[str, Any]] = field(default_factory=list)
    last_iter_stats: Dict[str, Any] = field(default_factory=dict)
    _path: str = ""

    _CUMULATIVE_CAP = 5000
    _RECENT_TRADES_CAP = 5000

    # ----- 累计 / 摘要 -----

    def bump_cumulative(self, signer: str) -> int:
        if not signer:
            return 0
        key = signer.lower()
        n = self.cumulative_trades.get(key, 0) + 1
        self.cumulative_trades[key] = n
        while len(self.cumulative_trades) > self._CUMULATIVE_CAP:
            self.cumulative_trades.popitem(last=False)
        return n

    def add_trade_for_summary(
        self,
        *,
        market_title: str,
        market_slug: str,
        event_slug: str,
        condition_id: str,
        value: Decimal,
        signer: str,
        timestamp: datetime,
    ) -> None:
        self.recent_trades.append({
            "title": market_title or "?",
            "slug": market_slug or "",
            "event_slug": event_slug or "",
            "cid": condition_id or "",
            "value": value,
            "signer": signer or "",
            "ts": timestamp,
        })
        cutoff = datetime.now(timezone.utc) - timedelta(days=1)
        if len(self.recent_trades) > self._RECENT_TRADES_CAP or len(self.recent_trades) % 100 == 0:
            self.recent_trades = [m for m in self.recent_trades if m["ts"] >= cutoff]
        if len(self.recent_trades) > self._RECENT_TRADES_CAP:
            self.recent_trades = self.recent_trades[-self._RECENT_TRADES_CAP:]

    def get_summary_baseline(self) -> Optional[datetime]:
        if not self.summary_baseline_iso:
            return None
        try:
            ts = datetime.fromisoformat(self.summary_baseline_iso)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            return ts
        except Exception:
            return None

    def reset_summary_baseline(self) -> None:
        self.summary_baseline_iso = datetime.now(timezone.utc).isoformat()

    def summarize_window(
        self, window_seconds: int, *, respect_baseline: bool = False,
    ) -> Optional[Dict[str, Any]]:
        if window_seconds <= 0:
            return None
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=window_seconds)
        if respect_baseline:
            baseline = self.get_summary_baseline()
            if baseline and baseline > cutoff:
                cutoff = baseline
        within = [m for m in self.recent_trades if m["ts"] >= cutoff]
        if not within:
            return None

        total_value = sum((m["value"] for m in within), Decimal("0"))
        max_trade = max(within, key=lambda m: m["value"])

        bucket: Dict[str, Dict[str, Any]] = {}
        for m in within:
            key = m["slug"] or m["event_slug"] or m["cid"] or m["title"]
            if key not in bucket:
                bucket[key] = {
                    "title": m["title"],
                    "slug": m["slug"],
                    "event_slug": m["event_slug"],
                    "cid": m["cid"],
                    "value": Decimal("0"),
                    "count": 0,
                }
            bucket[key]["value"] += m["value"]
            bucket[key]["count"] += 1

        top = sorted(bucket.values(), key=lambda x: x["value"], reverse=True)[:3]
        return {
            "count": len(within),
            "total_value": total_value,
            "max_value": max_trade["value"],
            "max_title": max_trade["title"],
            "top_markets": top,
            "window_seconds": window_seconds,
        }

    def count_signer_alerts(self, signer: str, hours: int = 1) -> int:
        if not signer:
            return 0
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        sig_lower = signer.lower()
        n = sum(
            1 for m in self.recent_trades
            if m["ts"] >= cutoff and str(m.get("signer", "")).lower() == sig_lower
        )
        return n + 1

    def count_market_alerts(self, slug: str = "", condition_id: str = "", hours: int = 1) -> int:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        slug_lower = (slug or "").lower()
        cid_str = str(condition_id or "")
        n = 0
        for m in self.recent_trades:
            if m["ts"] < cutoff:
                continue
            if slug_lower and str(m.get("slug", "")).lower() == slug_lower:
                n += 1
            elif not slug_lower and cid_str and str(m.get("cid", "")) == cid_str:
                n += 1
        return n + 1

    # ----- 持久化 -----

    @classmethod
    def from_config(cls, cfg: Config) -> "RuntimeState":
        lang = cfg.default_lang if cfg.default_lang in {"zh", "en"} else "zh"
        state = cls(
            threshold_usdc=cfg.threshold_usdc,
            lang=lang,
            summary_interval_sec=cfg.summary_interval_sec,
            main_chat_id=str(cfg.tg_chat_id),
            _path=cfg.runtime_state_path,
        )

        saved = load_runtime_state(cfg.runtime_state_path)
        if saved:
            try:
                if "threshold_usdc" in saved:
                    state.threshold_usdc = Decimal(str(saved["threshold_usdc"]))
                if "telegram_offset" in saved:
                    state.telegram_offset = int(saved["telegram_offset"])
                if "lang" in saved and saved["lang"] in {"zh", "en"}:
                    state.lang = saved["lang"]
                if "summary_interval_sec" in saved:
                    state.summary_interval_sec = int(saved["summary_interval_sec"])
                if "summary_baseline_iso" in saved:
                    state.summary_baseline_iso = str(saved["summary_baseline_iso"])
                if "paused" in saved:
                    state.paused = bool(saved["paused"])
                if "subscribers" in saved and isinstance(saved["subscribers"], dict):
                    valid: Dict[str, Dict[str, Any]] = {}
                    for cid, info in saved["subscribers"].items():
                        if not isinstance(info, dict):
                            continue
                        try:
                            Decimal(str(info.get("threshold_usdc", "0")))
                        except (InvalidOperation, ValueError, TypeError):
                            continue
                        valid[str(cid)] = {
                            "threshold_usdc": str(info.get("threshold_usdc", "0")),
                            "lang": info.get("lang", "zh") if info.get("lang") in {"zh", "en"} else "zh",
                            "created_at": str(info.get("created_at", "")),
                            "last_seen": str(info.get("last_seen", "")),
                            "paused": bool(info.get("paused", False)),
                        }
                    state.subscribers = valid
                if "address_labels" in saved and isinstance(saved["address_labels"], dict):
                    state.address_labels = {
                        str(k).lower(): str(v)[:30]
                        for k, v in saved["address_labels"].items()
                        if v
                    }
                LOG.info(
                    "已恢复 runtime state: threshold=%s offset=%s lang=%s subs=%d labels=%d",
                    state.threshold_usdc, state.telegram_offset, state.lang,
                    len(state.subscribers), len(state.address_labels),
                )
            except (InvalidOperation, ValueError, TypeError) as exc:
                LOG.warning("runtime state 字段格式异常: %s — 用 env 默认", exc)

        return state

    def persist(self) -> None:
        if not self._path:
            return
        save_runtime_state(self._path, {
            "threshold_usdc": str(self.threshold_usdc),
            "telegram_offset": self.telegram_offset,
            "lang": self.lang,
            "summary_interval_sec": self.summary_interval_sec,
            "summary_baseline_iso": self.summary_baseline_iso,
            "paused": self.paused,
            "subscribers": self.subscribers,
            "address_labels": self.address_labels,
        })

    # ----- 订阅者 -----

    def is_main_chat(self, chat_id: Any) -> bool:
        return str(chat_id) == self.main_chat_id

    def get_threshold_for(self, chat_id: Any) -> Decimal:
        key = str(chat_id)
        if key == self.main_chat_id:
            return self.threshold_usdc
        info = self.subscribers.get(key)
        if not info:
            return self.threshold_usdc
        try:
            return Decimal(str(info.get("threshold_usdc", "0")))
        except (InvalidOperation, ValueError, TypeError):
            return self.threshold_usdc

    def get_lang_for(self, chat_id: Any) -> str:
        key = str(chat_id)
        if key == self.main_chat_id:
            return self.lang
        info = self.subscribers.get(key)
        if info and info.get("lang") in {"zh", "en"}:
            return info["lang"]
        return self.lang

    def is_paused_for(self, chat_id: Any) -> bool:
        key = str(chat_id)
        if key == self.main_chat_id:
            return self.paused
        info = self.subscribers.get(key)
        return bool(info.get("paused")) if info else False

    def set_paused_for(self, chat_id: Any, paused: bool) -> None:
        key = str(chat_id)
        if key == self.main_chat_id:
            self.paused = paused
            return
        self.upsert_subscriber(chat_id)
        self.subscribers[key]["paused"] = paused

    def upsert_subscriber(
        self,
        chat_id: Any,
        *,
        threshold_usdc: Optional[Decimal] = None,
        lang: Optional[str] = None,
    ) -> Dict[str, Any]:
        key = str(chat_id)
        if key == self.main_chat_id:
            return {}
        now = datetime.now(timezone.utc).isoformat()
        existing = self.subscribers.get(key)
        if not existing:
            existing = {
                "threshold_usdc": str(self.threshold_usdc),
                "lang": self.lang,
                "created_at": now,
                "last_seen": now,
                "paused": False,
            }
            self.subscribers[key] = existing
            LOG.info("[subs] 新订阅 chat_id=%s 默认阈值=%s", key, existing["threshold_usdc"])
        existing["last_seen"] = now
        if threshold_usdc is not None:
            existing["threshold_usdc"] = str(threshold_usdc)
        if lang in {"zh", "en"}:
            existing["lang"] = lang
        return existing

    def remove_subscriber(self, chat_id: Any) -> bool:
        key = str(chat_id)
        if key in self.subscribers:
            del self.subscribers[key]
            return True
        return False

    # ----- 地址别名 -----

    def set_address_label(self, address: str, label: str) -> str:
        key = (address or "").strip().lower()
        clean = (label or "").strip()[:30]
        if not key or not clean:
            return ""
        self.address_labels[key] = clean
        return clean

    def remove_address_label(self, address: str) -> bool:
        key = (address or "").strip().lower()
        if key in self.address_labels:
            del self.address_labels[key]
            return True
        return False

    def get_address_label(self, address: str) -> str:
        key = (address or "").strip().lower()
        return self.address_labels.get(key, "")


def load_runtime_state(path: str) -> Optional[Dict[str, Any]]:
    if not path or not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
        LOG.warning("runtime state 文件格式异常 (%s)", path)
    except Exception as exc:
        LOG.warning("加载 runtime state 失败 (%s): %s", path, exc)
    return None


def save_runtime_state(path: str, data: Dict[str, Any]) -> None:
    if not path:
        return
    tmp = f"{path}.tmp"
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        os.replace(tmp, path)
    except Exception as exc:
        LOG.warning("保存 runtime state 失败 (%s): %s", path, exc)


def check_persistence_writable(cfg: Config) -> bool:
    path = cfg.runtime_state_path
    if not path:
        return True
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        probe = f"{path}.probe"
        with open(probe, "w", encoding="utf-8") as f:
            f.write("ok")
        os.remove(probe)
        return True
    except Exception as exc:
        LOG.warning("[persistence] %s 不可写: %s — 状态不会跨重启", path, exc)
        return False


# ==========================================================================
#  通用格式化
# ==========================================================================

def to_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    s = str(value).strip().replace(",", "")
    if not s or s.lower() in {"none", "null", "nan"}:
        return Decimal("0")
    if s.startswith("$"):
        s = s[1:]
    try:
        return Decimal(s)
    except InvalidOperation:
        return Decimal("0")


def fmt_decimal(x: Decimal, places: int = 2) -> str:
    q = Decimal(10) ** -places
    x = x.quantize(q, rounding=ROUND_DOWN)
    return f"{x:,.{places}f}"


def fmt_money(x: Decimal) -> str:
    if x <= 0:
        return "-"
    if x >= 100:
        return f"${int(x.to_integral_value(rounding=ROUND_DOWN)):,}"
    return f"${fmt_decimal(x, 2)}"


def fmt_money_full(x: Decimal) -> str:
    if x <= 0:
        return "-"
    q = x.quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    return f"${q:,.2f}"


def fmt_qty(x: Decimal) -> str:
    if x <= 0:
        return "0"
    integer_part = x.to_integral_value(rounding=ROUND_DOWN)
    if x == integer_part:
        return f"{int(integer_part):,}"
    return f"{x.quantize(Decimal('0.01'), rounding=ROUND_DOWN):,.2f}"


def get_display_tz(name: str) -> Tuple[Any, str]:
    if name and name.upper() != "UTC":
        try:
            return ZoneInfo(name), name
        except (ZoneInfoNotFoundError, ValueError):
            LOG.warning("DISPLAY_TZ=%r 无法解析，回退 UTC", name)
    return timezone.utc, "UTC"


def parse_unix_or_iso(raw: Any) -> Optional[datetime]:
    """Polymarket 的 timestamp 是 unix 秒。同时兼容 ISO 字符串。"""
    if raw is None or raw == "":
        return None
    try:
        if isinstance(raw, (int, float)):
            n = float(raw)
            if n > 1e12:
                n = n / 1000.0
            return datetime.fromtimestamp(n, tz=timezone.utc)
        s = str(raw).strip()
        if s.isdigit():
            n = float(s)
            if n > 1e12:
                n = n / 1000.0
            return datetime.fromtimestamp(n, tz=timezone.utc)
        s = s.replace("Z", "+00:00")
        ts = datetime.fromisoformat(s)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts
    except Exception:
        return None


def short_addr(addr: Any) -> str:
    s = str(addr or "")
    if len(s) <= 14:
        return s or "-"
    return f"{s[:6]}…{s[-6:]}"


def normalize_text(s: Any, limit: int = 180) -> str:
    text = str(s or "").strip()
    if len(text) > limit:
        text = text[: limit - 1] + "…"
    return html.escape(text)


# ==========================================================================
#  seen 列表（dedup 持久化）
# ==========================================================================

def load_seen(path: str, max_size: int) -> "OrderedDict[str, None]":
    seen: "OrderedDict[str, None]" = OrderedDict()
    if not path or not os.path.exists(path):
        return seen
    try:
        with open(path, "r", encoding="utf-8") as f:
            ids = json.load(f)
        if not isinstance(ids, list):
            LOG.warning("seen 状态文件格式异常 (%s)", path)
            return seen
        for eid in ids[-max_size:]:
            seen[str(eid)] = None
        LOG.info("已加载 %d 条 seen", len(seen))
    except Exception as exc:
        LOG.warning("加载 seen 状态失败 (%s): %s", path, exc)
    return seen


def save_seen(path: str, seen: "OrderedDict[str, None]") -> None:
    if not path:
        return
    tmp = f"{path}.tmp"
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(list(seen.keys()), f, ensure_ascii=False)
        os.replace(tmp, path)
    except Exception as exc:
        LOG.warning("保存 seen 状态失败 (%s): %s", path, exc)


# ==========================================================================
#  Polymarket trade 字段抽取
# ==========================================================================

def trade_value_usdc(trade: Dict[str, Any]) -> Decimal:
    """优先取 usdcSize，fallback 用 size * price。"""
    for key in ("usdcSize", "usdc_size", "valueUsdc", "valueUSDC", "value"):
        v = trade.get(key)
        if v not in (None, "", 0):
            d = to_decimal(v)
            if d > 0:
                return d
    size = to_decimal(trade.get("size") or trade.get("amount") or 0)
    price = to_decimal(trade.get("price") or 0)
    return size * price


def trade_size(trade: Dict[str, Any]) -> Decimal:
    return to_decimal(trade.get("size") or trade.get("amount") or 0)


def trade_price(trade: Dict[str, Any]) -> Decimal:
    return to_decimal(trade.get("price") or 0)


def trade_signer(trade: Dict[str, Any]) -> str:
    for key in ("proxyWallet", "proxy_wallet", "user", "wallet", "owner", "signer", "maker"):
        v = trade.get(key)
        if v:
            return str(v).strip()
    return ""


def trade_username(trade: Dict[str, Any]) -> str:
    for key in ("name", "pseudonym", "username", "displayName", "handle"):
        v = trade.get(key)
        if v:
            s = str(v).strip()
            if s and not (s.startswith("0x") and len(s) >= 20):
                return s
    return ""


def trade_tx_hash(trade: Dict[str, Any]) -> str:
    for key in ("transactionHash", "transaction_hash", "txHash", "tx_hash", "hash"):
        v = trade.get(key)
        if v:
            return str(v).strip()
    return ""


def trade_side(trade: Dict[str, Any]) -> str:
    """归一化为 'buy' / 'sell' / ''。"""
    raw = str(trade.get("side") or trade.get("action") or "").strip().lower()
    if raw in {"buy", "b", "bid"}:
        return "buy"
    if raw in {"sell", "s", "ask"}:
        return "sell"
    return raw


def trade_outcome(trade: Dict[str, Any]) -> str:
    return str(trade.get("outcome") or "").strip()


def trade_market_title(trade: Dict[str, Any]) -> str:
    return str(trade.get("title") or trade.get("question") or trade.get("name") or "").strip()


def trade_market_slug(trade: Dict[str, Any]) -> str:
    return str(trade.get("slug") or "").strip()


def trade_event_slug(trade: Dict[str, Any]) -> str:
    return str(trade.get("eventSlug") or trade.get("event_slug") or "").strip()


def trade_condition_id(trade: Dict[str, Any]) -> str:
    return str(trade.get("conditionId") or trade.get("condition_id") or "").strip()


def trade_timestamp(trade: Dict[str, Any]) -> Optional[datetime]:
    for key in ("timestamp", "ts", "createdAt", "created_at", "executedAt"):
        v = trade.get(key)
        ts = parse_unix_or_iso(v)
        if ts is not None:
            return ts
    return None


def stable_trade_id(trade: Dict[str, Any]) -> str:
    """业务字段拼接 → sha256，避免 API schema 变化引起重复。"""
    parts = [
        trade_tx_hash(trade),
        str(trade.get("timestamp") or trade.get("createdAt") or ""),
        trade_condition_id(trade),
        trade_signer(trade),
        trade_side(trade),
        trade_outcome(trade),
        str(trade.get("size") or trade.get("amount") or ""),
        str(trade.get("price") or ""),
        str(trade.get("asset") or trade.get("tokenId") or ""),
    ]
    raw = "\x1f".join(parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def infer_signal(side: str, outcome: str) -> str:
    """二元市场（YES/NO）：买 YES / 卖 NO = 看涨；买 NO / 卖 YES = 看空。"""
    out = (outcome or "").strip().lower()
    if out not in {"yes", "no", "y", "n"}:
        return ""
    is_yes = out in {"yes", "y"}
    if side == "buy":
        return "bullish" if is_yes else "bearish"
    if side == "sell":
        return "bearish" if is_yes else "bullish"
    return ""


# ==========================================================================
#  i18n
# ==========================================================================

TRANSLATIONS: Dict[str, Dict[str, str]] = {
    "zh": {
        "whale_title": "🐳 <b>巨鲸提醒</b>",
        "buy": "买入",
        "sell": "卖出",
        "anon_wallet": "匿名钱包",
        "view_market": "📊 查看市场",
        "view_event": "🗂 事件",
        "view_wallet": "👤 查看钱包",
        "view_tx": "🔗 交易哈希",
        "card_side": "方向",
        "card_amount": "数量",
        "card_price": "价格",
        "card_value": "成交额",
        "card_trader": "交易者",
        "card_time": "时间",
        "card_signal": "信号",
        "signal_bullish": "看涨",
        "signal_bearish": "看空",
        "shares_unit": "份",
        "tier_super": "超级鲸鱼单",
        "tier_big": "大鲸鱼单",
        "tier_mid": "中型鲸鱼单",
        "tier_normal": "普通大单",
        "started": "✅ <b>Polymarket 巨鲸监控已启动</b>",
        "stopped": "🛑 <b>Polymarket 巨鲸监控已停止</b>",
        "threshold": "阈值",
        "interval": "检查间隔",
        "menu_title": "🐋 <b>Polymarket 监控</b>",
        "match_short": "成交阈值",
        "open_menu_hint": "点底部「菜单」或发 /menu 调阈值",
        "lang_switched": "已切换到中文",
        "btn_lang_switch": "🌐 EN",
        "btn_refresh": "🔄",
        "btn_summary": "📊 摘要",
        "btn_pause": "⏸ 暂停",
        "btn_resume": "▶️ 恢复",
        "btn_custom": "✏️ 自定义",
        "cumulative_fmt": "本轮第 {n} 笔",
        "count_signer_fmt": "👤 钱包近 {window}: {n} 次",
        "count_market_fmt": "📊 市场近 {window}: {n} 笔",
        "window_1h": "1H",
        "summary_title": "📊 <b>Polymarket Whale 摘要</b>",
        "summary_period_fmt": "过去 {label}：",
        "summary_total_count": "总大单",
        "summary_total_value": "总成交额",
        "summary_max_trade": "最大单",
        "summary_top_markets": "最活跃市场",
        "summary_none_fmt": "过去 {label}没有大单",
        "summary_off": "自动摘要已关闭，仍可 /summary 手动查",
        "summary_set_ok_fmt": "摘要间隔已设为 <b>{label}</b>",
        "summary_set_off": "已关闭自动摘要",
        "summary_set_help": "用法：/set_summary 60（分钟），或 /set_summary 0 关闭",
        "summary_set_invalid": "❌ 无效时长（1m–24h，或 0 关闭）",
        "help_text": (
            "🐋 <b>Polymarket Whale Bot</b>\n"
            "盯 polymarket.com 大单成交，超过阈值就推给你。\n\n"
            "👉 <b>直接 /menu</b>，里面能改阈值、汇总频率、语言、暂停。\n\n"
            "<b>常用命令</b>\n"
            "<code>/menu</code> 主菜单\n"
            "<code>/summary</code> 立刻看摘要\n"
            "<code>/pause</code> · <code>/resume</code>\n"
            "<code>/lang zh|en</code>\n"
            "<code>/unsubscribe</code> 退订私聊\n\n"
            "<b>进阶</b>\n"
            "<code>/set 1000</code> 直接改阈值（USDC）\n"
            "<code>/label 0x… 别名</code> · <code>/labels</code> · <code>/unlabel 0x…</code>\n"
            "<code>/whoami</code> 当前设置\n"
            "<code>/subscribers</code> 订阅者列表（admin）\n"
        ),
    },
    "en": {
        "whale_title": "🐳 <b>Whale Alert</b>",
        "buy": "BUY",
        "sell": "SELL",
        "anon_wallet": "Anon wallet",
        "view_market": "📊 Market",
        "view_event": "🗂 Event",
        "view_wallet": "👤 Wallet",
        "view_tx": "🔗 Tx Hash",
        "card_side": "Side",
        "card_amount": "Size",
        "card_price": "Price",
        "card_value": "Value",
        "card_trader": "Trader",
        "card_time": "Time",
        "card_signal": "Signal",
        "signal_bullish": "Bullish",
        "signal_bearish": "Bearish",
        "shares_unit": "shares",
        "tier_super": "Mega Whale",
        "tier_big": "Big Whale",
        "tier_mid": "Mid Whale",
        "tier_normal": "Whale",
        "started": "✅ <b>Polymarket whale monitor started</b>",
        "stopped": "🛑 <b>Polymarket whale monitor stopped</b>",
        "threshold": "Threshold",
        "interval": "Poll interval",
        "menu_title": "🐋 <b>Polymarket Whale Bot</b>",
        "match_short": "Trade threshold",
        "open_menu_hint": "Tap “Menu” or send /menu to change threshold",
        "lang_switched": "Switched to English",
        "btn_lang_switch": "🌐 中",
        "btn_refresh": "🔄",
        "btn_summary": "📊 Summary",
        "btn_pause": "⏸ Pause",
        "btn_resume": "▶️ Resume",
        "btn_custom": "✏️ Custom",
        "cumulative_fmt": "Trade #{n} this session",
        "count_signer_fmt": "👤 Wallet last {window}: {n}",
        "count_market_fmt": "📊 Market last {window}: {n}",
        "window_1h": "1H",
        "summary_title": "📊 <b>Polymarket Whale Digest</b>",
        "summary_period_fmt": "Past {label}:",
        "summary_total_count": "Total trades",
        "summary_total_value": "Total volume",
        "summary_max_trade": "Largest",
        "summary_top_markets": "Top markets",
        "summary_none_fmt": "No big trades in past {label}",
        "summary_off": "Auto digest off; /summary still works",
        "summary_set_ok_fmt": "Digest interval set to <b>{label}</b>",
        "summary_set_off": "Auto digest disabled",
        "summary_set_help": "Usage: /set_summary 60 (minutes) or /set_summary 0 to disable",
        "summary_set_invalid": "❌ Invalid duration (1m–24h or 0)",
        "help_text": (
            "🐋 <b>Polymarket Whale Bot</b>\n"
            "Tracks polymarket.com large trades and pings you when above threshold.\n\n"
            "👉 Just send <b>/menu</b>.\n\n"
            "<b>Common</b>\n"
            "<code>/menu</code> · <code>/summary</code> · <code>/pause</code> · <code>/resume</code>\n"
            "<code>/lang zh|en</code> · <code>/unsubscribe</code>\n\n"
            "<b>Advanced</b>\n"
            "<code>/set 1000</code> set USDC threshold\n"
            "<code>/label 0x… name</code> · <code>/labels</code> · <code>/unlabel 0x…</code>\n"
            "<code>/whoami</code> · <code>/subscribers</code>\n"
        ),
    },
}


def t(state: RuntimeState, key: str, *, lang: Optional[str] = None) -> str:
    use = lang if lang in {"zh", "en"} else state.lang
    return TRANSLATIONS.get(use, TRANSLATIONS["zh"]).get(key, key)


def chunk_html_safely(text: str, limit: int = 3900) -> List[str]:
    if len(text) <= limit:
        return [text]
    chunks, buf = [], []
    cur = 0
    for line in text.split("\n"):
        if cur + len(line) + 1 > limit and buf:
            chunks.append("\n".join(buf))
            buf, cur = [], 0
        buf.append(line)
        cur += len(line) + 1
    if buf:
        chunks.append("\n".join(buf))
    return chunks


# ==========================================================================
#  Telegram client
# ==========================================================================

class Telegram:
    def __init__(self, cfg: Config, client: httpx.AsyncClient) -> None:
        self.cfg = cfg
        self.client = client
        self.base = f"https://api.telegram.org/bot{cfg.tg_bot_token}"
        self._lock = asyncio.Lock()

    async def send(
        self,
        text: str,
        *,
        chat_id: Optional[Any] = None,
        reply_markup: Optional[Dict[str, Any]] = None,
        disable_web_page_preview: bool = True,
    ) -> Optional[Dict[str, Any]]:
        chats = [str(chat_id)] if chat_id is not None else [self.cfg.tg_chat_id]
        last: Optional[Dict[str, Any]] = None
        for cid in chats:
            for piece in chunk_html_safely(text):
                last = await self._send_one(
                    cid, piece,
                    reply_markup=reply_markup,
                    disable_web_page_preview=disable_web_page_preview,
                )
        return last

    async def _send_one(
        self,
        chat_id: str,
        text: str,
        *,
        reply_markup: Optional[Dict[str, Any]] = None,
        disable_web_page_preview: bool = True,
    ) -> Optional[Dict[str, Any]]:
        payload: Dict[str, Any] = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": disable_web_page_preview,
        }
        if reply_markup is not None:
            payload["reply_markup"] = reply_markup

        url = f"{self.base}/sendMessage"
        backoff = 1.0
        for attempt in range(1, 6):
            async with self._lock:
                try:
                    resp = await self.client.post(url, json=payload, timeout=self.cfg.request_timeout_sec)
                except (httpx.HTTPError, asyncio.TimeoutError) as exc:
                    LOG.warning("Telegram 网络错误 #%d: %s", attempt, exc)
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 16)
                    continue

            if resp.status_code == 429:
                try:
                    retry = int(resp.json().get("parameters", {}).get("retry_after", 1))
                except Exception:
                    retry = 1
                LOG.warning("Telegram 429 retry_after=%ss", retry)
                await asyncio.sleep(retry + 0.5)
                continue
            if resp.status_code >= 500:
                LOG.warning("Telegram %s — 重试中", resp.status_code)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 16)
                continue
            if resp.status_code >= 400:
                LOG.warning("Telegram %s body=%s", resp.status_code, resp.text[:300])
                return None
            try:
                data = resp.json()
            except Exception:
                return None
            if data.get("ok"):
                return data.get("result")
            LOG.warning("Telegram ok=false: %s", data)
            return None
        return None

    async def edit_message(
        self,
        *,
        chat_id: Any,
        message_id: int,
        text: str,
        reply_markup: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        payload: Dict[str, Any] = {
            "chat_id": str(chat_id),
            "message_id": message_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if reply_markup is not None:
            payload["reply_markup"] = reply_markup
        try:
            r = await self.client.post(
                f"{self.base}/editMessageText", json=payload,
                timeout=self.cfg.request_timeout_sec,
            )
            if r.status_code >= 400:
                LOG.warning("editMessageText %s: %s", r.status_code, r.text[:200])
                return None
            return r.json().get("result")
        except Exception as exc:
            LOG.warning("editMessageText 异常: %s", exc)
            return None

    async def answer_callback_query(self, callback_id: str, text: str = "") -> None:
        try:
            await self.client.post(
                f"{self.base}/answerCallbackQuery",
                json={"callback_query_id": callback_id, "text": text},
                timeout=self.cfg.request_timeout_sec,
            )
        except Exception:
            pass


# ==========================================================================
#  Polymarket data-api client
# ==========================================================================

class Polymarket:
    """data-api.polymarket.com 包装，自带 429/5xx 退避。"""

    def __init__(self, cfg: Config, client: httpx.AsyncClient) -> None:
        self.cfg = cfg
        self.client = client

    async def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        url = f"{self.cfg.data_api_base}{path}"
        backoff = 1.0
        for attempt in range(1, self.cfg.api_max_retries + 1):
            try:
                r = await self.client.get(url, params=params, timeout=self.cfg.request_timeout_sec)
            except (httpx.HTTPError, asyncio.TimeoutError) as exc:
                LOG.warning("Polymarket 网络错误 #%d: %s", attempt, exc)
                await asyncio.sleep(backoff + random.uniform(0, 0.5))
                backoff = min(backoff * 2, 30)
                continue
            if r.status_code == 429 or r.status_code >= 500:
                LOG.warning("Polymarket %s (#%d) — 退避中", r.status_code, attempt)
                await asyncio.sleep(backoff + random.uniform(0, 0.5))
                backoff = min(backoff * 2, 30)
                continue
            if r.status_code >= 400:
                raise RuntimeError(f"Polymarket {r.status_code}: {r.text[:200]}")
            try:
                return r.json()
            except Exception as exc:
                raise RuntimeError(f"Polymarket 响应不是合法 JSON: {exc}") from exc
        raise RuntimeError(f"Polymarket {path} 重试 {self.cfg.api_max_retries} 次仍失败")

    async def fetch_recent_trades(
        self,
        seen: "OrderedDict[str, None]",
        *,
        min_value: Decimal,
    ) -> List[Dict[str, Any]]:
        """轮询 /trades，遇到已 seen 就停翻页。返回新 trade 列表（按 timestamp 升序）。"""
        out: List[Dict[str, Any]] = []
        for page in range(self.cfg.trades_max_pages):
            params = {
                "limit": self.cfg.trades_page_size,
                "offset": page * self.cfg.trades_page_size,
                "takerOnly": "true",
                "filterType": "CASH",
                # filterAmount 让服务端就过滤掉小单；可选，省带宽
                "filterAmount": int(min_value) if min_value > 0 else 0,
            }
            data = await self.get("/trades", params=params)
            items = data if isinstance(data, list) else data.get("data") or []
            if not items:
                break

            hit_seen = False
            for trade in items:
                tid = stable_trade_id(trade)
                if tid in seen:
                    hit_seen = True
                    continue
                out.append(trade)
            if hit_seen:
                break
        out.sort(key=lambda x: trade_timestamp(x) or datetime.fromtimestamp(0, timezone.utc))
        return out


# ==========================================================================
#  菜单 / 键盘
# ==========================================================================

PRESETS = (
    Decimal("100"), Decimal("500"), Decimal("1000"),
    Decimal("5000"), Decimal("10000"), Decimal("50000"),
)


def _amount_label(amt: Decimal) -> str:
    if amt >= 1000:
        n = amt / Decimal("1000")
        n = n.quantize(Decimal("1"), rounding=ROUND_DOWN) if n == n.to_integral_value() else n
        return f"${n}K"
    return f"${int(amt)}"


def _menu_text(state: RuntimeState, *, chat_id: Optional[Any] = None) -> str:
    lang = state.get_lang_for(chat_id) if chat_id is not None else state.lang
    threshold = state.get_threshold_for(chat_id) if chat_id is not None else state.threshold_usdc
    paused = state.is_paused_for(chat_id) if chat_id is not None else state.paused
    title = t(state, "menu_title", lang=lang)
    short = t(state, "match_short", lang=lang)
    if lang == "zh":
        body = (
            f"{title}\n\n"
            f"<b>{short}</b>: <code>{fmt_money_full(threshold)}</code>\n"
            f"汇总间隔: <code>{_format_duration_label(state.summary_interval_sec, lang)}</code>\n"
            f"状态: {'<b>已暂停</b>' if paused else '运行中'}\n\n"
            "选预设阈值，或点 ✏️ 自定义直接输入金额。"
        )
    else:
        body = (
            f"{title}\n\n"
            f"<b>{short}</b>: <code>{fmt_money_full(threshold)}</code>\n"
            f"Digest: <code>{_format_duration_label(state.summary_interval_sec, lang)}</code>\n"
            f"Status: {'<b>Paused</b>' if paused else 'Running'}\n\n"
            "Pick a preset or tap ✏️ to enter a custom amount."
        )
    return body


def _menu_keyboard(state: RuntimeState, *, chat_id: Optional[Any] = None) -> Dict[str, Any]:
    lang = state.get_lang_for(chat_id) if chat_id is not None else state.lang
    paused = state.is_paused_for(chat_id) if chat_id is not None else state.paused
    rows: List[List[Dict[str, str]]] = []
    row: List[Dict[str, str]] = []
    for amt in PRESETS:
        row.append({"text": _amount_label(amt), "callback_data": f"set:{amt}"})
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([
        {"text": t(state, "btn_custom", lang=lang), "callback_data": "custom"},
        {"text": t(state, "btn_summary", lang=lang), "callback_data": "summary"},
    ])
    pause_btn = (
        {"text": t(state, "btn_resume", lang=lang), "callback_data": "resume"}
        if paused else
        {"text": t(state, "btn_pause", lang=lang), "callback_data": "pause"}
    )
    rows.append([
        pause_btn,
        {"text": t(state, "btn_lang_switch", lang=lang), "callback_data": "lang"},
        {"text": t(state, "btn_refresh", lang=lang), "callback_data": "refresh"},
    ])
    return {"inline_keyboard": rows}


def _format_duration_label(seconds: int, lang: str = "zh") -> str:
    if seconds <= 0:
        return "off" if lang == "en" else "已关闭"
    if seconds % 3600 == 0:
        n = seconds // 3600
        return f"{n}h" if lang == "en" else f"{n} 小时"
    if seconds % 60 == 0:
        n = seconds // 60
        return f"{n}m" if lang == "en" else f"{n} 分钟"
    return f"{seconds}s"


def _parse_duration(raw: str) -> Optional[int]:
    s = (raw or "").strip().lower()
    if not s:
        return None
    m = re.match(r"^(\d+)\s*([smh]?)$", s)
    if not m:
        return None
    n, unit = int(m.group(1)), m.group(2)
    if unit == "s":
        return n
    if unit == "h":
        return n * 3600
    return n * 60  # 默认分钟


def _parse_amount(text: str) -> Optional[Decimal]:
    s = (text or "").strip().replace(",", "").replace("$", "").lower()
    if not s:
        return None
    mult = 1
    if s.endswith("k"):
        mult, s = 1000, s[:-1]
    elif s.endswith("m"):
        mult, s = 1_000_000, s[:-1]
    try:
        v = Decimal(s) * mult
    except InvalidOperation:
        return None
    if v < 0:
        return None
    return v


# ==========================================================================
#  告警渲染
# ==========================================================================

def _render_template(template: str, **kwargs: Any) -> str:
    out = template
    for k, v in kwargs.items():
        out = out.replace("{" + k + "}", str(v))
    return out


def whale_tier(value: Decimal, state: RuntimeState) -> Tuple[str, str]:
    base = state.threshold_usdc if state.threshold_usdc > 0 else Decimal("1")
    ratio = value / base
    if ratio >= 50:
        return ("tier_super", "🔥🔥🔥")
    if ratio >= 10:
        return ("tier_big", "🔥🔥")
    if ratio >= 3:
        return ("tier_mid", "🔥")
    return ("tier_normal", "🐳")


def format_trade_alert(
    cfg: Config,
    state: RuntimeState,
    trade: Dict[str, Any],
    *,
    chat_id: Optional[Any] = None,
) -> Tuple[str, Dict[str, Any]]:
    lang = state.get_lang_for(chat_id) if chat_id is not None else state.lang
    title = trade_market_title(trade) or "?"
    slug = trade_market_slug(trade)
    event_slug = trade_event_slug(trade)
    side = trade_side(trade)
    side_label = t(state, "buy", lang=lang) if side == "buy" else t(state, "sell", lang=lang) if side == "sell" else side.upper()
    outcome = trade_outcome(trade)
    size = trade_size(trade)
    price = trade_price(trade)
    value = trade_value_usdc(trade)
    signer = trade_signer(trade)
    username = trade_username(trade)
    ts = trade_timestamp(trade) or datetime.now(timezone.utc)

    tier_key, fire = whale_tier(value, state)
    tier_label = t(state, tier_key, lang=lang)

    tz, tz_label = get_display_tz(cfg.display_tz)
    time_str = ts.astimezone(tz).strftime("%H:%M:%S")

    label = state.get_address_label(signer)
    if label:
        trader_text = f"<b>{normalize_text(label, 30)}</b> · <code>{short_addr(signer)}</code>"
    elif username:
        trader_text = f"<b>{normalize_text(username, 30)}</b> · <code>{short_addr(signer)}</code>"
    elif signer:
        trader_text = f"<code>{short_addr(signer)}</code>"
    else:
        trader_text = t(state, "anon_wallet", lang=lang)

    n_signer = state.count_signer_alerts(signer, hours=1)
    n_market = state.count_market_alerts(slug=slug, condition_id=trade_condition_id(trade), hours=1)
    cum = state.bump_cumulative(signer)

    sig_key = infer_signal(side, outcome)
    if sig_key == "bullish":
        signal_line = f"  <b>{t(state, 'card_signal', lang=lang)}</b>: {t(state, 'signal_bullish', lang=lang)} 📈\n"
    elif sig_key == "bearish":
        signal_line = f"  <b>{t(state, 'card_signal', lang=lang)}</b>: {t(state, 'signal_bearish', lang=lang)} 📉\n"
    else:
        signal_line = ""

    text = (
        f"{fire} <b>{tier_label}</b> · {fmt_money(value)}\n"
        f"{t(state, 'whale_title', lang=lang)}\n\n"
        f"<b>{normalize_text(title, 140)}</b>\n"
        f"{('<i>' + normalize_text(outcome, 30) + '</i>') if outcome else ''}\n\n"
        f"  <b>{t(state, 'card_side', lang=lang)}</b>: {side_label}\n"
        f"  <b>{t(state, 'card_amount', lang=lang)}</b>: {fmt_qty(size)} {t(state, 'shares_unit', lang=lang)}\n"
        f"  <b>{t(state, 'card_price', lang=lang)}</b>: {fmt_decimal(price, 4)}\n"
        f"  <b>{t(state, 'card_value', lang=lang)}</b>: {fmt_money_full(value)}\n"
        f"{signal_line}"
        f"  <b>{t(state, 'card_trader', lang=lang)}</b>: {trader_text}\n"
        f"  <b>{t(state, 'card_time', lang=lang)}</b>: <code>{time_str}</code> {tz_label}\n\n"
        f"{t(state, 'cumulative_fmt', lang=lang).format(n=cum)} · "
        f"{t(state, 'count_signer_fmt', lang=lang).format(window=t(state, 'window_1h', lang=lang), n=n_signer)} · "
        f"{t(state, 'count_market_fmt', lang=lang).format(window=t(state, 'window_1h', lang=lang), n=n_market)}"
    ).strip()

    buttons: List[List[Dict[str, str]]] = []
    btn_row: List[Dict[str, str]] = []
    if slug:
        btn_row.append({
            "text": t(state, "view_market", lang=lang),
            "url": _render_template(cfg.market_url_template, slug=slug, event_slug=event_slug),
        })
    if event_slug:
        btn_row.append({
            "text": t(state, "view_event", lang=lang),
            "url": _render_template(cfg.event_url_template, event_slug=event_slug, slug=slug),
        })
    if btn_row:
        buttons.append(btn_row)
    btn_row = []
    if signer:
        btn_row.append({
            "text": t(state, "view_wallet", lang=lang),
            "url": _render_template(cfg.user_url_template, address=signer, username=username or ""),
        })
    tx = trade_tx_hash(trade)
    if tx:
        btn_row.append({
            "text": t(state, "view_tx", lang=lang),
            "url": _render_template(cfg.tx_url_template, hash=tx),
        })
    if btn_row:
        buttons.append(btn_row)

    return text, {"inline_keyboard": buttons} if buttons else {}


def format_summary_alert(
    state: RuntimeState,
    summary: Dict[str, Any],
    *,
    chat_id: Optional[Any] = None,
) -> str:
    lang = state.get_lang_for(chat_id) if chat_id is not None else state.lang
    label = _format_duration_label(summary["window_seconds"], lang)
    head = t(state, "summary_title", lang=lang)
    period = t(state, "summary_period_fmt", lang=lang).format(label=label)
    lines = [
        head, "", period, "",
        f"  • {t(state, 'summary_total_count', lang=lang)}: <b>{summary['count']}</b>",
        f"  • {t(state, 'summary_total_value', lang=lang)}: <b>{fmt_money_full(summary['total_value'])}</b>",
        f"  • {t(state, 'summary_max_trade', lang=lang)}: "
        f"<b>{fmt_money_full(summary['max_value'])}</b> · "
        f"<i>{normalize_text(summary['max_title'], 80)}</i>",
        "",
        f"<b>{t(state, 'summary_top_markets', lang=lang)}:</b>",
    ]
    for i, m in enumerate(summary["top_markets"], 1):
        lines.append(
            f"  {i}. <b>{normalize_text(m['title'], 80)}</b> "
            f"— {fmt_money_full(m['value'])} ({m['count']})"
        )
    return "\n".join(lines)


# ==========================================================================
#  Telegram bot 命令处理
# ==========================================================================

class TelegramBot:
    def __init__(
        self,
        cfg: Config,
        state: RuntimeState,
        tg: Telegram,
    ) -> None:
        self.cfg = cfg
        self.state = state
        self.tg = tg
        self._pending_custom: Dict[str, bool] = {}

    def _is_admin(self, user_id: Any) -> bool:
        if not self.cfg.allowed_user_ids:
            return True
        try:
            return int(user_id) in self.cfg.allowed_user_ids
        except (TypeError, ValueError):
            return False

    async def run(self, stop: asyncio.Event) -> None:
        await self._delete_webhook()
        LOG.info("[bot] long-polling 启动 offset=%s", self.state.telegram_offset)
        while not stop.is_set():
            try:
                params = {
                    "timeout": 25,
                    "offset": self.state.telegram_offset,
                    "allowed_updates": json.dumps(["message", "callback_query"]),
                }
                r = await self.tg.client.get(
                    f"{self.tg.base}/getUpdates",
                    params=params,
                    timeout=self.cfg.request_timeout_sec + 30,
                )
                if r.status_code != 200:
                    LOG.warning("getUpdates %s — %s", r.status_code, r.text[:200])
                    await asyncio.sleep(2)
                    continue
                data = r.json()
                for upd in data.get("result") or []:
                    self.state.telegram_offset = max(
                        self.state.telegram_offset, int(upd["update_id"]) + 1
                    )
                    await self._handle_update(upd)
                if data.get("result"):
                    self.state.persist()
            except (httpx.HTTPError, asyncio.TimeoutError) as exc:
                LOG.warning("getUpdates 网络错误: %s", exc)
                await asyncio.sleep(2)
            except Exception as exc:
                LOG.exception("getUpdates 未捕获异常: %s", exc)
                await asyncio.sleep(2)

    async def _delete_webhook(self) -> None:
        try:
            await self.tg.client.post(
                f"{self.tg.base}/deleteWebhook",
                json={"drop_pending_updates": False},
                timeout=self.cfg.request_timeout_sec,
            )
        except Exception:
            pass

    async def _handle_update(self, upd: Dict[str, Any]) -> None:
        if "message" in upd:
            await self._handle_message(upd["message"])
        elif "callback_query" in upd:
            await self._handle_callback(upd["callback_query"])

    async def _handle_message(self, msg: Dict[str, Any]) -> None:
        chat = msg.get("chat") or {}
        chat_id = chat.get("id")
        user = msg.get("from") or {}
        user_id = user.get("id")
        text = (msg.get("text") or "").strip()
        if chat_id is None:
            return

        if not self.state.is_main_chat(chat_id):
            self.state.upsert_subscriber(chat_id)

        # 自定义阈值输入
        if self._pending_custom.get(str(chat_id)) and text and not text.startswith("/"):
            amt = _parse_amount(text)
            self._pending_custom.pop(str(chat_id), None)
            if amt is None or amt < 1:
                await self.tg.send("❌ 无效金额（示例：500、5K、1000）", chat_id=chat_id)
                return
            await self._apply_threshold(chat_id, amt)
            await self.tg.send(
                _menu_text(self.state, chat_id=chat_id),
                chat_id=chat_id,
                reply_markup=_menu_keyboard(self.state, chat_id=chat_id),
            )
            return

        if not text.startswith("/"):
            return
        cmd, _, args = text.partition(" ")
        cmd = cmd.split("@", 1)[0].lower()
        args = args.strip()

        if cmd in {"/start", "/help"}:
            await self.tg.send(t(self.state, "help_text", lang=self.state.get_lang_for(chat_id)), chat_id=chat_id)
            return
        if cmd == "/menu":
            await self.tg.send(
                _menu_text(self.state, chat_id=chat_id),
                chat_id=chat_id,
                reply_markup=_menu_keyboard(self.state, chat_id=chat_id),
            )
            return
        if cmd == "/whoami":
            lang = self.state.get_lang_for(chat_id)
            thr = self.state.get_threshold_for(chat_id)
            paused = self.state.is_paused_for(chat_id)
            await self.tg.send(
                f"chat_id: <code>{chat_id}</code>\n"
                f"user_id: <code>{user_id}</code>\n"
                f"lang: <code>{lang}</code>\n"
                f"threshold: <code>{fmt_money_full(thr)}</code>\n"
                f"paused: <code>{paused}</code>\n"
                f"is_admin: <code>{self._is_admin(user_id)}</code>",
                chat_id=chat_id,
            )
            return
        if cmd == "/lang":
            new_lang = args.lower()
            if new_lang not in {"zh", "en"}:
                new_lang = "en" if self.state.get_lang_for(chat_id) == "zh" else "zh"
            if self.state.is_main_chat(chat_id):
                if not self._is_admin(user_id):
                    return
                self.state.lang = new_lang
            else:
                self.state.upsert_subscriber(chat_id, lang=new_lang)
            self.state.persist()
            await self.tg.send(t(self.state, "lang_switched", lang=new_lang), chat_id=chat_id)
            return
        if cmd == "/pause":
            self.state.set_paused_for(chat_id, True)
            self.state.persist()
            await self.tg.send("⏸ paused", chat_id=chat_id)
            return
        if cmd == "/resume":
            self.state.set_paused_for(chat_id, False)
            self.state.persist()
            await self.tg.send("▶️ resumed", chat_id=chat_id)
            return
        if cmd == "/unsubscribe":
            if self.state.remove_subscriber(chat_id):
                self.state.persist()
                await self.tg.send("✅ 已退订", chat_id=chat_id)
            else:
                await self.tg.send("没有订阅记录", chat_id=chat_id)
            return
        if cmd in {"/set", "/set_threshold", "/set_match"}:
            amt = _parse_amount(args)
            if amt is None:
                await self.tg.send("用法：/set 1000", chat_id=chat_id)
                return
            if self.state.is_main_chat(chat_id) and not self._is_admin(user_id):
                return
            await self._apply_threshold(chat_id, amt)
            await self.tg.send(
                f"✅ {fmt_money_full(amt)}",
                chat_id=chat_id,
                reply_markup=_menu_keyboard(self.state, chat_id=chat_id),
            )
            return
        if cmd == "/summary":
            await self._send_summary(chat_id)
            return
        if cmd == "/set_summary":
            await self._handle_set_summary(chat_id, user_id, args)
            return
        if cmd == "/label":
            await self._handle_label(chat_id, user_id, args)
            return
        if cmd == "/unlabel":
            await self._handle_unlabel(chat_id, user_id, args)
            return
        if cmd == "/labels":
            await self._handle_labels_list(chat_id)
            return
        if cmd == "/subscribers":
            if not self._is_admin(user_id):
                return
            await self._send_subscriber_list(chat_id)
            return

    async def _handle_callback(self, cb: Dict[str, Any]) -> None:
        cb_id = cb.get("id", "")
        data = cb.get("data") or ""
        msg = cb.get("message") or {}
        chat = msg.get("chat") or {}
        chat_id = chat.get("id")
        msg_id = msg.get("message_id")
        user = cb.get("from") or {}
        user_id = user.get("id")
        if chat_id is None:
            await self.tg.answer_callback_query(cb_id)
            return

        if not self.state.is_main_chat(chat_id):
            self.state.upsert_subscriber(chat_id)

        if data.startswith("set:"):
            try:
                amt = Decimal(data.split(":", 1)[1])
            except InvalidOperation:
                await self.tg.answer_callback_query(cb_id, "bad amount")
                return
            if self.state.is_main_chat(chat_id) and not self._is_admin(user_id):
                await self.tg.answer_callback_query(cb_id, "no perm")
                return
            await self._apply_threshold(chat_id, amt)
            await self.tg.edit_message(
                chat_id=chat_id, message_id=msg_id,
                text=_menu_text(self.state, chat_id=chat_id),
                reply_markup=_menu_keyboard(self.state, chat_id=chat_id),
            )
            await self.tg.answer_callback_query(cb_id, fmt_money_full(amt))
            return
        if data == "custom":
            self._pending_custom[str(chat_id)] = True
            await self.tg.send(
                "✏️ 输入金额（USDC），如 <code>500</code> / <code>5K</code> / <code>1000</code>",
                chat_id=chat_id,
            )
            await self.tg.answer_callback_query(cb_id)
            return
        if data == "summary":
            await self._send_summary(chat_id)
            await self.tg.answer_callback_query(cb_id)
            return
        if data in {"pause", "resume"}:
            self.state.set_paused_for(chat_id, data == "pause")
            self.state.persist()
            await self.tg.edit_message(
                chat_id=chat_id, message_id=msg_id,
                text=_menu_text(self.state, chat_id=chat_id),
                reply_markup=_menu_keyboard(self.state, chat_id=chat_id),
            )
            await self.tg.answer_callback_query(cb_id, "ok")
            return
        if data == "lang":
            new_lang = "en" if self.state.get_lang_for(chat_id) == "zh" else "zh"
            if self.state.is_main_chat(chat_id):
                if not self._is_admin(user_id):
                    await self.tg.answer_callback_query(cb_id, "no perm")
                    return
                self.state.lang = new_lang
            else:
                self.state.upsert_subscriber(chat_id, lang=new_lang)
            self.state.persist()
            await self.tg.edit_message(
                chat_id=chat_id, message_id=msg_id,
                text=_menu_text(self.state, chat_id=chat_id),
                reply_markup=_menu_keyboard(self.state, chat_id=chat_id),
            )
            await self.tg.answer_callback_query(cb_id, t(self.state, "lang_switched", lang=new_lang))
            return
        if data == "refresh":
            await self.tg.edit_message(
                chat_id=chat_id, message_id=msg_id,
                text=_menu_text(self.state, chat_id=chat_id),
                reply_markup=_menu_keyboard(self.state, chat_id=chat_id),
            )
            await self.tg.answer_callback_query(cb_id, "🔄")
            return
        await self.tg.answer_callback_query(cb_id)

    async def _apply_threshold(self, chat_id: Any, amount: Decimal) -> None:
        if self.state.is_main_chat(chat_id):
            self.state.threshold_usdc = amount
        else:
            self.state.upsert_subscriber(chat_id, threshold_usdc=amount)
        self.state.reset_summary_baseline()
        self.state.persist()
        LOG.info("[threshold] chat=%s -> %s", chat_id, amount)

    async def _send_summary(self, chat_id: Any) -> None:
        window = self.state.summary_interval_sec or 3600
        summary = self.state.summarize_window(window)
        if not summary:
            label = _format_duration_label(window, self.state.get_lang_for(chat_id))
            await self.tg.send(
                t(self.state, "summary_none_fmt", lang=self.state.get_lang_for(chat_id)).format(label=label),
                chat_id=chat_id,
            )
            return
        await self.tg.send(format_summary_alert(self.state, summary, chat_id=chat_id), chat_id=chat_id)

    async def _handle_set_summary(self, chat_id: Any, user_id: Any, args: str) -> None:
        if self.state.is_main_chat(chat_id) and not self._is_admin(user_id):
            return
        if not args:
            await self.tg.send(t(self.state, "summary_set_help", lang=self.state.get_lang_for(chat_id)), chat_id=chat_id)
            return
        secs = _parse_duration(args)
        if secs is None or (secs and (secs < 60 or secs > 86400)):
            await self.tg.send(t(self.state, "summary_set_invalid", lang=self.state.get_lang_for(chat_id)), chat_id=chat_id)
            return
        self.state.summary_interval_sec = secs
        if secs:
            self.state.reset_summary_baseline()
        self.state.persist()
        lang = self.state.get_lang_for(chat_id)
        if secs:
            await self.tg.send(
                t(self.state, "summary_set_ok_fmt", lang=lang).format(label=_format_duration_label(secs, lang)),
                chat_id=chat_id,
            )
        else:
            await self.tg.send(t(self.state, "summary_set_off", lang=lang), chat_id=chat_id)

    async def _send_subscriber_list(self, chat_id: Any) -> None:
        if not self.state.subscribers:
            await self.tg.send("(无订阅者)", chat_id=chat_id)
            return
        lines = [f"<b>订阅者 {len(self.state.subscribers)}</b>"]
        for cid, info in sorted(self.state.subscribers.items()):
            lines.append(
                f"<code>{cid}</code> · ${info.get('threshold_usdc', '?')} · "
                f"{info.get('lang', '?')} · "
                f"{'paused' if info.get('paused') else 'on'}"
            )
        await self.tg.send("\n".join(lines), chat_id=chat_id)

    async def _handle_label(self, chat_id: Any, user_id: Any, args: str) -> None:
        if self.state.is_main_chat(chat_id) and not self._is_admin(user_id):
            return
        parts = args.split(maxsplit=1)
        if len(parts) < 2:
            await self.tg.send("用法：/label 0xabc... 别名", chat_id=chat_id)
            return
        addr, label = parts[0], parts[1]
        if not re.match(r"^0x[0-9a-fA-F]{40}$", addr):
            await self.tg.send("❌ 地址格式不对（要 0x 开头 42 位）", chat_id=chat_id)
            return
        clean = self.state.set_address_label(addr, label)
        self.state.persist()
        await self.tg.send(f"✅ <code>{short_addr(addr)}</code> → <b>{normalize_text(clean, 30)}</b>", chat_id=chat_id)

    async def _handle_unlabel(self, chat_id: Any, user_id: Any, args: str) -> None:
        if self.state.is_main_chat(chat_id) and not self._is_admin(user_id):
            return
        addr = args.strip()
        if not addr:
            await self.tg.send("用法：/unlabel 0xabc...", chat_id=chat_id)
            return
        if self.state.remove_address_label(addr):
            self.state.persist()
            await self.tg.send(f"✅ 已移除 <code>{short_addr(addr)}</code>", chat_id=chat_id)
        else:
            await self.tg.send("没找到该地址别名", chat_id=chat_id)

    async def _handle_labels_list(self, chat_id: Any) -> None:
        if not self.state.address_labels:
            await self.tg.send("(没有别名)", chat_id=chat_id)
            return
        lines = [f"<b>别名 {len(self.state.address_labels)}</b>"]
        for addr, label in sorted(self.state.address_labels.items(), key=lambda kv: kv[1].lower()):
            lines.append(f"<code>{short_addr(addr)}</code> → {normalize_text(label, 30)}")
        await self.tg.send("\n".join(lines), chat_id=chat_id)


# ==========================================================================
#  trade 分发
# ==========================================================================

async def _dispatch_trade(
    cfg: Config,
    state: RuntimeState,
    tg: Telegram,
    trade: Dict[str, Any],
) -> None:
    value = trade_value_usdc(trade)
    ts = trade_timestamp(trade) or datetime.now(timezone.utc)

    # 全局过期保护
    if cfg.max_alert_age_sec > 0:
        age = (datetime.now(timezone.utc) - ts).total_seconds()
        if age > cfg.max_alert_age_sec:
            return

    # 主告警频道
    if value >= state.threshold_usdc and not state.paused:
        text, markup = format_trade_alert(cfg, state, trade)
        await tg.send(text, reply_markup=markup if markup else None)

    state.add_trade_for_summary(
        market_title=trade_market_title(trade),
        market_slug=trade_market_slug(trade),
        event_slug=trade_event_slug(trade),
        condition_id=trade_condition_id(trade),
        value=value,
        signer=trade_signer(trade),
        timestamp=ts,
    )

    # 私聊订阅者
    for cid in list(state.subscribers.keys()):
        if cid == state.main_chat_id:
            continue
        thr = state.get_threshold_for(cid)
        if value < thr:
            continue
        if state.is_paused_for(cid):
            continue
        text, markup = format_trade_alert(cfg, state, trade, chat_id=cid)
        await tg.send(text, chat_id=cid, reply_markup=markup if markup else None)


# ==========================================================================
#  monitor loop
# ==========================================================================

async def monitor_trades(
    cfg: Config,
    state: RuntimeState,
    tg: Telegram,
    poly: Polymarket,
    seen: "OrderedDict[str, None]",
    stop: asyncio.Event,
    *,
    cold_start: bool,
) -> None:
    LOG.info("[monitor] 启动 poll=%ss threshold=%s seen=%d cold=%s",
             cfg.poll_interval_sec, state.threshold_usdc, len(seen), cold_start)

    iter_no = 0
    while not stop.is_set():
        iter_no += 1
        try:
            # 取所有订阅者中最低阈值，作为 server-side filter
            thresholds = [state.threshold_usdc] + [
                state.get_threshold_for(cid) for cid in state.subscribers
            ]
            min_thr = min((t_ for t_ in thresholds if t_ > 0), default=Decimal("0"))

            new_trades = await poly.fetch_recent_trades(seen, min_value=min_thr)
            if cold_start and not cfg.alert_on_startup:
                # 第一轮只 seed seen，不推
                LOG.info("[monitor] cold-start seed: %d trades 标记为 seen", len(new_trades))
                for trade in new_trades:
                    seen[stable_trade_id(trade)] = None
                cold_start = False
            else:
                cold_start = False
                for trade in new_trades:
                    tid = stable_trade_id(trade)
                    if tid in seen:
                        continue
                    seen[tid] = None
                    try:
                        await _dispatch_trade(cfg, state, tg, trade)
                    except Exception as exc:
                        LOG.exception("dispatch 异常: %s", exc)

            # 修剪 seen
            while len(seen) > cfg.max_seen_ids:
                seen.popitem(last=False)
            if iter_no % 10 == 0:
                save_seen(cfg.seen_state_path, seen)

            state.last_iter_stats = {
                "iter": iter_no,
                "fetched": len(new_trades),
                "seen": len(seen),
                "min_threshold": str(min_thr),
                "ts": datetime.now(timezone.utc).isoformat(),
            }
        except Exception as exc:
            LOG.exception("[monitor] 轮询异常: %s", exc)

        try:
            await asyncio.wait_for(stop.wait(), timeout=cfg.poll_interval_sec)
        except asyncio.TimeoutError:
            pass


async def summary_runner(
    cfg: Config,
    state: RuntimeState,
    tg: Telegram,
    stop: asyncio.Event,
) -> None:
    LOG.info("[summary] 启动")
    while not stop.is_set():
        interval = state.summary_interval_sec
        if interval <= 0:
            try:
                await asyncio.wait_for(stop.wait(), timeout=60)
            except asyncio.TimeoutError:
                pass
            continue
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
            return
        except asyncio.TimeoutError:
            pass

        # 主频道
        if not state.paused:
            summary = state.summarize_window(interval, respect_baseline=True)
            if summary:
                await tg.send(format_summary_alert(state, summary))

        # 各订阅者
        for cid, info in list(state.subscribers.items()):
            if info.get("paused"):
                continue
            summary = state.summarize_window(interval, respect_baseline=True)
            if not summary:
                continue
            try:
                await tg.send(format_summary_alert(state, summary, chat_id=cid), chat_id=cid)
            except Exception as exc:
                LOG.warning("summary -> %s 失败: %s", cid, exc)


# ==========================================================================
#  main
# ==========================================================================

async def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    cfg = Config.from_env()
    state = RuntimeState.from_config(cfg)
    if not check_persistence_writable(cfg):
        LOG.warning("⚠️ %s 不可写，订阅者/阈值不会跨重启", cfg.runtime_state_path)

    seen = load_seen(cfg.seen_state_path, cfg.max_seen_ids)
    cold_start = len(seen) == 0

    stop = asyncio.Event()

    def _stop(*_: Any) -> None:
        LOG.info("收到退出信号")
        stop.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _stop)
        except NotImplementedError:
            signal.signal(sig, lambda *a: _stop())

    async with httpx.AsyncClient(http2=False) as client:
        tg = Telegram(cfg, client)
        poly = Polymarket(cfg, client)
        bot = TelegramBot(cfg, state, tg)

        try:
            await tg.send(
                f"{t(state, 'started')}\n"
                f"{t(state, 'threshold')}: <code>{fmt_money_full(state.threshold_usdc)}</code>\n"
                f"{t(state, 'interval')}: <code>{cfg.poll_interval_sec}s</code>\n\n"
                f"{t(state, 'open_menu_hint')}"
            )
        except Exception as exc:
            LOG.warning("启动通知发送失败: %s", exc)

        tasks = [
            asyncio.create_task(bot.run(stop), name="bot"),
            asyncio.create_task(
                monitor_trades(cfg, state, tg, poly, seen, stop, cold_start=cold_start),
                name="monitor",
            ),
            asyncio.create_task(summary_runner(cfg, state, tg, stop), name="summary"),
        ]
        try:
            await stop.wait()
        finally:
            for t_ in tasks:
                t_.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            save_seen(cfg.seen_state_path, seen)
            state.persist()
            try:
                await tg.send(t(state, "stopped"))
            except Exception:
                pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
