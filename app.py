import asyncio
import json
import logging
import os
import random
import signal
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

import aiohttp
from aiohttp import web

# ============================================================
# Ahmed Order Flow Intelligence Pro v13.2
# Binance USDT-M Futures -> Telegram
# 15m / 1h / 4h | Bullish OF / Bearish OF only
# ============================================================

RIYADH = ZoneInfo("Asia/Riyadh")
TIMEFRAMES = ("15m", "1h", "4h")
TF_LABEL = {"15m": "15 دقيقة", "1h": "ساعة", "4h": "4 ساعات"}

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
PORT = int(os.getenv("PORT", "8080"))
MIN_SCORE = int(os.getenv("MIN_SCORE", "75"))
SEND_TEST_MESSAGES = os.getenv("SEND_TEST_MESSAGES", "true").lower() == "true"
MIN_STRONG_FACTORS = int(os.getenv("MIN_STRONG_FACTORS", "4"))
FACTOR_GATE_ENABLED = os.getenv("FACTOR_GATE_ENABLED", "false").lower() == "true"
COOLDOWN_HOURS = int(os.getenv("COOLDOWN_HOURS", "12"))
DEBUG_BLOCKS = os.getenv("DEBUG_BLOCKS", "true").lower() == "true"

OF_SWING = int(os.getenv("OF_SWING", "4"))
OF_ATR_LEN = int(os.getenv("OF_ATR_LEN", "14"))
OF_IMPULSE = float(os.getenv("OF_IMPULSE", "1.20"))
ZONE_SOURCE = os.getenv("ZONE_SOURCE", "Body + Wick")
BREAK_BY_CLOSE = os.getenv("BREAK_BY_CLOSE", "true").lower() == "true"
OF_USE_VOL = os.getenv("OF_USE_VOL", "true").lower() == "true"
OF_VOL_LEN = int(os.getenv("OF_VOL_LEN", "20"))
OF_VOL_MULT = float(os.getenv("OF_VOL_MULT", "1.20"))
HISTORY_LIMIT = int(os.getenv("HISTORY_LIMIT", "60"))
REST_CONCURRENCY = int(os.getenv("REST_CONCURRENCY", "1"))
HISTORY_WORKERS = int(os.getenv("HISTORY_WORKERS", "2"))
REST_GAP = float(os.getenv("REST_GAP", "1.25"))
REST_MIN_INTERVAL = float(os.getenv("REST_MIN_INTERVAL", "1.10"))
REST_BAN_EXTRA_SECONDS = float(os.getenv("REST_BAN_EXTRA_SECONDS", "3"))
STREAMS_PER_WS = int(os.getenv("STREAMS_PER_WS", "180"))
FIRST_LIVE_TOUCH_ONLY = os.getenv("FIRST_LIVE_TOUCH_ONLY", "true").lower() == "true"

REQUIRE_STRUCTURE_BREAK = os.getenv("REQUIRE_STRUCTURE_BREAK", "true").lower() == "true"
STRUCTURE_LOOKBACK = int(os.getenv("STRUCTURE_LOOKBACK", "20"))
MAX_ZONE_AGE = int(os.getenv("MAX_ZONE_AGE", "36"))
REQUIRE_LIVE_DEFENSE = os.getenv("REQUIRE_LIVE_DEFENSE", "true").lower() == "true"

REST_BASES = [
    "https://fapi.binance.com",
    "https://fapi1.binance.com",
    "https://fapi2.binance.com",
    "https://fapi3.binance.com",
    "https://fapi4.binance.com",
]
WS_BASE = os.getenv("BINANCE_WS_BASE", "wss://fstream.binance.com/ws")
TG_API = "https://api.telegram.org"

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("ahmed-of-v13.2")

SESSION: Optional[aiohttp.ClientSession] = None
STOP = asyncio.Event()
BUFFERS: Dict[str, List["Candle"]] = {}
STATES: Dict[str, "ZoneState"] = {}
PENDING: Dict[str, "PendingConfirmation"] = {}
DEDUP: set[str] = set()
EVENT_COUNT = 0
RAW_WS_COUNT = 0
WS_ERROR_COUNT = 0
SUBSCRIPTION_ACK_COUNT = 0
LAST_WS_MESSAGE_AT: Optional[float] = None
PRICE_POLL_COUNT = 0
PRICE_EVENT_COUNT = 0
LAST_PRICE_UPDATE_AT: Optional[float] = None
LIVE_PRICES: Dict[str, float] = {}
SYMBOL_SET: set[str] = set()
ZONE_COUNT = 0
TOUCH_COUNT = 0
SENT_COUNT = 0
REJECT_COUNT = 0
REJECT_REASONS: Dict[str, int] = {"score": 0, "factors": 0, "cooldown": 0, "duplicate": 0, "second_touch": 0}
MISSING_FACTORS: Dict[str, int] = {
    "Delta": 0, "CVD": 0, "OI": 0, "Sweep": 0, "Absorption": 0,
    "POC": 0, "FVG": 0, "Volume": 0, "OrderBook": 0, "Funding": 0,
}
SCORE_BUCKETS: Dict[str, int] = {"under_60": 0, "60_74": 0, "75_84": 0, "85_94": 0, "95_100": 0}
LAST_TOUCH_AT: Dict[str, float] = {}
LAST_ALERT_AT: Dict[str, float] = {}
REST_LOCK = asyncio.Lock()
REST_LAST_REQUEST_AT = 0.0
REST_BLOCKED_UNTIL = 0.0
REST_REQUEST_COUNT = 0
REST_RATE_LIMIT_COUNT = 0
SYMBOL_CACHE_PATH = os.getenv("SYMBOL_CACHE_PATH", "/tmp/ahmed_of_symbols.json")
BACKGROUND_TASKS: set[asyncio.Task] = set()



def spawn(coro, *, name: Optional[str] = None) -> asyncio.Task:
    """Create and track a background task so shutdown can cancel it cleanly."""
    task = asyncio.create_task(coro, name=name)
    BACKGROUND_TASKS.add(task)
    task.add_done_callback(BACKGROUND_TASKS.discard)
    return task


async def cancel_tasks(tasks) -> None:
    pending = [t for t in tasks if t is not None and not t.done()]
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)


@dataclass
class Candle:
    open_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: float
    taker_buy_volume: float
    closed: bool

    @property
    def delta(self) -> float:
        return (2.0 * self.taker_buy_volume) - self.volume


@dataclass
class ZoneState:
    bull_top: Optional[float] = None
    bull_bottom: Optional[float] = None
    bull_created: Optional[int] = None
    bull_created_index: Optional[int] = None
    bull_detected_index: Optional[int] = None
    bull_broken: bool = True
    bull_inside: bool = False
    bull_tests: int = 0
    bull_live_tests: int = 0
    bull_impulse: float = 0.0
    bull_structure: str = ""
    bull_launch_volume: bool = False

    bear_top: Optional[float] = None
    bear_bottom: Optional[float] = None
    bear_created: Optional[int] = None
    bear_created_index: Optional[int] = None
    bear_detected_index: Optional[int] = None
    bear_broken: bool = True
    bear_inside: bool = False
    bear_tests: int = 0
    bear_live_tests: int = 0
    bear_impulse: float = 0.0
    bear_structure: str = ""
    bear_launch_volume: bool = False


@dataclass
class PendingConfirmation:
    symbol: str
    timeframe: str
    side: str
    bottom: float
    top: float
    score: int
    touch_time: int
    expires_after_bars: int = 4
    bars_seen: int = 0


def key(symbol: str, tf: str) -> str:
    return f"{symbol}:{tf}"


def fmt(v: float) -> str:
    if v >= 1000:
        return f"{v:,.2f}".rstrip("0").rstrip(".")
    if v >= 1:
        return f"{v:.6f}".rstrip("0").rstrip(".")
    return f"{v:.10f}".rstrip("0").rstrip(".")


def sma(values: List[float], n: int) -> Optional[float]:
    if len(values) < n:
        return None
    return sum(values[-n:]) / n


def atr(candles: List[Candle], n: int) -> Optional[float]:
    if len(candles) < n + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        c, p = candles[i], candles[i - 1]
        trs.append(max(c.high - c.low, abs(c.high - p.close), abs(c.low - p.close)))
    if len(trs) < n:
        return None
    a = sum(trs[:n]) / n
    for tr in trs[n:]:
        a = ((a * (n - 1)) + tr) / n
    return a


def pivot_low(c: List[Candle], i: int, n: int) -> bool:
    if i - n < 0 or i + n >= len(c):
        return False
    v = c[i].low
    return v <= min(x.low for x in c[i-n:i]) and v <= min(x.low for x in c[i+1:i+n+1])


def pivot_high(c: List[Candle], i: int, n: int) -> bool:
    if i - n < 0 or i + n >= len(c):
        return False
    v = c[i].high
    return v >= max(x.high for x in c[i-n:i]) and v >= max(x.high for x in c[i+1:i+n+1])


def structure_break(c: List[Candle], candidate: int, current_index: int, side: str) -> bool:
    """Require displacement to break a meaningful pre-pivot structure level."""
    start = max(0, candidate - STRUCTURE_LOOKBACK)
    prior = c[start:candidate]
    if len(prior) < max(5, OF_SWING):
        return False
    current = c[current_index]
    if side == "bull":
        return current.close > max(x.high for x in prior)
    return current.close < min(x.low for x in prior)


def launch_volume_ok(c: List[Candle], current_index: int) -> bool:
    start = max(0, current_index - OF_VOL_LEN)
    prior = [x.volume for x in c[start:current_index]]
    avg = sma(prior, min(OF_VOL_LEN, len(prior))) if prior else None
    return bool(avg and c[current_index].volume >= avg * OF_VOL_MULT)


def fvg_present(c: List[Candle], idx: int, side: str) -> bool:
    start = max(2, idx - 2)
    end = min(len(c), idx + 5)
    for i in range(start, end):
        if side == "bull" and c[i].low > c[i-2].high:
            return True
        if side == "bear" and c[i].high < c[i-2].low:
            return True
    return False


def local_metrics(c: List[Candle], side: str, zone_bottom: float, zone_top: float, created_index: int) -> dict:
    cur = c[-1]
    vols = [x.volume for x in c[:-1]]
    avg_vol = sma(vols, 20) or max(cur.volume, 1e-12)
    volume_spike = cur.volume >= avg_vol * 1.35
    delta_ok = cur.delta > 0 if side == "bull" else cur.delta < 0

    recent = c[-6:]
    previous = c[-11:-6]
    recent_cvd = sum(x.delta for x in recent)
    prev_cvd = sum(x.delta for x in previous) if previous else 0.0
    cvd_ok = recent_cvd > prev_cvd if side == "bull" else recent_cvd < prev_cvd

    body = abs(cur.close - cur.open)
    lower_wick = min(cur.open, cur.close) - cur.low
    upper_wick = cur.high - max(cur.open, cur.close)
    absorption = volume_spike and ((lower_wick > body * 1.2) if side == "bull" else (upper_wick > body * 1.2))

    before = c[-7:-1]
    if side == "bull":
        sweep = bool(before) and cur.low < min(x.low for x in before) and cur.close > zone_bottom
    else:
        sweep = bool(before) and cur.high > max(x.high for x in before) and cur.close < zone_top

    age = max(0, len(c) - 1 - created_index)
    freshness = age <= MAX_ZONE_AGE
    fvg = fvg_present(c, created_index, side)

    # Candle-volume POC proxy: highest-volume candle whose typical price is inside the block.
    zone_candles = c[max(0, created_index - 1):]
    poc = False
    if zone_candles:
        hv = max(zone_candles, key=lambda x: x.volume)
        typical = (hv.high + hv.low + hv.close) / 3.0
        poc = zone_bottom <= typical <= zone_top

    return {
        "volume_spike": volume_spike,
        "delta_ok": delta_ok,
        "cvd_ok": cvd_ok,
        "absorption": absorption,
        "sweep": sweep,
        "freshness": freshness,
        "fvg": fvg,
        "poc": poc,
        "age": age,
        "recent_cvd": recent_cvd,
    }


async def rest_get(path: str, params: Optional[dict] = None, attempts: int = 8):
    """Rate-limited Binance REST request with 418/429 ban recovery.

    All REST calls share one limiter so warm-up, price polling and confirmations
    cannot collectively exceed a safe request rate. A Binance ban no longer
    terminates the container; the service stays online and waits for expiry.
    """
    global REST_LAST_REQUEST_AT, REST_BLOCKED_UNTIL, REST_REQUEST_COUNT, REST_RATE_LIMIT_COUNT
    assert SESSION is not None
    last: Optional[Exception] = None

    for attempt in range(max(1, attempts)):
        if STOP.is_set():
            raise asyncio.CancelledError

        async with REST_LOCK:
            now = time.time()
            if REST_BLOCKED_UNTIL > now:
                wait = REST_BLOCKED_UNTIL - now
                log.warning("Binance REST paused %.1fs until ban window expires", wait)
                try:
                    await asyncio.wait_for(STOP.wait(), timeout=wait)
                    raise asyncio.CancelledError
                except asyncio.TimeoutError:
                    pass

            gap = REST_MIN_INTERVAL - (time.monotonic() - REST_LAST_REQUEST_AT)
            if gap > 0:
                await asyncio.sleep(gap)

            # Do not rotate hosts on every attempt: Binance limits are IP-wide,
            # and rapid host rotation can multiply retries during a ban.
            base = REST_BASES[min(attempt, len(REST_BASES) - 1)]
            try:
                REST_LAST_REQUEST_AT = time.monotonic()
                REST_REQUEST_COUNT += 1
                async with SESSION.get(base + path, params=params, timeout=30) as r:
                    body = await r.text()
                    if r.status == 200:
                        try:
                            return json.loads(body)
                        except json.JSONDecodeError as exc:
                            last = exc
                            log.warning("Binance returned invalid JSON for %s", path)
                    elif r.status in (418, 429):
                        REST_RATE_LIMIT_COUNT += 1
                        # Binance error -1003 often includes: banned until <epoch_ms>
                        import re
                        match = re.search(r"banned until\s+(\d+)", body)
                        if match:
                            epoch = int(match.group(1))
                            if epoch > 10_000_000_000:
                                epoch /= 1000.0
                            REST_BLOCKED_UNTIL = max(
                                REST_BLOCKED_UNTIL,
                                epoch + REST_BAN_EXTRA_SECONDS,
                            )
                        else:
                            retry_after = float(r.headers.get("Retry-After", "0") or 0)
                            REST_BLOCKED_UNTIL = max(
                                REST_BLOCKED_UNTIL,
                                time.time() + max(retry_after, min(300.0, 15.0 * (attempt + 1))),
                            )
                        log.warning(
                            "Binance HTTP %s on %s; REST paused until %s: %s",
                            r.status,
                            path,
                            datetime.fromtimestamp(REST_BLOCKED_UNTIL, RIYADH).strftime("%H:%M:%S"),
                            body[:240],
                        )
                    elif r.status == 451 or r.status >= 500 or r.status == 202:
                        last = RuntimeError(f"HTTP {r.status}: {body[:250]}")
                        log.warning("Binance HTTP %s on %s; retrying", r.status, path)
                    else:
                        raise RuntimeError(f"HTTP {r.status}: {body[:250]}")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                last = exc
                log.warning("Binance request error %s attempt=%s: %s", path, attempt + 1, exc)

        # Sleep outside the lock so cancellation and health endpoints stay responsive.
        await asyncio.sleep(min(60.0, 2.0 ** min(attempt, 5)) + random.random())

    raise RuntimeError(f"Binance request failed {path}: {last}")


async def telegram(text: str) -> bool:
    if not BOT_TOKEN or not CHAT_ID:
        log.error("Telegram variables missing")
        return False
    assert SESSION is not None
    for attempt in range(4):
        try:
            async with SESSION.post(
                f"{TG_API}/bot{BOT_TOKEN}/sendMessage",
                json={"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True},
                timeout=20,
            ) as r:
                data = await r.json(content_type=None)
                if r.status == 200 and data.get("ok"):
                    return True
                log.error("Telegram HTTP %s: %s", r.status, data)
        except Exception as exc:
            log.warning("Telegram try %s failed: %s", attempt + 1, exc)
        await asyncio.sleep(2 ** attempt)
    return False


async def market_confirmation(symbol: str, tf: str, side: str) -> dict:
    """Fetch limited live confirmation data only at the first touch."""
    result = {
        "oi_up": False,
        "book_ok": False,
        "iceberg": False,
        "funding_ok": False,
        "funding_rate": 0.0,
    }
    try:
        oi_period = "5m" if tf == "15m" else ("15m" if tf == "1h" else "1h")
        oi_data, depth, premium = await asyncio.gather(
            rest_get("/futures/data/openInterestHist", {"symbol": symbol, "period": oi_period, "limit": 3}, attempts=4),
            rest_get("/fapi/v1/depth", {"symbol": symbol, "limit": 20}, attempts=4),
            rest_get("/fapi/v1/premiumIndex", {"symbol": symbol}, attempts=4),
        )
        if isinstance(oi_data, list) and len(oi_data) >= 2:
            a = float(oi_data[-2].get("sumOpenInterest", 0))
            b = float(oi_data[-1].get("sumOpenInterest", 0))
            result["oi_up"] = b > a

        bids = sum(float(p) * float(q) for p, q in depth.get("bids", []))
        asks = sum(float(p) * float(q) for p, q in depth.get("asks", []))
        total = max(bids + asks, 1e-12)
        imbalance = (bids - asks) / total
        result["book_ok"] = imbalance >= 0.12 if side == "bull" else imbalance <= -0.12
        result["iceberg"] = abs(imbalance) >= 0.25

        funding = float(premium.get("lastFundingRate", 0.0)) if isinstance(premium, dict) else 0.0
        result["funding_rate"] = funding
        # Avoid heavily crowded positioning against the intended reversal.
        result["funding_ok"] = funding <= 0.0005 if side == "bull" else funding >= -0.0005
    except Exception as exc:
        log.warning("Market confirmation failed %s %s: %s", symbol, tf, exc)
    return result


def score_block(local: dict, market: dict, first_test: bool, event: Optional[dict] = None) -> tuple[int, List[str], List[str], int, dict]:
    """Institutional-style weighted quality score; not a success probability."""
    event = event or {}
    weights = [
        (bool(event.get("structure_ok")), 16, "BOS/CHoCH", True),
        (float(event.get("impulse", 0.0)) >= OF_IMPULSE, 12, "اندفاع", True),
        (bool(event.get("launch_volume")), 10, "حجم الانطلاق", True),
        (first_test, 10, "أول اختبار", False),
        (local["delta_ok"], 12, "Delta", True),
        (local["cvd_ok"], 10, "CVD", True),
        (market["oi_up"], 8, "OI", True),
        (local["sweep"], 8, "Sweep", True),
        (local["absorption"], 8, "Absorption", True),
        (local["poc"], 5, "POC", False),
        (local["fvg"], 5, "FVG", False),
        (local["volume_spike"], 5, "Volume", True),
        (market["book_ok"], 4, "OrderBook", False),
        (market["funding_ok"], 2, "Funding", False),
        (local["freshness"], 5, "حديث", False),
    ]
    score = 0
    yes: List[str] = []
    no: List[str] = []
    points: dict[str, int] = {}
    strong = 0
    for ok, weight, name, is_strong in weights:
        if ok:
            score += weight
            yes.append(name)
            points[name] = weight
            if is_strong:
                strong += 1
        else:
            no.append(name)
            points[name] = 0
    # A touch without visible defense is downgraded heavily.
    live_defense = local["delta_ok"] or local["absorption"] or local["sweep"]
    if REQUIRE_LIVE_DEFENSE and not live_defense:
        score = max(0, score - 20)
        no.append("دفاع حي")
    return min(100, score), yes, no, strong, points

def links(symbol: str) -> str:
    tv = f"https://www.tradingview.com/chart/?symbol=BINANCE:{symbol}.P"
    bn = f"https://www.binance.com/en/futures/{symbol}"
    return f'🔗 <a href="{tv}">TradingView</a> | <a href="{bn}">Binance</a>'


def touch_message(symbol: str, tf: str, side: str, bottom: float, top: float, price: float, score: int, local: dict, market: dict, yes: List[str], no: List[str]) -> str:
    bull = side == "bull"
    if score >= 95:
        strength = "🔥 استثنائية"
    elif score >= 85:
        strength = "🟢 قوية"
    else:
        strength = "🟡 جيدة"

    title = "🟢 <b>فرصة شراء من بلوك مؤسسي</b>" if bull else "🔴 <b>فرصة بيع من بلوك مؤسسي</b>"
    # تنبيهات لمس البلوك في هذا المشروع هي فرص ارتداد من المنطقة، وليست تنبيهات اقتراب.
    opportunity_type = "ارتداد (Reversal)"

    # اعرض فقط العوامل التي تحققت، وبشكل مختصر مناسب للهاتف.
    preferred = ["BOS/CHoCH", "اندفاع", "حجم الانطلاق", "أول اختبار", "Delta", "CVD", "OI", "Sweep", "Absorption", "POC", "FVG", "Volume", "OrderBook", "Funding"]
    factor_names = [name for name in preferred if name in yes]
    factor_names = ["First Touch" if name == "أول اختبار" else name for name in factor_names]
    rows = [" • ".join(factor_names[i:i+4]) for i in range(0, min(len(factor_names), 8), 4)]
    factors = "\n✅ ".join(rows) if rows else "OF • ATR"

    now = datetime.now(RIYADH).strftime("%d-%m-%Y %H:%M")
    return (
        f"{title}\n\n"
        f"💰 <b>#{symbol}.P</b> | ⏰ <b>{TF_LABEL[tf]}</b>\n"
        f"💵 السعر: <b>{fmt(price)}</b>\n\n"
        f"🧱 البلوك: <b>{fmt(bottom)} ➜ {fmt(top)}</b>\n"
        f"🏷️ <b>{opportunity_type}</b>\n\n"
        f"⭐ التقييم: <b>{score}/100</b> — {strength}\n"
        f"✅ {factors}\n"
        f"⏳ عمر البلوك: <b>{local['age']} شموع</b>\n\n"
        f"🕒 {now} (السعودية)\n"
        f"{links(symbol)}\n"
        f"⚠️ تقييم جودة وليس ضمانًا للارتداد"
    )

def confirmation_message(p: PendingConfirmation, price: float, evidence: List[str]) -> str:
    bull = p.side == "bull"
    title = "🚀 <b>تأكيد ارتداد شرائي</b>" if bull else "📉 <b>تأكيد هبوط بيعي</b>"
    checks = " • ".join(evidence[:4])
    now = datetime.now(RIYADH).strftime("%d-%m-%Y %H:%M")
    return (
        f"{title}\n\n"
        f"💰 <b>#{p.symbol}.P</b> | ⏰ <b>{TF_LABEL[p.timeframe]}</b>\n"
        f"💵 السعر: <b>{fmt(price)}</b> | ⭐ <b>{p.score}/100</b>\n"
        f"✅ {checks}\n"
        f"🕒 {now} السعودية\n{links(p.symbol)}"
    )

def rebuild(c: List[Candle]) -> ZoneState:
    s = ZoneState()
    for i in range(len(c)):
        apply_bar(c, i, s, False)
    return s


def apply_bar(c: List[Candle], i: int, s: ZoneState, emit: bool) -> List[dict]:
    global ZONE_COUNT, TOUCH_COUNT
    out = []
    current = c[i]
    a = atr(c[:i+1], OF_ATR_LEN)
    if not a:
        return out
    candidate = i - OF_SWING
    if candidate >= OF_SWING and pivot_low(c, candidate, OF_SWING):
        p = c[candidate]
        top = p.high if ZONE_SOURCE == "Full Candle" else max(p.open, p.close)
        bottom = min(p.open, p.close) if ZONE_SOURCE == "Candle Body" else p.low
        impulse = (current.close - top) / max(a, 1e-12)
        pivot_avg_vol = sma([x.volume for x in c[:candidate+1]], OF_VOL_LEN)
        pivot_vol_ok = (not OF_USE_VOL) or (pivot_avg_vol is not None and p.volume > pivot_avg_vol * 0.80)
        launch_vol = launch_volume_ok(c, i)
        structure_ok = structure_break(c, candidate, i, "bull")
        vol_ok = pivot_vol_ok and ((not OF_USE_VOL) or launch_vol)
        if impulse >= OF_IMPULSE and vol_ok and ((not REQUIRE_STRUCTURE_BREAK) or structure_ok):
            is_new = s.bull_created != p.open_time
            s.bull_top, s.bull_bottom = top, bottom
            s.bull_created, s.bull_created_index, s.bull_detected_index = p.open_time, candidate, i
            s.bull_broken, s.bull_inside, s.bull_tests = False, False, 0
            s.bull_impulse, s.bull_structure, s.bull_launch_volume = impulse, "BOS/CHoCH", launch_vol
            if emit and is_new:
                ZONE_COUNT += 1
                if DEBUG_BLOCKS:
                    log.info("ZONE BULL created pivot=%s detected=%s impulse=%.2f zone=%s-%s", candidate, i, impulse, fmt(bottom), fmt(top))
    if candidate >= OF_SWING and pivot_high(c, candidate, OF_SWING):
        p = c[candidate]
        top = max(p.open, p.close) if ZONE_SOURCE == "Candle Body" else p.high
        bottom = p.low if ZONE_SOURCE == "Full Candle" else min(p.open, p.close)
        impulse = (bottom - current.close) / max(a, 1e-12)
        pivot_avg_vol = sma([x.volume for x in c[:candidate+1]], OF_VOL_LEN)
        pivot_vol_ok = (not OF_USE_VOL) or (pivot_avg_vol is not None and p.volume > pivot_avg_vol * 0.80)
        launch_vol = launch_volume_ok(c, i)
        structure_ok = structure_break(c, candidate, i, "bear")
        vol_ok = pivot_vol_ok and ((not OF_USE_VOL) or launch_vol)
        if impulse >= OF_IMPULSE and vol_ok and ((not REQUIRE_STRUCTURE_BREAK) or structure_ok):
            is_new = s.bear_created != p.open_time
            s.bear_top, s.bear_bottom = top, bottom
            s.bear_created, s.bear_created_index, s.bear_detected_index = p.open_time, candidate, i
            s.bear_broken, s.bear_inside, s.bear_tests = False, False, 0
            s.bear_impulse, s.bear_structure, s.bear_launch_volume = impulse, "BOS/CHoCH", launch_vol
            if emit and is_new:
                ZONE_COUNT += 1
                if DEBUG_BLOCKS:
                    log.info("ZONE BEAR created pivot=%s detected=%s impulse=%.2f zone=%s-%s", candidate, i, impulse, fmt(bottom), fmt(top))

    inside_bull = not s.bull_broken and s.bull_top is not None and current.high >= s.bull_bottom and current.low <= s.bull_top
    inside_bear = not s.bear_broken and s.bear_top is not None and current.high >= s.bear_bottom and current.low <= s.bear_top
    if inside_bull and not s.bull_inside:
        # Pine counts any entry; Telegram waits for the first RETURN after formation.
        if s.bull_detected_index is None or i > s.bull_detected_index:
            s.bull_tests += 1
            if emit:
                s.bull_live_tests += 1
                TOUCH_COUNT += 1
                out.append({"side": "bull", "bottom": s.bull_bottom, "top": s.bull_top, "tests": s.bull_live_tests if FIRST_LIVE_TOUCH_ONLY else s.bull_tests, "historical_tests": s.bull_tests, "created": s.bull_created, "created_index": s.bull_created_index, "impulse": s.bull_impulse, "structure_ok": bool(s.bull_structure), "launch_volume": s.bull_launch_volume})
    if inside_bear and not s.bear_inside:
        if s.bear_detected_index is None or i > s.bear_detected_index:
            s.bear_tests += 1
            if emit:
                s.bear_live_tests += 1
                TOUCH_COUNT += 1
                out.append({"side": "bear", "bottom": s.bear_bottom, "top": s.bear_top, "tests": s.bear_live_tests if FIRST_LIVE_TOUCH_ONLY else s.bear_tests, "historical_tests": s.bear_tests, "created": s.bear_created, "created_index": s.bear_created_index, "impulse": s.bear_impulse, "structure_ok": bool(s.bear_structure), "launch_volume": s.bear_launch_volume})
    s.bull_inside, s.bear_inside = inside_bull, inside_bear

    if not s.bull_broken and s.bull_bottom is not None and (current.close if BREAK_BY_CLOSE else current.low) < s.bull_bottom:
        s.bull_broken = True
    if not s.bear_broken and s.bear_top is not None and (current.close if BREAK_BY_CLOSE else current.high) > s.bear_top:
        s.bear_broken = True
    return out


async def handle_touch(symbol: str, tf: str, c: List[Candle], event: dict) -> None:
    global SENT_COUNT, REJECT_COUNT
    side = event["side"]
    touch_key = f"{symbol}:{tf}:{side}:{event['created']}"
    # حماية من تكرار تحديثات نفس الشمعة الحية
    if time.time() - LAST_TOUCH_AT.get(touch_key, 0) < 30:
        return
    LAST_TOUCH_AT[touch_key] = time.time()
    log.info("TOUCH_DETECTED symbol=%s tf=%s side=%s live_test=%s historical_test=%s zone=%s-%s", symbol, tf, side, event["tests"], event.get("historical_tests", event["tests"]), fmt(float(event["bottom"])), fmt(float(event["top"])))
    if event["tests"] != 1:
        REJECT_COUNT += 1
        REJECT_REASONS["second_touch"] += 1
        log.info("REJECT reason=second_touch symbol=%s tf=%s side=%s test=%s", symbol, tf, side, event["tests"])
        return
    dedup = touch_key
    cooldown_key = f"{symbol}:{tf}"
    if dedup in DEDUP:
        REJECT_COUNT += 1
        REJECT_REASONS["duplicate"] += 1
        log.info("REJECT reason=duplicate symbol=%s tf=%s side=%s", symbol, tf, side)
        return
    if time.time() - LAST_ALERT_AT.get(cooldown_key, 0) < COOLDOWN_HOURS * 3600:
        REJECT_COUNT += 1
        REJECT_REASONS["cooldown"] += 1
        log.info("REJECT reason=cooldown symbol=%s tf=%s side=%s", symbol, tf, side)
        return
    local = local_metrics(c, side, float(event["bottom"]), float(event["top"]), int(event["created_index"] or 0))
    market = await market_confirmation(symbol, tf, side)
    score, yes, no, strong, points = score_block(local, market, True, event)
    if score < 60:
        SCORE_BUCKETS["under_60"] += 1
    elif score < 75:
        SCORE_BUCKETS["60_74"] += 1
    elif score < 85:
        SCORE_BUCKETS["75_84"] += 1
    elif score < 95:
        SCORE_BUCKETS["85_94"] += 1
    else:
        SCORE_BUCKETS["95_100"] += 1

    log.info("TOUCH %s %s %s score=%s factors=%s points=%s zone=%s-%s", symbol, tf, side, score, strong, points, fmt(float(event['bottom'])), fmt(float(event['top'])))
    if score < MIN_SCORE:
        REJECT_COUNT += 1
        REJECT_REASONS["score"] += 1
        for name in no:
            if name in MISSING_FACTORS:
                MISSING_FACTORS[name] += 1
        log.info("REJECT reason=score symbol=%s tf=%s side=%s score=%s required=%s missing=%s", symbol, tf, side, score, MIN_SCORE, ','.join(no[:8]))
        return
    if FACTOR_GATE_ENABLED and strong < MIN_STRONG_FACTORS:
        REJECT_COUNT += 1
        REJECT_REASONS["factors"] += 1
        for name in no:
            if name in MISSING_FACTORS:
                MISSING_FACTORS[name] += 1
        log.info("REJECT reason=factors symbol=%s tf=%s side=%s factors=%s required=%s missing=%s", symbol, tf, side, strong, MIN_STRONG_FACTORS, ','.join(no[:8]))
        return
    DEDUP.add(dedup)
    LAST_ALERT_AT[cooldown_key] = time.time()
    text = touch_message(symbol, tf, side, float(event["bottom"]), float(event["top"]), c[-1].close, score, local, market, yes, no)
    ok = await telegram(text)
    log.info("TELEGRAM sent=%s %s %s", ok, symbol, tf)
    if ok:
        SENT_COUNT += 1
        PENDING[key(symbol, tf)] = PendingConfirmation(symbol, tf, side, float(event["bottom"]), float(event["top"]), score, int(time.time()*1000))


async def check_confirmation(symbol: str, tf: str, c: List[Candle]) -> None:
    k = key(symbol, tf)
    p = PENDING.get(k)
    if not p or not c[-1].closed:
        return
    p.bars_seen += 1
    cur = c[-1]
    mid = (p.bottom + p.top) / 2
    vols = [x.volume for x in c[:-1]]
    vol_ok = cur.volume >= (sma(vols, 20) or cur.volume) * 1.15
    delta_ok = cur.delta > 0 if p.side == "bull" else cur.delta < 0
    cvd = sum(x.delta for x in c[-5:])
    cvd_ok = cvd > 0 if p.side == "bull" else cvd < 0
    price_ok = cur.close > mid if p.side == "bull" else cur.close < mid
    evidence = []
    if price_ok: evidence.append("إغلاق مؤيد خارج منتصف البلوك")
    if delta_ok: evidence.append("Delta مؤيد")
    if cvd_ok: evidence.append("CVD مؤيد")
    if vol_ok: evidence.append("Volume مرتفع")
    if price_ok and sum([delta_ok, cvd_ok, vol_ok]) >= 2:
        ok = await telegram(confirmation_message(p, cur.close, evidence))
        log.info("Confirmation sent=%s %s %s", ok, symbol, tf)
        PENDING.pop(k, None)
    elif p.bars_seen >= p.expires_after_bars:
        PENDING.pop(k, None)


async def process_event(payload: dict) -> None:
    global EVENT_COUNT
    e = payload.get("data", payload)
    if e.get("e") != "kline":
        return
    symbol = e.get("s")
    kl = e.get("k", {})
    tf = kl.get("i")
    if not symbol or tf not in TIMEFRAMES:
        return
    EVENT_COUNT += 1
    if EVENT_COUNT % 10000 == 0:
        log.info("Kline events received: %s", EVENT_COUNT)
    k = key(symbol, tf)
    c = Candle(int(kl["t"]), float(kl["o"]), float(kl["h"]), float(kl["l"]), float(kl["c"]), float(kl["v"]), float(kl.get("q", 0)), float(kl.get("V", 0)), bool(kl["x"]))
    buf = BUFFERS.get(k)
    if not buf:
        return
    old_state = STATES.get(k, ZoneState())
    if buf[-1].open_time == c.open_time:
        buf[-1] = c
    else:
        buf.append(c)
        if len(buf) > HISTORY_LIMIT:
            del buf[:-HISTORY_LIMIT]
    fresh = rebuild(buf[:-1]) if len(buf) > 1 else ZoneState()
    events = apply_bar(buf, len(buf)-1, fresh, True)
    # منع تكرار نفس اللمس خلال تحديثات الشمعة الحية
    filtered = []
    for ev in events:
        if ev["side"] == "bull" and old_state.bull_inside:
            continue
        if ev["side"] == "bear" and old_state.bear_inside:
            continue
        filtered.append(ev)
    STATES[k] = fresh
    for ev in filtered:
        spawn(handle_touch(symbol, tf, list(buf), ev), name=f"touch:{symbol}:{tf}")
    if c.closed:
        await check_confirmation(symbol, tf, buf)


async def symbols_list() -> List[str]:
    """Return active USDT perpetual symbols without crashing during an IP ban."""
    cache = Path(SYMBOL_CACHE_PATH)

    # Prefer a fresh cache on container restarts to avoid an unnecessary exchangeInfo call.
    try:
        if cache.exists() and time.time() - cache.stat().st_mtime < 7 * 86400:
            cached = json.loads(cache.read_text())
            if isinstance(cached, list) and len(cached) >= 100:
                log.info("Loaded %s symbols from cache", len(cached))
                return sorted({str(x) for x in cached})
    except Exception as exc:
        log.warning("Symbol cache read failed: %s", exc)

    while not STOP.is_set():
        try:
            data = await rest_get("/fapi/v1/exchangeInfo", attempts=20)
            symbols = sorted({
                x["symbol"] for x in data.get("symbols", [])
                if x.get("contractType") == "PERPETUAL"
                and x.get("quoteAsset") == "USDT"
                and x.get("status") == "TRADING"
            })
            if symbols:
                try:
                    cache.write_text(json.dumps(symbols))
                except Exception as exc:
                    log.warning("Symbol cache write failed: %s", exc)
                return symbols
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.error("exchangeInfo unavailable; retrying in 60s: %s", exc)
            try:
                await asyncio.wait_for(STOP.wait(), timeout=60)
            except asyncio.TimeoutError:
                pass
    return []


async def fetch_history(symbol: str, tf: str):
    try:
        rows = await rest_get(
            "/fapi/v1/klines",
            {"symbol": symbol, "interval": tf, "limit": HISTORY_LIMIT},
            attempts=6,
        )
        await asyncio.sleep(REST_GAP + random.random() * 0.03)
        now = int(time.time() * 1000)
        candles = [
            Candle(
                int(r[0]), float(r[1]), float(r[2]), float(r[3]),
                float(r[4]), float(r[5]), float(r[7]), float(r[9]),
                int(r[6]) < now,
            )
            for r in rows
        ]
        return symbol, tf, candles
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        log.error("History failed %s %s: %s", symbol, tf, exc)
        return symbol, tf, []


async def history_worker(
    queue: asyncio.Queue,
    result_queue: asyncio.Queue,
    worker_id: int,
) -> None:
    while True:
        item = await queue.get()
        try:
            if item is None:
                return
            symbol, tf = item
            result = await fetch_history(symbol, tf)
            await result_queue.put(result)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("History worker %s failed", worker_id)
        finally:
            queue.task_done()


async def warmup(symbols: List[str]) -> None:
    """Hydrate states with a fixed worker pool; no thousands of pending tasks."""
    jobs = [(symbol, tf) for symbol in symbols for tf in TIMEFRAMES]
    total = len(jobs)
    queue: asyncio.Queue = asyncio.Queue(maxsize=max(10, HISTORY_WORKERS * 4))
    result_queue: asyncio.Queue = asyncio.Queue()
    workers = [
        asyncio.create_task(
            history_worker(queue, result_queue, i + 1),
            name=f"history-worker-{i + 1}",
        )
        for i in range(max(1, HISTORY_WORKERS))
    ]

    async def producer() -> None:
        for item in jobs:
            if STOP.is_set():
                break
            await queue.put(item)
        for _ in workers:
            await queue.put(None)

    producer_task = asyncio.create_task(producer(), name="history-producer")
    done = loaded = 0
    log.info(
        "Warming %s symbol/timeframe states with %s workers...",
        total, len(workers),
    )
    try:
        while done < total and not STOP.is_set():
            try:
                symbol, tf, candles = await asyncio.wait_for(
                    result_queue.get(), timeout=5
                )
            except asyncio.TimeoutError:
                if producer_task.done() and all(w.done() for w in workers):
                    break
                continue
            done += 1
            if candles:
                BUFFERS[key(symbol, tf)] = candles
                STATES[key(symbol, tf)] = rebuild(candles)
                loaded += 1
            if done % 50 == 0 or done == total:
                log.info("Warm-up progress: %s/%s loaded=%s", done, total, loaded)
    finally:
        await cancel_tasks([producer_task, *workers])
    log.info("Warm-up finished loaded=%s failed=%s", loaded, max(0, total - loaded))


async def ws_worker(streams: List[str], wid: int) -> None:
    global RAW_WS_COUNT, WS_ERROR_COUNT, SUBSCRIPTION_ACK_COUNT, LAST_WS_MESSAGE_AT
    assert SESSION is not None

    # نستخدم اتصال /ws ثم اشتراكًا رسميًا بعد الاتصال.
    # هذا يتجنب روابط Combined Streams الطويلة التي كانت تتصل دون استقبال بيانات.
    while not STOP.is_set():
        try:
            async with SESSION.ws_connect(
                WS_BASE,
                heartbeat=30,
                receive_timeout=90,
                max_msg_size=5_000_000,
                autoclose=True,
                autoping=True,
            ) as ws:
                log.info("WS %s connected; subscribing to %s streams", wid, len(streams))

                request_id = (wid * 100000) + 1
                # Binance يسمح بعدد كبير من الاشتراكات، لكن التقسيم إلى دفعات أصغر أكثر ثباتًا.
                for offset in range(0, len(streams), 100):
                    batch = streams[offset:offset + 100]
                    await ws.send_json({
                        "method": "SUBSCRIBE",
                        "params": batch,
                        "id": request_id,
                    })
                    log.info(
                        "WS %s subscribe sent id=%s streams=%s",
                        wid, request_id, len(batch),
                    )
                    request_id += 1
                    await asyncio.sleep(0.25)

                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        RAW_WS_COUNT += 1
                        LAST_WS_MESSAGE_AT = time.time()
                        try:
                            data = json.loads(msg.data)

                            # رد الاشتراك الرسمي: {"result": null, "id": ...}
                            if isinstance(data, dict) and "id" in data and "result" in data:
                                if data.get("result") is None:
                                    SUBSCRIPTION_ACK_COUNT += 1
                                    log.info(
                                        "WS %s subscription acknowledged id=%s total_acks=%s",
                                        wid, data.get("id"), SUBSCRIPTION_ACK_COUNT,
                                    )
                                else:
                                    WS_ERROR_COUNT += 1
                                    log.warning("WS %s subscription rejected: %s", wid, data)
                                continue

                            await process_event(data)
                            if RAW_WS_COUNT == 1 or RAW_WS_COUNT % 5000 == 0:
                                log.info(
                                    "WS raw messages=%s kline_events=%s acks=%s",
                                    RAW_WS_COUNT, EVENT_COUNT, SUBSCRIPTION_ACK_COUNT,
                                )
                        except Exception:
                            WS_ERROR_COUNT += 1
                            log.exception("WS event error wid=%s payload=%s", wid, msg.data[:300])

                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        WS_ERROR_COUNT += 1
                        log.warning("WS %s message error: %s", wid, ws.exception())
                        break
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSE):
                        log.warning("WS %s closed by remote", wid)
                        break

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            WS_ERROR_COUNT += 1
            log.warning("WS %s disconnected: %s", wid, exc)

        if not STOP.is_set():
            await asyncio.sleep(5)


async def process_live_price(symbol: str, price: float) -> None:
    """Detect first return into active zones from a lightweight all-symbol ticker poll."""
    global PRICE_EVENT_COUNT, TOUCH_COUNT
    previous = LIVE_PRICES.get(symbol)
    LIVE_PRICES[symbol] = price
    PRICE_EVENT_COUNT += 1

    for tf in TIMEFRAMES:
        k = key(symbol, tf)
        state = STATES.get(k)
        candles = BUFFERS.get(k)
        if state is None or not candles:
            continue

        # Keep the current candle usable for scoring without changing its open time.
        cur = candles[-1]
        cur.close = price
        cur.high = max(cur.high, price)
        cur.low = min(cur.low, price)

        events = []
        if not state.bull_broken and state.bull_top is not None and state.bull_bottom is not None:
            inside = state.bull_bottom <= price <= state.bull_top
            was_inside = state.bull_inside
            if inside and not was_inside:
                state.bull_tests += 1
                state.bull_live_tests += 1
                TOUCH_COUNT += 1
                events.append({
                    "side": "bull",
                    "bottom": state.bull_bottom,
                    "top": state.bull_top,
                    "tests": state.bull_live_tests if FIRST_LIVE_TOUCH_ONLY else state.bull_tests,
                    "historical_tests": state.bull_tests,
                    "created": state.bull_created,
                    "created_index": state.bull_created_index,
                    "impulse": state.bull_impulse, "structure_ok": bool(state.bull_structure), "launch_volume": state.bull_launch_volume,
                })
            state.bull_inside = inside

        if not state.bear_broken and state.bear_top is not None and state.bear_bottom is not None:
            inside = state.bear_bottom <= price <= state.bear_top
            was_inside = state.bear_inside
            if inside and not was_inside:
                state.bear_tests += 1
                state.bear_live_tests += 1
                TOUCH_COUNT += 1
                events.append({
                    "side": "bear",
                    "bottom": state.bear_bottom,
                    "top": state.bear_top,
                    "tests": state.bear_live_tests if FIRST_LIVE_TOUCH_ONLY else state.bear_tests,
                    "historical_tests": state.bear_tests,
                    "created": state.bear_created,
                    "created_index": state.bear_created_index,
                    "impulse": state.bear_impulse, "structure_ok": bool(state.bear_structure), "launch_volume": state.bear_launch_volume,
                })
            state.bear_inside = inside

        for event in events:
            spawn(handle_touch(symbol, tf, list(candles), event), name=f"touch:{symbol}:{tf}")


async def ticker_price_poller() -> None:
    """Reliable fallback: one REST request returns prices for all futures symbols."""
    global PRICE_POLL_COUNT, LAST_PRICE_UPDATE_AT
    while not STOP.is_set():
        started = time.monotonic()
        try:
            rows = await rest_get('/fapi/v1/ticker/price', attempts=5)
            if isinstance(rows, list):
                PRICE_POLL_COUNT += 1
                LAST_PRICE_UPDATE_AT = time.time()
                for row in rows:
                    symbol = row.get('symbol')
                    if symbol and symbol in SYMBOL_SET:
                        try:
                            await process_live_price(symbol, float(row['price']))
                        except (TypeError, ValueError, KeyError):
                            continue
                if PRICE_POLL_COUNT == 1 or PRICE_POLL_COUNT % 30 == 0:
                    log.info('PRICE POLL polls=%s price_events=%s symbols=%s', PRICE_POLL_COUNT, PRICE_EVENT_COUNT, len(LIVE_PRICES))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning('Ticker price poll failed: %s', exc)

        delay = max(2.0, 12.0 - (time.monotonic() - started))
        try:
            await asyncio.wait_for(STOP.wait(), timeout=delay)
        except asyncio.TimeoutError:
            pass


def preserve_zone_runtime(old: ZoneState, fresh: ZoneState) -> ZoneState:
    """Keep first-touch counters when the same zone survives a candle refresh."""
    if old.bull_created is not None and old.bull_created == fresh.bull_created:
        fresh.bull_tests = old.bull_tests
        fresh.bull_live_tests = old.bull_live_tests
        fresh.bull_inside = old.bull_inside
        fresh.bull_impulse = old.bull_impulse
        fresh.bull_structure = old.bull_structure
        fresh.bull_launch_volume = old.bull_launch_volume
    if old.bear_created is not None and old.bear_created == fresh.bear_created:
        fresh.bear_tests = old.bear_tests
        fresh.bear_live_tests = old.bear_live_tests
        fresh.bear_inside = old.bear_inside
        fresh.bear_impulse = old.bear_impulse
        fresh.bear_structure = old.bear_structure
        fresh.bear_launch_volume = old.bear_launch_volume
    return fresh


async def refresh_one_state(symbol: str, tf: str) -> None:
    """Refresh the most recent candles slowly so zones continue updating without WS data."""
    try:
        rows = await rest_get('/fapi/v1/klines', {'symbol': symbol, 'interval': tf, 'limit': 6}, attempts=4)
        if not isinstance(rows, list) or not rows:
            return
        now = int(time.time() * 1000)
        incoming = [
            Candle(int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5]), float(r[7]), float(r[9]), int(r[6]) < now)
            for r in rows
        ]
        k = key(symbol, tf)
        buf = BUFFERS.get(k)
        if not buf:
            return
        by_time = {x.open_time: x for x in buf}
        for candle in incoming:
            by_time[candle.open_time] = candle
        merged = sorted(by_time.values(), key=lambda x: x.open_time)[-HISTORY_LIMIT:]
        old = STATES.get(k, ZoneState())
        fresh = preserve_zone_runtime(old, rebuild(merged))
        BUFFERS[k] = merged
        STATES[k] = fresh
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        log.debug('State refresh failed %s %s: %s', symbol, tf, exc)


async def candle_refresh_loop(symbols: List[str]) -> None:
    """Stagger REST candle updates under Binance request limits."""
    items = [(s, tf) for s in symbols for tf in TIMEFRAMES]
    index = 0
    while not STOP.is_set():
        symbol, tf = items[index]
        index = (index + 1) % len(items)
        await refresh_one_state(symbol, tf)
        try:
            await asyncio.wait_for(STOP.wait(), timeout=1.5)
        except asyncio.TimeoutError:
            pass


async def test_messages(symbol_count: int) -> None:
    if not SEND_TEST_MESSAGES:
        return
    start = (
        "✅ <b>Ahmed Order Flow Intelligence بدأ العمل</b>\n\n"
        f"💹 العقود: <b>{symbol_count} Binance USDT Futures</b>\n"
        "⏰ الفريمات: <b>15m — 1H — 4H</b>\n"
        f"🎯 الجودة: <b>{MIN_SCORE}/100+</b> | بوابة العوامل: <b>{'مفعلة' if FACTOR_GATE_ENABLED else 'غير مفعلة'}</b>\n"
        "🧪 أول اختبار فقط\n"
        "🔁 منع التكرار مفعل\n"
        f"⏳ منع التكرار: <b>{COOLDOWN_HOURS} ساعة</b>\n📩 رسالة مختصرة + تأكيد فقط"
    )
    await telegram(start)
    await telegram(
        "🧪 <b>رسالة اختبار Bullish OF</b>\n\n"
        "هذه رسالة اختبار فقط للتأكد أن تنبيهات الشراء تصل إلى تيليجرام.\n"
        "عند تحقق فرصة حقيقية ستظهر العملة والفريم والمنطقة ودرجة الجودة."
    )
    await telegram(
        "🧪 <b>رسالة اختبار Bearish OF</b>\n\n"
        "هذه رسالة اختبار فقط للتأكد أن تنبيهات البيع تصل إلى تيليجرام.\n"
        "إذا وصلت الرسائل الثلاث فإعدادات Telegram صحيحة."
    )


def stats_payload() -> dict:
    return {
        "ok": True,
        "version": "v13.2",
        "states": len(STATES),
        "raw_ws_messages": RAW_WS_COUNT,
        "ws_errors": WS_ERROR_COUNT,
        "subscription_acks": SUBSCRIPTION_ACK_COUNT,
        "last_ws_message_seconds_ago": (None if LAST_WS_MESSAGE_AT is None else round(time.time() - LAST_WS_MESSAGE_AT, 1)),
        "price_polls": PRICE_POLL_COUNT,
        "price_events": PRICE_EVENT_COUNT,
        "last_price_update_seconds_ago": (None if LAST_PRICE_UPDATE_AT is None else round(time.time() - LAST_PRICE_UPDATE_AT, 1)),
        "live_prices": len(LIVE_PRICES),
        "rest_requests": REST_REQUEST_COUNT,
        "rest_rate_limits": REST_RATE_LIMIT_COUNT,
        "rest_blocked_seconds": round(max(0.0, REST_BLOCKED_UNTIL - time.time()), 1),
        "events": EVENT_COUNT,
        "zones_created_live": ZONE_COUNT,
        "touches_detected": TOUCH_COUNT,
        "alerts_sent": SENT_COUNT,
        "rejected_total": REJECT_COUNT,
        "rejected_reasons": dict(REJECT_REASONS),
        "missing_factors_on_reject": dict(MISSING_FACTORS),
        "score_buckets": dict(SCORE_BUCKETS),
        "pending_confirmations": len(PENDING),
        "active_bull": sum(1 for st in STATES.values() if not st.bull_broken and st.bull_top is not None),
        "active_bear": sum(1 for st in STATES.values() if not st.bear_broken and st.bear_top is not None),
        "telegram_configured": bool(BOT_TOKEN and CHAT_ID),
        "minimum_score": MIN_SCORE,
        "minimum_strong_factors": MIN_STRONG_FACTORS,
        "of_impulse": OF_IMPULSE,
        "require_structure_break": REQUIRE_STRUCTURE_BREAK,
        "max_zone_age": MAX_ZONE_AGE,
        "require_live_defense": REQUIRE_LIVE_DEFENSE,
        "factor_gate_enabled": FACTOR_GATE_ENABLED,
        "first_live_touch_only": FIRST_LIVE_TOUCH_ONLY,
    }

async def health(_: web.Request) -> web.Response:
    return web.json_response(stats_payload())

async def stats(_: web.Request) -> web.Response:
    data = stats_payload()
    html = f"""<!doctype html><html lang='ar' dir='rtl'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width'><title>Ahmed OF Stats</title><style>body{{font-family:Arial;background:#111;color:#eee;padding:24px}}.card{{max-width:700px;margin:auto;background:#1d1d1d;padding:22px;border-radius:14px}}h1{{font-size:22px}}pre{{white-space:pre-wrap;line-height:1.8;background:#0b0b0b;padding:16px;border-radius:10px}}</style></head><body><div class='card'><h1>Ahmed Order Flow Intelligence Pro v13.2</h1><pre>{json.dumps(data, ensure_ascii=False, indent=2)}</pre></div></body></html>"""
    return web.Response(text=html, content_type="text/html")


async def start_health() -> web.AppRunner:
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    app.router.add_get("/stats", stats)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    log.info("Health server running on port %s", PORT)
    return runner


async def heartbeat() -> None:
    while not STOP.is_set():
        await asyncio.sleep(300)
        d = stats_payload()
        log.info("HEARTBEAT raw=%s events=%s price_polls=%s price_events=%s ws_errors=%s active_bull=%s active_bear=%s touches=%s sent=%s rejected=%s", d["raw_ws_messages"], d["events"], d["price_polls"], d["price_events"], d["ws_errors"], d["active_bull"], d["active_bear"], d["touches_detected"], d["alerts_sent"], d["rejected_total"])

async def main() -> None:
    global SESSION, SYMBOL_SET
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_signal)
        except (NotImplementedError, RuntimeError):
            pass

    timeout = aiohttp.ClientTimeout(total=45)
    SESSION = aiohttp.ClientSession(
        timeout=timeout,
        connector=aiohttp.TCPConnector(limit=80, ttl_dns_cache=300),
    )
    runner = await start_health()
    managed_tasks: List[asyncio.Task] = []
    try:
        if not BOT_TOKEN or not CHAT_ID:
            log.error("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")
        symbols = await symbols_list()
        if not symbols:
            log.error("No symbols available; stopping cleanly")
            return
        SYMBOL_SET = set(symbols)
        log.info("Found %s active USDT perpetual contracts", len(symbols))
        await warmup(symbols)
        if STOP.is_set():
            return
        active_bull = sum(1 for st in STATES.values() if not st.bull_broken and st.bull_top is not None)
        active_bear = sum(1 for st in STATES.values() if not st.bear_broken and st.bear_top is not None)
        log.info("Warm-up active zones: bull=%s bear=%s", active_bull, active_bear)
        await test_messages(len(symbols))
        streams = [f"{s.lower()}@kline_{tf}" for s in symbols for tf in TIMEFRAMES]
        chunks = [streams[i:i + STREAMS_PER_WS] for i in range(0, len(streams), STREAMS_PER_WS)]
        log.info("Starting %s WebSocket connections", len(chunks))
        managed_tasks.extend(
            asyncio.create_task(ws_worker(chunk, i + 1), name=f"ws-{i + 1}")
            for i, chunk in enumerate(chunks)
        )
        managed_tasks.append(asyncio.create_task(ticker_price_poller(), name="ticker-poller"))
        managed_tasks.append(asyncio.create_task(candle_refresh_loop(symbols), name="candle-refresh"))
        managed_tasks.append(asyncio.create_task(heartbeat(), name="heartbeat"))
        await STOP.wait()
    except asyncio.CancelledError:
        STOP.set()
        raise
    except Exception:
        log.exception("Fatal application error")
        STOP.set()
    finally:
        STOP.set()
        await cancel_tasks(managed_tasks)
        await cancel_tasks(list(BACKGROUND_TASKS))
        await runner.cleanup()
        if SESSION is not None and not SESSION.closed:
            await SESSION.close()
        log.info("Shutdown complete; pending tasks cleaned")


def stop_signal() -> None:
    STOP.set()


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_signal)
        except NotImplementedError:
            pass
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
