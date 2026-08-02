import asyncio
import json
import logging
import os
import random
import signal
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

import aiohttp
from aiohttp import web

# ============================================================
# Ahmed Order Flow Intelligence Pro v7
# Binance USDT-M Futures -> Telegram
# 15m / 1h / 4h | Bullish OF / Bearish OF only
# ============================================================

RIYADH = ZoneInfo("Asia/Riyadh")
TIMEFRAMES = ("15m", "1h", "4h")
TF_LABEL = {"15m": "15 دقيقة", "1h": "ساعة", "4h": "4 ساعات"}

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
PORT = int(os.getenv("PORT", "8080"))
MIN_SCORE = int(os.getenv("MIN_SCORE", "85"))
SEND_TEST_MESSAGES = os.getenv("SEND_TEST_MESSAGES", "true").lower() == "true"
MIN_STRONG_FACTORS = int(os.getenv("MIN_STRONG_FACTORS", "4"))
COOLDOWN_HOURS = int(os.getenv("COOLDOWN_HOURS", "12"))
DEBUG_BLOCKS = os.getenv("DEBUG_BLOCKS", "true").lower() == "true"

OF_SWING = int(os.getenv("OF_SWING", "4"))
OF_ATR_LEN = int(os.getenv("OF_ATR_LEN", "14"))
OF_IMPULSE = float(os.getenv("OF_IMPULSE", "0.70"))
ZONE_SOURCE = os.getenv("ZONE_SOURCE", "Body + Wick")
BREAK_BY_CLOSE = os.getenv("BREAK_BY_CLOSE", "true").lower() == "true"
OF_USE_VOL = os.getenv("OF_USE_VOL", "false").lower() == "true"
OF_VOL_LEN = int(os.getenv("OF_VOL_LEN", "20"))
OF_VOL_MULT = float(os.getenv("OF_VOL_MULT", "1.10"))
HISTORY_LIMIT = int(os.getenv("HISTORY_LIMIT", "99"))
REST_CONCURRENCY = int(os.getenv("REST_CONCURRENCY", "2"))
REST_GAP = float(os.getenv("REST_GAP", "0.45"))
STREAMS_PER_WS = int(os.getenv("STREAMS_PER_WS", "180"))

REST_BASES = [
    "https://fapi.binance.com",
    "https://fapi1.binance.com",
    "https://fapi2.binance.com",
    "https://fapi3.binance.com",
    "https://fapi4.binance.com",
]
WS_BASE = os.getenv("BINANCE_WS_BASE", "wss://fstream.binance.com/stream?streams=")
TG_API = "https://api.telegram.org"

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("ahmed-of-v7")

SESSION: Optional[aiohttp.ClientSession] = None
STOP = asyncio.Event()
BUFFERS: Dict[str, List["Candle"]] = {}
STATES: Dict[str, "ZoneState"] = {}
PENDING: Dict[str, "PendingConfirmation"] = {}
DEDUP: set[str] = set()
EVENT_COUNT = 0
RAW_WS_COUNT = 0
WS_ERROR_COUNT = 0
ZONE_COUNT = 0
TOUCH_COUNT = 0
SENT_COUNT = 0
REJECT_COUNT = 0
REJECT_REASONS: Dict[str, int] = {"score": 0, "factors": 0, "cooldown": 0, "duplicate": 0, "second_touch": 0}
LAST_TOUCH_AT: Dict[str, float] = {}
LAST_ALERT_AT: Dict[str, float] = {}


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

    bear_top: Optional[float] = None
    bear_bottom: Optional[float] = None
    bear_created: Optional[int] = None
    bear_created_index: Optional[int] = None
    bear_detected_index: Optional[int] = None
    bear_broken: bool = True
    bear_inside: bool = False
    bear_tests: int = 0


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
    freshness = age <= 12
    fvg = fvg_present(c, created_index, side)
    return {
        "volume_spike": volume_spike,
        "delta_ok": delta_ok,
        "cvd_ok": cvd_ok,
        "absorption": absorption,
        "sweep": sweep,
        "freshness": freshness,
        "fvg": fvg,
        "age": age,
        "recent_cvd": recent_cvd,
    }


async def rest_get(path: str, params: Optional[dict] = None, attempts: int = 8):
    assert SESSION is not None
    last = None
    for attempt in range(attempts):
        base = REST_BASES[attempt % len(REST_BASES)]
        try:
            async with SESSION.get(base + path, params=params, timeout=30) as r:
                if r.status == 200:
                    return await r.json()
                body = (await r.text())[:250]
                if r.status in (418, 429, 451) or r.status >= 500:
                    wait = min(45, 2 ** min(attempt, 5)) + random.random()
                    log.warning("Binance HTTP %s, retry %.1fs: %s", r.status, wait, body)
                    await asyncio.sleep(wait)
                    continue
                raise RuntimeError(f"HTTP {r.status}: {body}")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            last = exc
            await asyncio.sleep(min(20, 1.5 * (attempt + 1)))
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
    """تُطلب فقط عند أول لمس، لتجنب ضغط Binance."""
    result = {"oi_up": False, "book_ok": False, "iceberg": False}
    try:
        oi_period = "5m" if tf == "15m" else ("15m" if tf == "1h" else "1h")
        oi_data, depth = await asyncio.gather(
            rest_get("/futures/data/openInterestHist", {"symbol": symbol, "period": oi_period, "limit": 3}, attempts=4),
            rest_get("/fapi/v1/depth", {"symbol": symbol, "limit": 20}, attempts=4),
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
    except Exception as exc:
        log.warning("Market confirmation failed %s %s: %s", symbol, tf, exc)
    return result


def score_block(local: dict, market: dict, first_test: bool) -> tuple[int, List[str], List[str], int]:
    # الدرجة ليست احتمال نجاح؛ هي جودة توافق العوامل الحالية.
    score = 35  # تكوّن OF مطابق للمؤشر + اندفاع ATR
    yes, no = ["OF + ATR"], []
    factors = [
        (first_test, 12, "أول اختبار"),
        (local["freshness"], 6, "حديث"),
        (local["sweep"], 12, "Sweep"),
        (local["absorption"], 10, "Absorption"),
        (local["delta_ok"], 9, "Delta"),
        (local["cvd_ok"], 8, "CVD"),
        (local["volume_spike"], 7, "Volume"),
        (local["fvg"], 5, "FVG"),
        (market["oi_up"], 4, "OI"),
        (market["book_ok"], 4, "OrderBook"),
    ]
    strong = 0
    for ok, points, name in factors:
        if ok:
            score += points
            yes.append(name)
            if name not in ("أول اختبار", "حديث"):
                strong += 1
        else:
            no.append(name)
    return min(100, score), yes, no, strong

def links(symbol: str) -> str:
    tv = f"https://www.tradingview.com/chart/?symbol=BINANCE:{symbol}.P"
    bn = f"https://www.binance.com/en/futures/{symbol}"
    return f'🔗 <a href="{tv}">TradingView</a> | <a href="{bn}">Binance</a>'


def touch_message(symbol: str, tf: str, side: str, bottom: float, top: float, price: float, score: int, local: dict, market: dict, yes: List[str], no: List[str]) -> str:
    bull = side == "bull"
    title = "🟢 <b>Bullish OF قوي</b>" if bull else "🔴 <b>Bearish OF قوي</b>"
    factors = " • ".join(yes[1:6])
    now = datetime.now(RIYADH).strftime("%d-%m-%Y %H:%M")
    return (
        f"{title}\n\n"
        f"💰 <b>#{symbol}.P</b> | ⏰ <b>{TF_LABEL[tf]}</b>\n"
        f"💵 <b>{fmt(price)}</b> | 💪 <b>{score}%</b>\n"
        f"🧱 <b>{fmt(bottom)} — {fmt(top)}</b>\n"
        f"✅ {factors}\n"
        f"🧪 أول اختبار | ⏳ {local['age']} شموع\n"
        f"🕒 {now} السعودية\n"
        f"{links(symbol)}\n"
        f"⚠️ جودة إحصائية وليست ضمانًا"
    )

def confirmation_message(p: PendingConfirmation, price: float, evidence: List[str]) -> str:
    bull = p.side == "bull"
    title = "🚀 <b>تأكيد ارتداد شرائي</b>" if bull else "📉 <b>تأكيد هبوط بيعي</b>"
    checks = " • ".join(evidence[:4])
    now = datetime.now(RIYADH).strftime("%d-%m-%Y %H:%M")
    return (
        f"{title}\n\n"
        f"💰 <b>#{p.symbol}.P</b> | ⏰ <b>{TF_LABEL[p.timeframe]}</b>\n"
        f"💵 <b>{fmt(price)}</b> | 💪 <b>{p.score}%</b>\n"
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
        vol_ok = (not OF_USE_VOL) or (pivot_avg_vol is not None and p.volume > pivot_avg_vol * OF_VOL_MULT)
        if impulse >= OF_IMPULSE and vol_ok:
            is_new = s.bull_created != p.open_time
            s.bull_top, s.bull_bottom = top, bottom
            s.bull_created, s.bull_created_index, s.bull_detected_index = p.open_time, candidate, i
            s.bull_broken, s.bull_inside, s.bull_tests = False, False, 0
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
        vol_ok = (not OF_USE_VOL) or (pivot_avg_vol is not None and p.volume > pivot_avg_vol * OF_VOL_MULT)
        if impulse >= OF_IMPULSE and vol_ok:
            is_new = s.bear_created != p.open_time
            s.bear_top, s.bear_bottom = top, bottom
            s.bear_created, s.bear_created_index, s.bear_detected_index = p.open_time, candidate, i
            s.bear_broken, s.bear_inside, s.bear_tests = False, False, 0
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
                TOUCH_COUNT += 1
                out.append({"side": "bull", "bottom": s.bull_bottom, "top": s.bull_top, "tests": s.bull_tests, "created": s.bull_created, "created_index": s.bull_created_index})
    if inside_bear and not s.bear_inside:
        if s.bear_detected_index is None or i > s.bear_detected_index:
            s.bear_tests += 1
            if emit:
                TOUCH_COUNT += 1
                out.append({"side": "bear", "bottom": s.bear_bottom, "top": s.bear_top, "tests": s.bear_tests, "created": s.bear_created, "created_index": s.bear_created_index})
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
    log.info("TOUCH_DETECTED symbol=%s tf=%s side=%s test=%s zone=%s-%s", symbol, tf, side, event["tests"], fmt(float(event["bottom"])), fmt(float(event["top"])))
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
    score, yes, no, strong = score_block(local, market, True)
    log.info("TOUCH %s %s %s score=%s factors=%s zone=%s-%s", symbol, tf, side, score, strong, fmt(float(event['bottom'])), fmt(float(event['top'])))
    if score < MIN_SCORE:
        REJECT_COUNT += 1
        REJECT_REASONS["score"] += 1
        log.info("REJECT reason=score symbol=%s tf=%s side=%s score=%s required=%s missing=%s", symbol, tf, side, score, MIN_SCORE, ','.join(no[:6]))
        return
    if strong < MIN_STRONG_FACTORS:
        REJECT_COUNT += 1
        REJECT_REASONS["factors"] += 1
        log.info("REJECT reason=factors symbol=%s tf=%s side=%s factors=%s required=%s missing=%s", symbol, tf, side, strong, MIN_STRONG_FACTORS, ','.join(no[:6]))
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
        asyncio.create_task(handle_touch(symbol, tf, list(buf), ev))
    if c.closed:
        await check_confirmation(symbol, tf, buf)


async def symbols_list() -> List[str]:
    data = await rest_get("/fapi/v1/exchangeInfo", attempts=12)
    return sorted({x["symbol"] for x in data.get("symbols", []) if x.get("contractType") == "PERPETUAL" and x.get("quoteAsset") == "USDT" and x.get("status") == "TRADING"})


async def fetch_history(sem: asyncio.Semaphore, symbol: str, tf: str):
    async with sem:
        try:
            rows = await rest_get("/fapi/v1/klines", {"symbol": symbol, "interval": tf, "limit": HISTORY_LIMIT}, attempts=6)
            await asyncio.sleep(REST_GAP + random.random()*0.03)
            now = int(time.time()*1000)
            candles = [Candle(int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5]), float(r[7]), float(r[9]), int(r[6]) < now) for r in rows]
            return symbol, tf, candles
        except Exception as exc:
            log.error("History failed %s %s: %s", symbol, tf, exc)
            return symbol, tf, []


async def warmup(symbols: List[str]) -> None:
    sem = asyncio.Semaphore(REST_CONCURRENCY)
    jobs = [fetch_history(sem, s, tf) for s in symbols for tf in TIMEFRAMES]
    total, done = len(jobs), 0
    log.info("Warming %s symbol/timeframe states...", total)
    for fut in asyncio.as_completed(jobs):
        symbol, tf, candles = await fut
        done += 1
        if candles:
            BUFFERS[key(symbol, tf)] = candles
            STATES[key(symbol, tf)] = rebuild(candles)
        if done % 100 == 0 or done == total:
            log.info("Warm-up progress: %s/%s", done, total)


async def ws_worker(streams: List[str], wid: int) -> None:
    global RAW_WS_COUNT, WS_ERROR_COUNT
    assert SESSION is not None
    # Combined Stream: الاشتراك موجود داخل الرابط نفسه، لذلك لا نعتمد على
    # رسائل SUBSCRIBE التي كانت تتصل دون أن تستقبل أحداثًا على Railway.
    url = WS_BASE + "/".join(streams)
    while not STOP.is_set():
        try:
            async with SESSION.ws_connect(
                url,
                heartbeat=60,
                receive_timeout=180,
                max_msg_size=5_000_000,
                autoclose=True,
                autoping=True,
            ) as ws:
                log.info("WS %s combined connected (%s streams)", wid, len(streams))
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        RAW_WS_COUNT += 1
                        try:
                            data = json.loads(msg.data)
                            await process_event(data)
                            if RAW_WS_COUNT == 1 or RAW_WS_COUNT % 5000 == 0:
                                log.info("WS raw messages=%s kline_events=%s", RAW_WS_COUNT, EVENT_COUNT)
                        except Exception:
                            WS_ERROR_COUNT += 1
                            log.exception("WS event error wid=%s", wid)
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        WS_ERROR_COUNT += 1
                        log.warning("WS %s message error: %s", wid, ws.exception())
                        break
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSE):
                        break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            WS_ERROR_COUNT += 1
            log.warning("WS %s disconnected: %s", wid, exc)
        if not STOP.is_set():
            await asyncio.sleep(5)


async def test_messages(symbol_count: int) -> None:
    if not SEND_TEST_MESSAGES:
        return
    start = (
        "✅ <b>Ahmed Order Flow Intelligence بدأ العمل</b>\n\n"
        f"💹 العقود: <b>{symbol_count} Binance USDT Futures</b>\n"
        "⏰ الفريمات: <b>15m — 1H — 4H</b>\n"
        f"🎯 الجودة: <b>{MIN_SCORE}%+</b> | العوامل: <b>{MIN_STRONG_FACTORS}+</b>\n"
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
        "version": "v7",
        "states": len(STATES),
        "raw_ws_messages": RAW_WS_COUNT,
        "ws_errors": WS_ERROR_COUNT,
        "events": EVENT_COUNT,
        "zones_created_live": ZONE_COUNT,
        "touches_detected": TOUCH_COUNT,
        "alerts_sent": SENT_COUNT,
        "rejected_total": REJECT_COUNT,
        "rejected_reasons": dict(REJECT_REASONS),
        "pending_confirmations": len(PENDING),
        "active_bull": sum(1 for st in STATES.values() if not st.bull_broken and st.bull_top is not None),
        "active_bear": sum(1 for st in STATES.values() if not st.bear_broken and st.bear_top is not None),
        "telegram_configured": bool(BOT_TOKEN and CHAT_ID),
        "minimum_score": MIN_SCORE,
        "minimum_strong_factors": MIN_STRONG_FACTORS,
    }

async def health(_: web.Request) -> web.Response:
    return web.json_response(stats_payload())

async def stats(_: web.Request) -> web.Response:
    data = stats_payload()
    html = f"""<!doctype html><html lang='ar' dir='rtl'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width'><title>Ahmed OF Stats</title><style>body{{font-family:Arial;background:#111;color:#eee;padding:24px}}.card{{max-width:700px;margin:auto;background:#1d1d1d;padding:22px;border-radius:14px}}h1{{font-size:22px}}pre{{white-space:pre-wrap;line-height:1.8;background:#0b0b0b;padding:16px;border-radius:10px}}</style></head><body><div class='card'><h1>Ahmed Order Flow Intelligence Pro v7</h1><pre>{json.dumps(data, ensure_ascii=False, indent=2)}</pre></div></body></html>"""
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
        log.info("HEARTBEAT raw=%s events=%s ws_errors=%s active_bull=%s active_bear=%s touches=%s sent=%s rejected=%s", d["raw_ws_messages"], d["events"], d["ws_errors"], d["active_bull"], d["active_bear"], d["touches_detected"], d["alerts_sent"], d["rejected_total"])

async def main() -> None:
    global SESSION
    timeout = aiohttp.ClientTimeout(total=45)
    SESSION = aiohttp.ClientSession(timeout=timeout, connector=aiohttp.TCPConnector(limit=80, ttl_dns_cache=300))
    runner = await start_health()
    try:
        if not BOT_TOKEN or not CHAT_ID:
            log.error("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")
        symbols = await symbols_list()
        log.info("Found %s active USDT perpetual contracts", len(symbols))
        await warmup(symbols)
        active_bull = sum(1 for st in STATES.values() if not st.bull_broken and st.bull_top is not None)
        active_bear = sum(1 for st in STATES.values() if not st.bear_broken and st.bear_top is not None)
        log.info("Warm-up active zones: bull=%s bear=%s", active_bull, active_bear)
        await test_messages(len(symbols))
        streams = [f"{s.lower()}@kline_{tf}" for s in symbols for tf in TIMEFRAMES]
        chunks = [streams[i:i+STREAMS_PER_WS] for i in range(0, len(streams), STREAMS_PER_WS)]
        log.info("Starting %s WebSocket connections", len(chunks))
        tasks = [asyncio.create_task(ws_worker(chunk, i+1)) for i, chunk in enumerate(chunks)]
        tasks.append(asyncio.create_task(heartbeat()))
        await STOP.wait()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        await runner.cleanup()
        await SESSION.close()


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
