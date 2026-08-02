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
# Ahmed Order Flow Intelligence v3
# Binance USDT-M Futures -> Telegram
# 15m / 1h / 4h | Bullish OF / Bearish OF only
# ============================================================

RIYADH = ZoneInfo("Asia/Riyadh")
TIMEFRAMES = ("15m", "1h", "4h")
TF_LABEL = {"15m": "15 دقيقة", "1h": "ساعة", "4h": "4 ساعات"}

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
PORT = int(os.getenv("PORT", "8080"))
MIN_SCORE = int(os.getenv("MIN_SCORE", "80"))
SEND_TEST_MESSAGES = os.getenv("SEND_TEST_MESSAGES", "true").lower() == "true"

OF_SWING = int(os.getenv("OF_SWING", "4"))
OF_ATR_LEN = int(os.getenv("OF_ATR_LEN", "14"))
OF_IMPULSE = float(os.getenv("OF_IMPULSE", "0.70"))
ZONE_SOURCE = os.getenv("ZONE_SOURCE", "Body + Wick")
BREAK_BY_CLOSE = os.getenv("BREAK_BY_CLOSE", "true").lower() == "true"
HISTORY_LIMIT = int(os.getenv("HISTORY_LIMIT", "220"))
REST_CONCURRENCY = int(os.getenv("REST_CONCURRENCY", "4"))
REST_GAP = float(os.getenv("REST_GAP", "0.10"))
STREAMS_PER_WS = int(os.getenv("STREAMS_PER_WS", "450"))

REST_BASES = [
    "https://fapi.binance.com",
    "https://fapi1.binance.com",
    "https://fapi2.binance.com",
    "https://fapi3.binance.com",
    "https://fapi4.binance.com",
]
WS_URL = os.getenv("BINANCE_WS", "wss://fstream.binance.com/ws")
TG_API = "https://api.telegram.org"

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("ahmed-of-v3")

SESSION: Optional[aiohttp.ClientSession] = None
STOP = asyncio.Event()
BUFFERS: Dict[str, List["Candle"]] = {}
STATES: Dict[str, "ZoneState"] = {}
PENDING: Dict[str, "PendingConfirmation"] = {}
DEDUP: set[str] = set()
EVENT_COUNT = 0


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
    bull_broken: bool = True
    bull_inside: bool = False
    bull_tests: int = 0

    bear_top: Optional[float] = None
    bear_bottom: Optional[float] = None
    bear_created: Optional[int] = None
    bear_created_index: Optional[int] = None
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
    return v < min(x.low for x in c[i-n:i]) and v <= min(x.low for x in c[i+1:i+n+1])


def pivot_high(c: List[Candle], i: int, n: int) -> bool:
    if i - n < 0 or i + n >= len(c):
        return False
    v = c[i].high
    return v > max(x.high for x in c[i-n:i]) and v >= max(x.high for x in c[i+1:i+n+1])


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


def score_block(local: dict, market: dict, first_test: bool) -> tuple[int, List[str], List[str]]:
    score = 25  # صلاحية البنية والاندفاع الأصلي
    yes, no = ["بنية OF + اندفاع ATR"], []
    factors = [
        (first_test, 15, "الاختبار الأول"),
        (local["freshness"], 8, "بلوك حديث"),
        (local["sweep"], 12, "Liquidity Sweep"),
        (local["absorption"], 10, "Absorption"),
        (local["delta_ok"], 8, "Delta مؤيد"),
        (local["cvd_ok"], 7, "CVD مؤيد"),
        (local["volume_spike"], 6, "Volume Spike"),
        (local["fvg"], 4, "FVG متداخل"),
        (market["oi_up"], 3, "OI يرتفع"),
        (market["book_ok"], 2, "Order Book مؤيد"),
    ]
    for ok, points, name in factors:
        if ok:
            score += points
            yes.append(name)
        else:
            no.append(name)
    score = min(100, score)
    return score, yes, no


def links(symbol: str) -> str:
    tv = f"https://www.tradingview.com/chart/?symbol=BINANCE:{symbol}.P"
    bn = f"https://www.binance.com/en/futures/{symbol}"
    return f'🔗 <a href="{tv}">TradingView</a> | <a href="{bn}">Binance</a>'


def touch_message(symbol: str, tf: str, side: str, bottom: float, top: float, price: float, score: int, local: dict, market: dict, yes: List[str], no: List[str]) -> str:
    bull = side == "bull"
    title = "🟢 <b>دخول بلوك شرائي قوي — Bullish OF</b>" if bull else "🔴 <b>دخول بلوك بيعي قوي — Bearish OF</b>"
    quality = "قوي جدًا" if score >= 90 else "قوي"
    checks = "\n".join(f"✅ {x}" for x in yes[:8])
    warnings = "\n".join(f"⚠️ {x}" for x in no[:3])
    now = datetime.now(RIYADH).strftime("%d-%m-%Y %H:%M:%S")
    return (
        f"{title}\n\n"
        f"💰 العملة: <b>#{symbol}.P</b>\n"
        f"⏰ الفريم: <b>{TF_LABEL[tf]}</b>\n"
        f"💵 السعر: <b>{fmt(price)}</b>\n"
        f"🧱 المنطقة: <b>{fmt(bottom)} — {fmt(top)}</b>\n\n"
        f"💪 الجودة: <b>{score}% — {quality}</b>\n"
        f"🧪 الاختبار: <b>الأول</b>\n"
        f"⏳ عمر البلوك: <b>{local['age']} شموع</b>\n\n"
        f"{checks}\n{warnings}\n\n"
        f"🧊 Iceberg محتمل: <b>{'نعم' if market['iceberg'] else 'لا'}</b>\n"
        f"🎯 POC التقريبي: <b>{fmt((bottom + top) / 2)}</b>\n\n"
        f"🕒 {now} (السعودية)\n{links(symbol)}\n\n"
        f"⚠️ تقييم إحصائي وليس ضمانًا للانعكاس"
    )


def confirmation_message(p: PendingConfirmation, price: float, evidence: List[str]) -> str:
    bull = p.side == "bull"
    title = "🚀 <b>تأكيد ارتداد من البلوك الشرائي</b>" if bull else "📉 <b>تأكيد هبوط من البلوك البيعي</b>"
    now = datetime.now(RIYADH).strftime("%d-%m-%Y %H:%M:%S")
    checks = "\n".join(f"✅ {x}" for x in evidence)
    return (
        f"{title}\n\n"
        f"💰 العملة: <b>#{p.symbol}.P</b>\n"
        f"⏰ الفريم: <b>{TF_LABEL[p.timeframe]}</b>\n"
        f"💵 سعر التأكيد: <b>{fmt(price)}</b>\n"
        f"💪 جودة البلوك الأصلية: <b>{p.score}%</b>\n\n"
        f"{checks}\n\n"
        f"🕒 {now} (السعودية)\n{links(p.symbol)}"
    )


def rebuild(c: List[Candle]) -> ZoneState:
    s = ZoneState()
    for i in range(len(c)):
        apply_bar(c, i, s, False)
    return s


def apply_bar(c: List[Candle], i: int, s: ZoneState, emit: bool) -> List[dict]:
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
        if (current.close - top) / max(a, 1e-12) >= OF_IMPULSE:
            s.bull_top, s.bull_bottom = top, bottom
            s.bull_created, s.bull_created_index = p.open_time, candidate
            s.bull_broken, s.bull_inside, s.bull_tests = False, False, 0
    if candidate >= OF_SWING and pivot_high(c, candidate, OF_SWING):
        p = c[candidate]
        top = max(p.open, p.close) if ZONE_SOURCE == "Candle Body" else p.high
        bottom = p.low if ZONE_SOURCE == "Full Candle" else min(p.open, p.close)
        if (bottom - current.close) / max(a, 1e-12) >= OF_IMPULSE:
            s.bear_top, s.bear_bottom = top, bottom
            s.bear_created, s.bear_created_index = p.open_time, candidate
            s.bear_broken, s.bear_inside, s.bear_tests = False, False, 0

    inside_bull = not s.bull_broken and s.bull_top is not None and current.high >= s.bull_bottom and current.low <= s.bull_top
    inside_bear = not s.bear_broken and s.bear_top is not None and current.high >= s.bear_bottom and current.low <= s.bear_top
    if inside_bull and not s.bull_inside:
        s.bull_tests += 1
        if emit:
            out.append({"side": "bull", "bottom": s.bull_bottom, "top": s.bull_top, "tests": s.bull_tests, "created": s.bull_created, "created_index": s.bull_created_index})
    if inside_bear and not s.bear_inside:
        s.bear_tests += 1
        if emit:
            out.append({"side": "bear", "bottom": s.bear_bottom, "top": s.bear_top, "tests": s.bear_tests, "created": s.bear_created, "created_index": s.bear_created_index})
    s.bull_inside, s.bear_inside = inside_bull, inside_bear

    if not s.bull_broken and s.bull_bottom is not None and (current.close if BREAK_BY_CLOSE else current.low) < s.bull_bottom:
        s.bull_broken = True
    if not s.bear_broken and s.bear_top is not None and (current.close if BREAK_BY_CLOSE else current.high) > s.bear_top:
        s.bear_broken = True
    return out


async def handle_touch(symbol: str, tf: str, c: List[Candle], event: dict) -> None:
    if event["tests"] != 1:
        return
    side = event["side"]
    dedup = f"{symbol}:{tf}:{side}:{event['created']}"
    if dedup in DEDUP:
        return
    local = local_metrics(c, side, float(event["bottom"]), float(event["top"]), int(event["created_index"] or 0))
    market = await market_confirmation(symbol, tf, side)
    score, yes, no = score_block(local, market, True)
    log.info("Block touch %s %s %s score=%s", symbol, tf, side, score)
    if score < MIN_SCORE:
        log.info("Skipped below MIN_SCORE: %s %s %s", symbol, tf, score)
        return
    DEDUP.add(dedup)
    text = touch_message(symbol, tf, side, float(event["bottom"]), float(event["top"]), c[-1].close, score, local, market, yes, no)
    ok = await telegram(text)
    log.info("Telegram touch sent=%s %s %s", ok, symbol, tf)
    if ok:
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
    assert SESSION is not None
    rid = wid * 10000
    while not STOP.is_set():
        try:
            async with SESSION.ws_connect(WS_URL, heartbeat=120, receive_timeout=240, max_msg_size=3_000_000) as ws:
                log.info("WS %s connected (%s streams)", wid, len(streams))
                for i in range(0, len(streams), 200):
                    rid += 1
                    await ws.send_json({"method": "SUBSCRIBE", "params": streams[i:i+200], "id": rid})
                    await asyncio.sleep(0.3)
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            data = json.loads(msg.data)
                            if "result" not in data:
                                await process_event(data)
                        except Exception:
                            log.exception("WS event error")
                    elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                        break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("WS %s disconnected: %s", wid, exc)
        await asyncio.sleep(5)


async def test_messages(symbol_count: int) -> None:
    if not SEND_TEST_MESSAGES:
        return
    start = (
        "✅ <b>Ahmed Order Flow Intelligence بدأ العمل</b>\n\n"
        f"💹 العقود: <b>{symbol_count} Binance USDT Futures</b>\n"
        "⏰ الفريمات: <b>15m — 1H — 4H</b>\n"
        f"🎯 الحد الأدنى للجودة: <b>{MIN_SCORE}%</b>\n"
        "🧪 أول اختبار فقط\n"
        "🔁 منع التكرار مفعل\n"
        "📩 تنبيه دخول + تنبيه تأكيد فقط"
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


async def health(_: web.Request) -> web.Response:
    return web.json_response({"ok": True, "states": len(STATES), "events": EVENT_COUNT, "pending": len(PENDING), "telegram": bool(BOT_TOKEN and CHAT_ID)})


async def start_health() -> web.AppRunner:
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    log.info("Health server running on port %s", PORT)
    return runner


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
        await test_messages(len(symbols))
        streams = [f"{s.lower()}@kline_{tf}" for s in symbols for tf in TIMEFRAMES]
        chunks = [streams[i:i+STREAMS_PER_WS] for i in range(0, len(streams), STREAMS_PER_WS)]
        log.info("Starting %s WebSocket connections", len(chunks))
        tasks = [asyncio.create_task(ws_worker(chunk, i+1)) for i, chunk in enumerate(chunks)]
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
