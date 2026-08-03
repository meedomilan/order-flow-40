Ahmed Order Flow Intelligence Pro v15 Stable

Railway variables:
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=-100...
MIN_SCORE=75
FIRST_LIVE_TOUCH_ONLY=true
SEND_TEST_MESSAGES=false
STATE_DIR=/data
BOOTSTRAP_GAP_SECONDS=3.5
CANDLE_REFRESH_SECONDS=15
STATE_SAVE_SECONDS=60
REST_MIN_INTERVAL=2.5
REST_CONCURRENCY=1
HISTORY_WORKERS=1

IMPORTANT: Add a Railway persistent Volume mounted at /data.
This prevents the bot from downloading all historical candles after every redeploy.
The service starts WebSocket price discovery immediately and hydrates missing history progressively.
