Ahmed Order Flow Intelligence Pro v12

الملفات:
- app.py
- requirements.txt
- Procfile

متغيرات Railway المقترحة:
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=-100...
MIN_SCORE=75
MIN_STRONG_FACTORS=4
FACTOR_GATE_ENABLED=false
FIRST_LIVE_TOUCH_ONLY=true
COOLDOWN_HOURS=12
STREAMS_PER_WS=180
DEBUG_BLOCKS=true
SEND_TEST_MESSAGES=false

ملاحظات V12:
- التقييم أصبح موزونًا بدل الاعتماد الأساسي على عدد العوامل.
- بوابة عدد العوامل متوقفة افتراضيًا عبر FACTOR_GATE_ENABLED=false.
- /stats يعرض missing_factors_on_reject و score_buckets.
- POC المستخدم تقدير شموعي يعتمد على أعلى شمعة حجمًا داخل نطاق البلوك، وليس Volume Profile tick-level كاملًا.
- الدرجة تقييم جودة وليست ضمانًا أو احتمال نجاح مؤكدًا.
