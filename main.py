"""
╔══════════════════════════════════════════╗
║   لوحة تحكم سحابة شرورة | Pro Edition   ║
║   ✅ مراقبة حية  ⏰ جدولة  🔐 أمان      ║
╚══════════════════════════════════════════╝

pip install aiogram aiohttp psutil apscheduler cryptography
"""

import os, sys, json, shutil, logging, psutil, asyncio, fcntl, aiohttp, re
from datetime import datetime, timedelta
from pathlib import Path
from cryptography.fernet import Fernet
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from aiogram import Bot, Dispatcher, types, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
from aiogram.enums import ParseMode

# ═══════════════════════════════════════════════
#                   الإعدادات
# ═══════════════════════════════════════════════
API_TOKEN  = '8413739496:AAGta42-7QTvVH_VN2UqnFfXXUqc_bBD52Q'
ADMIN_ID   = 1184628168
DO_TOKEN   = 'YOUR_DO_TOKEN_HERE'

BASE_DIR          = Path.cwd()
CLIENTS_DIR       = BASE_DIR / "hosted_bots"
LOGS_DIR          = BASE_DIR / "bot_logs"
BACKUPS_DIR       = BASE_DIR / "backups"
DB_FILE           = BASE_DIR / "subscriptions.json"
ACTION_LOG_FILE   = BASE_DIR / "actions_history.log"
STATS_FILE        = BASE_DIR / "stats_history.json"
FERNET_KEY_FILE   = BASE_DIR / ".fernet.key"

ALERT_CPU_THRESHOLD  = 80    # % تنبيه CPU
ALERT_RAM_THRESHOLD  = 200   # MB تنبيه RAM
MONITOR_INTERVAL     = 600   # ثانية (10 دقائق)
WATCHDOG_INTERVAL    = 30
MAX_RESTART_ATTEMPTS = 3
AUTO_RESTART_DELAY   = 5

# أنماط الكود الخطير للفحص الأمني
DANGEROUS_PATTERNS = [
    (r'os\.system\s*\(',             "تنفيذ أوامر نظام مباشرة"),
    (r'\beval\s*\(',                 "تنفيذ كود ديناميكي (eval)"),
    (r'\bexec\s*\(',                 "تنفيذ كود ديناميكي (exec)"),
    (r'__import__\s*\(',             "استيراد ديناميكي مشبوه"),
    (r'shutil\.rmtree\s*\(',         "حذف مجلدات كاملة"),
    (r'os\.remove\s*\(["\']/',       "حذف ملفات من مسارات جذر"),
    (r'(?:cryptominer|hashrate)',     "مشبوه: تعدين عملات"),
    (r'base64\.b64decode.*exec',     "كود مخفي/مشفر مشبوه"),
    (r'socket\.connect\s*\(',        "اتصال شبكي مباشر"),
    (r'subprocess\.call\s*\(',       "تشغيل عملية نظام مباشرة"),
]

# خيارات الجدولة الجاهزة
RESTART_OPTIONS = {
    "0 4 * * *":    "يومياً 04:00 ص",
    "0 6 * * *":    "يومياً 06:00 ص",
    "0 0 * * *":    "يومياً منتصف الليل",
    "0 */6 * * *":  "كل 6 ساعات",
    "0 */12 * * *": "كل 12 ساعة",
    "0 4 * * 0":    "أسبوعياً (الأحد)",
}

for folder in [CLIENTS_DIR, LOGS_DIR, BACKUPS_DIR]:
    folder.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════
#               🔐 وحدة الأمان
# ═══════════════════════════════════════════════
_fernet: Fernet | None = None

def get_fernet() -> Fernet:
    global _fernet
    if _fernet:
        return _fernet
    if not FERNET_KEY_FILE.exists():
        FERNET_KEY_FILE.write_bytes(Fernet.generate_key())
        FERNET_KEY_FILE.chmod(0o600)  # مالك فقط
    _fernet = Fernet(FERNET_KEY_FILE.read_bytes())
    return _fernet

def encrypt_text(text: str) -> str:
    return get_fernet().encrypt(text.encode()).decode()

def decrypt_text(text: str) -> str:
    try:
        return get_fernet().decrypt(text.encode()).decode()
    except Exception:
        return text  # إذا لم يكن مشفراً، أعده كما هو

def scan_code(code: str) -> list[tuple[str, str]]:
    """فحص الكود المرفوع بحثاً عن أنماط خطيرة"""
    return [
        (pattern, desc) for pattern, desc in DANGEROUS_PATTERNS
        if re.search(pattern, code, re.IGNORECASE)
    ]

def risk_level(warnings: list) -> str:
    if not warnings:   return "✅ آمن"
    if len(warnings) <= 2: return "⚠️ تحذيرات"
    return "🚨 خطر عالي"

# Rate Limiter بسيط لمنع الضغط المتكرر
_rate_cache: dict[str, datetime] = {}
RATE_SECONDS = 1.5

def is_rate_limited(key: str) -> bool:
    now  = datetime.now()
    last = _rate_cache.get(key)
    if last and (now - last).total_seconds() < RATE_SECONDS:
        return True
    _rate_cache[key] = now
    return False

# ═══════════════════════════════════════════════
#               قاعدة البيانات
# ═══════════════════════════════════════════════
def load_db() -> dict:
    if DB_FILE.exists():
        try:
            with open(DB_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}
    return {}

def save_db(data: dict):
    with open(DB_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

def load_stats() -> dict:
    if STATS_FILE.exists():
        try:
            with open(STATS_FILE, 'r') as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_stats(data: dict):
    with open(STATS_FILE, 'w') as f:
        json.dump(data, f)

def log_action(action: str):
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(ACTION_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {action}\n")

# ═══════════════════════════════════════════════
#           DigitalOcean API (Async)
# ═══════════════════════════════════════════════
DO_HEADERS = {"Authorization": f"Bearer {DO_TOKEN}", "Content-Type": "application/json"}

async def _do_req(method: str, endpoint: str, **kwargs) -> tuple[int, dict]:
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(headers=DO_HEADERS, timeout=timeout) as s:
            async with s.request(method, f"https://api.digitalocean.com/v2/{endpoint}", **kwargs) as r:
                return r.status, await r.json()
    except Exception as e:
        logger.error(f"DO API: {e}")
        return 0, {}

async def get_do_droplets() -> list:
    _, d = await _do_req("GET", "droplets")
    return d.get('droplets', [])

async def get_do_balance() -> dict:
    _, d = await _do_req("GET", "customers/my/balance")
    return d

async def do_reboot_droplet(d_id: str) -> bool:
    status, _ = await _do_req("POST", f"droplets/{d_id}/actions", json={"type": "reboot"})
    return status == 201

# ═══════════════════════════════════════════════
#            إدارة العمليات
# ═══════════════════════════════════════════════
def kill_process(pid) -> bool:
    if not pid:
        return False
    try:
        pid = int(pid)
        if psutil.pid_exists(pid):
            p = psutil.Process(pid)
            p.terminate()
            try:    p.wait(timeout=5)
            except: p.kill()
            return True
    except Exception:
        pass
    return False

def get_process_stats(pid) -> dict:
    if not pid:
        return {}
    try:
        pid = int(pid)
        if not psutil.pid_exists(pid):
            return {}
        p = psutil.Process(pid)
        with p.oneshot():
            secs = (datetime.now() - datetime.fromtimestamp(p.create_time())).total_seconds()
            h, r = divmod(int(secs), 3600)
            m, s = divmod(r, 60)
            return {
                "cpu":    round(p.cpu_percent(interval=0.1), 1),
                "ram":    round(p.memory_info().rss / 1024 / 1024, 1),
                "uptime": f"{h}س {m}د {s}ث",
            }
    except Exception:
        return {}

def is_alive(info: dict) -> bool:
    pid = info.get('pid')
    return bool(pid and psutil.pid_exists(int(pid))) and info.get('status') == 'active'

def tail_file(path: Path, lines: int = 40) -> str:
    try:
        size = path.stat().st_size
        if size == 0:
            return "الملف فارغ"
        with open(path, 'rb') as f:
            f.seek(-min(size, 16384), 2)
            raw = f.read().decode('utf-8', errors='replace')
        return '\n'.join(raw.splitlines()[-lines:])
    except Exception as e:
        return f"خطأ: {e}"

def backup_bot(bot_id: str, name: str, file: str) -> str | None:
    try:
        dest = BACKUPS_DIR / f"{name}_{bot_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.py"
        shutil.copy2(file, dest)
        return str(dest)
    except Exception as e:
        log_action(f"خطأ نسخ {name}: {e}")
        return None

def get_bot_stats() -> dict:
    db = load_db()
    active = pending = broken = 0
    for info in db.values():
        if is_alive(info):                    active  += 1
        elif info.get('status') == 'pending': pending += 1
        else:                                 broken  += 1
    return {"total": len(db), "active": active, "pending": pending, "broken": broken}

async def start_bot_process(bot_id: str, info: dict) -> tuple[bool, str]:
    fp = Path(info['file'])
    if not fp.exists():
        return False, "ملف البوت غير موجود"
    try:
        log_p = LOGS_DIR / f"{info['name']}.log"
        with open(log_p, "a", encoding='utf-8') as lf:
            lf.write(f"\n{'='*40}\n[{datetime.now()}] بدء التشغيل\n{'='*40}\n")
        log_fd = open(log_p, "ab")
        proc   = await asyncio.create_subprocess_exec(
            sys.executable, str(fp), stdout=log_fd, stderr=log_fd, cwd=str(BASE_DIR)
        )
        log_fd.close()
        return True, str(proc.pid)
    except Exception as e:
        return False, str(e)

# ═══════════════════════════════════════════════
#           📊 وحدة المراقبة والتحليل
# ═══════════════════════════════════════════════
def record_bot_stats(bot_id: str, cpu: float, ram: float):
    stats = load_stats()
    if bot_id not in stats:
        stats[bot_id] = []
    stats[bot_id].append({"t": datetime.now().strftime('%H:%M'), "c": cpu, "r": ram})
    stats[bot_id] = stats[bot_id][-144:]   # آخر 24 ساعة (10 دقائق × 144)
    save_stats(stats)

def mini_chart(data: list, key: str, title: str, unit: str) -> str:
    """رسم بياني نصي من آخر 12 نقطة بيانات"""
    if not data:
        return f"<b>{title}</b>\n<i>لا بيانات</i>"
    values = [d[key] for d in data[-12:]]
    mx     = max(values) or 1
    bars   = "".join("▁▂▃▄▅▆▇█"[min(int(v / mx * 7), 7)] for v in values)
    avg    = sum(values) / len(values)
    return (
        f"<b>{title}</b>\n"
        f"<code>{bars}</code>\n"
        f"متوسط {avg:.1f}{unit}  |  أعلى {max(values):.1f}{unit}  |  أدنى {min(values):.1f}{unit}"
    )

_bot_ref: Bot  # يُعيَّن لاحقاً

async def monitor_task():
    """مهمة خلفية: جمع إحصائيات + تنبيهات تجاوز الحدود"""
    await asyncio.sleep(30)
    logger.info("📊 مراقب الأداء بدأ")
    alerted: dict[str, set] = {}

    while True:
        try:
            db = load_db()
            for bid, info in db.items():
                if not is_alive(info):
                    continue
                ps = get_process_stats(info.get('pid'))
                if not ps:
                    continue

                cpu, ram = ps['cpu'], ps['ram']
                record_bot_stats(bid, cpu, ram)

                alerted.setdefault(bid, set())

                # تنبيه CPU
                if cpu > ALERT_CPU_THRESHOLD and 'cpu' not in alerted[bid]:
                    alerted[bid].add('cpu')
                    await _bot_ref.send_message(ADMIN_ID,
                        f"🔥 <b>تحذير CPU مرتفع!</b>\n"
                        f"📁 <code>{info['name']}</code>\n"
                        f"💻 {cpu}% (الحد: {ALERT_CPU_THRESHOLD}%)"
                    )
                elif cpu <= ALERT_CPU_THRESHOLD * 0.9:
                    alerted[bid].discard('cpu')

                # تنبيه RAM
                if ram > ALERT_RAM_THRESHOLD and 'ram' not in alerted[bid]:
                    alerted[bid].add('ram')
                    await _bot_ref.send_message(ADMIN_ID,
                        f"💾 <b>تحذير RAM مرتفع!</b>\n"
                        f"📁 <code>{info['name']}</code>\n"
                        f"🧠 {ram}MB (الحد: {ALERT_RAM_THRESHOLD}MB)"
                    )
                elif ram <= ALERT_RAM_THRESHOLD * 0.9:
                    alerted[bid].discard('ram')

        except Exception as e:
            logger.error(f"خطأ في المراقبة: {e}")

        await asyncio.sleep(MONITOR_INTERVAL)

async def send_daily_report():
    """تقرير يومي صباحي"""
    db    = load_db()
    bs    = get_bot_stats()
    vm    = psutil.virtual_memory()
    stats = load_stats()

    lines = ""
    for bid, info in db.items():
        hist = stats.get(bid, [])
        if hist:
            avg_c = sum(d['c'] for d in hist) / len(hist)
            avg_r = sum(d['r'] for d in hist) / len(hist)
            icon  = "🟢" if is_alive(info) else "🔴"
            lines += f"  {icon} {info['name'][:18]}: CPU {avg_c:.1f}% | RAM {avg_r:.1f}MB\n"

    await _bot_ref.send_message(ADMIN_ID,
        f"📅 <b>التقرير اليومي — {datetime.now().strftime('%Y-%m-%d')}</b>\n\n"
        f"🤖 بوتات: 🟢{bs['active']} | ⏳{bs['pending']} | 🔴{bs['broken']}\n"
        f"🖥️ CPU: {psutil.cpu_percent()}%  |  RAM: {vm.percent}%\n\n"
        f"<b>تفاصيل (متوسط 24س):</b>\n{lines or '  لا بيانات'}"
    )
    log_action("إرسال التقرير اليومي")

# ═══════════════════════════════════════════════
#           ⏰ وحدة الجدولة
# ═══════════════════════════════════════════════
scheduler = AsyncIOScheduler(timezone="Asia/Riyadh")

def _jid(bot_id: str, jtype: str) -> str:
    return f"{jtype}_{bot_id}"

async def _do_scheduled_restart(bot_id: str):
    db = load_db()
    if bot_id not in db:
        return
    info = db[bot_id]
    kill_process(info.get('pid'))
    ok, result = await start_bot_process(bot_id, info)
    if ok:
        db[bot_id].update({"pid": int(result), "status": "active",
                           "start_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                           "restart_count": db[bot_id].get('restart_count', 0) + 1})
        save_db(db)
        log_action(f"[مجدول] إعادة تشغيل {info['name']}")
        await _bot_ref.send_message(ADMIN_ID,
            f"⏰ <b>إعادة تشغيل مجدولة</b>\n📁 <code>{info['name']}</code> ✅"
        )

async def _do_expire_bot(bot_id: str):
    db = load_db()
    if bot_id not in db:
        return
    info = db[bot_id]
    kill_process(info.get('pid'))
    db[bot_id].update({"status": "expired", "pid": None})
    save_db(db)
    log_action(f"[منتهي] {info['name']} انتهت صلاحيته")
    await _bot_ref.send_message(ADMIN_ID,
        f"⌛ <b>البوت انتهت صلاحيته</b>\n📁 <code>{info['name']}</code>"
    )

async def _do_auto_backup():
    db = load_db()
    n  = sum(1 for bid, info in db.items()
             if Path(info['file']).exists() and backup_bot(bid, info['name'], info['file']))
    log_action(f"[تلقائي] نسخ احتياطي لـ {n} بوت")
    await _bot_ref.send_message(ADMIN_ID, f"💾 <b>نسخ احتياطي تلقائي</b>\nتم نسخ {n} بوت")

def add_restart_schedule(bot_id: str, cron: str):
    scheduler.add_job(_do_scheduled_restart, CronTrigger.from_crontab(cron, timezone="Asia/Riyadh"),
                      args=[bot_id], id=_jid(bot_id, "restart"), replace_existing=True)

def add_expiry_schedule(bot_id: str, exp: datetime):
    scheduler.add_job(_do_expire_bot, "date", run_date=exp,
                      args=[bot_id], id=_jid(bot_id, "expire"), replace_existing=True)

def remove_schedules(bot_id: str):
    for jt in ("restart", "expire"):
        j = scheduler.get_job(_jid(bot_id, jt))
        if j:
            j.remove()

def load_all_schedules():
    db = load_db()
    for bid, info in db.items():
        s = info.get('schedule', {})
        if s.get('restart'):
            try:   add_restart_schedule(bid, s['restart'])
            except Exception as e: logger.error(f"جدول {info['name']}: {e}")
        if s.get('expires'):
            try:
                exp = datetime.fromisoformat(s['expires'])
                if exp > datetime.now():
                    add_expiry_schedule(bid, exp)
            except Exception as e: logger.error(f"انتهاء {info['name']}: {e}")

# ═══════════════════════════════════════════════
#                  الواتشدوج
# ═══════════════════════════════════════════════
async def watchdog():
    await asyncio.sleep(15)
    logger.info("🐕 الواتشدوج بدأ")
    while True:
        try:
            db = load_db(); changed = False
            for bid, info in db.items():
                if info.get('status') != 'active':
                    continue
                pid = info.get('pid')
                if pid and psutil.pid_exists(int(pid)):
                    continue
                restarts = info.get('restart_count', 0)
                if info.get('auto_restart', True) and restarts < MAX_RESTART_ATTEMPTS:
                    await asyncio.sleep(AUTO_RESTART_DELAY)
                    ok, result = await start_bot_process(bid, info)
                    if ok:
                        db[bid].update({"pid": int(result), "restart_count": restarts + 1,
                                        "last_restart": datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
                        changed = True
                        log_action(f"[واتشدوج] إعادة تشغيل {info['name']} (محاولة {restarts+1})")
                        await _bot_ref.send_message(ADMIN_ID,
                            f"🔄 <b>إعادة تشغيل تلقائي</b>\n📁 <code>{info['name']}</code>\n"
                            f"🔢 المحاولة {restarts+1}/{MAX_RESTART_ATTEMPTS}"
                        )
                    else:
                        db[bid]['status'] = 'broken'; changed = True
                        await _bot_ref.send_message(ADMIN_ID,
                            f"❌ <b>فشل إعادة التشغيل</b>\n📁 <code>{info['name']}</code>\n⚠️ {result}"
                        )
                else:
                    db[bid]['status'] = 'broken'; changed = True
                    reason = "تجاوز الحد الأقصى" if info.get('auto_restart', True) else "الإعادة معطلة"
                    await _bot_ref.send_message(ADMIN_ID,
                        f"💀 <b>بوت معطل نهائياً</b>\n📁 <code>{info['name']}</code>\n⛔ {reason}"
                    )
            if changed:
                save_db(db)
        except Exception as e:
            logger.error(f"واتشدوج: {e}")
        await asyncio.sleep(WATCHDOG_INTERVAL)

# ═══════════════════════════════════════════════
#               FSM للإدخال النصي
# ═══════════════════════════════════════════════
class InputState(StatesGroup):
    expiry_date  = State()
    alert_cpu    = State()
    alert_ram    = State()

# ═══════════════════════════════════════════════
#               البوت الرئيسي
# ═══════════════════════════════════════════════
_bot_ref = Bot(token=API_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp       = Dispatcher()

def main_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🤖 إدارة البوتات",   callback_data="manage_bots"),
         InlineKeyboardButton(text="📤 رفع بوت",         callback_data="upload_bot")],
        [InlineKeyboardButton(text="📊 مراقبة حية",      callback_data="monitoring_menu"),
         InlineKeyboardButton(text="⏰ الجدولة",         callback_data="schedule_menu")],
        [InlineKeyboardButton(text="🔐 الأمان",          callback_data="security_menu"),
         InlineKeyboardButton(text="🌐 DigitalOcean",    callback_data="do_menu")],
        [InlineKeyboardButton(text="💾 نسخ احتياطي",    callback_data="backup_menu"),
         InlineKeyboardButton(text="📊 إحصائيات",       callback_data="stats")],
        [InlineKeyboardButton(text="🧹 تنظيف معطل",     callback_data="clean_broken"),
         InlineKeyboardButton(text="📋 سجل الإجراءات",  callback_data="action_log")],
    ])

# ─── /start ──────────────────────────────────────────────
@dp.message(Command("start"))
async def cmd_start(msg: types.Message):
    if msg.from_user.id != ADMIN_ID:
        return
    bs = get_bot_stats()
    await cb.message.edit_text(
        f"🚀 <b>لوحة تحكم سحابة شرورة | Pro</b>\n\n"
        f"🟢 {bs['active']} نشط  ⏳ {bs['pending']} انتظار  🔴 {bs['broken']} معطل\n"
        f"⏰ جداول نشطة: {len(scheduler.get_jobs())}",
        reply_markup=main_kb()
    )

@dp.callback_query(F.data == "back_main")
async def back_main(cb: types.CallbackQuery): await _back_main(cb)

@dp.callback_query(F.data == "noop")
async def noop(cb: types.CallbackQuery): await cb.answer()

# ═══════════════════════════════════════════════
#              نقطة التشغيل
# ═══════════════════════════════════════════════
async def main():
    print("🚀 بدء تشغيل لوحة التحكم Pro...")

    # تهيئة مفتاح التشفير
    get_fernet()

    # تحميل الجداول المحفوظة
    load_all_schedules()

    # جدولة التقرير اليومي الساعة 8:00 صباحاً
    scheduler.add_job(send_daily_report, CronTrigger(hour=8, minute=0, timezone="Asia/Riyadh"),
                      id="daily_report", replace_existing=True)

    # جدولة نسخ احتياطي تلقائي الساعة 3:00 فجراً
    scheduler.add_job(_do_auto_backup, CronTrigger(hour=3, minute=0, timezone="Asia/Riyadh"),
                      id="daily_backup", replace_existing=True)

    scheduler.start()

    asyncio.create_task(watchdog())
    asyncio.create_task(monitor_task())

    await dp.start_polling(_bot_ref)

if __name__ == "__main__":
    lock_file = None
    try:
        lock_file = open('/tmp/sh-combined.lock', 'w')
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        print("❌ البوت يعمل بالفعل!"); sys.exit(1)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("🛑 تم الإيقاف")
    finally:
        if lock_file:
            fcntl.flock(lock_file, fcntl.LOCK_UN)
            lock_file.close()

