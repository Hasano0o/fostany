import os, sys, json, shutil, logging, psutil, asyncio, fcntl, aiohttp, ast, re
from dotenv import load_dotenv
from datetime import datetime
from pathlib import Path
from aiogram import Bot, Dispatcher, types, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
from aiogram.enums import ParseMode

# ========== تحميل المتغيرات ==========
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'))
API_TOKEN  = os.getenv('API_TOKEN', '')
ADMIN_ID   = int(os.getenv('ADMIN_ID', '1184628168'))
DO_TOKEN   = os.getenv('DO_TOKEN', '')

BASE_DIR        = Path.cwd()
CLIENTS_DIR     = BASE_DIR / "hosted_bots"
LOGS_DIR        = BASE_DIR / "bot_logs"
BACKUPS_DIR     = BASE_DIR / "backups"
DB_FILE         = BASE_DIR / "subscriptions.json"
ACTION_LOG_FILE = BASE_DIR / "actions_history.log"

WATCHDOG_INTERVAL    = 30   # فحص كل 30 ثانية
MAX_RESTART_ATTEMPTS = 3    # أقصى إعادات تلقائية
AUTO_RESTART_DELAY   = 5    # ثواني قبل الإعادة

for folder in [CLIENTS_DIR, LOGS_DIR, BACKUPS_DIR]:
    folder.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DO_HEADERS = {"Authorization": f"Bearer {DO_TOKEN}", "Content-Type": "application/json"}

# ========== قاعدة البيانات ==========
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

def log_action(action: str):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(ACTION_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {action}\n")

# ========== DigitalOcean API (Async - لا blocking) ==========
async def _do_request(method: str, endpoint: str, **kwargs) -> tuple[int, dict]:
    url = f"https://api.digitalocean.com/v2/{endpoint}"
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(headers=DO_HEADERS, timeout=timeout) as session:
            async with session.request(method, url, **kwargs) as r:
                return r.status, await r.json()
    except Exception as e:
        logger.error(f"DO API خطأ: {e}")
        return 0, {}

async def get_do_droplets() -> list:
    _, data = await _do_request("GET", "droplets")
    return data.get('droplets', [])

async def get_do_balance() -> dict:
    _, data = await _do_request("GET", "customers/my/balance")
    return data

async def do_reboot_droplet(d_id: str) -> bool:
    status, _ = await _do_request("POST", f"droplets/{d_id}/actions", json={"type": "reboot"})
    return status == 201

# ========== إدارة العمليات ==========
def kill_process(pid) -> bool:
    if not pid:
        return False
    try:
        if psutil.pid_exists(int(pid)):
            proc = psutil.Process(int(pid))
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except psutil.TimeoutExpired:
                proc.kill()
            return True
    except (psutil.NoSuchProcess, psutil.AccessDenied, ValueError):
        pass
    return False

def get_process_stats(pid) -> dict:
    """إحصائيات العملية: CPU، RAM، وقت التشغيل"""
    if not pid:
        return {}
    try:
        pid = int(pid)
        if not psutil.pid_exists(pid):
            return {}
        proc = psutil.Process(pid)
        with proc.oneshot():
            uptime_secs = (datetime.now() - datetime.fromtimestamp(proc.create_time())).total_seconds()
            h, rem = divmod(int(uptime_secs), 3600)
            m, s   = divmod(rem, 60)
            return {
                "cpu":    round(proc.cpu_percent(interval=0.1), 1),
                "ram":    round(proc.memory_info().rss / (1024 * 1024), 1),
                "uptime": f"{h}س {m}د {s}ث",
            }
    except (psutil.NoSuchProcess, psutil.AccessDenied, ValueError):
        return {}

def is_alive(info: dict) -> bool:
    pid = info.get('pid')
    return bool(pid and psutil.pid_exists(int(pid))) and info.get('status') == 'active'

def tail_file(path: Path, lines: int = 40) -> str:
    """قراءة آخر N سطر من الملف"""
    try:
        size = path.stat().st_size
        if size == 0:
            return "الملف فارغ"
        with open(path, 'rb') as f:
            buf_size = min(size, 1024 * 16)
            f.seek(-buf_size, 2)
            raw = f.read().decode('utf-8', errors='replace')
        return '\n'.join(raw.splitlines()[-lines:])
    except Exception as e:
        return f"خطأ في القراءة: {e}"

def backup_bot(bot_id: str, name: str, file: str) -> str | None:
    try:
        dest = BACKUPS_DIR / f"{name}_{bot_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.py"
        shutil.copy2(file, dest)
        return str(dest)
    except Exception as e:
        log_action(f"خطأ في نسخ {name}: {e}")
        return None

def get_bot_stats() -> dict:
    db = load_db()
    active = pending = broken = 0
    for info in db.values():
        if is_alive(info):
            active += 1
        elif info.get('status') == 'pending':
            pending += 1
        else:
            broken += 1
    return {"total": len(db), "active": active, "pending": pending, "broken": broken}

# ========== تشغيل بوت ==========
# ========== 📦 استخراج وتثبيت المكتبات تلقائياً ==========

# المكتبات المثبتة مسبقاً على السيرفر (لا نثبّتها مجدداً)
BUILTIN_MODULES = {
    "os", "sys", "json", "re", "time", "math", "random", "datetime", "pathlib",
    "shutil", "logging", "asyncio", "fcntl", "subprocess", "hashlib", "base64",
    "collections", "itertools", "functools", "typing", "abc", "io", "copy",
    "threading", "multiprocessing", "socket", "ssl", "http", "urllib", "email",
    "html", "xml", "csv", "sqlite3", "pickle", "struct", "uuid", "enum",
    "dataclasses", "contextlib", "warnings", "traceback", "inspect", "ast",
}

# خريطة من اسم الاستيراد → اسم حزمة pip
IMPORT_TO_PIP = {
    "aiogram":        "aiogram",
    "aiohttp":        "aiohttp",
    "requests":       "requests",
    "psutil":         "psutil",
    "apscheduler":    "apscheduler",
    "cryptography":   "cryptography",
    "dotenv":         "python-dotenv",
    "PIL":            "Pillow",
    "cv2":            "opencv-python",
    "numpy":          "numpy",
    "pandas":         "pandas",
    "matplotlib":     "matplotlib",
    "scipy":          "scipy",
    "sklearn":        "scikit-learn",
    "telegram":       "python-telegram-bot",
    "telebot":        "pyTelegramBotAPI",
    "pymongo":        "pymongo",
    "redis":          "redis",
    "sqlalchemy":     "SQLAlchemy",
    "flask":          "Flask",
    "fastapi":        "fastapi",
    "uvicorn":        "uvicorn",
    "pydantic":       "pydantic",
    "bs4":            "beautifulsoup4",
    "lxml":           "lxml",
    "yaml":           "PyYAML",
    "toml":           "toml",
    "httpx":          "httpx",
    "aiosqlite":      "aiosqlite",
    "motor":          "motor",
    "celery":         "celery",
    "paramiko":       "paramiko",
    "boto3":          "boto3",
    "google":         "google-api-python-client",
    "openai":         "openai",
    "anthropic":      "anthropic",
    "langchain":      "langchain",
    "pytz":           "pytz",
    "arrow":          "arrow",
    "click":          "click",
    "rich":           "rich",
    "tqdm":           "tqdm",
    "colorama":       "colorama",
    "tabulate":       "tabulate",
    "qrcode":         "qrcode",
    "barcode":        "python-barcode",
    "pyttsx3":        "pyttsx3",
    "speech_recognition": "SpeechRecognition",
    "gtts":           "gTTS",
    "selenium":       "selenium",
    "playwright":     "playwright",
    "scrapy":         "Scrapy",
    "schedule":       "schedule",
    "cachetools":     "cachetools",
    "jwt":            "PyJWT",
    "passlib":        "passlib",
    "bcrypt":         "bcrypt",
}

def extract_imports(code: str) -> set[str]:
    """استخراج أسماء المكتبات من الكود"""
    imports = set()
    try:
        tree = ast.parse(code)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports.add(alias.name.split('.')[0])
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    imports.add(node.module.split('.')[0])
    except SyntaxError:
        # fallback بالـ regex إذا فشل ast
        for match in re.finditer(r'^(?:import|from)\s+([a-zA-Z_][a-zA-Z0-9_]*)', code, re.MULTILINE):
            imports.add(match.group(1))
    return imports

def get_missing_packages(code: str) -> list[str]:
    """إيجاد المكتبات غير المثبتة"""
    imports    = extract_imports(code)
    to_install = []
    for imp in imports:
        if imp in BUILTIN_MODULES:
            continue
        try:
            __import__(imp)
        except ImportError:
            pkg = IMPORT_TO_PIP.get(imp, imp)
            if pkg not in to_install:
                to_install.append(pkg)
    return to_install

async def install_packages(packages: list[str]) -> tuple[bool, str]:
    """تثبيت المكتبات بشكل async"""
    if not packages:
        return True, ""
    try:
        cmd  = [sys.executable, "-m", "pip", "install", "--break-system-packages", "-q"] + packages
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=120)
        if proc.returncode == 0:
            return True, stdout.decode()
        return False, stderr.decode()
    except asyncio.TimeoutError:
        return False, "انتهت مهلة التثبيت (120 ثانية)"
    except Exception as e:
        return False, str(e)


async def start_bot_process(bot_id: str, info: dict) -> tuple[bool, str]:
    """تشغيل بوت بشكل async حقيقي"""
    file_path = Path(info['file'])
    if not file_path.exists():
        return False, "ملف البوت غير موجود"
    try:
        log_p = LOGS_DIR / f"{info['name']}.log"
        with open(log_p, "a", encoding='utf-8') as lf:
            lf.write(f"\n{'='*40}\n[{datetime.now()}] بدء التشغيل\n{'='*40}\n")

        log_fd = open(log_p, "ab")
        proc = await asyncio.create_subprocess_exec(
            sys.executable, str(file_path),
            stdout=log_fd, stderr=log_fd,
            cwd=str(BASE_DIR)
        )
        log_fd.close()  # آمن: العملية الفرعية ورثت الـ FD
        return True, str(proc.pid)
    except Exception as e:
        return False, str(e)

# ========== الواتشدوج (مراقب تلقائي) ==========
_bot_ref: Bot = None

async def watchdog():
    """مهمة خلفية: تراقب البوتات وتُعيد تشغيل المتوقف تلقائياً"""
    await asyncio.sleep(15)  # انتظار بدء التشغيل
    logger.info("🐕 الواتشدوج بدأ")

    while True:
        try:
            db = load_db()
            changed = False

            for bid, info in db.items():
                if info.get('status') != 'active':
                    continue
                pid = info.get('pid')
                if pid and psutil.pid_exists(int(pid)):
                    continue

                # البوت توقف!
                restarts     = info.get('restart_count', 0)
                auto_restart = info.get('auto_restart', True)

                if auto_restart and restarts < MAX_RESTART_ATTEMPTS:
                    await asyncio.sleep(AUTO_RESTART_DELAY)
                    success, result = await start_bot_process(bid, info)

                    if success:
                        db[bid]['pid']           = int(result)
                        db[bid]['restart_count'] = restarts + 1
                        db[bid]['last_restart']  = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        changed = True
                        log_action(f"[واتشدوج] إعادة تشغيل {info['name']} (محاولة {restarts+1})")

                        if _bot_ref:
                            await _bot_ref.send_message(ADMIN_ID,
                                f"🔄 <b>إعادة تشغيل تلقائي</b>\n"
                                f"📁 <code>{info['name']}</code>\n"
                                f"🔢 المحاولة: {restarts+1}/{MAX_RESTART_ATTEMPTS}"
                            )
                    else:
                        db[bid]['status'] = 'broken'
                        changed = True
                        log_action(f"[واتشدوج] فشل إعادة تشغيل {info['name']}: {result}")
                        if _bot_ref:
                            await _bot_ref.send_message(ADMIN_ID,
                                f"❌ <b>فشل إعادة التشغيل</b>\n"
                                f"📁 <code>{info['name']}</code>\n"
                                f"⚠️ {result}"
                            )
                else:
                    db[bid]['status'] = 'broken'
                    changed = True
                    if _bot_ref:
                        reason = "تجاوز الحد الأقصى" if auto_restart else "الإعادة التلقائية معطلة"
                        await _bot_ref.send_message(ADMIN_ID,
                            f"💀 <b>بوت معطل نهائياً!</b>\n"
                            f"📁 <code>{info['name']}</code>\n"
                            f"⛔ {reason}"
                        )

            if changed:
                save_db(db)

        except Exception as e:
            logger.error(f"خطأ في الواتشدوج: {e}")

        await asyncio.sleep(WATCHDOG_INTERVAL)

# ========== البوت الرئيسي ==========
_bot_ref = Bot(token=os.getenv('API_TOKEN',''), default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp       = Dispatcher()

def main_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🤖 إدارة البوتات",        callback_data="manage_bots"),
         InlineKeyboardButton(text="📤 رفع بوت جديد",         callback_data="upload_bot")],
        [InlineKeyboardButton(text="🌐 سيرفر DigitalOcean",   callback_data="do_menu")],
        [InlineKeyboardButton(text="📊 الإحصائيات",           callback_data="stats"),
         InlineKeyboardButton(text="🧹 تنظيف المعطل",         callback_data="clean_broken")],
        [InlineKeyboardButton(text="💾 نسخ احتياطي",          callback_data="backup_menu"),
         InlineKeyboardButton(text="📋 سجل الإجراءات",        callback_data="action_log")],
    ])

# ─── /start ───────────────────────────────────────────────
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    stats = get_bot_stats()
    await message.answer(
        f"🚀 <b>لوحة تحكم سحابة شرورة</b>\n\n"
        f"🟢 نشطة: {stats['active']}  |  ⏳ انتظار: {stats['pending']}  |  🔴 معطلة: {stats['broken']}",
        reply_markup=main_kb()
    )

# ─── رفع بوت ──────────────────────────────────────────────
@dp.callback_query(F.data == "upload_bot")
async def upload_bot_prompt(cb: types.CallbackQuery):
    await cb.message.answer(
        "📤 <b>رفع بوت جديد</b>\n"
        "أرسل ملف <code>.py</code> للبوت.\n"
        "<i>سيتم مراجعته قبل التشغيل.</i>"
    )
    await cb.answer()

@dp.message(F.document)
async def handle_upload(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return

    fname = message.document.file_name
    if not fname.endswith(".py"):
        await message.answer("❌ يرجى رفع ملف <code>.py</code> فقط!")
        return

    file_path = CLIENTS_DIR / fname
    if file_path.exists():
        await message.answer(f"⚠️ يوجد بوت بنفس الاسم: <code>{fname}</code>\nاحذف القديم أولاً.")
        return

    try:
        tg_file = await _bot_ref.get_file(message.document.file_id)
        await _bot_ref.download_file(tg_file.file_path, file_path)
    except Exception as e:
        await message.answer(f"❌ خطأ في التحميل: {e}")
        return

    bot_id = str(int(datetime.now().timestamp()))
    db = load_db()
    db[bot_id] = {
        "file":          str(file_path),
        "name":          fname,
        "status":        "pending",
        "pid":           None,
        "upload_date":   datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "restart_count": 0,
        "auto_restart":  True,
    }
    save_db(db)
    log_action(f"رُفع بوت جديد: {fname} (ID: {bot_id})")

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ موافقة وتشغيل", callback_data=f"approve_{bot_id}"),
         InlineKeyboardButton(text="🗑️ رفض وحذف",     callback_data=f"delete_{bot_id}")],
        [InlineKeyboardButton(text="📄 عرض الكود",     callback_data=f"viewcode_{bot_id}")],
    ])
    await message.answer(
        f"📩 <b>بوت جديد مرفوع</b>\n"
        f"📁 الاسم: <code>{fname}</code>\n"
        f"🆔 المعرف: <code>{bot_id}</code>\n"
        f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        f"⏳ بانتظار موافقتك يا حسن.",
        reply_markup=kb
    )

# ─── عرض الكود ────────────────────────────────────────────
@dp.callback_query(F.data.startswith("viewcode_"))
async def view_code(cb: types.CallbackQuery):
    bot_id = cb.data[9:]          # إزالة "viewcode_"
    db = load_db()
    if bot_id not in db:
        await cb.answer("❌ البوت غير موجود!", show_alert=True); return

    fp = Path(db[bot_id]['file'])
    if not fp.exists():
        await cb.answer("❌ الملف غير موجود!", show_alert=True); return

    with open(fp, 'r', encoding='utf-8') as f:
        code = f.read(2500)
    suffix = "\n<i>... (عُرض أول 2500 حرف فقط)</i>" if len(code) >= 2500 else ""
    await cb.message.answer(
        f"📄 <b>كود البوت:</b> <code>{db[bot_id]['name']}</code>\n\n"
        f"<pre>{code}</pre>{suffix}"
    )
    await cb.answer()

# ─── موافقة وتشغيل ────────────────────────────────────────
@dp.callback_query(F.data.startswith("approve_"))
async def approve_bot(cb: types.CallbackQuery):
    bot_id = cb.data[8:]          # إزالة "approve_"
    db = load_db()
    if bot_id not in db:
        await cb.answer("❌ البوت غير موجود!", show_alert=True); return

    info = db[bot_id]
    await cb.message.edit_text(f"🔍 جاري فحص مكتبات <code>{info['name']}</code>...")

    # ── استخراج المكتبات وتثبيتها تلقائياً ──
    try:
        with open(info['file'], 'r', encoding='utf-8', errors='replace') as f:
            code = f.read()
        missing = get_missing_packages(code)
    except Exception:
        missing = []

    if missing:
        await cb.message.edit_text(
            f"📦 <b>تثبيت المكتبات المطلوبة...</b>\n"
            f"📁 <code>{info['name']}</code>\n\n"
            f"المكتبات: <code>{', '.join(missing)}</code>\n"
            f"⏳ قد يستغرق دقيقة..."
        )
        ok, pip_out = await install_packages(missing)
        if not ok:
            await cb.message.edit_text(
                f"⚠️ <b>تحذير: فشل تثبيت بعض المكتبات</b>\n"
                f"<code>{pip_out[:500]}</code>\n\n"
                f"⏳ جاري محاولة التشغيل على أي حال..."
            )
            log_action(f"فشل تثبيت مكتبات {info['name']}: {pip_out[:200]}")
        else:
            installed_txt = '\n'.join(f"  ✅ {p}" for p in missing)
            await cb.message.edit_text(
                f"✅ <b>تم تثبيت المكتبات بنجاح!</b>\n\n"
                f"{installed_txt}\n\n"
                f"⏳ جاري تشغيل البوت..."
            )
            log_action(f"تثبيت مكتبات {info['name']}: {', '.join(missing)}")
    else:
        await cb.message.edit_text(f"✅ كل المكتبات موجودة\n⏳ جاري تشغيل <code>{info['name']}</code>...")

    # ── تشغيل البوت ──
    success, result = await start_bot_process(bot_id, info)

    if success:
        db[bot_id].update({
            "pid":              int(result),
            "status":           "active",
            "start_date":       datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "restart_count":    0,
            "auto_restart":     True,
            "installed_pkgs":   missing,
        })
        save_db(db)
        log_action(f"تم تشغيل {info['name']} (PID: {result})")
        pkgs_txt = f"\n📦 مكتبات مثبّتة: <code>{', '.join(missing)}</code>" if missing else ""
        await cb.message.edit_text(
            f"✅ <b>البوت يعمل!</b>\n"
            f"📁 <code>{info['name']}</code>\n"
            f"🆔 PID: <code>{result}</code>\n"
            f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            f"{pkgs_txt}"
        )
    else:
        db[bot_id]['status'] = 'broken'
        save_db(db)
        log_action(f"فشل تشغيل {info['name']}: {result}")
        await cb.message.edit_text(f"❌ <b>فشل التشغيل:</b>\n<code>{result}</code>")

# ─── حذف بوت ──────────────────────────────────────────────
@dp.callback_query(F.data.startswith("delete_"))
async def delete_bot(cb: types.CallbackQuery):
    bot_id = cb.data[7:]          # إزالة "delete_"
    db = load_db()
    if bot_id not in db:
        await cb.answer("❌ البوت غير موجود!", show_alert=True); return

    info = db[bot_id]
    kill_process(info.get('pid'))
    backup_bot(bot_id, info['name'], info['file'])

    if Path(info['file']).exists():
        os.remove(info['file'])
    del db[bot_id]
    save_db(db)
    log_action(f"تم حذف {info['name']} (ID: {bot_id})")

    await cb.answer("🗑️ تم الحذف بنجاح", show_alert=True)
    await _show_main(cb)

# ─── إدارة البوتات ────────────────────────────────────────
@dp.callback_query(F.data == "manage_bots")
async def manage_bots(cb: types.CallbackQuery):
    db = load_db()
    btns = []

    if not db:
        btns.append([InlineKeyboardButton(text="❌ لا توجد بوتات", callback_data="noop")])
    else:
        for bid, info in db.items():
            if is_alive(info):         st = "🟢"
            elif info['status'] == 'pending': st = "⏳"
            else:                      st = "🔴"
            btns.append([InlineKeyboardButton(
                text=f"{st} {info['name'][:35]}",
                callback_data=f"det_{bid}"
            )])

    btns.append([InlineKeyboardButton(text="🔙 عودة", callback_data="back_main")])
    await cb.message.edit_text(
        "🤖 <b>إدارة البوتات:</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=btns)
    )

# ─── تفاصيل بوت ───────────────────────────────────────────
async def _show_details(cb: types.CallbackQuery, bid: str):
    """عرض تفاصيل بوت محدد"""
    db = load_db()
    if bid not in db:
        await cb.answer("❌ البوت غير موجود!", show_alert=True); return

    info = db[bid]
    alive = is_alive(info)

    if alive:             status_txt = "🟢 يعمل"
    elif info['status'] == 'pending': status_txt = "⏳ انتظار"
    elif info['status'] == 'stopped': status_txt = "⏸️ موقوف"
    else:                 status_txt = "🔴 معطل"

    text = (
        f"📄 <b>تفاصيل البوت</b>\n\n"
        f"📁 الاسم: <code>{info['name']}</code>\n"
        f"🆔 ID: <code>{bid}</code>\n"
        f"📊 الحالة: {status_txt}\n"
        f"🔢 PID: <code>{info.get('pid') or 'لا يوجد'}</code>\n"
        f"📅 الرفع: {info.get('upload_date', '—')}\n"
        f"▶️ آخر تشغيل: {info.get('start_date', '—')}\n"
        f"🔄 إعادات تلقائية: {info.get('restart_count', 0)}\n"
        f"🤖 إعادة تلقائية: {'✅' if info.get('auto_restart', True) else '❌'}\n"
    )

    if alive:
        ps = get_process_stats(info.get('pid'))
        if ps:
            text += (
                f"\n📈 <b>موارد العملية:</b>\n"
                f"• CPU: {ps['cpu']}%\n"
                f"• RAM: {ps['ram']} MB\n"
                f"• وقت التشغيل: {ps['uptime']}\n"
            )

    # إضافة المكتبات المثبّتة في النص
    pkgs = info.get('installed_pkgs', [])
    if pkgs:
        text += f"📦 مكتبات مثبّتة: <code>{', '.join(pkgs)}</code>\n"

    ar_label = "🔕 تعطيل إعادة تلقائية" if info.get('auto_restart', True) else "🔔 تفعيل إعادة تلقائية"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 إعادة تشغيل", callback_data=f"res_{bid}"),
         InlineKeyboardButton(text="⏹️ إيقاف",       callback_data=f"stop_{bid}")],
        [InlineKeyboardButton(text="📄 عرض الكود",   callback_data=f"viewcode_{bid}"),
         InlineKeyboardButton(text="📋 آخر السجلات", callback_data=f"logs_{bid}")],
        [InlineKeyboardButton(text="📥 تحميل السجل", callback_data=f"dllog_{bid}"),
         InlineKeyboardButton(text="📦 إعادة تثبيت المكتبات", callback_data=f"pkgs_{bid}")],
        [InlineKeyboardButton(text=ar_label,          callback_data=f"togglear_{bid}"),
         InlineKeyboardButton(text="🗑️ حذف جذري",   callback_data=f"delete_{bid}")],
        [InlineKeyboardButton(text="🔙 رجوع",         callback_data="manage_bots")],
    ])

    await cb.message.edit_text(text, reply_markup=kb)

@dp.callback_query(F.data.startswith("det_"))
async def details(cb: types.CallbackQuery):
    await _show_details(cb, cb.data[4:])

# ─── إعادة تثبيت المكتبات ─────────────────────────────────
@dp.callback_query(F.data.startswith("pkgs_"))
async def reinstall_packages(cb: types.CallbackQuery):
    bid = cb.data[5:]
    db  = load_db()
    if bid not in db:
        await cb.answer("❌ البوت غير موجود!", show_alert=True); return

    info = db[bid]
    fp   = Path(info['file'])
    if not fp.exists():
        await cb.answer("❌ ملف البوت غير موجود!", show_alert=True); return

    await cb.message.edit_text(f"🔍 جاري فحص مكتبات <code>{info['name']}</code>...")

    with open(fp, 'r', encoding='utf-8', errors='replace') as f:
        code = f.read()

    missing = get_missing_packages(code)
    all_imports = list(extract_imports(code) - BUILTIN_MODULES)
    all_pkgs = [IMPORT_TO_PIP.get(i, i) for i in all_imports]

    if not all_pkgs:
        await cb.answer("✅ لا توجد مكتبات خارجية", show_alert=True)
        await _show_details(cb, bid)
        return

    await cb.message.edit_text(
        f"📦 <b>مكتبات البوت: {info['name']}</b>\n\n"
        f"🔍 المكتبات المكتشفة:\n"
        f"<code>{', '.join(all_pkgs)}</code>\n\n"
        f"{'⚠️ غير مثبّتة: ' + ', '.join(missing) if missing else '✅ كلها مثبّتة'}\n\n"
        f"⏳ جاري التثبيت..."
    )

    ok, pip_out = await install_packages(all_pkgs)

    if ok:
        db[bid]['installed_pkgs'] = all_pkgs
        save_db(db)
        log_action(f"إعادة تثبيت مكتبات {info['name']}: {', '.join(all_pkgs)}")
        await cb.message.edit_text(
            f"✅ <b>تم تثبيت المكتبات بنجاح!</b>\n\n"
            f"📦 <code>{', '.join(all_pkgs)}</code>"
        )
    else:
        await cb.message.edit_text(
            f"❌ <b>فشل تثبيت بعض المكتبات:</b>\n"
            f"<code>{pip_out[:800]}</code>"
        )

    await asyncio.sleep(2)
    await _show_details(cb, bid)


@dp.callback_query(F.data.startswith("stop_"))
async def stop_bot(cb: types.CallbackQuery):
    bid = cb.data[5:]
    db = load_db()
    if bid not in db:
        await cb.answer("❌ البوت غير موجود!", show_alert=True); return

    info = db[bid]
    killed = kill_process(info.get('pid'))
    db[bid].update({"status": "stopped", "pid": None, "auto_restart": False})
    save_db(db)
    log_action(f"تم إيقاف {info['name']} (ID: {bid})")

    await cb.answer("⏹️ تم الإيقاف" if killed else "⚠️ لم يكن يعمل", show_alert=True)
    await _show_details(cb, bid)

# ─── تبديل الإعادة التلقائية ──────────────────────────────
@dp.callback_query(F.data.startswith("togglear_"))
async def toggle_auto_restart(cb: types.CallbackQuery):
    bid = cb.data[9:]
    db = load_db()
    if bid not in db:
        await cb.answer("❌ البوت غير موجود!", show_alert=True); return

    new_val = not db[bid].get('auto_restart', True)
    db[bid]['auto_restart'] = new_val
    save_db(db)
    await cb.answer(f"الإعادة التلقائية: {'✅ مفعلة' if new_val else '❌ معطلة'}", show_alert=True)
    await _show_details(cb, bid)

# ─── إعادة تشغيل ──────────────────────────────────────────
@dp.callback_query(F.data.startswith("res_"))
async def restart_bot(cb: types.CallbackQuery):
    bid = cb.data[4:]
    db = load_db()
    if bid not in db:
        await cb.answer("❌ البوت غير موجود!", show_alert=True); return

    info = db[bid]
    kill_process(info.get('pid'))

    success, result = await start_bot_process(bid, info)

    if success:
        db[bid].update({
            "pid":           int(result),
            "status":        "active",
            "start_date":    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "restart_count": db[bid].get('restart_count', 0) + 1,
            "auto_restart":  True,
        })
        save_db(db)
        log_action(f"إعادة تشغيل {info['name']} (PID: {result})")
        await cb.answer("🔄 تم إعادة التشغيل", show_alert=True)
    else:
        db[bid]['status'] = 'broken'
        save_db(db)
        await cb.answer(f"❌ فشل: {result}", show_alert=True)

    await _show_details(cb, bid)

# ─── سجل الأخطاء (آخر 40 سطر) ────────────────────────────
@dp.callback_query(F.data.startswith("logs_"))
async def show_logs(cb: types.CallbackQuery):
    bot_id = cb.data[5:]
    db = load_db()
    if bot_id not in db:
        await cb.answer("❌ البوت غير موجود!", show_alert=True); return

    info = db[bot_id]
    log_file = LOGS_DIR / f"{info['name']}.log"

    if not log_file.exists():
        await cb.answer("لا يوجد سجل لهذا البوت", show_alert=True); return

    logs = tail_file(log_file, lines=40)
    if len(logs) > 3500:
        logs = "...\n" + logs[-3500:]

    await cb.message.answer(
        f"📋 <b>آخر سجلات:</b> <code>{info['name']}</code>\n\n<pre>{logs}</pre>"
    )
    await cb.answer()

# ─── تحميل ملف السجل ──────────────────────────────────────
@dp.callback_query(F.data.startswith("dllog_"))
async def download_log(cb: types.CallbackQuery):
    bot_id = cb.data[6:]
    db = load_db()
    if bot_id not in db:
        await cb.answer("❌ البوت غير موجود!", show_alert=True); return

    info = db[bot_id]
    log_file = LOGS_DIR / f"{info['name']}.log"

    if not log_file.exists() or log_file.stat().st_size == 0:
        await cb.answer("لا يوجد سجل للإرسال", show_alert=True); return

    await cb.answer("📥 جاري الإرسال...")
    await cb.message.answer_document(
        FSInputFile(log_file, filename=f"log_{info['name']}.txt"),
        caption=f"📋 سجل البوت: <code>{info['name']}</code>"
    )

# ─── الإحصائيات ───────────────────────────────────────────
@dp.callback_query(F.data == "stats")
async def stats(cb: types.CallbackQuery):
    vm   = psutil.virtual_memory()
    cpu  = psutil.cpu_percent(interval=0.5)
    disk = psutil.disk_usage('/')
    bs   = get_bot_stats()
    db   = load_db()

    total_cpu = total_ram = 0.0
    for info in db.values():
        ps = get_process_stats(info.get('pid'))
        if ps:
            total_cpu += ps['cpu']
            total_ram += ps['ram']

    text = (
        f"📊 <b>إحصائيات شاملة</b>\n\n"
        f"🖥️ <b>السيرفر:</b>\n"
        f"• CPU: {cpu}%\n"
        f"• RAM: {round(vm.used/1024**3,1)}GB / {round(vm.total/1024**3,1)}GB ({vm.percent}%)\n"
        f"• مساحة: {round(disk.used/1024**3,1)}GB / {round(disk.total/1024**3,1)}GB ({disk.percent}%)\n\n"
        f"🤖 <b>البوتات:</b>\n"
        f"• إجمالي: {bs['total']}\n"
        f"• نشطة: 🟢 {bs['active']}\n"
        f"• انتظار: ⏳ {bs['pending']}\n"
        f"• معطلة: 🔴 {bs['broken']}\n\n"
        f"📈 <b>استهلاك البوتات:</b>\n"
        f"• CPU: {round(total_cpu, 1)}%\n"
        f"• RAM: {round(total_ram, 1)} MB\n\n"
        f"💾 <b>النسخ الاحتياطية:</b> {len(list(BACKUPS_DIR.glob('*.py')))}\n"
        f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 تحديث", callback_data="stats"),
         InlineKeyboardButton(text="🔙 عودة",  callback_data="back_main")]
    ])
    await cb.message.edit_text(text, reply_markup=kb)

# ─── سجل الإجراءات ────────────────────────────────────────
@dp.callback_query(F.data == "action_log")
async def show_action_log(cb: types.CallbackQuery):
    if not ACTION_LOG_FILE.exists():
        await cb.answer("لا يوجد سجل بعد", show_alert=True); return

    logs = tail_file(ACTION_LOG_FILE, lines=25)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 تحميل السجل الكامل", callback_data="dl_action_log")],
        [InlineKeyboardButton(text="🔙 عودة", callback_data="back_main")]
    ])
    await cb.message.edit_text(
        f"📋 <b>آخر 25 إجراء:</b>\n\n<pre>{logs}</pre>",
        reply_markup=kb
    )

@dp.callback_query(F.data == "dl_action_log")
async def download_action_log(cb: types.CallbackQuery):
    if not ACTION_LOG_FILE.exists():
        await cb.answer("لا يوجد سجل", show_alert=True); return
    await cb.answer("📥 جاري الإرسال...")
    await cb.message.answer_document(
        FSInputFile(ACTION_LOG_FILE, filename="actions_history.log"),
        caption="📋 سجل الإجراءات الكامل"
    )

# ─── تنظيف المعطل ─────────────────────────────────────────
@dp.callback_query(F.data == "clean_broken")
async def clean_broken(cb: types.CallbackQuery):
    db = load_db()
    to_del = [
        bid for bid, info in db.items()
        if not Path(info['file']).exists() or info['status'] == 'broken'
    ]
    for bid in to_del:
        del db[bid]
    save_db(db)
    log_action(f"تنظيف {len(to_del)} بوت معطل")
    await cb.answer(f"🧹 تم تنظيف {len(to_del)} بوت", show_alert=True)

# ─── DigitalOcean ──────────────────────────────────────────
@dp.callback_query(F.data == "do_menu")
async def do_menu(cb: types.CallbackQuery):
    await cb.message.edit_text("⏳ جاري جلب معلومات السيرفر...")

    droplets, bal = await asyncio.gather(get_do_droplets(), get_do_balance())

    d_id = None
    text = "🌐 <b>إدارة DigitalOcean</b>\n\n"

    if droplets:
        d    = droplets[0]
        d_id = str(d['id'])
        nets = d.get('networks', {}).get('v4', [])
        ip   = nets[0]['ip_address'] if nets else '—'
        text += (
            f"🖥️ <b>{d['name']}</b>\n"
            f"• IP: <code>{ip}</code>\n"
            f"• الحالة: {d['status']}\n"
            f"• CPU: {d.get('vcpus', '?')} vCPU\n"
            f"• RAM: {d.get('memory', '?')} MB\n"
            f"• Disk: {d.get('disk', '?')} GB\n"
            f"• المنطقة: {d.get('region', {}).get('name', '?')}\n\n"
        )
    else:
        text += "❌ لا توجد سيرفرات\n\n"

    text += (
        f"💰 الفاتورة الشهرية: <code>{bal.get('month_to_date_balance', '0.00')}$</code>\n"
        f"🏦 رصيد الحساب: <code>{bal.get('account_balance', '0.00')}$</code>"
    )

    reboot_cb = f"do_reboot_{d_id}" if d_id else "noop"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 ريبوت السيرفر",    callback_data=reboot_cb)],
        [InlineKeyboardButton(text="📊 تفاصيل الموارد",   callback_data="do_resources")],
        [InlineKeyboardButton(text="🔙 عودة",             callback_data="back_main")],
    ])
    await cb.message.edit_text(text, reply_markup=kb)

@dp.callback_query(F.data.startswith("do_reboot_"))
async def do_reboot(cb: types.CallbackQuery):
    d_id = cb.data[10:]   # إزالة "do_reboot_"
    if not d_id or d_id == "None":
        await cb.answer("❌ لا يوجد سيرفر", show_alert=True); return

    success = await do_reboot_droplet(d_id)
    if success:
        await cb.answer("⚠️ جاري إعادة تشغيل السيرفر...", show_alert=True)
        log_action(f"ريبوت سيرفر DO (ID: {d_id})")
    else:
        await cb.answer("❌ فشل في إعادة التشغيل", show_alert=True)

@dp.callback_query(F.data == "do_resources")
async def do_resources(cb: types.CallbackQuery):
    droplets = await get_do_droplets()

    text = "📊 <b>موارد DigitalOcean</b>\n\n"
    t_cpu = t_ram = t_disk = 0

    for d in droplets:
        t_cpu  += d.get('vcpus', 0)
        t_ram  += d.get('memory', 0)
        t_disk += d.get('disk', 0)
        nets = d.get('networks', {}).get('v4', [])
        ip   = nets[0]['ip_address'] if nets else '—'
        text += (
            f"🖥️ <b>{d['name']}</b> ({d['status']})\n"
            f"  IP: <code>{ip}</code>  |  "
            f"CPU: {d.get('vcpus','?')}  |  "
            f"RAM: {d.get('memory','?')}MB  |  "
            f"Disk: {d.get('disk','?')}GB\n\n"
        )

    if droplets:
        text += f"<b>📈 الإجمالي:</b> {len(droplets)} سيرفر | {t_cpu} vCPU | {t_ram}MB | {t_disk}GB"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 تحديث", callback_data="do_resources"),
         InlineKeyboardButton(text="🔙 رجوع",  callback_data="do_menu")]
    ])
    await cb.message.edit_text(text, reply_markup=kb)

# ─── نسخ احتياطي ──────────────────────────────────────────
@dp.callback_query(F.data == "backup_menu")
async def backup_menu(cb: types.CallbackQuery):
    files     = list(BACKUPS_DIR.glob('*.py'))
    count     = len(files)
    size_kb   = round(sum(f.stat().st_size for f in files) / 1024, 1)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💾 نسخ جميع البوتات",       callback_data="backup_all")],
        [InlineKeyboardButton(text="🗑️ حذف نسخ أقدم من 30 يوم", callback_data="clean_backups")],
        [InlineKeyboardButton(text="🔙 عودة",                   callback_data="back_main")],
    ])
    await cb.message.edit_text(
        f"💾 <b>النسخ الاحتياطي</b>\n\n"
        f"• عدد النسخ: {count}\n"
        f"• الحجم الكلي: {size_kb} KB",
        reply_markup=kb
    )

@dp.callback_query(F.data == "backup_all")
async def backup_all_bots(cb: types.CallbackQuery):
    db = load_db()
    backed = sum(
        1 for bid, info in db.items()
        if Path(info['file']).exists() and backup_bot(bid, info['name'], info['file'])
    )
    log_action(f"نسخ احتياطي لـ {backed} بوت")
    await cb.answer(f"💾 تم نسخ {backed} بوت", show_alert=True)

@dp.callback_query(F.data == "clean_backups")
async def clean_backups(cb: types.CallbackQuery):
    cutoff  = datetime.now().timestamp() - 30 * 24 * 3600
    deleted = 0
    for f in BACKUPS_DIR.glob('*.py'):
        if f.stat().st_mtime < cutoff:
            f.unlink()
            deleted += 1
    log_action(f"حذف {deleted} نسخة قديمة")
    await cb.answer(f"🗑️ تم حذف {deleted} نسخة قديمة", show_alert=True)

# ─── مشتركات ──────────────────────────────────────────────
async def _show_main(cb: types.CallbackQuery):
    stats = get_bot_stats()
    await cb.message.edit_text(
        f"🚀 <b>لوحة تحكم سحابة شرورة</b>\n\n"
        f"🟢 {stats['active']}  ⏳ {stats['pending']}  🔴 {stats['broken']}",
        reply_markup=main_kb()
    )

@dp.callback_query(F.data == "back_main")
async def back_main(cb: types.CallbackQuery):
    await _show_main(cb)

@dp.callback_query(F.data == "noop")
async def noop(cb: types.CallbackQuery):
    await cb.answer()

# ========== نقطة التشغيل ==========
async def main():
    global _bot_ref

    print("🚀 بدء تشغيل لوحة التحكم...")
    asyncio.create_task(watchdog())
    await dp.start_polling(_bot_ref)

if __name__ == "__main__":
    lock_file = None
    try:
        lock_file = open('/tmp/sh-combined.lock', 'w')
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        print("❌ البوت يعمل بالفعل!")
        sys.exit(1)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("🛑 تم إيقاف لوحة التحكم")
    finally:
        if lock_file:
            fcntl.flock(lock_file, fcntl.LOCK_UN)
            lock_file.close()
