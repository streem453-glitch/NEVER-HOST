import asyncio
import contextlib
import html
import json
import logging
import os
import re
import sys
import time
import uuid
from collections import deque
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramRetryAfter
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    BusinessConnection,
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton,
)
from cryptography.fernet import Fernet

BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
OWNER_ID_RAW = os.environ.get("OWNER_ID", "").strip()
FERNET_KEY = os.environ.get("FERNET_KEY", "").strip()
DATA_DIR = Path(os.environ.get("DATA_DIR", "./data")).resolve()
MAX_UPLOAD_BYTES = 512 * 1024
HISTORY_LINES = 30          # сколько строк показать при подключении консоли
TG_CHUNK_LIMIT = 3500       # лимит символов одного сообщения консоли

if not BOT_TOKEN:
    raise RuntimeError("Set BOT_TOKEN environment variable.")
if not OWNER_ID_RAW.isdigit():
    raise RuntimeError("Set OWNER_ID to your numeric Telegram user ID.")
if not FERNET_KEY:
    raise RuntimeError("Set FERNET_KEY environment variable.")

OWNER_ID = int(OWNER_ID_RAW)
DATA_DIR.mkdir(parents=True, exist_ok=True)
BOTS_DIR = DATA_DIR / "bots"
BOTS_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "manager_data.json"
fernet = Fernet(FERNET_KEY.encode())

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("host-manager")

dp = Dispatcher()
runtime: dict[str, dict[str, Any]] = {}
pending_uploads: dict[int, dict[str, Any]] = {}
logs: dict[str, list[str]] = {}
user_states: dict[int, str] = {}
# bot_id -> множество chat_id, которым нужно слать вывод этого бота
subscribers: dict[str, set[int]] = {}
# очередь вывода: (bot_id, строка). Отправляется сразу, как только появляется.
outbox: asyncio.Queue = asyncio.Queue()
# chat_id -> {"lines": deque, "message_id": int | None}: одно обновляемое сообщение на чат
live_consoles: dict[int, dict[str, Any]] = {}
CONSOLE_FLUSH_SECONDS = 1.0   # Telegram не даёт редактировать чаще примерно раза в секунду
CONSOLE_BODY_LIMIT = 3300     # запас под HTML-разметку, лимит сообщения 4096
bot_ref: Bot | None = None

# ---- Режим секретаря (Telegram Business) ----
SECRETARY_ENABLED = os.environ.get("SECRETARY_ENABLED", "1").strip() == "1"
SECRETARY_TEXT = os.environ.get(
    "SECRETARY_TEXT", "Здравствуйте! Сейчас я недоступен, отвечу как только освобожусь."
).strip()
SECRETARY_COOLDOWN_SECONDS = int(os.environ.get("SECRETARY_COOLDOWN_MIN", "60")) * 60
secretary_last_reply: dict[int, float] = {}
NOTES_PATH = DATA_DIR / "notes.json"
SETTINGS_PATH = DATA_DIR / "settings.json"


def load_settings() -> None:
    global SECRETARY_ENABLED, SECRETARY_TEXT, SECRETARY_COOLDOWN_SECONDS
    if not SETTINGS_PATH.exists():
        return
    try:
        data = json.loads(SETTINGS_PATH.read_text(encoding="utf-8"))
        SECRETARY_ENABLED = bool(data.get("enabled", SECRETARY_ENABLED))
        SECRETARY_TEXT = str(data.get("text", SECRETARY_TEXT))
        SECRETARY_COOLDOWN_SECONDS = int(data.get("cooldown_seconds", SECRETARY_COOLDOWN_SECONDS))
    except Exception:
        log.exception("Could not read settings")


def save_settings() -> None:
    SETTINGS_PATH.write_text(json.dumps({
        "enabled": SECRETARY_ENABLED,
        "text": SECRETARY_TEXT,
        "cooldown_seconds": SECRETARY_COOLDOWN_SECONDS,
    }, ensure_ascii=False, indent=2), encoding="utf-8")


load_settings()
BUSINESS_HELP = (
    "🧰 Команды в чатах Telegram для бизнеса (пиши их в чат, результат придёт тебе в ЛС):\n"
    "/help — этот список\n"
    "/ping — проверка связи\n"
    "/time — текущее время\n"
    "/calc 2+2*3 — калькулятор\n"
    "/remind 15 текст — напоминание через N минут\n"
    "/note текст — сохранить заметку\n"
    "/notes — последние заметки\n"
    "/delnote N — удалить заметку по номеру\n"
    "/who — кто в этом чате\n"
    "/afk on|off — включить/выключить автоответ\n"
    "/setreply текст — поменять текст автоответа\n"
    "/status — состояние ботов хостинга"
)


def is_owner(user_id: int) -> bool:
    return user_id == OWNER_ID


def load_db() -> dict:
    if not DB_PATH.exists():
        return {"bots": {}}
    try:
        return json.loads(DB_PATH.read_text(encoding="utf-8"))
    except Exception:
        log.exception("Could not read manager database")
        return {"bots": {}}


def save_db(db: dict) -> None:
    DB_PATH.write_text(json.dumps(db, ensure_ascii=False, indent=2), encoding="utf-8")


def get_bots() -> dict:
    return load_db().setdefault("bots", {})


def add_log(bot_id: str, line: str) -> None:
    text = f"{time.strftime('%H:%M:%S')} {line}"
    logs.setdefault(bot_id, []).append(text)
    logs[bot_id] = logs[bot_id][-150:]
    if subscribers.get(bot_id):
        outbox.put_nowait((bot_id, text))


def main_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🟢 Мои боты"), KeyboardButton(text="📤 Загрузить .py")],
            [KeyboardButton(text="🖥 Консоль"), KeyboardButton(text="📊 Статус хостинга")],
            [KeyboardButton(text="👨‍💻 О моём создателе"), KeyboardButton(text="ℹ️ Помощь")],
        ],
        resize_keyboard=True,
        input_field_placeholder="Выбери действие…",
    )


def bot_actions_keyboard(bot_id: str, running: bool) -> InlineKeyboardMarkup:
    toggle = InlineKeyboardButton(
        text=("🔴 Остановить" if running else "🟢 Запустить"),
        callback_data=f"toggle:{bot_id}",
    )
    return InlineKeyboardMarkup(inline_keyboard=[
        [toggle, InlineKeyboardButton(text="📟 Консоль", callback_data=f"console:{bot_id}")],
        [InlineKeyboardButton(text="🗑 Удалить", callback_data=f"delete:{bot_id}")],
    ])


def console_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="⏸ Отключить консоль", callback_data="unsub_all"),
    ]])


def render_console(lines) -> str:
    """Последние строки, которые поместятся в одно сообщение."""
    parts: list[str] = []
    total = 0
    for line in reversed(lines):
        escaped = html.escape(line[:300])
        if total + len(escaped) + 1 > CONSOLE_BODY_LIMIT:
            break
        parts.append(escaped)
        total += len(escaped) + 1
    body = "\n".join(reversed(parts)) or "(вывода пока нет)"
    return f"📟 <b>Консоль</b>\n<pre>{body}</pre>"


async def flush_live_console(chat_id: int, live: dict[str, Any]) -> None:
    if bot_ref is None:
        return
    text = render_console(live["lines"])
    try:
        if live["message_id"] is None:
            msg = await bot_ref.send_message(
                chat_id, text, parse_mode=ParseMode.HTML,
                disable_notification=True, reply_markup=console_keyboard(),
            )
            live["message_id"] = msg.message_id
        else:
            await bot_ref.edit_message_text(
                text, chat_id=chat_id, message_id=live["message_id"],
                parse_mode=ParseMode.HTML, reply_markup=console_keyboard(),
            )
    except TelegramRetryAfter as exc:
        live["dirty"] = True
        await asyncio.sleep(exc.retry_after)
    except Exception as exc:
        if "message is not modified" in str(exc).lower():
            return
        # Сообщение удалили вручную — создадим новое на следующем обновлении.
        log.warning("Console update failed for chat %s: %s", chat_id, exc)
        live["message_id"] = None


async def console_sender() -> None:
    """Собирает вывод и обновляет одно сообщение консоли в каждом чате раз в секунду."""
    while True:
        items = [await outbox.get()]
        while not outbox.empty():
            items.append(outbox.get_nowait())

        bots = get_bots()
        for bot_id, text in items:
            name = bots.get(bot_id, {}).get("name", bot_id)
            tagged = f"[{name}] {text}"
            for chat_id in subscribers.get(bot_id, ()):
                live = live_consoles.setdefault(chat_id, {"lines": deque(maxlen=300), "message_id": None})
                live["lines"].append(tagged)
                live["dirty"] = True

        for chat_id, live in list(live_consoles.items()):
            if live.get("dirty"):
                live["dirty"] = False
                await flush_live_console(chat_id, live)

        await asyncio.sleep(CONSOLE_FLUSH_SECONDS)


def subscribe(chat_id: int, bot_id: str) -> None:
    subscribers.setdefault(bot_id, set()).add(chat_id)


def auto_subscribe_owner(bot_id: str) -> None:
    """Владелец получает вывод бота автоматически, без нажатия кнопки."""
    subscribe(OWNER_ID, bot_id)


def unsubscribe(chat_id: int, bot_id: str | None = None) -> None:
    targets = [bot_id] if bot_id else list(subscribers)
    for bid in targets:
        subscribers.get(bid, set()).discard(chat_id)
    if bot_id is None or not any(chat_id in subs for subs in subscribers.values()):
        live_consoles.pop(chat_id, None)


def safe_name(name: str) -> str:
    cleaned = re.sub(r"[^a-zA-Zа-яА-ЯёЁ0-9 _.-]", "", name).strip()
    cleaned = re.sub(r"\s+", "_", cleaned)
    return cleaned[:40].strip("._") or "my_bot"


def bot_status(bot_id: str) -> bool:
    proc = runtime.get(bot_id, {}).get("process")
    return bool(proc and proc.returncode is None)


async def read_stream(stream, bot_id: str, prefix: str) -> None:
    try:
        while True:
            line = await stream.readline()
            if not line:
                break
            decoded = line.decode("utf-8", errors="replace").rstrip()
            add_log(bot_id, f"{prefix}: {decoded}")
            log.info("[%s] %s: %s", bot_id, prefix, decoded)
    except Exception as exc:
        add_log(bot_id, f"LOG-ERROR: {exc}")


async def run_bot(bot_id: str) -> None:
    db = load_db()
    item = db.get("bots", {}).get(bot_id)
    if not item:
        return
    script = BOTS_DIR / f"{bot_id}.py"
    if not script.exists():
        add_log(bot_id, "ERROR: script file is missing")
        return
    if bot_status(bot_id):
        return

    add_log(bot_id, "Starting process…")
    try:
        proc = await asyncio.create_subprocess_exec(
            sys.executable, "-u", str(script),
            cwd=str(BOTS_DIR),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            # Child bots inherit environment variables; do not upload untrusted code.
            env=os.environ.copy(),
        )
        runtime[bot_id] = {"process": proc, "started_at": time.time()}
        auto_subscribe_owner(bot_id)
        add_log(bot_id, f"Started (PID {proc.pid})")
        stdout_task = asyncio.create_task(read_stream(proc.stdout, bot_id, "OUT"))
        stderr_task = asyncio.create_task(read_stream(proc.stderr, bot_id, "ERR"))
        runtime[bot_id]["readers"] = [stdout_task, stderr_task]
        asyncio.create_task(watch_process(bot_id, proc))
    except Exception as exc:
        add_log(bot_id, f"START ERROR: {exc}")
        log.exception("Failed to start bot %s", bot_id)


async def watch_process(bot_id: str, proc: asyncio.subprocess.Process) -> None:
    code = await proc.wait()
    add_log(bot_id, f"Process exited with code {code}")
    # Automatic restart is deliberately not enabled by default to avoid crash loops.


async def stop_bot(bot_id: str) -> None:
    item = runtime.get(bot_id)
    proc = item.get("process") if item else None
    if not proc or proc.returncode is not None:
        add_log(bot_id, "Already stopped")
        return
    add_log(bot_id, "Stopping process…")
    proc.terminate()
    try:
        await asyncio.wait_for(proc.wait(), timeout=8)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
    add_log(bot_id, "Stopped")


async def owner_only(message: Message) -> bool:
    if message.from_user and is_owner(message.from_user.id):
        return True
    await message.answer("⛔ Эта функция доступна только владельцу хостинга.")
    return False


@dp.message(CommandStart())
async def start(message: Message):
    if not message.from_user or not await owner_only(message):
        return
    await message.answer(
        "✨ <b>NEVER HOST</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "Привет! Здесь можно загружать и запускать Python-ботов.\n"
        "Выбирай цветные категории кнопок ниже.",
        reply_markup=main_menu(),
        parse_mode=ParseMode.HTML,
    )


@dp.message(F.text == "ℹ️ Помощь")
async def help_cmd(message: Message):
    if not message.from_user or not await owner_only(message):
        return
    await message.answer(
        "ℹ️ <b>Помощь</b>\n"
        "🟢 Мои боты — список и управление.\n"
        "📤 Загрузить .py — отправь Python-файл, затем задай название.\n"
        "🖥 Консоль — вывод ботов обновляется в одном сообщении примерно раз в секунду.\n"
        "📊 Статус хостинга — состояние процессов.\n\n"
        + COMMANDS_HELP,
        parse_mode=ParseMode.HTML,
    )


@dp.message(Command("stop_console"))
async def stop_console_cmd(message: Message):
    if not message.from_user or not await owner_only(message):
        return
    unsubscribe(message.chat.id)
    await message.answer("⏸ Все консоли в этом чате отключены.")


@dp.message(F.text == "👨‍💻 О моём создателе")
async def creator(message: Message):
    if not message.from_user or not await owner_only(message):
        return
    await message.answer(
        "👨‍💻 <b>Мой создатель</b>\n\n"
        "Это 𝖓𝖊𝖛𝖊𝖗\n"
        "Telegram: @F1uxfg",
        parse_mode=ParseMode.HTML,
        reply_markup=main_menu(),
    )


@dp.message(F.text == "📤 Загрузить .py")
async def upload_hint(message: Message):
    if not message.from_user or not await owner_only(message):
        return
    user_states[message.from_user.id] = "upload"
    await message.answer("📤 Отправь файл бота с расширением <code>.py</code> как документ.", parse_mode=ParseMode.HTML)


@dp.message(F.document)
async def receive_file(message: Message, bot: Bot):
    if not message.from_user or not await owner_only(message):
        return
    doc = message.document
    if not doc or not doc.file_name or not doc.file_name.lower().endswith(".py"):
        await message.answer("❌ Нужен файл с расширением .py.")
        return
    if doc.file_size and doc.file_size > MAX_UPLOAD_BYTES:
        await message.answer("❌ Файл слишком большой. Максимум 512 КБ.")
        return
    try:
        file = await bot.get_file(doc.file_id)
        buffer = BytesIO()
        await bot.download_file(file.file_path, destination=buffer)
        content = buffer.getvalue()
        if len(content) > MAX_UPLOAD_BYTES:
            await message.answer("❌ Файл превышает лимит 512 КБ.")
            return
        # Reject obvious non-text Python files.
        content.decode("utf-8")
    except UnicodeDecodeError:
        await message.answer("❌ Не удалось прочитать файл как UTF-8 текст.")
        return
    except Exception:
        log.exception("Upload failed")
        await message.answer("❌ Не удалось скачать файл из Telegram. Попробуй ещё раз.")
        return

    upload_id = uuid.uuid4().hex[:10]
    temp_path = BOTS_DIR / f"pending_{upload_id}.py"
    temp_path.write_bytes(content)
    pending_uploads[message.from_user.id] = {
        "temp_path": str(temp_path),
        "original_name": doc.file_name,
    }
    user_states[message.from_user.id] = "name"
    await message.answer(
        f"📄 Файл <b>{html.escape(doc.file_name)}</b> получен.\n"
        "Какое название дать этому боту на хостинге?\n"
        "Отправь название сообщением (до 40 символов).",
        parse_mode=ParseMode.HTML,
    )


@dp.message(F.text)
async def text_router(message: Message, bot: Bot):
    if not message.from_user or not await owner_only(message):
        return
    uid = message.from_user.id
    text = (message.text or "").strip()

    if text.startswith("/"):
        reply = await execute_command(text, message.chat, bot)
        if reply is None:
            await message.answer("❓ Нет такой команды. Напиши /help")
        else:
            await message.answer(reply, parse_mode=ParseMode.HTML)
        return

    if text == "🟢 Мои боты":
        bots = get_bots()
        if not bots:
            await message.answer("Пока ботов нет. Нажми «📤 Загрузить .py».")
            return
        for bot_id, item in bots.items():
            running = bot_status(bot_id)
            await message.answer(
                f"{'🟢' if running else '🔴'} <b>{html.escape(item['name'])}</b>\n"
                f"Файл: <code>{html.escape(item['filename'])}</code>\n"
                f"Статус: {'работает' if running else 'остановлен'}",
                parse_mode=ParseMode.HTML,
                reply_markup=bot_actions_keyboard(bot_id, running),
            )
        return

    if text == "🖥 Консоль":
        bots = get_bots()
        if not bots:
            await message.answer("Сначала загрузи .py-бота.")
            return
        await message.answer("Выбери бота, чтобы смотреть его вывод:", reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text=f"📟 {item['name'][:30]}", callback_data=f"console:{bot_id}")]
                for bot_id, item in bots.items()
            ]
        ))
        return

    if text == "📊 Статус хостинга":
        bots = get_bots()
        running = sum(1 for bot_id in bots if bot_status(bot_id))
        await message.answer(
            f"📊 <b>Статус хостинга</b>\n"
            f"Всего ботов: {len(bots)}\n"
            f"Работают: {running}\n"
            f"Остановлены: {len(bots) - running}\n"
            f"Папка данных: <code>{html.escape(str(DATA_DIR))}</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    if uid in pending_uploads and user_states.get(uid) == "name":
        name = safe_name(text)
        if not name or len(name) > 40:
            await message.answer("Введи название длиной до 40 символов.")
            return
        upload = pending_uploads.pop(uid)
        bot_id = uuid.uuid4().hex[:10]
        target = BOTS_DIR / f"{bot_id}.py"
        os.replace(upload["temp_path"], target)
        db = load_db()
        db.setdefault("bots", {})[bot_id] = {
            "name": name,
            "filename": upload["original_name"],
            "added_at": time.time(),
        }
        save_db(db)
        auto_subscribe_owner(bot_id)
        user_states.pop(uid, None)
        await message.answer(
            f"✅ Бот <b>{html.escape(name)}</b> добавлен на хостинг.\n"
            "Запустить его можно в разделе «🟢 Мои боты».",
            parse_mode=ParseMode.HTML,
            reply_markup=main_menu(),
        )
        return

    await message.answer("Выбери действие с помощью кнопок меню.", reply_markup=main_menu())


@dp.callback_query(F.data.startswith("toggle:"))
async def toggle_callback(callback: CallbackQuery):
    if not callback.from_user or not is_owner(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    bot_id = callback.data.split(":", 1)[1]
    item = get_bots().get(bot_id)
    if not item:
        await callback.answer("Бот не найден", show_alert=True)
        return
    if bot_status(bot_id):
        await stop_bot(bot_id)
        await callback.answer("Бот остановлен")
    else:
        await run_bot(bot_id)
        await callback.answer("Команда запуска отправлена")
    running = bot_status(bot_id)
    with contextlib.suppress(Exception):
        await callback.message.edit_reply_markup(reply_markup=bot_actions_keyboard(bot_id, running))


@dp.callback_query(F.data.startswith("console:"))
async def console_callback(callback: CallbackQuery):
    if not callback.from_user or not is_owner(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    bot_id = callback.data.split(":", 1)[1]
    item = get_bots().get(bot_id)
    if not item:
        await callback.answer("Бот не найден", show_alert=True)
        return
    chat_id = callback.message.chat.id
    subscribe(chat_id, bot_id)
    history = logs.get(bot_id, [])[-HISTORY_LINES:]
    # Начинаем новое живое сообщение с историей этого бота.
    live_consoles[chat_id] = {
        "lines": deque([f"[{item['name']}] {line}" for line in history], maxlen=300),
        "message_id": None,
        "dirty": True,
    }
    await callback.answer("📟 Консоль открыта")


@dp.callback_query(F.data == "unsub_all")
async def unsub_callback(callback: CallbackQuery):
    if not callback.from_user or not is_owner(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    unsubscribe(callback.message.chat.id)
    with contextlib.suppress(Exception):
        await callback.message.edit_text("⏸ Консоль отключена. Чтобы открыть снова, нажми 🖥 Консоль.")
    await callback.answer("Консоль отключена")


@dp.callback_query(F.data.startswith("delete:"))
async def delete_callback(callback: CallbackQuery):
    if not callback.from_user or not is_owner(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    bot_id = callback.data.split(":", 1)[1]
    item = get_bots().get(bot_id)
    if not item:
        await callback.answer("Бот не найден", show_alert=True)
        return
    await stop_bot(bot_id)
    subscribers.pop(bot_id, None)
    script = BOTS_DIR / f"{bot_id}.py"
    with contextlib.suppress(FileNotFoundError):
        script.unlink()
    db = load_db()
    db.get("bots", {}).pop(bot_id, None)
    save_db(db)
    await callback.message.edit_text(f"🗑 Бот «{html.escape(item['name'])}» удалён.")
    await callback.answer("Удалено")


@dp.business_connection()
async def business_connection_changed(connection: "BusinessConnection"):
    state = "подключён" if connection.is_enabled else "отключён"
    log.info("Business connection %s %s (user %s)", connection.id, state, connection.user.id)


def safe_calc(expr: str) -> float:
    """Вычисляет арифметику без eval: только числа и + - * / ** % ()."""
    import ast
    import operator as op
    ops = {ast.Add: op.add, ast.Sub: op.sub, ast.Mult: op.mul, ast.Div: op.truediv,
           ast.Pow: op.pow, ast.Mod: op.mod}
    unary = {ast.USub: op.neg, ast.UAdd: op.pos}

    def ev(node):
        if isinstance(node, ast.Expression):
            return ev(node.body)
        if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)) and not isinstance(node.value, bool):
            return node.value
        if isinstance(node, ast.BinOp) and type(node.op) in ops:
            left, right = ev(node.left), ev(node.right)
            if isinstance(node.op, ast.Pow) and abs(right) > 1000:
                raise ValueError("степень слишком большая")
            return ops[type(node.op)](left, right)
        if isinstance(node, ast.UnaryOp) and type(node.op) in unary:
            return unary[type(node.op)](ev(node.operand))
        raise ValueError("недопустимое выражение")

    return ev(ast.parse(expr, mode="eval"))


def load_notes() -> list[dict]:
    if not NOTES_PATH.exists():
        return []
    try:
        return json.loads(NOTES_PATH.read_text(encoding="utf-8"))
    except Exception:
        return []


def save_notes(notes: list[dict]) -> None:
    NOTES_PATH.write_text(json.dumps(notes, ensure_ascii=False, indent=2), encoding="utf-8")


async def remind_later(bot: Bot, minutes: int, text: str) -> None:
    await asyncio.sleep(minutes * 60)
    with contextlib.suppress(Exception):
        await bot.send_message(OWNER_ID, f"⏰ <b>Напоминание</b>\n{html.escape(text)}")


COMMANDS_HELP = (
    "🧰 <b>Команды</b> (работают и в личке с ботом, и в чатах Telegram для бизнеса):\n"
    "/help — этот список\n"
    "/settings — текущие настройки автоответа\n"
    "/afk on|off — включить или выключить автоответ\n"
    "/setreply текст — текст автоответа\n"
    "/cooldown N — не отвечать клиенту чаще, чем раз в N минут\n"
    "/ping — проверка связи\n"
    "/time — текущее время\n"
    "/calc 2+2*3 — калькулятор\n"
    "/remind 15 текст — напоминание через N минут\n"
    "/note текст · /notes · /delnote N — заметки\n"
    "/who — информация о текущем чате\n"
    "/status — состояние ботов хостинга"
)


async def execute_command(text: str, chat, bot: Bot) -> str | None:
    """Выполняет команду и возвращает HTML-ответ. None — команда неизвестна."""
    global SECRETARY_ENABLED, SECRETARY_TEXT, SECRETARY_COOLDOWN_SECONDS
    cmd, _, arg = text.partition(" ")
    cmd = cmd.lower().split("@")[0]
    arg = arg.strip()
    chat_name = html.escape(chat.full_name or chat.title or str(chat.id))

    if cmd in ("/help", "/start"):
        return COMMANDS_HELP
    if cmd == "/settings":
        return (
            "⚙️ <b>Настройки автоответа</b>\n"
            f"Статус: {'🟢 включён' if SECRETARY_ENABLED else '🔴 выключен'}\n"
            f"Текст: {html.escape(SECRETARY_TEXT)}\n"
            f"Кулдаун: {SECRETARY_COOLDOWN_SECONDS // 60} мин."
        )
    if cmd == "/afk":
        if arg.lower() in ("on", "вкл", "1"):
            SECRETARY_ENABLED = True
        elif arg.lower() in ("off", "выкл", "0"):
            SECRETARY_ENABLED = False
        else:
            SECRETARY_ENABLED = not SECRETARY_ENABLED
        save_settings()
        return "🟢 Автоответ включён" if SECRETARY_ENABLED else "🔴 Автоответ выключен"
    if cmd == "/setreply":
        if not arg:
            return "Пример: <code>/setreply Сейчас занят, отвечу позже</code>"
        SECRETARY_TEXT = arg
        save_settings()
        return f"✏️ Новый текст автоответа: {html.escape(arg)}"
    if cmd == "/cooldown":
        if not arg.isdigit() or not (0 <= int(arg) <= 1440):
            return "Пример: <code>/cooldown 60</code> (минуты, от 0 до 1440)"
        SECRETARY_COOLDOWN_SECONDS = int(arg) * 60
        save_settings()
        return f"⏱ Кулдаун: {int(arg)} мин."
    if cmd == "/ping":
        return "🏓 На связи"
    if cmd == "/time":
        return datetime.now().strftime("🕒 %d.%m.%Y %H:%M:%S")
    if cmd == "/calc":
        if not arg:
            return "Пример: <code>/calc (12+8)*3</code>"
        try:
            value = safe_calc(arg)
            return f"🧮 <code>{html.escape(arg)}</code> = <b>{html.escape(str(value))}</b>"
        except (ValueError, SyntaxError, ZeroDivisionError, TypeError, OverflowError):
            return "❌ Не могу посчитать. Допустимы числа и + - * / ** % ()"
    if cmd == "/remind":
        parts = arg.split(maxsplit=1)
        if len(parts) < 2 or not parts[0].isdigit() or not (1 <= int(parts[0]) <= 10080):
            return "Пример: <code>/remind 15 позвонить клиенту</code> (от 1 до 10080 минут)"
        minutes, reminder = int(parts[0]), parts[1]
        asyncio.create_task(remind_later(bot, minutes, reminder))
        return f"⏰ Напомню через {minutes} мин.: {html.escape(reminder)}"
    if cmd == "/note":
        if not arg:
            return "Пример: <code>/note текст заметки</code>"
        notes = load_notes()
        notes.append({"text": arg, "chat": chat_name, "at": datetime.now().strftime("%d.%m %H:%M")})
        save_notes(notes)
        return f"📝 Заметка #{len(notes)} сохранена"
    if cmd == "/notes":
        notes = load_notes()
        if not notes:
            return "Заметок пока нет."
        start_no = max(1, len(notes) - 19)
        lines = [f"#{i} · {n['at']} · {html.escape(n['chat'])}\n{html.escape(n['text'])}"
                 for i, n in enumerate(notes[-20:], start=start_no)]
        return "\n\n".join(lines)
    if cmd == "/delnote":
        notes = load_notes()
        if not arg.isdigit() or not (1 <= int(arg) <= len(notes)):
            return "Пример: <code>/delnote 3</code> (номер из /notes)"
        removed = notes.pop(int(arg) - 1)
        save_notes(notes)
        return f"🗑 Удалена заметка: {html.escape(removed['text'][:60])}"
    if cmd == "/who":
        return (f"👤 {chat_name}\n"
                f"ID чата: <code>{chat.id}</code>\n"
                f"Username: @{html.escape(chat.username or '—')}")
    if cmd == "/status":
        bots = get_bots()
        running = sum(1 for bid in bots if bot_status(bid))
        return f"📊 Ботов: {len(bots)}, работают: {running}"
    return None


async def handle_business_command(message: Message, bot: Bot) -> None:
    """Команда, которую ты пишешь в чате с клиентом. Ответ приходит тебе в личку."""
    reply = await execute_command((message.text or "").strip(), message.chat, bot)
    if reply is None:
        return  # неизвестная команда в клиентском чате — молча игнорируем
    chat_name = html.escape(message.chat.full_name or message.chat.title or str(message.chat.id))
    cmd = (message.text or "").split()[0]
    await bot.send_message(OWNER_ID, f"⚙️ <b>{html.escape(cmd)}</b> · чат с {chat_name}\n{reply}")


@dp.business_message()
async def secretary_reply(message: Message, bot: Bot):
    """Команды владельца в бизнес-чатах и автоответ секретаря для клиентов."""
    if message.from_user is None or message.from_user.is_bot:
        return
    text = (message.text or "").strip()
    if message.from_user.id == OWNER_ID:
        if text.startswith("/"):
            await handle_business_command(message, bot)
        return
    if not SECRETARY_ENABLED or not SECRETARY_TEXT:
        return
    now = time.time()
    chat_id = message.chat.id
    if now - secretary_last_reply.get(chat_id, 0) < SECRETARY_COOLDOWN_SECONDS:
        return
    secretary_last_reply[chat_id] = now
    try:
        await bot.send_message(
            chat_id=chat_id,
            text=SECRETARY_TEXT,
            business_connection_id=message.business_connection_id,
        )
    except Exception:
        log.exception("Secretary auto-reply failed")


async def main():
    global bot_ref
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    bot_ref = bot
    for bot_id in get_bots():
        auto_subscribe_owner(bot_id)
    sender = asyncio.create_task(console_sender())
    try:
        await dp.start_polling(bot)
    finally:
        sender.cancel()
        for bot_id in list(runtime):
            await stop_bot(bot_id)
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
