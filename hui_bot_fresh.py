# ===================== hui_bot_fresh.py =====================
# Telegram Hui Bot (SQLite version)
# Dependencies: python-telegram-bot==20.3, pandas
import os, sqlite3, json, asyncio, random, re, unicodedata
from datetime import datetime, timedelta, time as dtime, date
from typing import Optional, Tuple, Dict

import pandas as pd
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from telegram.ext import (
    ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, CallbackQueryHandler, filters
)

# ========= CONFIG =========
TOKEN = (os.getenv("TELEGRAM_TOKEN") or os.getenv("BOT_TOKEN") or "").strip()
if not TOKEN:
    raise SystemExit("Missing TELEGRAM_TOKEN/BOT_TOKEN in environment variables")

DB_FILE = "hui.db"
CONFIG_FILE = "config.json"

REPORT_HOUR = 8                 # 08:00 gửi báo cáo tháng (chỉ mùng 1)
REMINDER_TICK_SECONDS = 60      # vòng lặp check nhắc hẹn

# ====== DATE HELPERS ======
ISO_FMT = "%Y-%m-%d"   # lưu DB

def strip_accents(s: str) -> str:
    return ''.join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))

def parse_iso(s: str) -> datetime:
    return datetime.strptime(s, ISO_FMT)

def _smart_parse_dmy(s: str) -> Tuple[int,int,int]:
    """Nhận '2-8-25', '2/8/25', '02-08-2025', ... → (d,m,y) với y 4 chữ số."""
    s = s.strip().replace("/", "-")
    parts = s.split("-")
    if len(parts) != 3:
        raise ValueError(f"Không hiểu ngày: {s}")
    d, m, y = parts
    d, m, y = int(d), int(m), int(y)
    if y < 100:  # 2-digit year → 2000+
        y += 2000
    datetime(y, m, d)  # validate
    return d, m, y

def parse_user_date(s: str) -> datetime:
    d, m, y = _smart_parse_dmy(s)
    return datetime(y, m, d)

def to_iso_str(d: datetime) -> str:
    return d.strftime(ISO_FMT)

def to_user_str(d: datetime) -> str:
    return d.strftime("%d-%m-%Y")

# ----- MONEY PARSER -----
def parse_money(text: str) -> int:
    """
    '1tr'/'1000k'/'1000n' -> 1_000_000; '1k'/'1n' -> 1_000; '100k'/'100n' -> 100_000; hỗ trợ số thập phân.
    Hỗ trợ 'm'/'t' ~ triệu. Cho phép viết có dấu chấm, phẩy.
    """
    s = str(text).strip().lower()
    s = s.replace(",", "").replace("_", "").replace(" ", "").replace(".", "")
    if s.isdigit():
        return int(s)
    try:
        if s.endswith("tr"):  # triệu
            num = float(s[:-2]); return int(num * 1_000_000)
        if s.endswith(("k","n")):  # nghìn
            num = float(s[:-1]);   return int(num * 1_000)
        if s.endswith(("m","t")):  # triệu
            num = float(s[:-1]);   return int(num * 1_000_000)
        return int(float(s))
    except Exception:
        raise ValueError(f"Không hiểu giá trị tiền: {text}")

# ---------- DB ----------
def db():
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def init_db():
    conn = db(); c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS lines(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        period_days INTEGER NOT NULL,
        start_date TEXT NOT NULL,
        legs INTEGER NOT NULL,
        contrib INTEGER NOT NULL,                 -- mệnh giá M
        bid_type TEXT DEFAULT 'dynamic',
        bid_value REAL DEFAULT 0,
        status TEXT DEFAULT 'OPEN',
        created_at TEXT NOT NULL,
        base_rate REAL DEFAULT 0,                 -- % sàn trên M
        cap_rate  REAL DEFAULT 100,               -- % trần trên M
        thau_rate REAL DEFAULT 0,                 -- % đầu thảo trên M (trừ cố định mỗi kỳ)
        remind_hour INTEGER DEFAULT 8,            -- giờ nhắc (0..23)
        remind_min  INTEGER DEFAULT 0,            -- phút nhắc (0..59)
        last_remind_iso TEXT                      -- YYYY-MM-DD (chống gửi trùng)
    )""")
    c.execute("""
    CREATE TABLE IF NOT EXISTS payments(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        line_id INTEGER NOT NULL,
        pay_date TEXT NOT NULL,
        amount INTEGER NOT NULL,
        FOREIGN KEY(line_id) REFERENCES lines(id) ON DELETE CASCADE
    )""")
    c.execute("""
    CREATE TABLE IF NOT EXISTS rounds(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        line_id INTEGER NOT NULL,
        k INTEGER NOT NULL,           -- kỳ (1..legs)
        bid INTEGER NOT NULL,         -- số tiền thăm thực tế T_k (VND)
        round_date TEXT,              -- ngày kỳ (tuỳ chọn)
        UNIQUE(line_id, k),
        FOREIGN KEY(line_id) REFERENCES lines(id) ON DELETE CASCADE
    )""")
    conn.commit(); conn.close()

def ensure_schema():
    conn = db(); cur = conn.cursor()
    for col, decl in [
        ("base_rate", "REAL DEFAULT 0"),
        ("cap_rate",  "REAL DEFAULT 100"),
        ("thau_rate", "REAL DEFAULT 0"),
        ("remind_hour", "INTEGER DEFAULT 8"),
        ("remind_min",  "INTEGER DEFAULT 0"),
        ("last_remind_iso", "TEXT")
    ]:
        try: cur.execute(f"ALTER TABLE lines ADD COLUMN {col} {decl}")
        except Exception: pass
    conn.commit(); conn.close()

def load_cfg():
    if os.path.exists(CONFIG_FILE):
        try: return json.load(open(CONFIG_FILE, "r", encoding="utf-8"))
        except Exception: return {}
    return {}

def save_cfg(cfg: dict):
    json.dump(cfg, open(CONFIG_FILE, "w", encoding="utf-8"))

# ---------- Helpers & Tính toán ----------
def k_date(line, k: int) -> datetime:
    return parse_iso(line["start_date"]) + timedelta(days=(k-1)*int(line["period_days"]))

def roi_to_str(r: float) -> str:
    return f"{r*100:.2f}%"

def get_bids(line_id: int):
    conn = db()
    rows = conn.execute("SELECT k, bid FROM rounds WHERE line_id=? ORDER BY k", (line_id,)).fetchall()
    conn.close()
    return {int(k): int(bid) for (k, bid) in rows}

def payout_at_k(line, bids: dict, k: int) -> int:
    """
    Payout_k = (k-1)*M + (N - k)*(M - T_k) - D
    """
    M, N = int(line["contrib"]), int(line["legs"])
    T_k = int(bids.get(k, 0))
    D   = int(round(M * float(line.get("thau_rate", 0)) / 100.0))
    return (k-1)*M + (N - k)*(M - T_k) - D

def paid_so_far_if_win_at_k(bids: dict, M: int, k: int) -> int:
    s = 0
    for j in range(1, k):
        s += (M - int(bids.get(j, 0)))
    return s

def compute_profit_var(line, k: int, bids: dict):
    M = int(line["contrib"])
    po = payout_at_k(line, bids, k)
    paid = paid_so_far_if_win_at_k(bids, M, k)
    base = paid if paid > 0 else M
    profit = po - paid
    roi = profit / base if base else 0.0
    return profit, roi, po, paid

def best_k_var(line, bids: dict, metric="roi"):
    bestk, bestkey, bestinfo = 1, -1e18, None
    for k in range(1, int(line["legs"]) + 1):
        p, r, po, paid = compute_profit_var(line, k, bids)
        key = r if metric == "roi" else p
        if key > bestkey:
            bestk, bestkey, bestinfo = k, key, (p, r, po, paid)
    return bestk, bestinfo

def is_finished(line) -> bool:
    if line["status"] == "CLOSED":
        return True
    last = k_date(line, int(line["legs"])).date()
    return datetime.now().date() >= last

# ---------- DB helpers ----------
def load_line_full(line_id: int):
    conn = db()
    row = conn.execute("SELECT * FROM lines WHERE id=?", (line_id,)).fetchone()
    if not row:
        conn.close(); return None, pd.DataFrame()
    cols = ["id","name","period_days","start_date","legs","contrib",
            "bid_type","bid_value","status","created_at",
            "base_rate","cap_rate","thau_rate","remind_hour","remind_min","last_remind_iso"]
    line = dict(zip(cols, row))
    pays = pd.read_sql_query(
        "SELECT pay_date, amount FROM payments WHERE line_id=? ORDER BY pay_date",
        conn, params=(line_id,)
    )
    conn.close()
    return line, pays

# ============= HELP TEXT (không dùng backtick cho dòng lệnh) =============
def help_text() -> str:
    return (
        "👋 **HỤI BOT – phiên bản SQLite (không cần Google Sheets)**\n\n"
        "🌟 **LỆNH CHÍNH** (không dấu, ngày **DD-MM-YYYY**):\n\n"
        "1) Tạo dây (đủ tham số):\n"
        "   /tao <tên> <tuần|tháng> <DD-MM-YYYY> <số_chân> <mệnh_giá> <giá_sàn_%> <giá_trần_%> <đầu_thảo_%>\n"
        "   Ví dụ: `\n"
        "   /tao Hui10tr tuần 10-10-2025 12 10tr 8 20 50\n"
        "   `\n"
        "   💡 Thiếu tham số? Gõ **/tao** trống, bot sẽ gửi **một biểu mẫu** để bạn điền 1 lần là xong.\n\n"
        "2) Nhập thăm kỳ:\n"
        "   /tham <mã_dây> <kỳ> <số_tiền_thăm> [DD-MM-YYYY]\n"
        "   Ví dụ: `\n"
        "   /tham 1 1 2tr 10-10-2025\n"
        "   `\n\n"
        "3) Đặt giờ nhắc riêng:\n"
        "   /hen <mã_dây> <HH:MM>  (ví dụ: /hen 1 07:45)\n\n"
        "4) Danh sách / Tóm tắt / Gợi ý hốt:\n"
        "   /danhsach\n"
        "   /tomtat <mã_dây>\n"
        "   /hottot <mã_dây> [Roi%|Lãi]\n\n"
        "5) Đóng dây: /dong <mã_dây>\n\n"
        "6) Cài nơi nhận báo cáo & nhắc (gửi vào chat hiện tại nếu không nhập):\n"
        "   /baocao [chat_id]\n\n"
        "📜 Gõ /lenh bất cứ lúc nào để hiện lại danh sách lệnh."
    )

# --------- SESSIONS cho wizard ---------
SESS: Dict[int, dict] = {}

def start_session(chat_id: int, mode: str, expect_keys: list, cmd: str):
    SESS[chat_id] = {"mode": mode, "expect": expect_keys, "data": {}, "cmd": cmd}

def end_session(chat_id: int):
    if chat_id in SESS: del SESS[chat_id]

def parse_pack_reply(text: str, expect_keys: list) -> dict:
    """
    Một tin nhắn:
      - nhiều dòng: theo đúng thứ tự expect_keys
      - hoặc một dòng ngăn cách bởi | hoặc ;
      - hoặc key=value
    """
    res = {}
    s = (text or "").strip()
    # key=value
    if "=" in s:
        for part in re.split(r"[|\n;]+", s):
            part = part.strip()
            if "=" in part:
                k, v = part.split("=", 1)
                k = k.strip().lower(); v = v.strip()
                alias = {
                    "ten":"ten",
                    "chu_ky":"chu_ky","tuan":"chu_ky","thang":"chu_ky",
                    "ngay":"ngay",
                    "sochan":"sochan","chan":"sochan",
                    "menhgia":"menhgia",
                    "san":"san","tran":"tran","thau":"thau",
                    "maday":"maday","ky":"ky","sotientham":"sotientham","sotien":"sotientham",
                    "gio":"gio"
                }
                key = alias.get(k, k)
                if key in expect_keys:
                    res[key] = v
        return res
    # theo dòng hoặc |
    parts = [p for p in re.split(r"[|\n;]+", s) if p.strip() != ""]
    for i, key in enumerate(expect_keys):
        if i < len(parts):
            res[key] = parts[i].strip()
    return res

# ---------- Helpers UI ----------
def list_text() -> str:
    conn = db()
    rows = conn.execute(
        "SELECT id,name,period_days,start_date,legs,contrib,base_rate,cap_rate,thau_rate,status,remind_hour,remind_min "
        "FROM lines ORDER BY id DESC"
    ).fetchall()
    conn.close()
    if not rows:
        return "📂 Chưa có dây nào."
    out = ["📋 **Danh sách dây**:"]
    for r in rows:
        kind = "Tuần" if r[2]==7 else "Tháng"
        out.append(
            f"• #{r[0]} · {r[1]} · {kind} · mở {to_user_str(parse_iso(r[3]))} · chân {r[4]} · M {int(r[5]):,} VND · "
            f"sàn {float(r[6]):.2f}% · trần {float(r[7]):.2f}% · thầu {float(r[8]):.2f}% · nhắc {int(r[10]):02d}:{int(r[11]):02d} · {r[9]}"
        )
    return "\n".join(out)

def tao_wizard_text() -> str:
    return (
        "🧩 **Điền nhanh tạo dây** – trả lời **một tin** theo thứ tự (mỗi dòng hoặc `|`):\n"
        "1) Tên dây\n2) Chu kỳ: `tuan`/`thang`\n3) Ngày mở DD-MM-YYYY\n4) Số chân\n"
        "5) Mệnh giá (vd: 10tr, 2500k)\n6) Sàn %\n7) Trần %\n8) Đầu thảo %\n\n"
        "VD:\n`Hui10tr | tuan | 10-10-2025 | 12 | 10tr | 8 | 20 | 50`\n"
        "🚫 Thoát wizard: /huy"
    )

def tham_wizard_text() -> str:
    return (
        "🧩 **Nhập thăm nhanh** – trả lời **một tin** theo thứ tự (mỗi dòng hoặc `|`):\n"
        "1) Mã dây (vd: 1)\n2) Kỳ (vd: 3)\n3) Số tiền thăm (vd: 2tr, 750k, ...)\n4) Ngày DD-MM-YYYY (trống = hôm nay)\n\n"
        "Ví dụ: `1 | 3 | 2tr | 10-10-2025`\n"
        "🚫 Thoát: /huy"
    )

# ---------- COMMANDS ----------
async def cmd_lenh(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    kb = [
        [InlineKeyboardButton("🧩 Tạo dây (wizard)", callback_data="wiz:tao")],
        [InlineKeyboardButton("💰 Nhập thăm (wizard)", callback_data="wiz:tham")],
        [InlineKeyboardButton("⏰ Đặt giờ nhắc (wizard)", callback_data="wiz:hen")],
        [InlineKeyboardButton("📋 Danh sách dây", callback_data="show:danhsach")],
        [InlineKeyboardButton("📊 Tóm tắt dây", callback_data="ask:tomtat")],
        [InlineKeyboardButton("💡 Gợi ý hốt (Roi%|Lãi)", callback_data="ask:hottot")],
    ]
    await upd.message.reply_text(
        help_text(),
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(kb),
        disable_web_page_preview=True
    )

async def on_menu_callback(cbq: CallbackQuery, ctx: ContextTypes.DEFAULT_TYPE):
    data = cbq.data or ""
    chat_id = cbq.message.chat_id
    await cbq.answer()
    if data == "wiz:tao":
        start_session(chat_id, "tao", ["ten","chu_ky","ngay","sochan","menhgia","san","tran","thau"], "/tao")
        await cbq.message.reply_text(tao_wizard_text(), parse_mode="Markdown")
    elif data == "wiz:tham":
        start_session(chat_id, "tham", ["maday","ky","sotientham","ngay"], "/tham")
        await cbq.message.reply_text(tham_wizard_text(), parse_mode="Markdown")
    elif data == "wiz:hen":
        await cbq.message.reply_text("Cú pháp: /hen <mã_dây> <HH:MM>  (VD: /hen 1 07:45)")
    elif data == "show:danhsach":
        await cbq.message.reply_text(list_text(), parse_mode="Markdown")
    elif data == "ask:tomtat":
        await cbq.message.reply_text("Nhập: /tomtat <mã_dây>")
    elif data == "ask:hottot":
        await cbq.message.reply_text("Nhập: /hottot <mã_dây> [Roi%|Lãi]")

async def cmd_start(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await cmd_lenh(upd, ctx)

async def cmd_setreport(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cfg = load_cfg()
    if ctx.args:
        try: cid = int(ctx.args[0])
        except Exception: return await upd.message.reply_text("❌ `chat_id` không hợp lệ.")
    else:
        cid = upd.effective_chat.id
    cfg["report_chat_id"] = cid
    save_cfg(cfg)
    await upd.message.reply_text(f"✅ Đã lưu nơi nhận báo cáo/nhắc: {cid}")

# ----- TẠO DÂY -----
async def cmd_new(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = upd.effective_chat.id
    try:
        if len(ctx.args) >= 8:
            name, kind, start_user, legs, contrib, base_rate, cap_rate, thau_rate = ctx.args[:8]
            await _create_line_and_reply(upd, name, kind, start_user, legs, contrib, base_rate, cap_rate, thau_rate)
            return
        start_session(chat_id, "tao", ["ten","chu_ky","ngay","sochan","menhgia","san","tran","thau"], "/tao")
        await upd.message.reply_text(tao_wizard_text(), parse_mode="Markdown")
    except Exception as e:
        await upd.message.reply_text(f"❌ Lỗi: {e}")

async def _create_line_and_reply(upd: Update, name, kind, start_user, legs, contrib, base_rate, cap_rate, thau_rate):
    kind_l = str(kind).lower()
    period_days = 7 if kind_l in ["tuan","tuần","t","week","weekly"] else 30
    start_dt  = parse_user_date(start_user)
    start_iso = to_iso_str(start_dt)
    legs      = int(legs)
    contrib_i = parse_money(contrib)
    base_rate = float(base_rate); cap_rate = float(cap_rate); thau_rate = float(thau_rate)
    if not (0 <= base_rate <= cap_rate <= 100): raise ValueError("sàn% <= trần% và nằm trong [0..100]")
    if not (0 <= thau_rate <= 100): raise ValueError("đầu thảo% trong [0..100]")

    conn = db()
    conn.execute(
        """INSERT INTO lines(name,period_days,start_date,legs,contrib,
                             bid_type,bid_value,status,created_at,
                             base_rate,cap_rate,thau_rate,remind_hour,remind_min,last_remind_iso)
           VALUES(?,?,?,?,?,'dynamic',0,'OPEN',?, ?, ?, ?, 8, 0, NULL)""",
        (name, period_days, start_iso, legs, contrib_i,
         datetime.now().isoformat(), base_rate, cap_rate, thau_rate)
    )
    conn.commit()
    line_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.close()

    await upd.message.reply_text(
        f"✅ Tạo dây #{line_id} ({name}) — {'Hụi Tuần' if period_days==7 else 'Hụi Tháng'}\n"
        f"• Mở: {to_user_str(start_dt)} · Chân: {legs} · Mệnh giá: {contrib_i:,} VND\n"
        f"• Sàn {base_rate:.2f}% · Trần {cap_rate:.2f}% · Đầu thảo {thau_rate:.2f}% (trên M)\n"
        f"⏰ Nhắc mặc định: 08:00 (đổi bằng /hen {line_id} HH:MM)\n"
        f"➡️ Nhập thăm: /tham {line_id} <kỳ> <số_tiền_thăm> [DD-MM-YYYY]"
    )

# ----- THĂM -----
def _int_like(s: str) -> int:
    """Lấy số nguyên đầu tiên trong chuỗi, chấp nhận '#1', 'k2', '2,' ..."""
    m = re.search(r"-?\d+", s or "")
    if not m: raise ValueError(f"Không phải số: {s}")
    return int(m.group(0))

async def cmd_tham(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = upd.effective_chat.id
    if len(ctx.args) >= 3:
        try:
            line_id = _int_like(ctx.args[0])
            k       = _int_like(ctx.args[1])
            bid     = parse_money(ctx.args[2])
            rdate   = None
            if len(ctx.args) >= 4:
                rdate = to_iso_str(parse_user_date(ctx.args[3]))
        except Exception as e:
            # Sai cú pháp -> mở wizard cho dễ điền
            start_session(chat_id, "tham", ["maday","ky","sotientham","ngay"], "/tham")
            return await upd.message.reply_text(f"⚠️ Tham số chưa đúng ({e}).\n\n" + tham_wizard_text(), parse_mode="Markdown")

        await _save_tham(upd, line_id, k, bid, rdate)
        return

    # thiếu tham số -> wizard
    start_session(chat_id, "tham", ["maday","ky","sotientham","ngay"], "/tham")
    await upd.message.reply_text(tham_wizard_text(), parse_mode="Markdown")

async def _save_tham(upd: Update, line_id: int, k: int, bid: int, rdate_iso: Optional[str]):
    line, _ = load_line_full(line_id)
    if not line: 
        return await upd.message.reply_text("❌ Không tìm thấy dây.")
    if not (1 <= k <= int(line["legs"])):
        return await upd.message.reply_text(f"❌ Kỳ hợp lệ 1..{line['legs']}.")

    M = int(line["contrib"])
    min_bid = int(round(M * float(line.get("base_rate", 0)) / 100.0))
    max_bid = int(round(M * float(line.get("cap_rate", 100)) / 100.0))
    if bid < min_bid or bid > max_bid:
        return await upd.message.reply_text(
            f"❌ Thăm phải trong [{min_bid:,} .. {max_bid:,}] VND "
            f"(Sàn {line['base_rate']}% · Trần {line['cap_rate']}% · M={M:,})"
        )

    conn = db()
    conn.execute("""
        INSERT INTO rounds(line_id,k,bid,round_date) VALUES(?,?,?,?)
        ON CONFLICT(line_id,k) DO UPDATE SET bid=excluded.bid, round_date=excluded.round_date
    """, (line_id, k, bid, rdate_iso))
    conn.commit(); conn.close()

    await upd.message.reply_text(
        f"✅ Lưu thăm kỳ {k} cho dây #{line_id}: {bid:,} VND"
        + (f" · ngày {to_user_str(parse_iso(rdate_iso))}" if rdate_iso else "")
    )

# ----- HẸN -----
async def cmd_set_remind(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if len(ctx.args) != 2:
        return await upd.message.reply_text("❌ Cú pháp: /hen <mã_dây> <HH:MM>  (VD: /hen 1 07:45)")
    try:
        line_id = _int_like(ctx.args[0])
        hh, mm = ctx.args[1].split(":")
        hh = int(hh); mm = int(mm)
        if not (0 <= hh <= 23 and 0 <= mm <= 59):
            raise ValueError("giờ/phút không hợp lệ")
    except Exception as e:
        return await upd.message.reply_text(f"❌ Tham số không hợp lệ: {e}")

    line, _ = load_line_full(line_id)
    if not line:
        return await upd.message.reply_text("❌ Không tìm thấy dây.")
    conn = db()
    conn.execute("UPDATE lines SET remind_hour=?, remind_min=? WHERE id=?", (hh, mm, line_id))
    conn.commit(); conn.close()
    await upd.message.reply_text(f"✅ Đã đặt giờ nhắc cho dây #{line_id}: {hh:02d}:{mm:02d}")

# ----- DANH SÁCH / TÓM TẮT / GỢI Ý / ĐÓNG -----
async def cmd_list(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await upd.message.reply_text(list_text(), parse_mode="Markdown")

async def cmd_summary(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        line_id = _int_like(ctx.args[0])
    except Exception:
        return await upd.message.reply_text("❌ Cú pháp: /tomtat <mã_dây>")

    line, _ = load_line_full(line_id)
    if not line:
        return await upd.message.reply_text("❌ Không tìm thấy dây.")

    bids = get_bids(line_id)
    M, N = int(line["contrib"]), int(line["legs"])
    cfg_line = f"Sàn {float(line.get('base_rate',0)):.2f}% · Trần {float(line.get('cap_rate',100)):.2f}% · Đầu thảo {float(line.get('thau_rate',0)):.2f}% (trên M)"
    k_now = max(1, min(len(bids)+1, N))
    p, r, po, paid = compute_profit_var(line, k_now, bids)
    bestk, (bp, br, bpo, bpaid) = best_k_var(line, bids, metric="roi")

    msg = [
        f"📌 Dây #{line['id']} · {line['name']} · {'Tuần' if line['period_days']==7 else 'Tháng'}",
        f"• Mở: {to_user_str(parse_iso(line['start_date']))} · Chân: {N} · Mệnh giá/kỳ: {M:,} VND",
        f"• {cfg_line} · Nhắc {int(line.get('remind_hour',8)):02d}:{int(line.get('remind_min',0)):02d}",
        f"• Thăm: " + (", ".join([f"k{kk}:{int(b):,}" for kk,b in sorted(bids.items())]) if bids else "(chưa có)"),
        f"• Kỳ hiện tại ước tính: {k_now} · Payout: {po:,} · Đã đóng: {paid:,} → Lãi: {int(round(p)):,} (ROI {roi_to_str(r)})",
        f"⭐ Đề xuất (ROI): kỳ {bestk} · ngày {to_user_str(k_date(line,bestk))} · Payout {bpo:,} · Đã đóng {bpaid:,} · Lãi {int(round(bp)):,} · ROI {roi_to_str(br)}"
    ]
    if is_finished(line):
        msg.append("✅ Dây đã đến hạn — /dong để lưu trữ.")
    await upd.message.reply_text("\n".join(msg))

async def cmd_whenhot(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if len(ctx.args) < 1:
        return await upd.message.reply_text("❌ Cú pháp: /hottot <mã_dây> [Roi%|Lãi]")
    try:
        line_id = _int_like(ctx.args[0])
    except Exception:
        return await upd.message.reply_text("❌ mã_dây phải là số.")
    metric = "roi"
    if len(ctx.args) >= 2:
        raw = ctx.args[1].strip().lower().replace("%", "")
        raw = strip_accents(raw)
        if raw in ("roi", "lai"):
            metric = raw

    line, _ = load_line_full(line_id)
    if not line: 
        return await upd.message.reply_text("❌ Không tìm thấy dây.")
    bids = get_bids(line_id)
    bestk, (bp, br, bpo, bpaid) = best_k_var(line, bids, metric=("roi" if metric=="roi" else "lai"))
    await upd.message.reply_text(
        f"🔎 Gợi ý theo {'ROI%' if metric=='roi' else 'Lãi'}:\n"
        f"• Nên hốt kỳ: {bestk}\n"
        f"• Ngày dự kiến: {to_user_str(k_date(line,bestk))}\n"
        f"• Payout kỳ đó: {bpo:,}\n"
        f"• Đã đóng trước đó: {bpaid:,}\n"
        f"• Lãi ước tính: {int(round(bp)):,} — ROI: {roi_to_str(br)}"
    )

async def cmd_close(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        line_id = _int_like(ctx.args[0])
    except Exception:
        return await upd.message.reply_text("❌ Cú pháp: /dong <mã_dây>")
    conn = db()
    conn.execute("UPDATE lines SET status='CLOSED' WHERE id=?", (line_id,))
    conn.commit(); conn.close()
    await upd.message.reply_text(f"🗂️ Đã đóng & lưu trữ dây #{line_id}.")

# ----- BÁO CÁO THÁNG -----
async def send_monthly_report_bot(app):
    cfg = load_cfg(); chat_id = cfg.get("report_chat_id")
    if not chat_id: return
    today = datetime.now().date()
    if today.day != 1: return

    conn = db()
    rows = conn.execute(
        "SELECT id,name,period_days,start_date,legs,contrib,base_rate,cap_rate,thau_rate,status FROM lines"
    ).fetchall()
    conn.close()
    if not rows:
        return await app.bot.send_message(chat_id=chat_id, text="📊 Báo cáo tháng: chưa có dây.")

    lines = []
    for r in rows:
        line = {
            "id": r[0], "name": r[1], "period_days": r[2], "start_date": r[3],
            "legs": r[4], "contrib": r[5], "base_rate": r[6], "cap_rate": r[7], "thau_rate": r[8],
            "status": r[9]
        }
        bids = get_bids(line["id"])
        k_now = max(1, min(len(bids)+1, int(line["legs"])))
        p, ro, po, paid = compute_profit_var(line, k_now, bids)
        bestk, (bp, br, bpo, bpaid) = best_k_var(line, bids, metric="roi")
        lines.append(
            f"#{line['id']} · {line['name']} · {('Tuần' if line['period_days']==7 else 'Tháng')} · "
            f"M {int(line['contrib']):,} · Sàn {float(line['base_rate']):.1f}% · Trần {float(line['cap_rate']):.1f}% · Thầu {float(line['thau_rate']):.1f}% · "
            f"Kỳ_now {k_now}: Lãi {int(round(p)):,} ({roi_to_str(ro)}) · Best k{bestk} {roi_to_str(br)}"
        )

    txt = "📊 **Báo cáo tháng**:\n" + "\n".join(lines)
    await app.bot.send_message(chat_id=chat_id, text=txt, parse_mode="Markdown")

# ----- NHẮC HẸN -----
async def send_periodic_reminders(app):
    cfg = load_cfg(); chat_id = cfg.get("report_chat_id")
    if not chat_id: return

    today = datetime.now()
    now_d = today.date()
    hh = today.hour; mm = today.minute

    weekly_prompts = [
        "⏰ Tuần này đoán thăm bao nhiêu?",
        "🤔 Bạn nghĩ kỳ này thăm sẽ về mức nào?",
        "💬 Nhắc nhẹ: nhập thăm kỳ này nhé!",
        "🔔 Kỳ mới bắt đầu, dự đoán thăm bao nhiêu?"
    ]
    monthly_prompts = [
        "📅 Tháng này đoán thăm bao nhiêu?",
        "🗓️ Đến hẹn lại lên, thăm kỳ này bao nhiêu đây?",
        "💡 Nhắc nè: nhập thăm kỳ mới nhé!",
        "🔔 Tháng mới bắt đầu, chốt thăm thôi!"
    ]

    conn = db()
    rows = conn.execute(
        "SELECT id,name,period_days,start_date,legs,contrib,base_rate,cap_rate,thau_rate,status,remind_hour,remind_min,last_remind_iso "
        "FROM lines WHERE status='OPEN'"
    ).fetchall()
    conn.close()

    for r in rows:
        (line_id, name, period_days, start_date_str, legs, M, base_rate, cap_rate, thau_rate,
         status, remind_hour, remind_min, last_remind_iso) = r

        if hh != int(remind_hour) or mm != int(remind_min):
            continue
        if last_remind_iso == now_d.isoformat():
            continue

        bids = get_bids(line_id)
        N = int(legs)
        k_now = max(1, min(len(bids) + 1, N))
        open_day = (parse_iso(start_date_str) + timedelta(days=(k_now-1)*int(period_days))).date()

        if open_day != now_d:
            continue

        is_weekly = (int(period_days) == 7)
        prompt = random.choice(weekly_prompts if is_weekly else monthly_prompts)
        min_bid = int(round(int(M) * float(base_rate) / 100.0))
        max_bid = int(round(int(M) * float(cap_rate)  / 100.0))
        D = int(round(int(M) * float(thau_rate) / 100.0))

        txt = (
            f"📣 Nhắc hẹn cho dây #{line_id} – {name}\n"
            f"• Kỳ {k_now}/{N} · Ngày: {to_user_str(parse_iso(start_date_str) + timedelta(days=(k_now-1)*int(period_days)))}\n"
            f"• Mệnh giá: {int(M):,} VND · Sàn {float(base_rate):.1f}% ({min_bid:,}) · Trần {float(cap_rate):.1f}% ({max_bid:,}) · Thầu {float(thau_rate):.1f}% ({D:,})\n\n"
            f"➡️ {prompt}\n"
            f"👉 Nhập: /tham {line_id} {k_now} <số_tiền_thăm>"
        )
        await app.bot.send_message(chat_id=chat_id, text=txt)

        conn2 = db()
        conn2.execute("UPDATE lines SET last_remind_iso=? WHERE id=?", (now_d.isoformat(), line_id))
        conn2.commit(); conn2.close()

# ----- VÒNG LẶP NỀN & KEEPALIVE -----
async def monthly_report_loop(app):
    while True:
        now = datetime.now()
        target = datetime.combine(now.date(), dtime(hour=REPORT_HOUR))
        if now >= target:
            target += timedelta(days=1)
        await asyncio.sleep(max(1.0, (target - now).total_seconds()))
        await send_monthly_report_bot(app)

async def reminder_loop(app):
    while True:
        await send_periodic_reminders(app)
        await asyncio.sleep(REMINDER_TICK_SECONDS)

async def start_keepalive_server():
    port = int(os.getenv("PORT", "10000"))
    async def handle_client(reader, writer):
        try:
            await reader.read(1024)
            resp = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK"
            writer.write(resp); await writer.drain()
        finally:
            try:
                writer.close(); await writer.wait_closed()
            except Exception:
                pass
    server = await asyncio.start_server(handle_client, host="0.0.0.0", port=port)
    sockets = ", ".join(str(s.getsockname()) for s in (server.sockets or []))
    print(f"🌐 Keep-alive HTTP on {sockets}")
    return server

async def _post_init(app):
    await start_keepalive_server()
    asyncio.create_task(monthly_report_loop(app))
    asyncio.create_task(reminder_loop(app))
    print("🕒 Nền: báo cáo tháng & nhắc hẹn đã bật.")

# ---------- /huy & xử lý wizard ----------
async def cmd_cancel(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    end_session(upd.effective_chat.id)
    await upd.message.reply_text("🛑 Đã huỷ chế độ điền nhanh.")

async def handle_text(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = upd.effective_chat.id
    if chat_id not in SESS:
        return
    sess = SESS[chat_id]
    mode = sess["mode"]; expect = sess["expect"]; data = sess["data"]

    filled = parse_pack_reply(upd.message.text or "", expect)
    data.update(filled)

    missing = [k for k in expect if (k not in data or str(data[k]).strip() == "")]
    if missing:
        labels = {
            "ten":"tên", "chu_ky":"chu kỳ (tuan/thang)", "ngay":"ngày DD-MM-YYYY",
            "sochan":"số chân", "menhgia":"mệnh giá", "san":"sàn %", "tran":"trần %", "thau":"đầu thảo %",
            "maday":"mã dây", "ky":"kỳ", "sotientham":"số tiền thăm", "gio":"HH:MM"
        }
        items = "\n".join([f"- {labels.get(k,k)}" for k in missing])
        await upd.message.reply_text(
            "⚠️ Còn thiếu:\n" + items + "\n\n👉 Vui lòng trả lời **một tin** bổ sung theo đúng thứ tự còn thiếu (nhiều dòng hoặc dùng `|`).",
            parse_mode="Markdown"
        )
        return

    try:
        if mode == "tao":
            await _create_line_and_reply(
                upd,
                data["ten"], data["chu_ky"], data["ngay"], data["sochan"],
                data["menhgia"], data["san"], data["tran"], data["thau"],
            )
        elif mode == "tham":
            rdate = None
            if data.get("ngay"):
                rdate = to_iso_str(parse_user_date(data["ngay"]))
            await _save_tham(
                upd, _int_like(data["maday"]), _int_like(data["ky"]),
                parse_money(data["sotientham"]), rdate
            )
        end_session(chat_id)
    except Exception as e:
        await upd.message.reply_text(f"❌ Lỗi xử lý: {e}")

# ---------- MAIN ----------
def main():
    init_db(); ensure_schema()

    app = ApplicationBuilder().token(TOKEN).post_init(_post_init).build()

    # Commands
    app.add_handler(CommandHandler("start",    cmd_start))
    app.add_handler(CommandHandler("lenh",     cmd_lenh))
    app.add_handler(CommandHandler("baocao",   cmd_setreport))
    app.add_handler(CommandHandler("tao",      cmd_new))
    app.add_handler(CommandHandler("tham",     cmd_tham))
    app.add_handler(CommandHandler("hen",      cmd_set_remind))
    app.add_handler(CommandHandler("danhsach", cmd_list))
    app.add_handler(CommandHandler("tomtat",   cmd_summary))
    app.add_handler(CommandHandler("hottot",   cmd_whenhot))
    app.add_handler(CommandHandler("dong",     cmd_close))
    app.add_handler(CommandHandler("huy",      cmd_cancel))

    # Callback buttons (/lenh menu)
    app.add_handler(CallbackQueryHandler(on_menu_callback))

    # Wizard text
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_text))

    print("✅ Hui Bot (Render) đang chạy...")
    app.run_polling()

if __name__ == "__main__":
    main()
# ===================== END FILE =====================