# hui_bot_fresh.py
# Dependencies: python-telegram-bot==20.3, pandas
import os, sqlite3, json, asyncio, random, re
from datetime import datetime, timedelta, time as dtime
import pandas as pd
from telegram import Update
from telegram.ext import (
    ApplicationBuilder, CommandHandler, ContextTypes,
    MessageHandler, filters
)

# ========= CONFIG =========
TOKEN = (os.getenv("TELEGRAM_TOKEN") or os.getenv("BOT_TOKEN") or "").strip()
if not TOKEN:
    raise SystemExit("Missing TELEGRAM_TOKEN/BOT_TOKEN in environment variables")

DB_FILE = "hui.db"
CONFIG_FILE = "config.json"

REPORT_HOUR = 8                   # 08:00 gửi báo cáo tháng (chỉ mùng 1)
REMINDER_TICK_SECONDS = 60        # vòng lặp check nhắc hẹn
# =========================

# ====== DATE HELPERS ======
USER_DATE_FMT = "%d-%m-%Y"   # người dùng nhập/xem
ISO_FMT       = "%Y-%m-%d"   # lưu DB

def parse_iso(s: str) -> datetime:
    return datetime.strptime(s, ISO_FMT)

def parse_user_date(s: str) -> datetime:
    return datetime.strptime(s, USER_DATE_FMT)

def to_iso_str(d: datetime) -> str:
    return d.strftime(ISO_FMT)

def to_user_str(d: datetime) -> str:
    return d.strftime(USER_DATE_FMT)

# ----- MONEY PARSER -----
def parse_money(text: str) -> int:
    """
    '1tr'/'2.5tr'/'1000k'/'1000n' -> 1_000_000; '1k'/'1n' -> 1_000; '100k'/'100n' -> 100_000.
    Hỗ trợ số thuần, dấu chấm, 'm'/'t' ~ triệu.
    """
    s = str(text).strip().lower().replace(",", "").replace("_", "").replace(" ", "")
    if s.isdigit():
        return int(s)
    try:
        if s.endswith("tr"):
            num = float(s[:-2]); return int(num * 1_000_000)
        if s.endswith(("k","n")):
            num = float(s[:-1]); return int(num * 1_000)
        if s.endswith(("m","t")):
            num = float(s[:-1]); return int(num * 1_000_000)
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
        thau_rate REAL DEFAULT 0,                 -- % đầu thảo trên M (trừ mỗi kỳ)
        remind_hour INTEGER DEFAULT 8,
        remind_min  INTEGER DEFAULT 0,
        last_remind_iso TEXT
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
        bid INTEGER NOT NULL,         -- số tiền thăm thực tế
        round_date TEXT,
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
    roi = profit / base
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
    if line["status"] == "CLOSED": return True
    last = k_date(line, int(line["legs"])).date()
    return datetime.now().date() >= last

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

# ================== WIZARD (NHẬP 1 LẦN) ==================
WIZ = {}  # state theo user

def _kv_pairs(text: str) -> dict:
    """Parse 'a=1 b=2, c=3' -> {'a':'1','b':'2','c':'3'}."""
    parts = re.split(r"[,\n]+|\s{2,}", (text or "").strip())
    out = {}
    for p in parts:
        if not p or "=" not in p: continue
        k, v = p.split("=", 1)
        out[k.strip().lower()] = v.strip()
    return out

def _fmt_missing(spec, have):
    missing = [k for k in spec if k not in have]
    tip_items = []
    for k, meta in spec.items():
        if k in have: continue
        tip_items.append(meta["hint"])
    return missing, (" • " + "\n • ".join(tip_items)) if tip_items else ""

async def _start_wiz(upd: Update, cmd: str, spec: dict, initial: dict):
    user_id = upd.effective_user.id if upd.effective_user else None
    if user_id is None: return
    WIZ[user_id] = {"cmd": cmd, "spec": spec, "data": initial}
    missing, tips = _fmt_missing(spec, initial)
    pretty = ", ".join(missing)
    prefill = "\n".join([f"  {k}={initial[k]}" for k in spec if k in initial])
    await upd.message.reply_text(
        f"🧩 Thiếu tham số cho **/{cmd}**: {pretty}\n"
        + (f"• Đã tự điền: \n{prefill}\n" if prefill else "")
        + "➡️ Trả lời **một tin** theo dạng `key=value` (nhiều cặp được), ví dụ:\n"
        f"{spec.get('__example__','')}",
        parse_mode="Markdown"
    )
    if tips.strip():
        await upd.message.reply_text("Gợi ý:\n" + tips)

async def _abort_wiz(upd: Update):
    user_id = upd.effective_user.id if upd.effective_user else None
    if user_id in WIZ:
        WIZ.pop(user_id, None)
        await upd.message.reply_text("⛔ Đã huỷ chế độ điền nhanh.")

async def _on_wiz_text(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = upd.effective_user.id if upd.effective_user else None
    if user_id not in WIZ: return
    state = WIZ[user_id]
    spec, data = state["spec"], state["data"]
    data.update(_kv_pairs(upd.message.text))

    missing, _ = _fmt_missing(spec, data)
    if missing:
        await upd.message.reply_text(f"⚠️ Vẫn thiếu: {', '.join(missing)}. Nhập tiếp key=value nhé.")
        return

    cmd = state["cmd"]
    try:
        if cmd == "tao":      await _exec_tao_from_kv(upd, data)
        elif cmd == "tham":   await _exec_tham_from_kv(upd, data)
        elif cmd == "hen":    await _exec_hen_from_kv(upd, data)
        elif cmd == "baocao": await _exec_baocao_from_kv(upd, data)
        elif cmd == "dong":   await _exec_dong_from_kv(upd, data)
        elif cmd == "tomtat": await _exec_tomtat_from_kv(upd, data)
        elif cmd == "hoitot": await _exec_hoitot_from_kv(upd, data)
        else: await upd.message.reply_text("❌ Lệnh không hỗ trợ wizard.")
    finally:
        WIZ.pop(user_id, None)

# ====== TỰ ĐỘNG ĐIỀN (AUTO-FILL) ======
_date_rx = re.compile(r"\b(\d{1,2}-\d{1,2}-\d{4})\b")

def _guess_tuan_thang(tokens):
    for t in tokens:
        tt = t.lower()
        if tt in ("tuan","tuần","week","weekly"): return "tuan"
        if tt in ("thang","tháng","month","monthly"): return "thang"
    return None

def _first_int(tokens):
    for t in tokens:
        if re.fullmatch(r"\d+", t): return int(t)
    return None

def _first_money(tokens):
    for t in tokens:
        try: return parse_money(t)
        except: pass
    return None

def _all_percents(tokens, need=3):
    vals = []
    for t in tokens:
        m = re.fullmatch(r"(\d+(?:\.\d+)?)%?", t)
        if m:
            vals.append(float(m.group(1)))
            if len(vals) == need: break
    return vals

def guess_tao_from_tokens(args_tokens):
    """Đoán các trường cho /tao từ danh sách token thô."""
    text = " ".join(args_tokens)
    got = {}

    # ngày
    m = _date_rx.search(text)
    if m: got["ngay"] = m.group(1)

    # loại
    typ = _guess_tuan_thang(args_tokens)
    if typ: got["loai"] = typ

    # số chân
    legs = _first_int(args_tokens)
    if legs: got["chan"] = str(legs)

    # mệnh
    money = _first_money(args_tokens)
    if money: got["menh"] = str(money)

    # phần trăm
    pcts = _all_percents(args_tokens, need=3)
    if len(pcts) >= 1: got["san"]  = str(pcts[0])
    if len(pcts) >= 2: got["tran"] = str(pcts[1])
    if len(pcts) >= 3: got["thao"] = str(pcts[2])

    # tên = phần còn lại đầu chuỗi (không khớp ngày/tiền/%/số)
    blacklist = set((got.get("ngay") or "").split()) | set(args_tokens)
    # cách đơn giản: lấy token đầu chưa trùng các pattern phổ biến
    name = []
    for t in args_tokens:
        if _date_rx.fullmatch(t): continue
        if t.lower() in ("tuan","tuần","thang","tháng","week","month","weekly","monthly"): continue
        if re.fullmatch(r"\d+|(\d+(\.\d+)?)%?", t): 
            # số thuần hoặc phần trăm
            continue
        try:
            parse_money(t); continue
        except: pass
        name.append(t)
        break
    if name: got["ten"] = name[0]
    return got

def guess_tham_from_tokens(args_tokens):
    """Đoán line, kỳ, thăm, ngày từ token tự do."""
    got = {}
    # mã dây & kỳ: lấy 2 số đầu tiên
    nums = [int(x) for x in args_tokens if re.fullmatch(r"\d+", x)]
    if len(nums) >= 1: got["line"] = str(nums[0])
    if len(nums) >= 2: got["ky"]   = str(nums[1])

    # thăm (tiền)
    mny = _first_money(args_tokens)
    if mny: got["tham"] = str(mny)

    # ngày
    m = _date_rx.search(" ".join(args_tokens))
    if m: got["ngay"] = m.group(1)
    return got

# ---------- HELP / LỆNH ----------
HELP_TEXT = (
    "👋 HỤI BOT – phiên bản SQLite (không cần Google Sheets)\n\n"
    "🌟 LỆNH CHÍNH (không dấu, ngày **DD-MM-YYYY**):\n\n"
    "1) Tạo dây (đủ tham số):\n"
    "   /tao <tên> <tuần|tháng> <DD-MM-YYYY> <số_chân> <mệnh_giá> <sàn_%> <trần_%> <thảo_%>\n"
    "   Ví dụ: /tao Hui10tr tuần 10-10-2025 12 10tr 8 20 50\n"
    "   💡 Thiếu tham số? Gõ /tao rồi trả lời **một tin** theo dạng `key=value` (bot còn **tự điền** những gì đoán được).\n\n"
    "2) Nhập thăm kỳ:\n"
    "   /tham <mã_dây> <kỳ> <số_tiền_thăm> [DD-MM-YYYY]\n\n"
    "3) Đặt giờ nhắc riêng:  /hen <mã_dây> <HH:MM>\n"
    "4) Danh sách:            /danhsach\n"
    "5) Tóm tắt:              /tomtat <mã_dây>\n"
    "6) Gợi ý hốt:            /hoitot <mã_dây> [roi|lai]\n"
    "7) Đóng dây:             /dong <mã_dây>\n"
    "8) Cài nơi nhận báo cáo & nhắc: /baocao [chat_id]\n\n"
    "🆘 Thoát wizard: /huy"
)

async def cmd_start(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await upd.message.reply_text(HELP_TEXT, disable_web_page_preview=True, parse_mode="Markdown")

async def cmd_lenh(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    # alias để show danh sách lệnh nhanh
    await cmd_start(upd, ctx)

async def cmd_huy(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await _abort_wiz(upd)

# ---------- /BAOCAO ----------
async def _exec_baocao_from_kv(upd: Update, kv: dict):
    cfg = load_cfg()
    cid = kv.get("chat") or kv.get("chat_id")
    if cid is None or str(cid).lower() in ("", "here", "this"):
        cid = upd.effective_chat.id
    try: cid = int(cid)
    except Exception:
        return await upd.message.reply_text("❌ chat_id không hợp lệ.")
    cfg["report_chat_id"] = cid
    save_cfg(cfg)
    await upd.message.reply_text(f"✅ Đã lưu nơi nhận báo cáo/nhắc: {cid}.")

async def cmd_setreport(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if ctx.args:
        await _exec_baocao_from_kv(upd, {"chat": ctx.args[0]})
        return
    spec = {
        "chat": {"hint": "chat=<id> (hoặc 'here' để dùng chat hiện tại)"},
        "__example__": "chat=here"
    }
    await _start_wiz(upd, "baocao", spec, {})

# ---------- /TAO ----------
async def _exec_tao_from_kv(upd: Update, d: dict):
    try:
        name = d["ten"]
        kind = d["loai"]
        start_user = d["ngay"]
        period_days = 7 if kind.strip().lower() in ("tuan","tuần","week","weekly") else 30
        legs = int(d["chan"])
        contrib = parse_money(d["menh"])
        base_rate = float(d["san"])
        cap_rate  = float(d["tran"])
        thau_rate = float(d["thao"])
        if not (0 <= base_rate <= cap_rate <= 100): raise ValueError("Sàn ≤ Trần và trong [0..100]")
        if not (0 <= thau_rate <= 100): raise ValueError("Thảo % trong [0..100]")
        start_iso = to_iso_str(parse_user_date(start_user))
    except Exception as e:
        return await upd.message.reply_text(f"❌ Dữ liệu không hợp lệ: {e}")

    conn = db()
    conn.execute(
        """INSERT INTO lines(name,period_days,start_date,legs,contrib,
                             bid_type,bid_value,status,created_at,
                             base_rate,cap_rate,thau_rate,remind_hour,remind_min,last_remind_iso)
           VALUES(?,?,?,?,?,'dynamic',0,'OPEN',?, ?, ?, ?, 8, 0, NULL)""",
        (name, period_days, start_iso, legs, contrib,
         datetime.now().isoformat(), base_rate, cap_rate, thau_rate)
    )
    conn.commit()
    line_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.close()

    await upd.message.reply_text(
        f"✅ Tạo dây #{line_id} ({name}) — {'Hụi tuần' if period_days==7 else 'Hụi tháng'}\n"
        f"Mở: {start_user} · Chân: {legs} · Mệnh giá: {contrib:,} VND\n"
        f"SÀN {base_rate:.2f}% · TRẦN {cap_rate:.2f}% · THẢO {thau_rate:.2f}% (trên M)\n"
        f"⏰ Nhắc mặc định: 08:00 (dùng /hen {line_id} HH:MM để đổi)\n"
        f"➡️ Nhập thăm: /tham {line_id} <kỳ> <số_tiền_thăm> [DD-MM-YYYY]"
    )

async def cmd_new(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if len(ctx.args) >= 8:
        ten, loai, ngay, chan, menh, san, tran, thao = ctx.args
        await _exec_tao_from_kv(upd, {
            "ten": ten, "loai": loai, "ngay": ngay, "chan": chan,
            "menh": menh, "san": san, "tran": tran, "thao": thao
        })
        return
    # Auto-fill: đoán từ chuỗi bạn gõ
    guessed = guess_tao_from_tokens(ctx.args)
    spec = {
        "ten":   {"hint":"ten=<tên_dây>"},
        "loai":  {"hint":"loai=tuan|thang"},
        "ngay":  {"hint":"ngay=DD-MM-YYYY"},
        "chan":  {"hint":"chan=<số_chân>"},
        "menh":  {"hint":"menh=<mệnh_giá, vd 10tr|2500k|2.5tr>"},
        "san":   {"hint":"san=<giá_sàn_%>"},
        "tran":  {"hint":"tran=<giá_trần_%>"},
        "thao":  {"hint":"thao=<đầu_thảo_%>"},
        "__example__": "ten=Hui10tr loai=tuan ngay=10-10-2025 chan=12 menh=10tr san=8 tran=20 thao=50"
    }
    await _start_wiz(upd, "tao", spec, guessed)

# ---------- /THAM ----------
async def _exec_tham_from_kv(upd: Update, d: dict):
    try:
        line_id = int(d["line"])
        k       = int(d["ky"])
        bid     = parse_money(d["tham"])
        rdate   = d.get("ngay")
        rdate_iso = to_iso_str(parse_user_date(rdate)) if rdate else None
    except Exception as e:
        return await upd.message.reply_text(f"❌ Dữ liệu không hợp lệ: {e}")

    line, _ = load_line_full(line_id)
    if not line: return await upd.message.reply_text("❌ Không tìm thấy dây.")
    if not (1 <= k <= int(line["legs"])):
        return await upd.message.reply_text(f"❌ Kỳ hợp lệ 1..{line['legs']}.")

    M = int(line["contrib"])
    min_bid = int(round(M * float(line.get("base_rate", 0)) / 100.0))
    max_bid = int(round(M * float(line.get("cap_rate", 100)) / 100.0))
    if bid < min_bid or bid > max_bid:
        return await upd.message.reply_text(
            f"❌ Thăm phải trong [{min_bid:,} .. {max_bid:,}] VND "
            f"(SÀN {line['base_rate']}% · TRẦN {line['cap_rate']}% trên M={M:,})"
        )

    conn = db()
    conn.execute("""
        INSERT INTO rounds(line_id,k,bid,round_date) VALUES(?,?,?,?)
        ON CONFLICT(line_id,k) DO UPDATE SET bid=excluded.bid, round_date=excluded.round_date
    """, (line_id, k, bid, rdate_iso))
    conn.commit(); conn.close()

    await upd.message.reply_text(
        f"✅ Lưu thăm kỳ {k} cho dây #{line_id}: {bid:,} VND"
        + (f" · ngày {d['ngay']}" if rdate else "")
    )

async def cmd_tham(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if len(ctx.args) >= 3:
        payload = {"line": ctx.args[0], "ky": ctx.args[1], "tham": ctx.args[2]}
        if len(ctx.args) >= 4: payload["ngay"] = ctx.args[3]
        await _exec_tham_from_kv(upd, payload); return
    # Auto-fill
    guessed = guess_tham_from_tokens(ctx.args)
    spec = {
        "line":{"hint":"line=<mã_dây>"},
        "ky":  {"hint":"ky=<kỳ>"},
        "tham":{"hint":"tham=<số_tiền_thăm, vd 2tr|1500k>"},
        "ngay":{"hint":"ngay=DD-MM-YYYY (tuỳ chọn)"},
        "__example__": "line=1 ky=1 tham=2tr ngay=10-10-2025"
    }
    await _start_wiz(upd, "tham", spec, guessed)

# ---------- /HEN ----------
async def _exec_hen_from_kv(upd: Update, d: dict):
    try:
        line_id = int(d["line"])
        hhmm = d["gio"]
        hh, mm = hhmm.split(":")
        hh = int(hh); mm = int(mm)
        if not (0 <= hh <= 23 and 0 <= mm <= 59): raise ValueError("Giờ/phút không hợp lệ")
    except Exception as e:
        return await upd.message.reply_text(f"❌ Dữ liệu không hợp lệ: {e}")

    line, _ = load_line_full(line_id)
    if not line: return await upd.message.reply_text("❌ Không tìm thấy dây.")
    conn = db()
    conn.execute("UPDATE lines SET remind_hour=?, remind_min=? WHERE id=?", (hh, mm, line_id))
    conn.commit(); conn.close()
    await upd.message.reply_text(f"✅ Đã đặt giờ nhắc cho dây #{line_id}: {hh:02d}:{mm:02d}")

async def cmd_set_remind(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if len(ctx.args) == 2:
        await _exec_hen_from_kv(upd, {"line": ctx.args[0], "gio": ctx.args[1]}); return
    spec = {
        "line":{"hint":"line=<mã_dây>"},
        "gio": {"hint":"gio=<HH:MM>"},
        "__example__": "line=1 gio=07:45"
    }
    await _start_wiz(upd, "hen", spec, {})

# ---------- /DANHSACH ----------
async def cmd_list(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    conn = db()
    rows = conn.execute(
        "SELECT id,name,period_days,start_date,legs,contrib,base_rate,cap_rate,thau_rate,status,remind_hour,remind_min "
        "FROM lines ORDER BY id DESC"
    ).fetchall()
    conn.close()
    if not rows:
        return await upd.message.reply_text("📂 Chưa có dây nào.")
    out = ["📋 Danh sách dây:"]
    for r in rows:
        kind = "Tuần" if r[2]==7 else "Tháng"
        out.append(
            f"#{r[0]} · {r[1]} · {kind} · mở {to_user_str(parse_iso(r[3]))} · chân {r[4]} · M {r[5]:,} VND · "
            f"SÀN {r[6]:.2f}% · TRẦN {r[7]:.2f}% · THẢO {r[8]:.2f}% · nhắc {int(r[10]):02d}:{int(r[11]):02d} · {r[9]}"
        )
    await upd.message.reply_text("\n".join(out))

# ---------- /TOMTAT ----------
async def _exec_tomtat_from_kv(upd: Update, d: dict):
    try: line_id = int(d["line"])
    except: return await upd.message.reply_text("❌ line phải là số.")
    line, _ = load_line_full(line_id)
    if not line: return await upd.message.reply_text("❌ Không tìm thấy dây.")
    bids = get_bids(line_id)
    M, N = int(line["contrib"]), int(line["legs"])
    cfg_line = f"SÀN {float(line.get('base_rate',0)):.2f}% · TRẦN {float(line.get('cap_rate',100)):.2f}% · THẢO {float(line.get('thau_rate',0)):.2f}% trên M"
    k_now = max(1, min(len(bids)+1, N))
    p, r, po, paid = compute_profit_var(line, k_now, bids)
    bestk, (bp, br, bpo, bpaid) = best_k_var(line, bids, metric="roi")
    msg = [
        f"📌 Dây #{line['id']} · {line['name']} · {'Tuần' if line['period_days']==7 else 'Tháng'}",
        f"• Mở: {to_user_str(parse_iso(line['start_date']))} · Chân: {N} · Mệnh giá: {M:,} VND",
        f"• {cfg_line} · Nhắc {int(line.get('remind_hour',8)):02d}:{int(line.get('remind_min',0)):02d}",
        f"• Thăm: " + (", ".join([f"k{kk}:{int(b):,}" for kk,b in sorted(bids.items())]) if bids else "(chưa có)"),
        f"• Kỳ hiện tại ước tính: {k_now} · Payout: {po:,} · Đã đóng: {paid:,} → Lãi: {int(round(p)):,} (ROI {roi_to_str(r)})",
        f"⭐ Đề xuất (ROI): kỳ {bestk} · ngày {to_user_str(k_date(line,bestk))} · Payout {bpo:,} · Đã đóng {bpaid:,} · Lãi {int(round(bp)):,} · ROI {roi_to_str(br)}"
    ]
    if is_finished(line): msg.append("✅ Dây đã đến hạn — /dong để lưu trữ.")
    await upd.message.reply_text("\n".join(msg))

async def cmd_summary(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if len(ctx.args) >= 1:
        await _exec_tomtat_from_kv(upd, {"line": ctx.args[0]}); return
    spec = {"line":{"hint":"line=<mã_dây>"}, "__example__":"line=1"}
    await _start_wiz(upd, "tomtat", spec, {})

# ---------- /HOITOT ----------
async def _exec_hoitot_from_kv(upd: Update, d: dict):
    try: line_id = int(d["line"])
    except: return await upd.message.reply_text("❌ line phải là số.")
    metric = (d.get("kieu") or "roi").lower()
    if metric not in ("roi","lai"): metric = "roi"

    line, _ = load_line_full(line_id)
    if not line: return await upd.message.reply_text("❌ Không tìm thấy dây.")
    bids = get_bids(line_id)

    bestk, (bp, br, bpo, bpaid) = best_k_var(line, bids, metric=("roi" if metric=="roi" else "lai"))
    await upd.message.reply_text(
        f"🔎 Gợi ý theo {'ROI%' if metric=='roi' else 'Lãi'}:\n"
        f"• Kỳ nên hốt: {bestk}\n"
        f"• Ngày dự kiến: {to_user_str(k_date(line,bestk))}\n"
        f"• Payout kỳ đó: {bpo:,}\n"
        f"• Đã đóng trước đó: {bpaid:,}\n"
        f"• Lãi ước tính: {int(round(bp)):,} — ROI: {roi_to_str(br)}"
    )

async def cmd_whenhot(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if len(ctx.args) >= 1:
        payload = {"line": ctx.args[0]}
        if len(ctx.args) >= 2: payload["kieu"] = ctx.args[1]
        await _exec_hoitot_from_kv(upd, payload); return
    spec = {
        "line":{"hint":"line=<mã_dây>"},
        "kieu":{"hint":"kieu=roi|lai (mặc định roi)"},
        "__example__": "line=1 kieu=roi"
    }
    await _start_wiz(upd, "hoitot", spec, {})

# ---------- /DONG ----------
async def _exec_dong_from_kv(upd: Update, d: dict):
    try: line_id = int(d["line"])
    except: return await upd.message.reply_text("❌ line phải là số.")
    conn = db()
    conn.execute("UPDATE lines SET status='CLOSED' WHERE id=?", (line_id,))
    conn.commit(); conn.close()
    await upd.message.reply_text(f"🗂️ Đã đóng & lưu trữ dây #{line_id}.")

async def cmd_close(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if len(ctx.args) >= 1:
        await _exec_dong_from_kv(upd, {"line": ctx.args[0]}); return
    spec = {"line":{"hint":"line=<mã_dây>"}, "__example__":"line=1"}
    await _start_wiz(upd, "dong", spec, {})

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
            f"M {int(line['contrib']):,} · SÀN {float(line['base_rate']):.1f}% · TRẦN {float(line['cap_rate']):.1f}% · THẢO {float(line['thau_rate']):.1f}% · "
            f"Kỳ_now {k_now}: Lãi {int(round(p)):,} ({roi_to_str(ro)}) · Best k{bestk} {roi_to_str(br)}"
        )

    txt = "📊 Báo cáo tháng:\n" + "\n".join(lines)
    await app.bot.send_message(chat_id=chat_id, text=txt)

# ----- NHẮC HẸN DÍ DỎM -----
async def send_periodic_reminders(app):
    cfg = load_cfg(); chat_id = cfg.get("report_chat_id")
    if not chat_id: return

    today = datetime.now()
    now_d = today.date()
    hh, mm = today.hour, today.minute

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

        if hh != int(remind_hour) or mm != int(remind_min): continue
        if last_remind_iso == now_d.isoformat(): continue

        bids = get_bids(line_id)
        N = int(legs)
        k_now = max(1, min(len(bids) + 1, N))
        open_day = (parse_iso(start_date_str) + timedelta(days=(k_now-1)*int(period_days))).date()
        if open_day != now_d: continue

        is_weekly = (int(period_days) == 7)
        prompt = random.choice(weekly_prompts if is_weekly else monthly_prompts)
        min_bid = int(round(int(M) * float(base_rate) / 100.0))
        max_bid = int(round(int(M) * float(cap_rate)  / 100.0))
        D = int(round(int(M) * float(thau_rate) / 100.0))

        txt = (
            f"📣 Nhắc dây #{line_id} – {name}\n"
            f"• Kỳ {k_now}/{N} · Ngày: {to_user_str(parse_iso(start_date_str) + timedelta(days=(k_now-1)*int(period_days)))}\n"
            f"• Mệnh giá: {int(M):,} VND · SÀN {float(base_rate):.1f}% ({min_bid:,}) · TRẦN {float(cap_rate):.1f}% ({max_bid:,}) · THẢO {float(thau_rate):.1f}% ({D:,})\n\n"
            f"➡️ {prompt}\n"
            f"👉 Gõ nhanh: /tham {line_id} {k_now} <số_tiền_thăm>"
        )
        await app.bot.send_message(chat_id=chat_id, text=txt)

        conn2 = db()
        conn2.execute("UPDATE lines SET last_remind_iso=? WHERE id=?", (now_d.isoformat(), line_id))
        conn2.commit(); conn2.close()

# ----- VÒNG LẶP NỀN & KEEP-ALIVE -----
async def monthly_report_loop(app):
    while True:
        now = datetime.now()
        target = datetime.combine(now.date(), dtime(hour=REPORT_HOUR))
        if now >= target: target += timedelta(days=1)
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

# ---------- MAIN ----------
def main():
    init_db()
    ensure_schema()

    app = ApplicationBuilder().token(TOKEN).post_init(_post_init).build()

    # Danh sách lệnh
    app.add_handler(CommandHandler("start",    cmd_start))
    app.add_handler(CommandHandler("lenh",     cmd_lenh))      # <== NEW
    app.add_handler(CommandHandler("huy",      cmd_huy))

    # Cấu hình & dữ liệu
    app.add_handler(CommandHandler("baocao",   cmd_setreport))
    app.add_handler(CommandHandler("tao",      cmd_new))
    app.add_handler(CommandHandler("tham",     cmd_tham))
    app.add_handler(CommandHandler("hen",      cmd_set_remind))
    app.add_handler(CommandHandler("danhsach", cmd_list))
    app.add_handler(CommandHandler("tomtat",   cmd_summary))
    app.add_handler(CommandHandler("hoitot",   cmd_whenhot))
    app.add_handler(CommandHandler("dong",     cmd_close))

    # Bắt văn bản để nhập một lần cho wizard
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, _on_wiz_text))

    print("✅ Hụi Bot (Render) đang chạy...")
    app.run_polling()

if __name__ == "__main__":
    main()