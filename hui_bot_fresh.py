# hui_bot_fresh.py
# Render Web Service (no JobQueue). Deps: python-telegram-bot==20.3, pandas
import os, re, json, asyncio, sqlite3, random
from datetime import datetime, timedelta, time as dtime
from typing import Dict, Any, Tuple, Optional

import pandas as pd
from telegram import Update
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters
)

# =================== CẤU HÌNH ===================
TOKEN = (os.getenv("TELEGRAM_TOKEN") or os.getenv("BOT_TOKEN") or "").strip()
if not TOKEN:
    raise SystemExit("Thiếu TELEGRAM_TOKEN/BOT_TOKEN trong biến môi trường.")

DB_FILE = "hui.db"
CONFIG_FILE = "config.json"

REPORT_HOUR = 8                 # gửi báo cáo tháng lúc 08:00 (mùng 1)
REMINDER_TICK_SECONDS = 60      # vòng lặp nhắc hẹn
# =================================================

# ====== ĐỊNH DẠNG NGÀY ======
USER_FMT = "%d-%m-%Y"   # người dùng nhập/xem
ISO_FMT  = "%Y-%m-%d"   # lưu DB

def parse_user_date(s: str) -> datetime:
    return datetime.strptime(s, USER_FMT)

def to_user(d: datetime) -> str:
    return d.strftime(USER_FMT)

def to_iso(d: datetime) -> str:
    return d.strftime(ISO_FMT)

def parse_iso(s: str) -> datetime:
    return datetime.strptime(s, ISO_FMT)

# ====== PARSER TIỀN ======
def parse_money(text: str) -> int:
    """
    1tr/2.5tr/1t/1m -> triệu; 1000k/1000n/1k/1n -> nghìn; số thường OK.
    Hỗ trợ dấu phẩy/chấm.
    """
    s = str(text).strip().lower().replace(",", "").replace(".", "").replace("_", "")
    if not s:
        raise ValueError("rỗng")
    # có thập phân cho 'tr'
    m = re.fullmatch(r"(\d+(?:\.\d+)?)\s*(tr|t|m|tri|trieu)", text.strip().lower().replace(",", ""))
    if m:
        return int(float(m.group(1)) * 1_000_000)
    if s.endswith(("k", "n")):
        return int(float(s[:-1]) * 1_000)
    if s.endswith(("tr", "t", "m")):
        return int(float(s[:-2 if s.endswith('tr') else -1]) * 1_000_000)
    # số nguyên thường
    if re.fullmatch(r"\d+", s):
        return int(s)
    # thập phân thường
    try:
        return int(float(s))
    except Exception:
        raise ValueError(f"không hiểu tiền: {text}")

# ============ DB ============
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
        start_date TEXT NOT NULL,   -- ISO
        legs INTEGER NOT NULL,
        contrib INTEGER NOT NULL,   -- mệnh giá/kỳ (M)
        base_rate REAL DEFAULT 0,   -- % sàn trên M
        cap_rate  REAL DEFAULT 100, -- % trần trên M
        thau_rate REAL DEFAULT 0,   -- % đầu thảo trên M (mỗi kỳ)
        status TEXT DEFAULT 'OPEN',
        created_at TEXT NOT NULL,
        remind_hour INTEGER DEFAULT 8,
        remind_min  INTEGER DEFAULT 0,
        last_remind_iso TEXT
    )""")
    c.execute("""
    CREATE TABLE IF NOT EXISTS rounds(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        line_id INTEGER NOT NULL,
        k INTEGER NOT NULL,
        bid INTEGER NOT NULL,
        round_date TEXT,
        UNIQUE(line_id,k),
        FOREIGN KEY(line_id) REFERENCES lines(id) ON DELETE CASCADE
    )""")
    c.execute("""
    CREATE TABLE IF NOT EXISTS payments(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        line_id INTEGER NOT NULL,
        pay_date TEXT NOT NULL,
        amount INTEGER NOT NULL,
        FOREIGN KEY(line_id) REFERENCES lines(id) ON DELETE CASCADE
    )""")
    # Trạng thái wizard theo chat
    c.execute("""
    CREATE TABLE IF NOT EXISTS wizard(
        chat_id INTEGER PRIMARY KEY,
        kind TEXT NOT NULL,    -- 'tao' | 'tham'
        data TEXT NOT NULL,    -- JSON các trường đã có
        ts   TEXT NOT NULL     -- ISO time
    )""")
    conn.commit(); conn.close()

def load_cfg() -> Dict[str, Any]:
    if os.path.exists(CONFIG_FILE):
        try:
            return json.load(open(CONFIG_FILE, "r", encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_cfg(cfg: dict):
    json.dump(cfg, open(CONFIG_FILE, "w", encoding="utf-8"))

# ======= TIỆN ÍCH & TÍNH TOÁN =======
def k_date(start_iso: str, period_days: int, k: int) -> datetime:
    return parse_iso(start_iso) + timedelta(days=(k-1)*period_days)

def payout_at_k(line: dict, bids: Dict[int, int], k: int) -> int:
    """
    Payout_k = (k-1)*M + (N-k)*(M - T_k) - D
    M=mệnh giá; N=số chân; T_k=thăm kỳ k; D=thầu% * M
    """
    M, N = int(line["contrib"]), int(line["legs"])
    Tk = int(bids.get(k, 0))
    D  = int(round(M * float(line.get("thau_rate", 0))/100.0))
    return (k-1)*M + (N-k)*(M - Tk) - D

def paid_before_k(bids: Dict[int,int], M: int, k: int) -> int:
    return sum((M - int(bids.get(j,0))) for j in range(1, k))

def profit_roi(line: dict, bids: Dict[int,int], k: int) -> Tuple[int,float,int,int]:
    M = int(line["contrib"])
    po = payout_at_k(line, bids, k)
    paid = paid_before_k(bids, M, k)
    base = paid if paid>0 else M
    p = po - paid
    return p, (p/base), po, paid

def get_bids(line_id: int) -> Dict[int,int]:
    conn = db()
    rows = conn.execute("SELECT k,bid FROM rounds WHERE line_id=? ORDER BY k", (line_id,)).fetchall()
    conn.close()
    return {int(k): int(b) for (k,b) in rows}

def load_line(line_id: int) -> Optional[dict]:
    conn = db()
    r = conn.execute("SELECT * FROM lines WHERE id=?", (line_id,)).fetchone()
    conn.close()
    if not r: return None
    cols = ["id","name","period_days","start_date","legs","contrib","base_rate","cap_rate",
            "thau_rate","status","created_at","remind_hour","remind_min","last_remind_iso"]
    return dict(zip(cols, r))

# ======= TEXT HIỂN THỊ =======
def help_text() -> str:
    return (
        "👋 **HỤI BOT – phiên bản SQLite (không cần Google Sheets)**\n\n"
        "✨ **LỆNH CHÍNH** (không dấu, ngày **DD-MM-YYYY**):\n\n"
        "1) **Tạo dây** (đủ tham số):\n"
        "   `/tao <ten> <tuan|thang> <DD-MM-YYYY> <sochan> <menhgia> <san%> <tran%> <thau%>`\n"
        "   Ví dụ: `/tao Hui10tr tuan 10-10-2025 12 10tr 8 20 50`\n"
        "   💡 Thiếu tham số? Gõ **/tao** rồi trả lời **một tin** theo dạng `key=value` (bot tự điền phần đoán được).\n\n"
        "2) **Nhập thăm kỳ**:\n"
        "   `/tham <maday> <ky> <sotientham> [DD-MM-YYYY]`\n"
        "   Ví dụ: `/tham 1 1 2tr 10-10-2025`\n\n"
        "3) **Đặt giờ nhắc riêng**:  `/hen <maday> <HH:MM>`  (ví dụ: `/hen 1 07:45`)\n\n"
        "4) **Danh sách**: `/danhsach`\n"
        "5) **Tóm tắt**: `/tomtat <maday>`\n"
        "6) **Gợi ý hốt**: `/hoitot <maday> [roi|lai]`\n"
        "7) **Đóng dây**: `/dong <maday>`\n"
        "8) **Cài nơi nhận báo cáo & nhắc**: `/baocao [chat_id]`\n\n"
        "🆘 Thoát wizard: `/huy`"
    )

# ======= WIZARD LƯU TRẠNG THÁI =======
def wiz_set(chat_id: int, kind: str, data: Dict[str, Any]):
    conn = db()
    conn.execute("REPLACE INTO wizard(chat_id,kind,data,ts) VALUES(?,?,?,?)",
                 (chat_id, kind, json.dumps(data, ensure_ascii=False), datetime.utcnow().isoformat()))
    conn.commit(); conn.close()

def wiz_get(chat_id: int) -> Optional[Tuple[str, Dict[str,Any]]]:
    conn = db()
    r = conn.execute("SELECT kind,data FROM wizard WHERE chat_id=?", (chat_id,)).fetchone()
    conn.close()
    if not r: return None
    return r[0], json.loads(r[1])

def wiz_clear(chat_id: int):
    conn = db(); conn.execute("DELETE FROM wizard WHERE chat_id=?", (chat_id,)); conn.commit(); conn.close()

# ======= LỆNH =======
async def cmd_lenh(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await upd.message.reply_text(help_text(), disable_web_page_preview=True, parse_mode="Markdown")

async def cmd_start(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await cmd_lenh(upd, ctx)

async def cmd_huy(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    wiz_clear(upd.effective_chat.id)
    await upd.message.reply_text("⛔ Đã huỷ chế độ điền nhanh.")

async def cmd_baocao(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cfg = load_cfg()
    if ctx.args:
        try: cid = int(ctx.args[0])
        except: return await upd.message.reply_text("❌ `chat_id` không hợp lệ.")
    else:
        cid = upd.effective_chat.id
    cfg["report_chat_id"] = cid
    save_cfg(cfg)
    await upd.message.reply_text(f"✅ Đã lưu nơi nhận báo cáo/nhắc: `{cid}`.", parse_mode="Markdown")

# ---- /tao ----
def _kind_to_days(s: str) -> Optional[int]:
    s = s.lower()
    if s in ("tuan","tuần","week","weekly"): return 7
    if s in ("thang","tháng","month","monthly"): return 30
    return None

async def cmd_tao(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = upd.effective_chat.id
    if len(ctx.args) == 8:
        # đủ tham số 1 dòng
        name, kind, dstr, legs, menh, san, tran, thau = ctx.args
        try:
            period_days = _kind_to_days(kind);  assert period_days is not None
            start_iso = to_iso(parse_user_date(dstr))
            legs = int(legs)
            contrib = parse_money(menh)
            base_rate = float(san); cap_rate = float(tran); thau_rate = float(thau)
            if not (0 <= base_rate <= cap_rate <= 100): raise ValueError("sàn <= trần trong [0..100]")
            if not (0 <= thau_rate <= 100): raise ValueError("đầu thảo 0..100")
        except Exception as e:
            return await upd.message.reply_text(f"❌ Tham số không hợp lệ: {e}")

        conn = db()
        conn.execute("""INSERT INTO lines(name,period_days,start_date,legs,contrib,base_rate,cap_rate,thau_rate,status,created_at)
                        VALUES(?,?,?,?,?,?,?,?,'OPEN',?)""",
                     (name, period_days, start_iso, legs, contrib, base_rate, cap_rate, thau_rate, datetime.now().isoformat()))
        conn.commit()
        lid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.close()
        return await upd.message.reply_text(
            f"✅ Đã tạo dây #{lid} ({name}) — {'Hụi tuần' if period_days==7 else 'Hụi tháng'}\n"
            f"• Mở: {dstr} · Chân: {legs} · Mệnh giá: {contrib:,} VND\n"
            f"• Sàn {base_rate:.2f}% · Trần {cap_rate:.2f}% · Đầu thảo {thau_rate:.2f}%"
        )

    # thiếu → vào wizard
    wiz_set(chat_id, "tao", {})
    await upd.message.reply_text(
        "🧩 **Điền nhanh tạo dây** – hãy gửi **một tin** chứa các cặp `key=value` (không cần thứ tự):\n"
        "`ten=... tuan|thang=... ngay=DD-MM-YYYY sochan=... menhgia=... san=... tran=... thau=...`\n"
        "Ví dụ: `ten=Hui10tr tuan=tuần ngay=10-10-2025 sochan=12 menhgia=10tr san=8 tran=20 thau=50`\n"
        "Gõ `/huy` để thoát.",
        parse_mode="Markdown"
    )

# ---- /tham ----
async def cmd_tham(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = upd.effective_chat.id
    if len(ctx.args) >= 3:
        # đủ tham số inline
        try:
            lid = int(ctx.args[0]); k = int(ctx.args[1]); bid = parse_money(ctx.args[2])
            rdate = to_iso(parse_user_date(ctx.args[3])) if len(ctx.args)>=4 else None
        except Exception as e:
            return await upd.message.reply_text(f"❌ Tham số không hợp lệ: {e}")
        line = load_line(lid)
        if not line: return await upd.message.reply_text("❌ Không tìm thấy dây.")
        if not (1 <= k <= int(line["legs"])): return await upd.message.reply_text(f"❌ Kỳ 1..{line['legs']}.")
        M = int(line["contrib"])
        min_bid = int(round(M * float(line.get("base_rate",0))/100.0))
        max_bid = int(round(M * float(line.get("cap_rate",100))/100.0))
        if bid < min_bid or bid > max_bid:
            return await upd.message.reply_text(
                f"❌ Thăm phải trong [{min_bid:,} .. {max_bid:,}] VND "
                f"(sàn {line['base_rate']}% · trần {line['cap_rate']}% trên M={M:,})"
            )
        conn = db()
        conn.execute("""INSERT INTO rounds(line_id,k,bid,round_date) VALUES(?,?,?,?)
                        ON CONFLICT(line_id,k) DO UPDATE SET bid=excluded.bid, round_date=excluded.round_date""",
                     (lid, k, bid, rdate))
        conn.commit(); conn.close()
        return await upd.message.reply_text(
            f"✅ Đã lưu thăm kỳ {k} cho dây #{lid}: {bid:,} VND" +
            (f" · ngày {to_user(parse_iso(rdate))}" if rdate else "")
        )

    # thiếu → wizard
    wiz_set(chat_id, "tham", {})
    await upd.message.reply_text(
        "🧩 **Điền nhanh nhập thăm** – hãy gửi **một tin** chứa các cặp `key=value`:\n"
        "`maday=... ky=... tien=... [ngay=DD-MM-YYYY]`\n"
        "Ví dụ: `maday=1 ky=1 tien=2tr ngay=10-10-2025`\n"
        "Gõ `/huy` để thoát.",
        parse_mode="Markdown"
    )

# ---- /hen ----
async def cmd_hen(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if len(ctx.args) != 2:
        return await upd.message.reply_text("❌ Cú pháp: `/hen <maday> <HH:MM>`", parse_mode="Markdown")
    try:
        lid = int(ctx.args[0]); hh, mm = map(int, ctx.args[1].split(":"))
        assert 0 <= hh <= 23 and 0 <= mm <= 59
    except Exception:
        return await upd.message.reply_text("❌ Giờ/phút không hợp lệ.")
    line = load_line(lid)
    if not line: return await upd.message.reply_text("❌ Không tìm thấy dây.")
    conn = db(); conn.execute("UPDATE lines SET remind_hour=?, remind_min=? WHERE id=?", (hh, mm, lid))
    conn.commit(); conn.close()
    await upd.message.reply_text(f"⏰ Đã đặt nhắc cho dây #{lid}: {hh:02d}:{mm:02d} mỗi {'tuần' if line['period_days']==7 else 'tháng'}.")

# ---- /danhsach ----
async def cmd_danhsach(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    conn = db()
    rows = conn.execute("""SELECT id,name,period_days,start_date,legs,contrib,base_rate,cap_rate,thau_rate,status,remind_hour,remind_min
                           FROM lines ORDER BY id DESC""").fetchall()
    conn.close()
    if not rows: return await upd.message.reply_text("📂 Chưa có dây nào.")
    out = ["📋 **Danh sách dây**:"]
    for r in rows:
        out.append(
            f"• #{r[0]} · {r[1]} · {'Tuần' if r[2]==7 else 'Tháng'} · mở {to_user(parse_iso(r[3]))} · "
            f"chân {r[4]} · M {int(r[5]):,} VND · sàn {r[6]}% · trần {r[7]}% · thầu {r[8]}% · "
            f"nhắc {int(r[10]):02d}:{int(r[11]):02d} · {r[9]}"
        )
    await upd.message.reply_text("\n".join(out), parse_mode="Markdown")

# ---- /tomtat ----
async def cmd_tomtat(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args: return await upd.message.reply_text("❌ Cú pháp: `/tomtat <maday>`", parse_mode="Markdown")
    try: lid = int(ctx.args[0])
    except: return await upd.message.reply_text("❌ `maday` phải là số.")
    line = load_line(lid)
    if not line: return await upd.message.reply_text("❌ Không tìm thấy dây.")
    bids = get_bids(lid)
    M, N = int(line["contrib"]), int(line["legs"])
    k_now = max(1, min(len(bids)+1, N))
    p, r, po, paid = profit_roi(line, bids, k_now)
    # best theo ROI
    best_k, best_val = 1, -1e18
    best_r, best_po, best_paid = 0.0, 0, 0
    for i in range(1, N+1):
        pp, rr, poo, pa = profit_roi(line, bids, i)
        if rr > best_val:
            best_val, best_k, best_r, best_po, best_paid = rr, i, rr, poo, pa
    rows = [f"📌 Dây #{line['id']} – {line['name']} ({'Tuần' if line['period_days']==7 else 'Tháng'})",
            f"• Mở: {to_user(parse_iso(line['start_date']))} · Chân: {N} · Mệnh giá/kỳ: {M:,} VND",
            f"• Sàn {line['base_rate']}% · Trần {line['cap_rate']}% · Đầu thảo {line['thau_rate']}% · Nhắc {int(line['remind_hour']):02d}:{int(line['remind_min']):02d}",
            "• Thăm:" + (" " + ", ".join([f"k{kk}:{int(b):,}" for kk,b in sorted(bids.items())]) if bids else " (chưa có)"),
            f"• Kỳ hiện tại ước: {k_now} → Payout {po:,} · Đã đóng {paid:,} → Lãi {int(round(p)):,} (ROI {r*100:.2f}%)",
            f"⭐ Gợi ý (ROI): kỳ {best_k} · ngày {to_user(k_date(line['start_date'], line['period_days'], best_k))} · Payout {best_po:,} · Đã đóng {best_paid:,} · ROI {best_r*100:.2f}%"]
    await upd.message.reply_text("\n".join(rows))

# ---- /hoitot ----
async def cmd_hoitot(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args: return await upd.message.reply_text("❌ Cú pháp: `/hoitot <maday> [roi|lai]`", parse_mode="Markdown")
    try: lid = int(ctx.args[0])
    except: return await upd.message.reply_text("❌ `maday` phải là số.")
    metric = (ctx.args[1].lower() if len(ctx.args)>=2 else "roi")
    if metric not in ("roi","lai"): metric = "roi"
    line = load_line(lid)
    if not line: return await upd.message.reply_text("❌ Không tìm thấy dây.")
    bids = get_bids(lid)
    M, N = int(line["contrib"]), int(line["legs"])
    best_k, best_key, best_r = 1, -1e18, 0.0
    for i in range(1, N+1):
        p, r, po, paid = profit_roi(line, bids, i)
        key = (r if metric=="roi" else p)
        if key > best_key:
            best_key, best_k, best_r = key, i, r
    await upd.message.reply_text(
        f"🔎 Gợi ý theo {'ROI%' if metric=='roi' else 'lãi'} cho dây #{lid} – {line['name']}:\n"
        f"• Nên hốt kỳ {best_k} (ngày {to_user(k_date(line['start_date'], line['period_days'], best_k))})\n"
        f"• ROI ước tính: {best_r*100:.2f}%"
    )

# ---- /dong ----
async def cmd_dong(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args: return await upd.message.reply_text("❌ Cú pháp: `/dong <maday>`", parse_mode="Markdown")
    try: lid = int(ctx.args[0])
    except: return await upd.message.reply_text("❌ `maday` phải là số.")
    conn = db(); conn.execute("UPDATE lines SET status='CLOSED' WHERE id=?", (lid,))
    conn.commit(); conn.close()
    await upd.message.reply_text(f"🗂️ Đã đóng & lưu trữ dây #{lid}.")

# ====== XỬ LÝ TIN THƯỜNG (điền nhanh) ======
def parse_kv_payload(text: str) -> Dict[str,str]:
    # tách theo khoảng trắng, mỗi phần key=val; value giữ nguyên (không cần quote)
    parts = re.findall(r"(\S+?=\S+)", text.strip())
    kv = {}
    for p in parts:
        k, v = p.split("=", 1)
        kv[k.strip().lower()] = v.strip()
    return kv

async def on_text(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = upd.effective_chat.id
    st = wiz_get(chat_id)
    if not st:
        return  # không ở wizard → bỏ qua
    kind, data = st
    kv = parse_kv_payload(upd.message.text)
    if not kv:
        return await upd.message.reply_text("⚠️ Hãy nhập theo dạng `key=value` (nhiều cặp trên **một** dòng).", parse_mode="Markdown")

    if kind == "tao":
        # map alias
        alias = {"ten":"ten","name":"ten","tuan":"tuan","thang":"thang","ngay":"ngay",
                 "sochan":"sochan","chan":"sochan","menhgia":"menhgia","m":"menhgia",
                 "san":"san","tran":"tran","thau":"thau"}
        for k,v in kv.items():
            data[alias.get(k,k)] = v
        # xác định tuần/tháng
        if "tuan" in data and "thang" in data:
            data["kieu"] = "tuan" if data["tuan"] else "thang"
        elif "tuan" in data: data["kieu"]="tuan"
        elif "thang" in data: data["kieu"]="thang"

        missing = []
        for k in ("ten","kieu","ngay","sochan","menhgia","san","tran","thau"):
            if k not in data: missing.append(k)

        if missing:
            wiz_set(chat_id, "tao", data)
            return await upd.message.reply_text("⛏️ Còn thiếu: " + ", ".join(missing))

        # đủ → tạo
        try:
            period_days = 7 if data["kieu"].lower().startswith("tuan") else 30
            start_iso = to_iso(parse_user_date(data["ngay"]))
            legs = int(data["sochan"])
            contrib = parse_money(data["menhgia"])
            base_rate = float(data["san"]); cap_rate = float(data["tran"]); thau_rate = float(data["thau"])
            if not (0 <= base_rate <= cap_rate <= 100): raise ValueError("sàn<=trần trong [0..100]")
            if not (0 <= thau_rate <= 100): raise ValueError("đầu thảo 0..100")
        except Exception as e:
            return await upd.message.reply_text(f"❌ Dữ liệu không hợp lệ: {e}")

        conn = db()
        conn.execute("""INSERT INTO lines(name,period_days,start_date,legs,contrib,base_rate,cap_rate,thau_rate,status,created_at)
                        VALUES(?,?,?,?,?,?,?,?,'OPEN',?)""",
                     (data["ten"], period_days, start_iso, legs, contrib, base_rate, cap_rate, thau_rate, datetime.now().isoformat()))
        conn.commit()
        lid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.close()
        wiz_clear(chat_id)
        return await upd.message.reply_text(
            f"✅ Đã tạo dây #{lid} ({data['ten']}) — {'Hụi tuần' if period_days==7 else 'Hụi tháng'}\n"
            f"• Mở: {data['ngay']} · Chân: {legs} · Mệnh giá: {contrib:,} VND\n"
            f"• Sàn {base_rate:.2f}% · Trần {cap_rate:.2f}% · Đầu thảo {thau_rate:.2f}%"
        )

    if kind == "tham":
        alias = {"maday":"maday","ky":"ky","k":"ky","tien":"tien","bid":"tien","ngay":"ngay","date":"ngay"}
        for k,v in kv.items():
            data[alias.get(k,k)] = v
        missing = [k for k in ("maday","ky","tien") if k not in data]
        if missing:
            wiz_set(chat_id, "tham", data)
            return await upd.message.reply_text("⛏️ Còn thiếu: " + ", ".join(missing))
        # đủ → ghi thăm
        try:
            lid = int(data["maday"]); k = int(data["ky"]); bid = parse_money(data["tien"])
            rdate = to_iso(parse_user_date(data["ngay"])) if "ngay" in data else None
        except Exception as e:
            return await upd.message.reply_text(f"❌ Dữ liệu không hợp lệ: {e}")
        line = load_line(lid)
        if not line: return await upd.message.reply_text("❌ Không tìm thấy dây.")
        if not (1 <= k <= int(line["legs"])): return await upd.message.reply_text(f"❌ Kỳ 1..{line['legs']}.")
        M = int(line["contrib"])
        min_bid = int(round(M * float(line.get("base_rate",0))/100.0))
        max_bid = int(round(M * float(line.get("cap_rate",100))/100.0))
        if bid < min_bid or bid > max_bid:
            return await upd.message.reply_text(
                f"❌ Thăm phải trong [{min_bid:,} .. {max_bid:,}] VND "
                f"(sàn {line['base_rate']}% · trần {line['cap_rate']}% trên M={M:,})"
            )
        conn = db()
        conn.execute("""INSERT INTO rounds(line_id,k,bid,round_date) VALUES(?,?,?,?)
                        ON CONFLICT(line_id,k) DO UPDATE SET bid=excluded.bid, round_date=excluded.round_date""",
                     (lid, k, bid, rdate))
        conn.commit(); conn.close()
        wiz_clear(chat_id)
        return await upd.message.reply_text(
            f"✅ Đã lưu thăm kỳ {k} cho dây #{lid}: {bid:,} VND" +
            (f" · ngày {to_user(parse_iso(rdate))}" if rdate else "")
        )

# ====== BÁO CÁO THÁNG & NHẮC HẸN (vòng lặp) ======
async def send_monthly_report(app):
    cfg = load_cfg(); chat_id = cfg.get("report_chat_id")
    if not chat_id: return
    today = datetime.now().date()
    if today.day != 1: return
    conn = db()
    rows = conn.execute("""SELECT id,name,period_days,start_date,legs,contrib,base_rate,cap_rate,thau_rate,status
                           FROM lines""").fetchall()
    conn.close()
    if not rows:
        return await app.bot.send_message(chat_id=chat_id, text="📊 Báo cáo tháng: chưa có dây.")
    lines = []
    for r in rows:
        lines.append(
            f"#{r[0]} · {r[1]} · {('Tuần' if r[2]==7 else 'Tháng')} · M {int(r[5]):,} · sàn {r[6]}% · trần {r[7]}% · thầu {r[8]}% · {r[9]}"
        )
    await app.bot.send_message(chat_id=chat_id, text="📊 **Báo cáo tháng**:\n" + "\n".join(lines), parse_mode="Markdown")

async def loop_monthly(app):
    while True:
        now = datetime.now()
        tgt = datetime.combine(now.date(), dtime(hour=REPORT_HOUR))
        if now >= tgt: tgt += timedelta(days=1)
        await asyncio.sleep(max(1.0, (tgt - now).total_seconds()))
        await send_monthly_report(app)

async def send_reminders(app):
    cfg = load_cfg(); chat_id = cfg.get("report_chat_id")
    if not chat_id: return
    now = datetime.now()
    hh, mm = now.hour, now.minute
    conn = db()
    rs = conn.execute("""SELECT id,name,period_days,start_date,legs,contrib,base_rate,cap_rate,thau_rate,remind_hour,remind_min,last_remind_iso
                         FROM lines WHERE status='OPEN'""").fetchall()
    conn.close()
    for (lid, name, pdays, start_iso, legs, M, base, cap, thau, rh, rm, last_iso) in rs:
        if hh != int(rh) or mm != int(rm): continue
        if last_iso == now.date().isoformat(): continue
        bids = get_bids(lid)
        N = int(legs)
        k_now = max(1, min(len(bids)+1, N))
        open_day = (parse_iso(start_iso) + timedelta(days=(k_now-1)*int(pdays))).date()
        if open_day != now.date(): continue
        min_bid = int(round(int(M)*float(base)/100.0))
        max_bid = int(round(int(M)*float(cap)/100.0))
        D = int(round(int(M)*float(thau)/100.0))
        prompt = random.choice([
            "⏰ Tuần này đoán thăm bao nhiêu?",
            "💬 Kỳ này chốt thăm mức nào?",
            "🔔 Nhắc nhẹ: nhập thăm kỳ mới nhé!",
            "🤔 Dự đoán con số đẹp hôm nay?"
        ]) if int(pdays)==7 else random.choice([
            "📅 Tháng này thăm bao nhiêu?",
            "🗓️ Đến hẹn lại lên, nhập thăm nhé!",
            "🔔 Tháng mới, chốt thăm thôi!",
            "💡 Bạn đoán mức thăm kỳ này?"
        ])
        txt = (
            f"📣 Nhắc dây #{lid} – {name} ({'tuần' if int(pdays)==7 else 'tháng'})\n"
            f"• Kỳ {k_now}/{N} – Ngày: {to_user(parse_iso(start_iso) + timedelta(days=(k_now-1)*int(pdays)))}\n"
            f"• M {int(M):,} · Sàn {float(base):.1f}% ({min_bid:,}) · Trần {float(cap):.1f}% ({max_bid:,}) · Thầu {float(thau):.1f}% ({D:,})\n"
            f"➡️ {prompt}\n👉 Gõ: /tham {lid} {k_now} <sotientham>"
        )
        await app.bot.send_message(chat_id=chat_id, text=txt)
        conn2 = db(); conn2.execute("UPDATE lines SET last_remind_iso=? WHERE id=?", (now.date().isoformat(), lid)); conn2.commit(); conn2.close()

async def loop_remind(app):
    while True:
        await send_reminders(app)
        await asyncio.sleep(REMINDER_TICK_SECONDS)

# ====== HTTP keep-alive cho Render ======
async def start_keepalive_server():
    port = int(os.getenv("PORT", "10000"))
    async def handle(reader, writer):
        try:
            await reader.read(1024)
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Length:2\r\nConnection: close\r\n\r\nOK")
            await writer.drain()
        finally:
            try:
                writer.close(); await writer.wait_closed()
            except Exception:
                pass
    srv = await asyncio.start_server(handle, "0.0.0.0", port)
    socks = ", ".join(str(s.getsockname()) for s in (srv.sockets or []))
    print(f"🌐 Keep-alive on {socks}")
    return srv

async def _post_init(app):
    await start_keepalive_server()
    asyncio.create_task(loop_monthly(app))
    asyncio.create_task(loop_remind(app))
    print("🕒 Nền: báo cáo tháng & nhắc hẹn đã bật.")

# ============ MAIN ============
def main():
    init_db()
    app = ApplicationBuilder().token(TOKEN).post_init(_post_init).build()

    # Lệnh chính
    app.add_handler(CommandHandler("start",     cmd_start))
    app.add_handler(CommandHandler("lenh",      cmd_lenh))
    app.add_handler(CommandHandler("huy",       cmd_huy))
    app.add_handler(CommandHandler("baocao",    cmd_baocao))
    app.add_handler(CommandHandler("tao",       cmd_tao))
    app.add_handler(CommandHandler("tham",      cmd_tham))
    app.add_handler(CommandHandler("hen",       cmd_hen))
    app.add_handler(CommandHandler("danhsach",  cmd_danhsach))
    app.add_handler(CommandHandler("tomtat",    cmd_tomtat))
    app.add_handler(CommandHandler("hoitot",    cmd_hoitot))
    app.add_handler(CommandHandler("dong",      cmd_dong))

    # Bắt tin nhắn thường để điền nhanh
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), on_text))

    print("✅ Hụi Bot (Render) đang chạy…")
    app.run_polling()

if __name__ == "__main__":
    main()