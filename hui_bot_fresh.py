# hui_bot_fresh.py
# Runtime: Python 3.12+
# Deps: python-telegram-bot==20.3, pandas

import os, re, sqlite3, json, threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from datetime import datetime, timedelta
import pandas as pd
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# ====================== CẤU HÌNH ======================
TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
if not TOKEN:
    raise SystemExit("Thiếu TELEGRAM_TOKEN trong biến môi trường.")
DB_FILE = "hui.db"
CONFIG_FILE = "config.json"
# ======================================================

# ----------------- DB -----------------
def db():
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def init_db():
    c = db()
    c.execute("""
    CREATE TABLE IF NOT EXISTS lines(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        period_days INTEGER NOT NULL,
        start_date TEXT NOT NULL,
        legs INTEGER NOT NULL,
        face_value INTEGER NOT NULL,      -- mệnh giá dây (M)
        floor_pct REAL NOT NULL,          -- giá sàn %
        cap_pct REAL NOT NULL,            -- giá trần %
        dau_thao_pct REAL NOT NULL,       -- đầu thảo %
        status TEXT DEFAULT 'OPEN',
        created_at TEXT NOT NULL
    )""")
    c.execute("""
    CREATE TABLE IF NOT EXISTS auctions(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        line_id INTEGER NOT NULL,
        k INTEGER NOT NULL,
        bid_amount INTEGER NOT NULL,      -- số tiền thăm của kỳ k (VND)
        bid_date TEXT NOT NULL,
        UNIQUE(line_id, k),
        FOREIGN KEY(line_id) REFERENCES lines(id) ON DELETE CASCADE
    )""")
    c.commit(); c.close()

# ----------------- UTIL -----------------
def load_cfg():
    if os.path.exists(CONFIG_FILE):
        try:
            return json.load(open(CONFIG_FILE, "r", encoding="utf-8"))
        except:
            return {}
    return {}

def save_cfg(cfg: dict):
    json.dump(cfg, open(CONFIG_FILE, "w", encoding="utf-8"))

def vn_money_to_int(s: str) -> int:
    """
    Hỗ trợ: 5tr, 2.5tr, 250k, 1n, 1000k, 1000n, số thuần.
    """
    raw = str(s).strip().lower().replace(",", "")
    # 2.5tr
    m = re.fullmatch(r"(\d+(?:\.\d+)?)\s*(tr|tri|trieu)", raw)
    if m:
        return int(float(m.group(1)) * 1_000_000)
    s = raw.replace(".", "")
    m = re.fullmatch(r"(\d+)\s*(k|ngan|ngàn|n)", s)
    if m:
        return int(m.group(1)) * 1_000
    if s.endswith(("k", "n")):
        num = re.sub(r"(k|n)$", "", s)
        return int(num) * 1_000
    return int(s)

def parse_date_ddmmyyyy(s: str) -> datetime:
    return datetime.strptime(s, "%d-%m-%Y")

def fmt_money(v: int | float) -> str:
    return f"{int(round(v)):,} đ".replace(",", ".")

def k_date(start_date: str, period_days: int, k: int) -> datetime:
    return parse_date_ddmmyyyy(start_date) + timedelta(days=(k - 1) * period_days)

# --- TÍNH TOÁN ---
def payout_for_k(M: int, N: int, D: float, bid: int) -> int:
    """
    Tiền người hốt kỳ k nhận về:
    (N-1) * (M - bid) - D * M
    """
    return int((N - 1) * (M - bid) - D * M)

def roi_for_k(M: int, contrib_paid: int, received: int) -> float:
    base = contrib_paid if contrib_paid > 0 else M
    return (received - contrib_paid) / base

# --------------- LOAD LINE ---------------
def load_line(line_id: int):
    conn = db()
    r = conn.execute("SELECT * FROM lines WHERE id=?", (line_id,)).fetchone()
    conn.close()
    if not r: return None
    cols = ["id","name","period_days","start_date","legs","face_value",
            "floor_pct","cap_pct","dau_thao_pct","status","created_at"]
    return dict(zip(cols, r))

# ================== COMMANDS ==================
HELP_TEXT = (
    "👋 **HỤI BOT – phiên bản SQLite (không cần Google Sheets)**\n\n"
    "✨ **LỆNH CHÍNH** (bạn gõ *không dấu* cũng được, ngày **DD-MM-YYYY**):\n\n"
    "1) **Tạo dây**:\n"
    "   `/tao <tên> <tuan|thang> <DD-MM-YYYY> <số_chân> <mệnh_giá> <giá_sàn_%> <giá_trần_%> <đầu_thảo_%>`\n"
    "   Ví dụ: `/tao Hui10tr tuan 10-10-2025 12 10tr 8 20 50`\n"
    "   💡 Tiền có thể viết: 5tr, 2.5tr, 250k, 1n, 1000k…\n\n"
    "2) **Nhập thăm theo kỳ**:\n"
    "   `/tham <mã_dây> <kỳ> <số_tiền_thăm> [DD-MM-YYYY]`\n"
    "   Ví dụ: `/tham 1 1 2tr 10-10-2025`\n\n"
    "3) **Danh sách / Tóm tắt / Gợi ý hốt**:\n"
    "   `/danhsach`\n"
    "   `/tomtat <mã_dây>`\n"
    "   `/hoitot <mã_dây> [roi|lai]`\n\n"
    "4) **Đóng dây**: `/dong <mã_dây>`\n"
)

async def cmd_start(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await upd.message.reply_text(HELP_TEXT, disable_web_page_preview=True, parse_mode="Markdown")

# (giữ lại để lưu chat_id, không chạy định kỳ trong bản web service free)
async def cmd_baocao(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cfg = load_cfg()
    if ctx.args:
        try: cid = int(ctx.args[0])
        except: return await upd.message.reply_text("❌ `chat_id` không hợp lệ.", parse_mode="Markdown")
    else:
        cid = upd.effective_chat.id
    cfg["report_chat_id"] = cid
    save_cfg(cfg)
    await upd.message.reply_text(
        f"✅ Đã lưu nơi nhận báo cáo: `{cid}` (bản free không gửi tự động).",
        parse_mode="Markdown"
    )

def _normalize_kind(kind_raw: str) -> str:
    k = kind_raw.strip().lower()
    return "tuan" if k in ("tuan","tuần","week","weekly") else "thang"

async def cmd_tao(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        name = ctx.args[0]
        kind = _normalize_kind(ctx.args[1])
        start = ctx.args[2]
        legs = int(ctx.args[3])
        face = vn_money_to_int(ctx.args[4])
        floor = float(ctx.args[5]); cap = float(ctx.args[6]); dau = float(ctx.args[7])
        period_days = 7 if kind == "tuan" else 30
        if not (0 <= floor < cap <= 100):
            return await upd.message.reply_text("❌ `giá_sàn_%` phải < `giá_trần_%` và nằm trong [0..100].")
        _ = parse_date_ddmmyyyy(start)
        conn = db()
        conn.execute("""
            INSERT INTO lines(name,period_days,start_date,legs,face_value,floor_pct,cap_pct,dau_thao_pct,status,created_at)
            VALUES(?,?,?,?,?,?,?,?, 'OPEN', ?)
        """, (name, period_days, start, legs, face, floor, cap, dau, datetime.now().isoformat()))
        conn.commit()
        line_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.close()
        await upd.message.reply_text(
            (f"✅ Đã tạo dây **#{line_id}** – **{name}** ({'Hụi tuần' if period_days==7 else 'Hụi tháng'})\n"
             f"• Mở: {start}\n"
             f"• Số chân: {legs}\n"
             f"• Mệnh giá: {fmt_money(face)}\n"
             f"• Sàn: {floor:.0f}% · Trần: {cap:.0f}% · Đầu thảo: {dau:.0f}%"),
            parse_mode="Markdown"
        )
    except Exception:
        await upd.message.reply_text(
            "❌ Sai cú pháp.\nVí dụ: `/tao Hui10tr tuan 10-10-2025 12 10tr 8 20 50`",
            parse_mode="Markdown"
        )

async def cmd_danhsach(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    conn = db()
    rows = conn.execute("SELECT id,name,period_days,start_date,legs,face_value,floor_pct,cap_pct,dau_thao_pct,status FROM lines ORDER BY id DESC").fetchall()
    conn.close()
    if not rows:
        return await upd.message.reply_text("📂 Chưa có dây nào.")
    out = ["📋 **Danh sách dây**:"]
    for r in rows:
        kind = "Tuần" if r[2]==7 else "Tháng"
        out.append(f"• #{r[0]} · {r[1]} · {kind} · mở {r[3]} · chân {r[4]} · mệnh giá {fmt_money(r[5])} · sàn {r[6]}% · trần {r[7]}% · đầu thảo {r[8]}% · {r[9]}")
    await upd.message.reply_text("\n".join(out), parse_mode="Markdown")

async def cmd_tham(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        line_id = int(ctx.args[0]); k = int(ctx.args[1])
        bid = vn_money_to_int(ctx.args[2])
        dt = ctx.args[3] if len(ctx.args)>=4 else datetime.now().strftime("%d-%m-%Y")
        _ = parse_date_ddmmyyyy(dt)
    except Exception:
        return await upd.message.reply_text("❌ Cú pháp: `/tham <mã_dây> <kỳ> <số_tiền_thăm> [DD-MM-YYYY]`", parse_mode="Markdown")

    line = load_line(line_id)
    if not line: return await upd.message.reply_text("❌ Không tìm thấy dây.")
    if not (1 <= k <= line["legs"]):
        return await upd.message.reply_text(f"❌ Kỳ phải trong [1..{line['legs']}].")

    floor_v = int(line["face_value"]*line["floor_pct"]/100.0)
    cap_v   = int(line["face_value"]*line["cap_pct"]/100.0)
    if bid < floor_v or bid > cap_v:
        return await upd.message.reply_text(
            f"❌ Số thăm {fmt_money(bid)} ngoài biên.\n"
            f"• Sàn: {fmt_money(floor_v)} ({line['floor_pct']}%)\n"
            f"• Trần: {fmt_money(cap_v)} ({line['cap_pct']}%)")

    conn = db()
    try:
        conn.execute("INSERT INTO auctions(line_id,k,bid_amount,bid_date) VALUES(?,?,?,?)", (line_id, k, bid, dt))
        conn.commit()
    except sqlite3.IntegrityError:
        conn.close(); return await upd.message.reply_text("⚠️ Kỳ này đã có thăm. Dùng số khác hoặc /tomtat để xem.")
    conn.close()

    M,N,D = line["face_value"], line["legs"], line["dau_thao_pct"]/100.0
    received = payout_for_k(M, N, D, bid)
    await upd.message.reply_text(
        (f"✅ Đã ghi **thăm kỳ {k}** cho dây **#{line_id} – {line['name']}**\n"
         f"• Thăm: {fmt_money(bid)}\n• Ngày: {dt}\n• Ước tiền nhận hốt: **{fmt_money(received)}**"),
        parse_mode="Markdown"
    )

async def cmd_tomtat(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try: line_id = int(ctx.args[0])
    except Exception:
        return await upd.message.reply_text("❌ Cú pháp: `/tomtat <mã_dây>`", parse_mode="Markdown")

    line = load_line(line_id)
    if not line: return await upd.message.reply_text("❌ Không tìm thấy dây.")

    conn = db()
    df = pd.read_sql_query("SELECT k, bid_amount, bid_date FROM auctions WHERE line_id=? ORDER BY k", conn, params=(line_id,))
    conn.close()

    M,N,D = line["face_value"], line["legs"], line["dau_thao_pct"]/100.0
    rows = []
    for i in range(1, N+1):
        if not df.empty and (df["k"]==i).any():
            bid = int(df[df["k"]==i]["bid_amount"].iloc[0])
            rec = payout_for_k(M,N,D,bid)
            contrib_paid = (i-1)*M
            roi = roi_for_k(M, contrib_paid, rec)
            rows.append(f"• Kỳ {i}: thăm {fmt_money(bid)} → nhận {fmt_money(rec)} · ROI {roi*100:.2f}%")
        else:
            rows.append(f"• Kỳ {i}: (chưa có thăm) — ngày dự kiến {k_date(line['start_date'], line['period_days'], i).strftime('%d-%m-%Y')}")
    head = (
        f"📌 **Dây #{line['id']} – {line['name']}** ({'Tuần' if line['period_days']==7 else 'Tháng'})\n"
        f"• Mở: {line['start_date']} · Chân: {line['legs']} · Mệnh giá/kỳ: {fmt_money(M)}\n"
        f"• Sàn: {line['floor_pct']}% · Trần: {line['cap_pct']}% · Đầu thảo: {line['dau_thao_pct']}%\n"
    )
    await upd.message.reply_text(head + "\n".join(rows), parse_mode="Markdown")

async def cmd_hoitot(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if len(ctx.args)<1:
        return await upd.message.reply_text("❌ Cú pháp: `/hoitot <mã_dây> [roi|lai]`", parse_mode="Markdown")
    line_id = int(ctx.args[0])
    metric = (ctx.args[1].lower() if len(ctx.args)>=2 else "roi")
    if metric not in ("roi","lai"): metric="roi"

    line = load_line(line_id)
    if not line: return await upd.message.reply_text("❌ Không tìm thấy dây.")

    conn = db()
    existing = pd.read_sql_query("SELECT k,bid_amount FROM auctions WHERE line_id=?", conn, params=(line_id,))
    conn.close()

    M,N,D = line["face_value"], line["legs"], line["dau_thao_pct"]/100.0
    best_k, best_val, best_roi = None, -1e18, None
    for i in range(1, N+1):
        if not existing.empty and (existing["k"]==i).any():
            bid = int(existing[existing["k"]==i]["bid_amount"].iloc[0])
        else:
            bid = int(M*line["floor_pct"]/100.0)  # giả định sàn
        rec = payout_for_k(M,N,D,bid)
        contrib_paid = (i-1)*M
        roi = roi_for_k(M, contrib_paid, rec)
        key = (roi if metric=="roi" else (rec - contrib_paid))
        if key > best_val:
            best_val, best_k, best_roi = key, i, roi

    await upd.message.reply_text(
        (f"🔎 **Gợi ý hốt theo {'ROI%' if metric=='roi' else 'lãi tuyệt đối'}** cho dây **#{line_id} – {line['name']}**:\n"
         f"• Nên hốt **kỳ {best_k}** (ngày {k_date(line['start_date'], line['period_days'], best_k).strftime('%d-%m-%Y')})\n"
         f"• ROI ước tính: **{best_roi*100:.2f}%**"),
        parse_mode="Markdown"
    )

async def cmd_dong(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try: line_id = int(ctx.args[0])
    except Exception:
        return await upd.message.reply_text("❌ Cú pháp: `/dong <mã_dây>`", parse_mode="Markdown")
    conn = db()
    conn.execute("UPDATE lines SET status='CLOSED' WHERE id=?", (line_id,))
    conn.commit(); conn.close()
    await upd.message.reply_text(f"🗂️ Đã đóng & lưu trữ dây #{line_id}.")

# ============== HTTP HEALTHCHECK (Render port binding) ==============
class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"OK")
    def log_message(self, fmt, *args):  # tắt log rườm rà
        return

def start_health_server():
    port = int(os.getenv("PORT", "10000"))
    server = HTTPServer(("0.0.0.0", port), _HealthHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    print(f"[health] Serving on 0.0.0.0:{port}")

# ================== MAIN ==================
def main():
    init_db()
    start_health_server()  # mở cổng cho Render

    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("baocao", cmd_baocao))
    app.add_handler(CommandHandler("tao", cmd_tao))
    app.add_handler(CommandHandler("danhsach", cmd_danhsach))
    app.add_handler(CommandHandler("tham", cmd_tham))
    app.add_handler(CommandHandler("tomtat", cmd_tomtat))
    app.add_handler(CommandHandler("hoitot", cmd_hoitot))
    app.add_handler(CommandHandler("dong", cmd_dong))

    print("✅ Hụi Bot đang chạy (polling)…")
    app.run_polling()

if __name__ == "__main__":
    main()