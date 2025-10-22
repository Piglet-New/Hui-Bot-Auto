# hui_bot_fresh.py
# Runtime: Python 3.12+
# deps: python-telegram-bot==20.3, pandas
import os, re, sqlite3, json
from datetime import datetime, timedelta, time as dtime
from typing import Tuple, Optional
import pandas as pd
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# ====================== CẤU HÌNH ======================
TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
if not TOKEN:
    raise SystemExit("Thiếu TELEGRAM_TOKEN trong biến môi trường.")
DB_FILE = "hui.db"
CONFIG_FILE = "config.json"
REPORT_HOUR = 8      # gửi báo cáo tháng lúc 08:00 sáng
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
    c.execute("""
    CREATE TABLE IF NOT EXISTS reminders(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        line_id INTEGER NOT NULL,
        at_hhmm TEXT NOT NULL,
        FOREIGN KEY(line_id) REFERENCES lines(id) ON DELETE CASCADE
    )""")
    c.commit(); c.close()

# ----------------- UTIL -----------------
def load_cfg():
    if os.path.exists(CONFIG_FILE):
        try: return json.load(open(CONFIG_FILE, "r", encoding="utf-8"))
        except: return {}
    return {}

def save_cfg(cfg: dict):
    json.dump(cfg, open(CONFIG_FILE, "w", encoding="utf-8"))

def vn_money_to_int(s: str) -> int:
    """1tr=1_000_000; 5000k=5_000_000; 1k/1n=1_000; 100k/100n=100_000; hỗ trợ số thường."""
    s = str(s).strip().lower().replace(".", "").replace(",", "")
    m = re.fullmatch(r"(\d+)\s*(tr|tri|trieu)", s)
    if m: return int(m.group(1)) * 1_000_000
    m = re.fullmatch(r"(\d+)\s*(k|ngan|ngàn|n)", s)
    if m: return int(m.group(1)) * 1_000
    if s.endswith("k") or s.endswith("n"):  # fallback
        num = re.sub(r"(k|n)$","", s)
        return int(num) * 1_000
    return int(s)

def parse_date_ddmmyyyy(s: str) -> datetime:
    return datetime.strptime(s, "%d-%m-%Y")

def fmt_money(v: int|float) -> str:
    return f"{int(round(v)):,} đ".replace(",", ".")

def k_date(start_date: str, period_days: int, k: int) -> datetime:
    return parse_date_ddmmyyyy(start_date) + timedelta(days=(k-1)*period_days)

# --- TÍNH TOÁN ---
def payout_for_k(M:int, N:int, D:float, bid:int) -> int:
    """
    Số tiền người hốt kỳ k nhận:
    - Lý thuyết: (N-1)*(M - bid) - D*M
    """
    return int((N-1)*(M - bid) - D*M)

def roi_for_k(M:int, contrib_paid:int, received:int) -> float:
    base = contrib_paid if contrib_paid>0 else M
    return (received - contrib_paid) / base

# --------------- LOAD LINE ---------------
def load_line(line_id:int):
    conn = db()
    r = conn.execute("SELECT * FROM lines WHERE id=?", (line_id,)).fetchone()
    if not r: conn.close(); return None
    cols = ["id","name","period_days","start_date","legs","face_value","floor_pct",
            "cap_pct","dau_thao_pct","status","created_at"]
    line = dict(zip(cols, r))
    conn.close()
    return line

def current_k(line_id:int) -> int:
    """k hiện tại = số bản ghi auctions + 1 (không vượt quá N)."""
    conn = db()
    cnt = conn.execute("SELECT COUNT(*) FROM auctions WHERE line_id=?", (line_id,)).fetchone()[0]
    conn.close()
    return min(cnt+1, load_line(line_id)["legs"])

# ================== COMMANDS ==================
HELP_TEXT = (
"👋 **HỤI BOT – phiên bản SQLite (không cần Google Sheets)**\n\n"
"✨ **LỆNH CHÍNH** (không dấu, ngày **DD-MM-YYYY**):\n\n"
"1) **Tạo dây**:\n"
"   `/tao <ten> <tuan|thang> <DD-MM-YYYY> <so_chan> <menh_gia> <gia_san_%> <gia_tran_%> <dau_thao_%>`\n"
"   Ví dụ: `/tao Hui10tr tuan 10-10-2025 12 10tr 8 20 50`\n"
"   💡 Tiền có thể viết: 5tr, 250k, 1n, 1000k, 2.5tr…\n\n"
"2) **Nhập thăm theo kỳ**:\n"
"   `/tham <ma_day> <ky> <so_tien_tham> [DD-MM-YYYY]`\n"
"   Ví dụ: `/tham 1 1 2tr 10-10-2025`\n\n"
"3) **Đặt giờ nhắc riêng cho từng dây**:\n"
"   `/hen <ma_day> <HH:MM>`  Ví dụ: `/hen 1 07:45`\n\n"
"4) **Danh sách / Tóm tắt / Gợi ý hốt**:\n"
"   `/danhsach`\n"
"   `/tomtat <ma_day>`\n"
"   `/hoitot <ma_day> [roi|lai]`\n\n"
"5) **Đóng dây**: `/dong <ma_day>`\n\n"
"6) **Bật báo cáo tự động hàng tháng (mùng 1, 08:00)**:\n"
"   `/baocao [chat_id]`\n"
)

async def cmd_start(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await upd.message.reply_text(HELP_TEXT, disable_web_page_preview=True)

async def cmd_baocao(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cfg = load_cfg()
    if ctx.args:
        try: cid = int(ctx.args[0])
        except: return await upd.message.reply_text("❌ `chat_id` không hợp lệ.")
    else:
        cid = upd.effective_chat.id
    cfg["report_chat_id"] = cid
    save_cfg(cfg)
    await upd.message.reply_text(f"✅ Đã lưu nơi nhận báo cáo tự động: `{cid}`.\n"
                                 f"Bot sẽ gửi lúc **{REPORT_HOUR:02d}:00** ngày **01** hàng tháng.",
                                 parse_mode="Markdown")

async def cmd_tao(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        name = ctx.args[0]
        kind = ctx.args[1].lower()
        start = ctx.args[2]
        legs = int(ctx.args[3])
        face = vn_money_to_int(ctx.args[4])
        floor = float(ctx.args[5])
        cap = float(ctx.args[6])
        dau = float(ctx.args[7])

        period_days = 7 if kind in ("tuan","tuần","week","weekly") else 30

        # validate % sàn/trần
        if not (0 <= floor < cap <= 100):
            return await upd.message.reply_text("❌ `gia_san_%` phải < `gia_tran_%` và trong [0..100].")

        conn = db()
        conn.execute("""
            INSERT INTO lines(name,period_days,start_date,legs,face_value,floor_pct,cap_pct,dau_thao_pct,status,created_at)
            VALUES(?,?,?,?,?,?,?,?, 'OPEN', ?)
        """, (name, period_days, start, legs, face, floor, cap, dau, datetime.now().isoformat()))
        conn.commit()
        line_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.close()

        await upd.message.reply_text(
            "✅ Đã tạo dây **#{}** – **{}** ({})\n"
            "• Mở: {}\n"
            "• Số chân: {}\n"
            "• Mệnh giá: {}\n"
            "• Sàn: {}% · Trần: {}% · Đầu thảo: {}%\n"
            .format(line_id, name, "Hụi tuần" if period_days==7 else "Hụi tháng",
                    start, legs, fmt_money(face), floor, cap, dau),
            parse_mode="Markdown"
        )
    except Exception as e:
        await upd.message.reply_text(
            "❌ Sai cú pháp.\n"
            "Ví dụ: `/tao Hui10tr tuan 10-10-2025 12 10tr 8 20 50`",
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
        out.append(f"• #{r[0]} · {r[1]} · {kind} · mở {r[3]} · chân {r[4]} · mệnh giá {r[5]:,} đ · sàn {r[6]}% · trần {r[7]}% · đầu thảo {r[8]}% · {r[9]}")
    await upd.message.reply_text("\n".join(out), parse_mode="Markdown")

async def cmd_tham(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        line_id = int(ctx.args[0])
        k = int(ctx.args[1])
        bid = vn_money_to_int(ctx.args[2])
        dt = ctx.args[3] if len(ctx.args)>=4 else datetime.now().strftime("%d-%m-%Y")
    except:
        return await upd.message.reply_text("❌ Cú pháp: `/tham <ma_day> <ky> <so_tien_tham> [DD-MM-YYYY]`", parse_mode="Markdown")

    line = load_line(line_id)
    if not line: return await upd.message.reply_text("❌ Không tìm thấy dây.")
    if not (1 <= k <= line["legs"]):
        return await upd.message.reply_text(f"❌ Kỳ phải trong [1..{line['legs']}].")

    # kiểm tra sàn/trần
    floor_v = int(line["face_value"]*line["floor_pct"]/100.0)
    cap_v   = int(line["face_value"]*line["cap_pct"]/100.0)
    if bid < floor_v or bid > cap_v:
        return await upd.message.reply_text(
            f"❌ Số thăm {fmt_money(bid)} ngoài biên.\n"
            f"• Sàn: {fmt_money(floor_v)} ({line['floor_pct']}%)\n"
            f"• Trần: {fmt_money(cap_v)} ({line['cap_pct']}%)")

    conn = db()
    try:
        conn.execute("INSERT INTO auctions(line_id,k,bid_amount,bid_date) VALUES(?,?,?,?)",
                     (line_id, k, bid, dt))
        conn.commit()
    except sqlite3.IntegrityError:
        conn.close()
        return await upd.message.reply_text("⚠️ Kỳ này đã có thăm. Dùng số khác hoặc /tomtat để xem.")
    conn.close()

    # tính nhanh kết quả kỳ k
    M,N,D = line["face_value"], line["legs"], line["dau_thao_pct"]/100.0
    received = payout_for_k(M, N, D, bid)

    await upd.message.reply_text(
        "✅ Đã ghi **thăm kỳ {}** cho dây **#{}** – {}\n"
        "• Thăm: {}\n• Ngày: {}\n• Ước tiền nhận hốt: **{}**"
        .format(k, line_id, line["name"], fmt_money(bid), dt, fmt_money(received)),
        parse_mode="Markdown"
    )

async def cmd_tomtat(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try: line_id = int(ctx.args[0])
    except: return await upd.message.reply_text("❌ Cú pháp: `/tomtat <ma_day>`", parse_mode="Markdown")

    line = load_line(line_id)
    if not line: return await upd.message.reply_text("❌ Không tìm thấy dây.")
    conn = db()
    df = pd.read_sql_query("SELECT k, bid_amount, bid_date FROM auctions WHERE line_id=? ORDER BY k", conn, params=(line_id,))
    conn.close()

    M,N,D = line["face_value"], line["legs"], line["dau_thao_pct"]/100.0
    total_paid_before_k = 0
    rows = []
    for i in range(1, N+1):
        bid = int(df[df["k"]==i]["bid_amount"].iloc[0]) if (not df.empty and (df["k"]==i).any()) else None
        rec = payout_for_k(M,N,D,bid) if bid is not None else None
        if bid is not None:
            # trước kỳ i, mỗi chân sống đã góp (i-1)*M
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
        return await upd.message.reply_text("❌ Cú pháp: `/hoitot <ma_day> [roi|lai]`", parse_mode="Markdown")
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
        # nếu kỳ đã có thăm => dùng đúng số đó; nếu chưa có, dùng mức… sàn để gợi ý thận trọng
        if not existing.empty and (existing["k"]==i).any():
            bid = int(existing[existing["k"]==i]["bid_amount"].iloc[0])
        else:
            bid = int(M*line["floor_pct"]/100.0)
        rec = payout_for_k(M,N,D,bid)
        contrib_paid = (i-1)*M
        roi = roi_for_k(M, contrib_paid, rec)
        key = (roi if metric=="roi" else (rec - contrib_paid))
        if key > best_val:
            best_val, best_k, best_roi = key, i, roi

    await upd.message.reply_text(
        "🔎 **Gợi ý hốt theo {}** cho dây **#{} – {}**:\n"
        "• Nên hốt **kỳ {}** (ngày {})\n"
        "• ROI ước tính: **{:.2f}%**"
        .format("ROI%" if metric=="roi" else "lãi tuyệt đối",
                line_id, line["name"],
                best_k, k_date(line["start_date"], line["period_days"], best_k).strftime("%d-%m-%Y"),
                best_roi*100),
        parse_mode="Markdown"
    )

async def cmd_hen(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        line_id = int(ctx.args[0])
        hhmm = ctx.args[1]
        assert re.fullmatch(r"\d{2}:\d{2}", hhmm)
    except:
        return await upd.message.reply_text("❌ Cú pháp: `/hen <ma_day> <HH:MM>`", parse_mode="Markdown")
    conn = db()
    conn.execute("INSERT INTO reminders(line_id, at_hhmm) VALUES(?,?)", (line_id, hhmm))
    conn.commit(); conn.close()
    await upd.message.reply_text(f"⏰ Đã đặt nhắc cho dây #{line_id} lúc {hhmm} mỗi {'tuần' if load_line(line_id)['period_days']==7 else 'tháng'}. "
                                 f\"Tuần này bạn đoán thăm bao nhiêu? 😉\"")

async def cmd_dong(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try: line_id = int(ctx.args[0])
    except: return await upd.message.reply_text("❌ Cú pháp: `/dong <ma_day>`", parse_mode="Markdown")
    conn = db()
    conn.execute("UPDATE lines SET status='CLOSED' WHERE id=?", (line_id,))
    conn.commit(); conn.close()
    await upd.message.reply_text(f"🗂️ Đã đóng & lưu trữ dây #{line_id}.")

# ================== JOBS ==================
async def monthly_report(ctx: ContextTypes.DEFAULT_TYPE):
    cfg = load_cfg(); chat_id = cfg.get("report_chat_id")
    if not chat_id: return
    today = datetime.now().date()
    if today.day != 1:  # gửi mùng 1
        return
    conn = db()
    rows = conn.execute("SELECT id,name,period_days,start_date,legs,face_value,floor_pct,cap_pct,dau_thao_pct,status FROM lines").fetchall()
    conn.close()
    if not rows:
        return await ctx.bot.send_message(chat_id=chat_id, text="📊 Báo cáo tháng: chưa có dây.")
    lines = []
    for r in rows:
        line = {"id": r[0],"name": r[1],"period_days": r[2],"start_date": r[3],"legs": r[4],
                "face_value": r[5],"floor_pct": r[6],"cap_pct": r[7],"dau_thao_pct": r[8],"status": r[9]}
        lines.append(f"#{line['id']} · {line['name']} · {'Tuần' if line['period_days']==7 else 'Tháng'} · mệnh giá {line['face_value']:,} đ · đầu thảo {line['dau_thao_pct']}% · trạng thái {line['status']}")
    await ctx.bot.send_message(chat_id=chat_id, text="📊 **Báo cáo tháng**:\n" + "\n".join(lines), parse_mode="Markdown")

async def daily_reminders(ctx: ContextTypes.DEFAULT_TYPE):
    """Mỗi phút quét lịch nhắc theo HH:MM; gửi câu dí dỏm tuỳ dây tuần/tháng."""
    now = datetime.now()
    hhmm = now.strftime("%H:%M")
    conn = db()
    rs = conn.execute("""
        SELECT r.line_id, r.at_hhmm, l.name, l.period_days
        FROM reminders r JOIN lines l ON l.id=r.line_id
        WHERE r.at_hhmm=?
    """, (hhmm,)).fetchall()
    conn.close()
    for line_id, hhmm, name, pdays in rs:
        style = "tuần" if pdays==7 else "tháng"
        text = (f"⏰ Nhắc dây #{line_id} – {name} ({style})\n"
                f"Hôm nay là giờ nhắc {hhmm} — bạn đoán **thăm** bao nhiêu đây? 😉\n"
                f"Gõ: `/tham {line_id} <ky> <so_tien_tham>`",
                )
        await ctx.bot.send_message(chat_id=load_cfg().get("report_chat_id", None) or ctx.application.bot.id, text=text[0], parse_mode="Markdown")

def schedule_jobs(app):
    # báo cáo tháng – cứ 24h tick 1 lần, hàm tự kiểm tra mùng 1
    app.job_queue.run_repeating(monthly_report, interval=24*60*60, first=10)
    # nhắc giờ theo phút
    app.job_queue.run_repeating(daily_reminders, interval=60, first=15)

# ================== MAIN ==================
def main():
    init_db()
    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("baocao", cmd_baocao))
    app.add_handler(CommandHandler("tao", cmd_tao))
    app.add_handler(CommandHandler("danhsach", cmd_danhsach))
    app.add_handler(CommandHandler("tham", cmd_tham))
    app.add_handler(CommandHandler("tomtat", cmd_tomtat))
    app.add_handler(CommandHandler("hoitot", cmd_hoitot))
    app.add_handler(CommandHandler("hen", cmd_hen))
    app.add_handler(CommandHandler("dong", cmd_dong))

    schedule_jobs(app)
    print("✅ Hụi Bot đang chạy…")
    app.run_polling()

if __name__ == "__main__":
    main()