# hui_bot_fresh.py
# Dependencies: python-telegram-bot==20.3, pandas
import os, sqlite3, json, asyncio
from datetime import datetime, timedelta, time as dtime
import pandas as pd
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# ========= CONFIG =========
TOKEN = (os.getenv("TELEGRAM_TOKEN") or os.getenv("BOT_TOKEN") or "").strip()
if not TOKEN:
    raise SystemExit("Missing TELEGRAM_TOKEN/BOT_TOKEN in environment variables")

DB_FILE = "hui.db"
CONFIG_FILE = "config.json"
REPORT_HOUR = 8  # Gửi báo cáo lúc 08:00 hằng ngày (mặc định chỉ gửi mùng 1)
# =========================

# ---------- DB ----------
def db():
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def init_db():
    conn = db()
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS lines(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        period_days INTEGER NOT NULL,
        start_date TEXT NOT NULL,
        legs INTEGER NOT NULL,
        contrib INTEGER NOT NULL,
        bid_type TEXT CHECK(bid_type IN ('amount','percent')) NOT NULL,
        bid_value REAL NOT NULL,
        status TEXT DEFAULT 'OPEN',
        created_at TEXT NOT NULL
    )""")
    c.execute("""
    CREATE TABLE IF NOT EXISTS payments(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        line_id INTEGER NOT NULL,
        pay_date TEXT NOT NULL,
        amount INTEGER NOT NULL,
        FOREIGN KEY(line_id) REFERENCES lines(id) ON DELETE CASCADE
    )""")
    conn.commit(); conn.close()

def load_cfg():
    if os.path.exists(CONFIG_FILE):
        try:
            return json.load(open(CONFIG_FILE, "r", encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_cfg(cfg: dict):
    json.dump(cfg, open(CONFIG_FILE, "w", encoding="utf-8"))

def parse_date(s: str):  return datetime.strptime(s, "%Y-%m-%d")
def fmt_money(x: float): return f"{int(round(x)):,} VND"

# ---------- TÍNH TOÁN ----------
def calc_pool(line):      return int(line["legs"] * line["contrib"])
def calc_discount(line):
    pool = calc_pool(line)
    return float(line["bid_value"]) if line["bid_type"]=="amount" else pool*float(line["bid_value"])/100.0
def payout(line):         return calc_pool(line) - calc_discount(line)
def k_date(line, k):      return parse_date(line["start_date"]) + timedelta(days=(k-1)*line["period_days"])
def compute_profit_for_k(line, k):
    pay_so_far = (k-1) * line["contrib"]
    profit = payout(line) - pay_so_far
    base = pay_so_far if pay_so_far>0 else max(line["contrib"], 1)
    roi = profit / base
    return profit, roi
def best_k(line, metric="roi"):
    bestk, bestkey, bestp, bestroi = 1, -1e18, 0.0, 0.0
    for k in range(1, line["legs"]+1):
        p, r = compute_profit_for_k(line, k)
        key = r if metric=="roi" else p
        if key > bestkey:
            bestk, bestkey, bestp, bestroi = k, key, p, r
    return bestk, bestp, bestroi
def is_finished(line):
    if line["status"]=="CLOSED": return True
    last = k_date(line, line["legs"]).date()
    return datetime.now().date() >= last
def roi_to_str(r): return f"{r*100:.2f}%"

# ---------- DB helpers ----------
def load_line_full(line_id: int):
    conn = db()
    row = conn.execute("SELECT * FROM lines WHERE id=?", (line_id,)).fetchone()
    if not row:
        conn.close(); return None, pd.DataFrame()
    cols = ["id","name","period_days","start_date","legs","contrib","bid_type","bid_value","status","created_at"]
    line = dict(zip(cols, row))
    pays = pd.read_sql_query("SELECT pay_date, amount FROM payments WHERE line_id=? ORDER BY pay_date", conn, params=(line_id,))
    conn.close()
    return line, pays

# ---------- COMMANDS ----------
async def cmd_start(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = (
        "👋 Hụi Bot (SQLite, không cần Google Sheets)\n\n"
        "Lệnh chính:\n"
        "• /new <tên> <tuan|thang> <YYYY-MM-DD> <so_chan> <tien_gop> <amount|percent> <bid_value>\n"
        "  VD: /new 2tr tuan 2025-10-22 27 1000000 percent 12.5\n"
        "• /list — liệt kê dây\n"
        "• /addpay <line_id> <YYYY-MM-DD> <so_tien>\n"
        "• /summary <line_id>\n"
        "• /whenhot <line_id> [roi|lai]\n"
        "• /close <line_id>\n"
        "• /setreport [chat_id] — bật báo cáo tự động hàng tháng\n"
        "\n📌 Mẹo: chỉ cần nhập **số tiền & ngày** bằng /addpay, bot tự tính toàn bộ."
    )
    await upd.message.reply_text(msg)

async def cmd_setreport(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cfg = load_cfg()
    if ctx.args:
        try: cid = int(ctx.args[0])
        except: return await upd.message.reply_text("❌ chat_id không hợp lệ.")
    else:
        cid = upd.effective_chat.id
    cfg["report_chat_id"] = cid
    save_cfg(cfg)
    await upd.message.reply_text(f"✅ Đã lưu nơi nhận báo cáo tự động: {cid} — bot sẽ gửi vào 08:00 ngày 1 hàng tháng.")

async def cmd_new(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        name, kind, start, legs, contrib, bid_type, bid_val = ctx.args
        period_days = 7 if kind.lower() in ["tuan","tuần","week","weekly"] else 30
        legs, contrib = int(legs), int(contrib)
        bid_type = bid_type.lower()
        if bid_type not in ("amount","percent"): raise ValueError("bid_type phải là amount hoặc percent")
        bid_val = float(bid_val)
        conn = db()
        conn.execute("""INSERT INTO lines(name,period_days,start_date,legs,contrib,bid_type,bid_value,status,created_at)
                        VALUES(?,?,?,?,?,?,?,'OPEN',?)""",
                     (name, period_days, start, legs, contrib, bid_type, bid_val, datetime.now().isoformat()))
        conn.commit()
        line_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.close()
        await upd.message.reply_text(
            f"✅ Đã tạo dây #{line_id} ({name}) — {'Hụi Tuần' if period_days==7 else 'Hụi Tháng'}\n"
            f"Số chân: {legs} | Góp/kỳ: {contrib:,} VND | Giá hốt: {bid_type} {bid_val}"
        )
    except Exception as e:
        await upd.message.reply_text(f"❌ Sai cú pháp.\nVD: /new 2tr tuan 2025-10-22 27 1000000 percent 12.5\n{e}")

async def cmd_list(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    conn = db()
    rows = conn.execute("SELECT id,name,period_days,start_date,legs,contrib,bid_type,bid_value,status FROM lines ORDER BY id DESC").fetchall()
    conn.close()
    if not rows: return await upd.message.reply_text("📂 Chưa có dây nào.")
    out = ["📋 Danh sách dây:"]
    for r in rows:
        kind = "Tuần" if r[2]==7 else "Tháng"
        out.append(f"#{r[0]} · {r[1]} · {kind} · mở {r[3]} · chân {r[4]} · góp/kỳ {r[5]:,} VND · {r[8]}")
    await upd.message.reply_text("\n".join(out))

async def cmd_addpay(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        line_id = int(ctx.args[0]); dt = ctx.args[1]; amt = int(ctx.args[2])
    except:
        return await upd.message.reply_text("❌ Cú pháp: /addpay <line_id> <YYYY-MM-DD> <so_tien>")
    conn = db()
    row = conn.execute("SELECT status FROM lines WHERE id=?", (line_id,)).fetchone()
    if not row: 
        conn.close(); return await upd.message.reply_text("❌ Không tìm thấy dây.")
    if row[0] != "OPEN":
        conn.close(); return await upd.message.reply_text("⚠️ Dây đã đóng.")
    conn.execute("INSERT INTO payments(line_id,pay_date,amount) VALUES(?,?,?)", (line_id, dt, amt))
    conn.commit(); conn.close()
    await upd.message.reply_text(f"✅ Đã ghi đóng góp {amt:,} VND cho dây #{line_id} ({dt})")

async def cmd_summary(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try: line_id = int(ctx.args[0])
    except: return await upd.message.reply_text("❌ Cú pháp: /summary <line_id>")
    line, pays = load_line_full(line_id)
    if not line: return await upd.message.reply_text("❌ Không tìm thấy dây.")
    pool = calc_pool(line); pay_now = payout(line)
    total_paid = int(pays["amount"].sum()) if not pays.empty else 0
    k_now = max(1, min(len(pays)+1, line["legs"]))
    pr, ro = compute_profit_for_k(line, k_now)
    bestk, bestp, bestroi = best_k(line, metric="roi")
    msg = [
        f"📌 Dây #{line['id']} · {line['name']} · {'Tuần' if line['period_days']==7 else 'Tháng'}",
        f"• Mở: {line['start_date']} · Chân: {line['legs']} · Góp/kỳ: {line['contrib']:,} VND",
        f"• Pool/kỳ: {pool:,} VND · Giá hốt: {line['bid_type']} {line['bid_value']} → Tiền nhận hốt: {pay_now:,} VND",
        f"• Đã đóng: {total_paid:,} VND · số lần: {len(pays)}",
        f"• Kỳ hiện tại: {k_now} → Lãi: {int(round(pr)):,} VND (ROI: {roi_to_str(ro)})",
        f"⭐ Kỳ tối ưu (ROI): {bestk} · ngày: {k_date(line,bestk).date()} · Lãi: {int(round(bestp)):,} VND · ROI: {roi_to_str(bestroi)}"
    ]
    if is_finished(line):
        msg.append("✅ Dây đã đến hạn — /close để lưu trữ.")
    await upd.message.reply_text("\n".join(msg))

async def cmd_whenhot(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if len(ctx.args)<1: return await upd.message.reply_text("❌ Cú pháp: /whenhot <line_id> [roi|lai]")
    line_id = int(ctx.args[0]); metric = ctx.args[1].lower() if len(ctx.args)>=2 else "roi"
    if metric not in ("roi","lai"): metric="roi"
    line, _ = load_line_full(line_id)
    if not line: return await upd.message.reply_text("❌ Không tìm thấy dây.")
    kbest, p, r = best_k(line, metric=metric if metric=="roi" else "lai")
    await upd.message.reply_text(
        f"🔎 Gợi ý theo {'ROI%' if metric=='roi' else 'Lãi tuyệt đối'}:\n"
        f"• Kỳ nên hốt: {kbest}\n"
        f"• Ngày dự kiến: {k_date(line,kbest).date()}\n"
        f"• Tiền nhận hốt/kỳ: {int(round(payout(line))):,} VND\n"
        f"• Lãi ước tính nếu hốt kỳ này: {int(round(p)):,} VND — ROI: {roi_to_str(r)}"
    )

async def cmd_close(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try: line_id = int(ctx.args[0])
    except: return await upd.message.reply_text("❌ Cú pháp: /close <line_id>")
    conn = db(); conn.execute("UPDATE lines SET status='CLOSED' WHERE id=?", (line_id,))
    conn.commit(); conn.close()
    await upd.message.reply_text(f"🗂️ Đã đóng & lưu trữ dây #{line_id}.")

# ----- TỰ ĐỘNG BÁO CÁO (không dùng JobQueue) -----
async def monthly_report_loop(app):
    """Chạy nền: ngủ tới 08:00 hàng ngày; chỉ gửi vào mùng 1."""
    while True:
        now = datetime.now()
        target = datetime.combine(now.date(), dtime(hour=REPORT_HOUR))
        if now >= target:
            target += timedelta(days=1)
        await asyncio.sleep((target - now).total_seconds())
        await send_monthly_report_bot(app)

async def send_monthly_report_bot(app):
    cfg = load_cfg()
    chat_id = cfg.get("report_chat_id")
    if not chat_id: 
        return
    today = datetime.now().date()
    if today.day != 1:
        return
    # tổng hợp nhẹ
    conn = db()
    rows = conn.execute(
        "SELECT id,name,period_days,start_date,legs,contrib,bid_type,bid_value,status FROM lines"
    ).fetchall()
    conn.close()
    if not rows:
        return await app.bot.send_message(chat_id=chat_id, text="📊 Báo cáo tháng: chưa có dây.")
    lines = []
    for r in rows:
        line = {"id": r[0],"name": r[1],"period_days": r[2],"start_date": r[3],
                "legs": r[4],"contrib": r[5],"bid_type": r[6],"bid_value": r[7],"status": r[8]}
        conn = db()
        total_paid = conn.execute("SELECT COALESCE(SUM(amount),0) FROM payments WHERE line_id=?", (line["id"],)).fetchone()[0]
        conn.close()
        k_now = max(1, 1 if total_paid>0 else 1)
        pr, ro = compute_profit_for_k(line, k_now)
        bestk, bestp, bestroi = best_k(line, metric="roi")
        lines.append(
            f"#{line['id']} · {line['name']} · {('Tuần' if line['period_days']==7 else 'Tháng')} · "
            f"góp/kỳ: {line['contrib']:,} · Lãi@k_now: {int(round(pr)):,} ({roi_to_str(ro)}) · "
            f"Kỳ tối ưu: {bestk} ({roi_to_str(bestroi)})"
        )
    txt = "📊 Báo cáo tháng:\n" + "\n".join(lines)
    await app.bot.send_message(chat_id=chat_id, text=txt)

# ---------- MAIN ----------
async def _post_init(app):
    # Mở HTTP keep-alive để Render pass port-scan
    await start_keepalive_server()
    # Bật vòng lặp báo cáo nền (không dùng JobQueue)
    asyncio.create_task(monthly_report_loop(app))
    print("🕒 Đã bật vòng lặp báo cáo nền (không dùng JobQueue).")
    
import asyncio, os

async def start_keepalive_server():
    """HTTP server tối giản để Render thấy cổng đang mở."""
    port = int(os.getenv("PORT", "10000"))

    async def handle_client(reader, writer):
        try:
            # Đọc request (bỏ nội dung)
            await reader.read(1024)
            # Trả về 200 OK siêu gọn
            resp = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK"
            writer.write(resp)
            await writer.drain()
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    server = await asyncio.start_server(handle_client, host="0.0.0.0", port=port)
    sockets = ", ".join(str(s.getsockname()) for s in (server.sockets or []))
    print(f"🌐 Keep-alive HTTP on {sockets}")
    return server
    
def main():
    init_db()
    app = ApplicationBuilder().token(TOKEN).post_init(_post_init).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("setreport", cmd_setreport))
    app.add_handler(CommandHandler("new", cmd_new))
    app.add_handler(CommandHandler("list", cmd_list))
    app.add_handler(CommandHandler("addpay", cmd_addpay))
    app.add_handler(CommandHandler("summary", cmd_summary))
    app.add_handler(CommandHandler("whenhot", cmd_whenhot))
    app.add_handler(CommandHandler("close", cmd_close))

    print("✅ Hụi Bot (Render) đang chạy...")
    app.run_polling()

if __name__ == "__main__":
    main()
