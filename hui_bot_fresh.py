# hui_bot_fresh.py
# Dependencies: python-telegram-bot==20.3, pandas
import os, sqlite3, json, asyncio, random
from datetime import datetime, timedelta, time as dtime, date
import pandas as pd
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# ========= CONFIG =========
TOKEN = (os.getenv("TELEGRAM_TOKEN") or os.getenv("BOT_TOKEN") or "").strip()
if not TOKEN:
    raise SystemExit("Missing TELEGRAM_TOKEN/BOT_TOKEN in environment variables")

DB_FILE = "hui.db"
CONFIG_FILE = "config.json"

REPORT_HOUR = 8  # 08:00 gửi báo cáo tháng (chỉ mùng 1)
REMINDER_TICK_SECONDS = 60  # vòng lặp check nhắc hẹn
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
    Chuyển '1tr'/'1000k'/'1000n' -> 1_000_000; '1k'/'1n' -> 1_000; '100k'/'100n' -> 100_000; hỗ trợ số thập phân.
    Hỗ trợ thêm 'm'/'t' ~ triệu.
    """
    s = str(text).strip().lower().replace(",", "").replace("_", "").replace(" ", "")
    if s.isdigit():
        return int(s)
    try:
        if s.endswith("tr"):
            num = float(s[:-2])
            return int(num * 1_000_000)
        elif s.endswith("k") or s.endswith("n"):
            num = float(s[:-1])
            return int(num * 1_000)
        elif s.endswith("m") or s.endswith("t"):
            num = float(s[:-1])
            return int(num * 1_000_000)
        else:
            return int(float(s))
    except Exception:
        raise ValueError(f"Khong hieu gia tri tien: {text}")

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
        remind_hour INTEGER DEFAULT 8,            -- giờ nhắc hẹn mỗi dây (0..23)
        remind_min  INTEGER DEFAULT 0,            -- phút nhắc (0..59)
        last_remind_iso TEXT                      -- YYYY-MM-DD của lần nhắc gần nhất (chống gửi trùng)
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
    """Migration nhẹ khi DB đã có trước."""
    conn = db(); cur = conn.cursor()
    for col, decl in [
        ("base_rate", "REAL DEFAULT 0"),
        ("cap_rate",  "REAL DEFAULT 100"),
        ("thau_rate", "REAL DEFAULT 0"),
        ("remind_hour", "INTEGER DEFAULT 8"),
        ("remind_min",  "INTEGER DEFAULT 0"),
        ("last_remind_iso", "TEXT")
    ]:
        try:
            cur.execute(f"ALTER TABLE lines ADD COLUMN {col} {decl}")
        except Exception:
            pass
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

# ---------- Helpers & Tính toán ----------
def k_date(line, k: int) -> datetime:
    """Ngày của kỳ k (k>=1)."""
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
      M = mệnh giá, N = số chân, T_k = thăm thực tế kỳ k
      D = thau_rate% * M (đầu thảo cố định theo mệnh giá, trừ mỗi kỳ)
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

# ---------- COMMANDS ----------
async def cmd_start(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = (
        "👋 HUI BOT – phien ban SQLite (khong can Google Sheets)\n\n"
        "🌟 LENH CHINH (khong dau, ngay DD-MM-YYYY):\n\n"
        "1) Tao day:\n"
        "   /tao <ten> <tuan|thang> <DD-MM-YYYY> <so_chan> <menh_gia> <gia_san_%> <gia_tran_%> <dau_thao_%>\n"
        "   Vi du: /tao Hui10tr tuan 10-10-2025 12 10000000 8 20 50\n"
        "   💡 Tien co the viet: 5tr, 250k, 1n, 1000k, 2.5tr...\n\n"
        "2) Nhap tham ky:\n"
        "   /tham <ma_day> <ky> <so_tien_tham> [DD-MM-YYYY]\n"
        "   Vi du: /tham 1 1 2tr 10-10-2025\n\n"
        "3) Dat gio nhac rieng cho tung day:\n"
        "   /hen <ma_day> <HH:MM>\n"
        "   Vi du: /hen 1 07:45\n\n"
        "4) Danh sach / Tom tat / Goi y hot:\n"
        "   /danhsach\n"
        "   /tomtat <ma_day>\n"
        "   /hoitot <ma_day> [roi|lai]\n\n"
        "5) Dong day:\n"
        "   /dong <ma_day>\n\n"
        "6) Cai dat noi nhan bao cao thang (08:00 mùng 1):\n"
        "   /baocao [chat_id]\n"
        "   Vi du: /baocao   (gui ve chat hien tai)\n\n"
        "💡 Den dung ngay mo ky, bot se nhac vui: “Tuan/Thang nay doan tham bao nhieu?”"
    )
    await upd.message.reply_text(msg)

async def cmd_setreport(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cfg = load_cfg()
    if ctx.args:
        try:
            cid = int(ctx.args[0])
        except Exception:
            return await upd.message.reply_text("❌ chat_id khong hop le.")
    else:
        cid = upd.effective_chat.id
    cfg["report_chat_id"] = cid
    save_cfg(cfg)
    await upd.message.reply_text(
        f"✅ Da luu noi nhan bao cao/nhac hen: {cid} — bot se gui tu dong."
    )

async def cmd_new(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        # /tao <ten> <tuan|thang> <DD-MM-YYYY> <so_chan> <menh_gia> <gia_san_%> <gia_tran_%> <dau_thao_%>
        name, kind, start_user, legs, contrib, base_rate, cap_rate, thau_rate = ctx.args
        start_dt  = parse_user_date(start_user)
        start_iso = to_iso_str(start_dt)
        period_days = 7 if kind.lower() in ["tuan","tuần","week","weekly"] else 30
        legs    = int(legs)
        contrib = parse_money(contrib)  # <<< dùng parser tiền rút gọn
        base_rate = float(base_rate)
        cap_rate  = float(cap_rate)
        thau_rate = float(thau_rate)

        if not (0 <= base_rate <= cap_rate <= 100):
            raise ValueError("gia_san_% <= gia_tran_% va trong [0..100]")
        if not (0 <= thau_rate <= 100):
            raise ValueError("dau_thao_% trong [0..100]")

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
            f"✅ Tao day #{line_id} ({name}) — {'Hui Tuan' if period_days==7 else 'Hui Thang'}\n"
            f"Mo: {to_user_str(start_dt)} · Chan: {legs} · Menh gia: {contrib:,} VND\n"
            f"SAN: {base_rate:.2f}% · TRAN: {cap_rate:.2f}% · DAU THAO: {thau_rate:.2f}% tren M\n"
            f"⏰ Nhac mac dinh: 08:00 (dung /hen {line_id} HH:MM de doi)\n"
            f"➡️ Nhap tham: /tham {line_id} <ky> <so_tien_tham> [DD-MM-YYYY]"
        )
    except Exception as e:
        await upd.message.reply_text(
            "❌ Cu phap: /tao <ten> <tuan|thang> <DD-MM-YYYY> <so_chan> <menh_gia> <gia_san_%> <gia_tran_%> <dau_thao_%>\n"
            "VD: /tao Hui10tr tuan 10-10-2025 12 10000000 8 20 50\n"
            f"Loi: {e}"
        )

async def cmd_tham(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if len(ctx.args) < 3:
        return await upd.message.reply_text("❌ Cu phap: /tham <ma_day> <ky> <so_tien_tham> [DD-MM-YYYY]")
    try:
        line_id = int(ctx.args[0])
        k       = int(ctx.args[1])
        bid     = parse_money(ctx.args[2])  # <<< dùng parser tiền rút gọn
        rdate   = None
        if len(ctx.args) >= 4:
            rdate = to_iso_str(parse_user_date(ctx.args[3]))
    except Exception as e:
        return await upd.message.reply_text(f"❌ Tham so khong hop le: {e}")

    line, _ = load_line_full(line_id)
    if not line:
        return await upd.message.reply_text("❌ Khong tim thay day.")
    if not (1 <= k <= int(line["legs"])):
        return await upd.message.reply_text(f"❌ Ky hop le 1..{line['legs']}.")

    M = int(line["contrib"])
    min_bid = int(round(M * float(line.get("base_rate", 0)) / 100.0))
    max_bid = int(round(M * float(line.get("cap_rate", 100)) / 100.0))
    if bid < min_bid or bid > max_bid:
        return await upd.message.reply_text(
            f"❌ Tham phai trong [{min_bid:,} .. {max_bid:,}] VND "
            f"(SAN {line['base_rate']}% · TRAN {line['cap_rate']}% tren M={M:,})"
        )

    conn = db()
    conn.execute("""
        INSERT INTO rounds(line_id,k,bid,round_date) VALUES(?,?,?,?)
        ON CONFLICT(line_id,k) DO UPDATE SET bid=excluded.bid, round_date=excluded.round_date
    """, (line_id, k, bid, rdate))
    conn.commit(); conn.close()

    await upd.message.reply_text(
        f"✅ Luu tham ky {k} cho day #{line_id}: {bid:,} VND"
        + (f" · ngay {ctx.args[3]}" if len(ctx.args)>=4 else "")
    )

async def cmd_set_remind(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if len(ctx.args) != 2:
        return await upd.message.reply_text("❌ Cu phap: /hen <ma_day> <HH:MM>  (VD: /hen 1 07:45)")
    try:
        line_id = int(ctx.args[0])
        hhmm = ctx.args[1]
        hh, mm = hhmm.split(":")
        hh = int(hh); mm = int(mm)
        if not (0 <= hh <= 23 and 0 <= mm <= 59):
            raise ValueError("gio phut khong hop le")
    except Exception as e:
        return await upd.message.reply_text(f"❌ Tham so khong hop le: {e}")

    line, _ = load_line_full(line_id)
    if not line:
        return await upd.message.reply_text("❌ Khong tim thay day.")
    conn = db()
    conn.execute("UPDATE lines SET remind_hour=?, remind_min=? WHERE id=?", (hh, mm, line_id))
    conn.commit(); conn.close()
    await upd.message.reply_text(f"✅ Da dat gio nhac cho day #{line_id}: {hh:02d}:{mm:02d}")

async def cmd_list(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    conn = db()
    rows = conn.execute(
        "SELECT id,name,period_days,start_date,legs,contrib,base_rate,cap_rate,thau_rate,status,remind_hour,remind_min "
        "FROM lines ORDER BY id DESC"
    ).fetchall()
    conn.close()
    if not rows:
        return await upd.message.reply_text("📂 Chua co day nao.")
    out = ["📋 Danh sach day:"]
    for r in rows:
        kind = "Tuan" if r[2]==7 else "Thang"
        out.append(
            f"#{r[0]} · {r[1]} · {kind} · mo {to_user_str(parse_iso(r[3]))} · chan {r[4]} · M {r[5]:,} VND · "
            f"SAN {r[6]:.2f}% · TRAN {r[7]:.2f}% · THAO {r[8]:.2f}% · nhac {int(r[10]):02d}:{int(r[11]):02d} · {r[9]}"
        )
    await upd.message.reply_text("\n".join(out))

async def cmd_summary(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        line_id = int(ctx.args[0])
    except Exception:
        return await upd.message.reply_text("❌ Cu phap: /tomtat <ma_day>")

    line, _ = load_line_full(line_id)
    if not line:
        return await upd.message.reply_text("❌ Khong tim thay day.")

    bids = get_bids(line_id)
    M, N = int(line["contrib"]), int(line["legs"])
    cfg_line = f"SAN {float(line.get('base_rate',0)):.2f}% · TRAN {float(line.get('cap_rate',100)):.2f}% · DAU THAO {float(line.get('thau_rate',0)):.2f}% tren M"
    k_now = max(1, min(len(bids)+1, N))
    p, r, po, paid = compute_profit_var(line, k_now, bids)
    bestk, (bp, br, bpo, bpaid) = best_k_var(line, bids, metric="roi")

    msg = [
        f"📌 Day #{line['id']} · {line['name']} · {'Tuan' if line['period_days']==7 else 'Thang'}",
        f"• Mo: {to_user_str(parse_iso(line['start_date']))} · Chan: {N} · Menh gia: {M:,} VND",
        f"• {cfg_line} · Nhac {int(line.get('remind_hour',8)):02d}:{int(line.get('remind_min',0)):02d}",
        f"• Tham: " + (", ".join([f"k{kk}:{int(b):,}" for kk,b in sorted(bids.items())]) if bids else "(chua co)"),
        f"• Ky hien tai uoc tinh: {k_now} · Payout: {po:,} · Da dong: {paid:,} → Lai: {int(round(p)):,} (ROI {roi_to_str(r)})",
        f"⭐ De xuat (ROI): ky {bestk} · ngay {to_user_str(k_date(line,bestk))} · Payout {bpo:,} · Da dong {bpaid:,} · Lai {int(round(bp)):,} · ROI {roi_to_str(br)}"
    ]
    if is_finished(line):
        msg.append("✅ Day da den han — /dong de luu tru.")
    await upd.message.reply_text("\n".join(msg))

async def cmd_whenhot(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if len(ctx.args) < 1:
        return await upd.message.reply_text("❌ Cu phap: /hoitot <ma_day> [roi|lai]")
    try:
        line_id = int(ctx.args[0])
    except Exception:
        return await upd.message.reply_text("❌ ma_day phai la so.")
    metric = ctx.args[1].lower() if len(ctx.args) >= 2 else "roi"
    if metric not in ("roi", "lai"):
        metric = "roi"

    line, _ = load_line_full(line_id)
    if not line: return await upd.message.reply_text("❌ Khong tim thay day.")
    bids = get_bids(line_id)

    bestk, (bp, br, bpo, bpaid) = best_k_var(line, bids, metric=("roi" if metric=="roi" else "lai"))
    await upd.message.reply_text(
        f"🔎 Goi y theo {'ROI%' if metric=='roi' else 'Lai'}:\n"
        f"• Ky nen hot: {bestk}\n"
        f"• Ngay du kien: {to_user_str(k_date(line,bestk))}\n"
        f"• Payout ky do: {bpo:,}\n"
        f"• Da dong truoc do: {bpaid:,}\n"
        f"• Lai uoc tinh: {int(round(bp)):,} — ROI: {roi_to_str(br)}"
    )

async def cmd_close(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        line_id = int(ctx.args[0])
    except Exception:
        return await upd.message.reply_text("❌ Cu phap: /dong <ma_day>")
    conn = db()
    conn.execute("UPDATE lines SET status='CLOSED' WHERE id=?", (line_id,))
    conn.commit(); conn.close()
    await upd.message.reply_text(f"🗂️ Da dong & luu tru day #{line_id}.")

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
        return await app.bot.send_message(chat_id=chat_id, text="📊 Bao cao thang: chua co day.")

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
            f"#{line['id']} · {line['name']} · {('Tuan' if line['period_days']==7 else 'Thang')} · "
            f"M {int(line['contrib']):,} · SAN {float(line['base_rate']):.1f}% · TRAN {float(line['cap_rate']):.1f}% · THAO {float(line['thau_rate']):.1f}% · "
            f"Ky_now {k_now}: Lai {int(round(p)):,} ({roi_to_str(ro)}) · Best k{bestk} {roi_to_str(br)}"
        )

    txt = "📊 Bao cao thang:\n" + "\n".join(lines)
    await app.bot.send_message(chat_id=chat_id, text=txt)

# ----- NHẮC HẸN DÍ DỎM THEO KỲ (TUỲ GIỜ TỪNG DÂY) -----
async def send_periodic_reminders(app):
    cfg = load_cfg(); chat_id = cfg.get("report_chat_id")
    if not chat_id: return

    today = datetime.now()
    now_d = today.date()
    hh = today.hour; mm = today.minute

    weekly_prompts = [
        "⏰ Tuan nay doan tham bao nhieu?",
        "🤔 Ban nghi ky nay tham se ve muc nao?",
        "💬 Nhac nho: Nhap tham ky nay nhe!",
        "🔔 Ky moi bat dau, ban du doan tham ky nay bao nhieu?"
    ]
    monthly_prompts = [
        "📅 Thang nay doan tham bao nhieu?",
        "🗓️ Den hen lai len, tham ky nay bao nhieu day?",
        "💡 Nhac nhe: Nhap tham ky moi nhe!",
        "🔔 Thang moi bat dau, chot tham thoi!"
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

        # nếu hôm nay đã nhắc rồi thì bỏ
        if last_remind_iso == now_d.isoformat():
            continue

        # xác định kỳ hiện tại (dựa theo thăm đã nhập)
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
            f"📣 Nhac hen hưu ich cho day #{line_id} – {name}\n"
            f"• Ky {k_now}/{N} · Ngay: {to_user_str(parse_iso(start_date_str) + timedelta(days=(k_now-1)*int(period_days)))}\n"
            f"• Menh gia: {int(M):,} VND · SAN {float(base_rate):.1f}% ({min_bid:,}) · TRAN {float(cap_rate):.1f}% ({max_bid:,}) · THAO {float(thau_rate):.1f}% ({D:,})\n\n"
            f"➡️ {prompt}\n"
            f"👉 Nhap: /tham {line_id} {k_now} <so_tien_tham>"
        )
        await app.bot.send_message(chat_id=chat_id, text=txt)

        # đánh dấu đã nhắc hôm nay
        conn2 = db()
        conn2.execute("UPDATE lines SET last_remind_iso=? WHERE id=?", (now_d.isoformat(), line_id))
        conn2.commit(); conn2.close()

# ----- VÒNG LẶP NỀN -----
async def monthly_report_loop(app):
    """Mỗi ngày chờ đến REPORT_HOUR rồi gửi báo cáo tháng (nếu mùng 1)."""
    while True:
        now = datetime.now()
        target = datetime.combine(now.date(), dtime(hour=REPORT_HOUR))
        if now >= target:
            target += timedelta(days=1)
        await asyncio.sleep(max(1.0, (target - now).total_seconds()))
        await send_monthly_report_bot(app)

async def reminder_loop(app):
    """Mỗi phút check nhắc hẹn cho từng dây theo giờ cấu hình."""
    while True:
        await send_periodic_reminders(app)
        await asyncio.sleep(REMINDER_TICK_SECONDS)

# ----- HTTP keep-alive cho Render Web Service -----
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
    print("🕒 Nen: bao cao thang & nhac hen da bat.")

# ---------- MAIN ----------
def main():
    init_db()
    ensure_schema()

    app = ApplicationBuilder().token(TOKEN).post_init(_post_init).build()

    # Lệnh tiếng Việt (không dấu)
    app.add_handler(CommandHandler("start",    cmd_start))
    app.add_handler(CommandHandler("baocao",   cmd_setreport))
    app.add_handler(CommandHandler("tao",      cmd_new))
    app.add_handler(CommandHandler("tham",     cmd_tham))
    app.add_handler(CommandHandler("hen",      cmd_set_remind))
    app.add_handler(CommandHandler("danhsach", cmd_list))
    app.add_handler(CommandHandler("tomtat",   cmd_summary))
    app.add_handler(CommandHandler("hoitot",   cmd_whenhot))
    app.add_handler(CommandHandler("dong",     cmd_close))

    print("✅ Hui Bot (Render) dang chay...")
    app.run_polling()

if __name__ == "__main__":
    main()