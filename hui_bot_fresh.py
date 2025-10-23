# ===================== hui_bot_fresh.py =====================
# Telegram Hui Bot (SQLite version)
# Dependencies: python-telegram-bot==20.3, pandas
import os, sqlite3, json, asyncio, random, re, unicodedata
from datetime import datetime, timedelta, time as dtime
import pandas as pd
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters

TOKEN = (os.getenv("TELEGRAM_TOKEN") or os.getenv("BOT_TOKEN") or "").strip()
if not TOKEN:
    raise SystemExit("Missing TELEGRAM_TOKEN/BOT_TOKEN")

DB_FILE = "hui.db"
CONFIG_FILE = "config.json"
REPORT_HOUR = 8
REMINDER_TICK_SECONDS = 60

# ---------- HELPERS ----------
def strip_accents(s: str) -> str:
    return ''.join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))

def parse_user_date(s: str) -> datetime:
    """Tự động nhận nhiều định dạng ngày: 2-8-25, 2/8/25, 02-08-2025, ..."""
    s = s.strip().replace("/", "-")
    parts = s.split("-")
    if len(parts) != 3:
        raise ValueError(f"Không hiểu ngày: {s}")
    d, m, y = parts
    d, m, y = int(d), int(m), int(y)
    if y < 100: y += 2000
    return datetime(y, m, d)

def to_iso_str(d: datetime): return d.strftime("%Y-%m-%d")
def to_user_str(d: datetime): return d.strftime("%d-%m-%Y")

# (Các hàm DB, logic payout... giữ nguyên không đổi)

# ---------- AUTO NORMALIZE COMMAND (fix dấu & lỗi gõ) ----------
COMMAND_ALIASES = {
    "tảo":"tao","tào":"tao","tảo":"tao",
    "thảm":"tham","thâm":"tham","thă":"tham",
    "tòmtat":"tomtat","tỏmtat":"tomtat","tơmtat":"tomtat",
}
async def normalize_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Tự nhận dạng lệnh sai chính tả nhẹ và redirect về đúng handler."""
    text = update.message.text or ""
    cmd = text.split()[0][1:].lower()  # bỏ dấu /
    norm = strip_accents(cmd)
    if norm in COMMAND_ALIASES:
        correct = COMMAND_ALIASES[norm]
        new_cmd = f"/{correct} " + " ".join(text.split()[1:])
        update.message.text = new_cmd
        await app.process_update(update)  # forward lại
        return True
    return False

# ---------- HELP TEXT (Markdown) ----------
def help_text() -> str:
    return """👋 **HỤI BOT – phiên bản SQLite (không cần Google Sheets)**

🌟 **LỆNH CHÍNH** (không dấu, ngày **DD-MM-YYYY**):

1) Tạo dây (đủ tham số):
   `/tao <tên> <tuần|tháng> <DD-MM-YYYY> <số_chân> <mệnh_giá> <giá_sàn_%> <giá_trần_%> <đầu_thảo_%>`
   Ví dụ: `/tao Hui10tr tuần 10-10-2025 12 10tr 8 20 50`
   💡 Thiếu tham số? Gõ **/tao** trống, bot sẽ gửi **một biểu mẫu** để bạn điền 1 lần là xong.

2) Nhập thăm kỳ:
   `/tham <mã_dây> <kỳ> <số_tiền_thăm> [DD-MM-YYYY]`
   Ví dụ: `/tham 1 1 2tr 10-10-2025`

3) Đặt giờ nhắc riêng:
   `/hen <mã_dây> <HH:MM>`  (ví dụ: `/hen 1 07:45`)

4) Danh sách / Tóm tắt / Gợi ý hốt:
   `/danhsach`
   `/tomtat <mã_dây>`
   `/hottot <mã_dây> [Roi%|Lãi]`

5) Đóng dây: `/dong <mã_dây>`

6) Cài nơi nhận báo cáo & nhắc:
   `/baocao [chat_id]`

📜 Gõ /lenh để hiện lại danh sách lệnh.
"""

# ---------- /lenh hiển thị dạng nút bấm tự gửi ----------
async def cmd_lenh(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    kb = [
        [InlineKeyboardButton("🧩 /tao – Tạo dây mới", switch_inline_query_current_chat="/tao")],
        [InlineKeyboardButton("💰 /tham – Nhập thăm kỳ", switch_inline_query_current_chat="/tham")],
        [InlineKeyboardButton("📅 /hen – Đặt giờ nhắc", switch_inline_query_current_chat="/hen")],
        [InlineKeyboardButton("📋 /danhsach – Xem danh sách", switch_inline_query_current_chat="/danhsach")],
        [InlineKeyboardButton("📊 /tomtat – Tóm tắt dây", switch_inline_query_current_chat="/tomtat")],
        [InlineKeyboardButton("💡 /hottot – Gợi ý hốt", switch_inline_query_current_chat="/hottot")],
    ]
    await upd.message.reply_text(
        help_text(),
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(kb),
        disable_web_page_preview=True
    )

# ---------- /hottot (chính thức, đã fix) ----------
async def cmd_whenhot(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if len(ctx.args) < 1:
        return await upd.message.reply_text("❌ Cú pháp: /hottot <mã_dây> [Roi%|Lãi]")

    try:
        line_id = int(ctx.args[0])
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

# (Các hàm cmd_tao, cmd_tham, cmd_summary... giữ nguyên, chỉ dùng parse_user_date mới.)

# ---------- MAIN ----------
def main():
    init_db(); ensure_schema()
    global app
    app = ApplicationBuilder().token(TOKEN).build()

    # Các lệnh chính
    app.add_handler(CommandHandler("start", cmd_lenh))
    app.add_handler(CommandHandler("lenh", cmd_lenh))
    app.add_handler(CommandHandler("baocao", cmd_setreport))
    app.add_handler(CommandHandler("tao", cmd_new))
    app.add_handler(CommandHandler("tham", cmd_tham))
    app.add_handler(CommandHandler("hen", cmd_set_remind))
    app.add_handler(CommandHandler("danhsach", cmd_list))
    app.add_handler(CommandHandler("tomtat", cmd_summary))
    app.add_handler(CommandHandler("hottot", cmd_whenhot))
    app.add_handler(CommandHandler("dong", cmd_close))
    app.add_handler(CommandHandler("huy", cmd_cancel))

    # Tự sửa lỗi gõ lệnh có dấu / sai chính tả
    app.add_handler(MessageHandler(filters.COMMAND, normalize_command))
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_text))

    print("✅ Hui Bot đang chạy...")
    app.run_polling()

if __name__ == "__main__":
    main()
# ===================== END FILE =====================
