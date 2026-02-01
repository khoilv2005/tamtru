import os
import asyncio
import pymysql
import logging
from datetime import datetime, date, timedelta
from telegram.ext import Application, ContextTypes, CommandHandler
import atexit
import threading
from dotenv import load_dotenv

# Nạp biến môi trường & logging
load_dotenv()
logging.basicConfig(level=logging.ERROR, format='[%(asctime)s] %(levelname)s %(message)s')

DB_TIMEOUT = 10
DB_HOST = os.getenv("DB_HOST", "")
DB_PORT = int(os.getenv("DB_PORT", "0") or 0)
DB_USER = os.getenv("DB_USER", "")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_NAME = os.getenv("DB_NAME", "")

TOKEN = os.getenv("BOT_TOKEN", "")
CHAT_ID = os.getenv("CHAT_ID", "")


async def send_initial_messages(context: ContextTypes.DEFAULT_TYPE) -> None:
    i = "Bot bắt đầu chạy ........"
    await context.bot.send_message(chat_id=CHAT_ID, text=str(i))
    await asyncio.sleep(0.5)


class DBManager:
    """Quản lý kết nối DB persistent (đơn giản, 1 connection)."""
    _instance = None
    _lock = threading.Lock()

    def __init__(self):
        self.conn = None
        self._connect()
        self.ensure_tables()

    def _connect(self):
        if not DB_HOST or not DB_USER or not DB_NAME:
            print("[DB] Thiếu cấu hình DB (HOST/USER/NAME).")
            return
        try:
            ssl_kwargs = {}
            ca_path = os.path.join(os.getcwd(), 'ca.pem')
            if os.path.isfile(ca_path):
                ssl_kwargs = { 'ssl': {'ca': ca_path} }
                pass
            try:
                self.conn = pymysql.connect(
                    charset="utf8mb4",
                    connect_timeout=DB_TIMEOUT,
                    cursorclass=pymysql.cursors.DictCursor,
                    db=DB_NAME,
                    host=DB_HOST,
                    password=DB_PASSWORD,
                    read_timeout=DB_TIMEOUT,
                    port=DB_PORT,
                    user=DB_USER,
                    write_timeout=DB_TIMEOUT,
                    autocommit=False,
                    **ssl_kwargs,
                )
                pass
            except pymysql.MySQLError as se:
                if ssl_kwargs:
                    print(f"[DB] Lỗi kết nối SSL, thử không SSL: {se}")
                    # Thử lại không SSL
                    self.conn = pymysql.connect(
                        charset="utf8mb4",
                        connect_timeout=DB_TIMEOUT,
                        cursorclass=pymysql.cursors.DictCursor,
                        db=DB_NAME,
                        host=DB_HOST,
                        password=DB_PASSWORD,
                        read_timeout=DB_TIMEOUT,
                        port=DB_PORT,
                        user=DB_USER,
                        write_timeout=DB_TIMEOUT,
                        autocommit=False,
                    )
                    pass
                else:
                    raise
        except pymysql.MySQLError as e:
            print(f"[DB] Lỗi kết nối: {e}")
            self.conn = None

    def ensure_alive(self):
        if self.conn is None or not self.conn.open:
            pass
            self._connect()

    def ensure_tables(self):
        # Tạo database nếu chưa tồn tại rồi chọn
        if DB_HOST and DB_USER and DB_NAME:
            try:
                raw_conn = pymysql.connect(
                    charset="utf8mb4",
                    connect_timeout=DB_TIMEOUT,
                    cursorclass=pymysql.cursors.DictCursor,
                    host=DB_HOST,
                    password=DB_PASSWORD,
                    read_timeout=DB_TIMEOUT,
                    port=DB_PORT,
                    user=DB_USER,
                    write_timeout=DB_TIMEOUT,
                    autocommit=True,
                )
                with raw_conn.cursor() as cur:
                    cur.execute(f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}` CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci")
                raw_conn.close()
            except pymysql.MySQLError as e:
                print(f"[DB] Lỗi tạo database {DB_NAME}: {e}")
        # Kết nối lại với DB mới (nếu cần)
        if (self.conn is None) or (self.conn and self.conn.open and self.conn.db.decode() != DB_NAME.encode().decode()):
            try:
                if self.conn and self.conn.open:
                    self.conn.close()
                self.conn = pymysql.connect(
                    charset="utf8mb4",
                    connect_timeout=DB_TIMEOUT,
                    cursorclass=pymysql.cursors.DictCursor,
                    db=DB_NAME,
                    host=DB_HOST,
                    password=DB_PASSWORD,
                    read_timeout=DB_TIMEOUT,
                    port=DB_PORT,
                    user=DB_USER,
                    write_timeout=DB_TIMEOUT,
                    autocommit=False,
                )
                pass
            except pymysql.MySQLError as e:
                print(f"[DB] Lỗi connect DB {DB_NAME}: {e}")
        self.ensure_alive()
        if not self.conn:
            return
        ddl_users = (
            """
            CREATE TABLE IF NOT EXISTS users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                birth_date DATE NOT NULL,
                cccd VARCHAR(20) NOT NULL UNIQUE,
                room_number VARCHAR(20) NOT NULL,
                registration_date DATE NOT NULL,
                expiry_date DATE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) CHARACTER SET utf8mb4;
            """
        )
        try:
            with self.conn.cursor() as cur:
                cur.execute(ddl_users)
            self.conn.commit()
        except pymysql.MySQLError as e:
            print(f"[DB] Lỗi tạo bảng: {e}")

    def execute(self, sql: str, params=None, commit=False):
        attempts = 0
        last_err = None
        while attempts < 3:
            self.ensure_alive()
            if not self.conn:
                raise RuntimeError("DB chưa sẵn sàng")
            try:
                with self.conn.cursor() as cur:
                    cur.execute(sql, params or ())
                    if commit:
                        self.conn.commit()
                    if attempts > 0:
                        pass
                    return cur.rowcount
            except pymysql.MySQLError as e:
                last_err = e
                print(f"[DB] Lỗi execute (attempt {attempts+1}/3): {e} | SQL={sql}")
                if commit and self.conn:
                    try:
                        self.conn.rollback()
                    except Exception:
                        pass
                # Thử reconnect rồi retry
                try:
                    if self.conn and self.conn.open:
                        self.conn.close()
                except Exception:
                    pass
                self.conn = None
                attempts += 1
                if attempts < 3:
                    self._connect()
                    continue
                break
        raise last_err if last_err else RuntimeError("Execute thất bại không rõ lý do")

    def query(self, sql: str, params=None):
        attempts = 0
        last_err = None
        while attempts < 3:
            self.ensure_alive()
            if not self.conn:
                raise RuntimeError("DB chưa sẵn sàng")
            try:
                with self.conn.cursor() as cur:
                    cur.execute(sql, params or ())
                    rows = cur.fetchall()
                    if attempts > 0:
                        pass
                    return rows
            except pymysql.MySQLError as e:
                last_err = e
                print(f"[DB] Lỗi query (attempt {attempts+1}/3): {e} | SQL={sql}")
                try:
                    if self.conn and self.conn.open:
                        self.conn.close()
                except Exception:
                    pass
                self.conn = None
                attempts += 1
                if attempts < 3:
                    self._connect()
                    continue
                break
        raise last_err if last_err else RuntimeError("Query thất bại không rõ lý do")

    def close(self):
        if self.conn and self.conn.open:
            try:
                self.conn.close()
                pass
            except Exception:
                pass

    @classmethod
    def instance(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance


# Khởi tạo persistent DB (nếu cấu hình đầy đủ)
DBM = DBManager.instance()
atexit.register(lambda: DBM.close())


def main():
    # Tạo connection DB persistent (giữ ấm)
    _ = DBManager.instance()
    app = Application.builder().token(TOKEN).build()
    # Bỏ gửi đếm 1->5 để xác nhận bot hoạt động & CHAT_ID đúng
    # app.job_queue.run_once(send_initial_messages, when=0)
    
    async def chatid(update, context):
        await update.message.reply_text(f"Chat ID hiện tại: {update.effective_chat.id}")

    app.add_handler(CommandHandler("chatid", chatid))

    async def pingdb(update, context):
        try:
            rows = await asyncio.to_thread(DBM.query, "SELECT 1 AS ok")
            await update.message.reply_text(f"DB OK: {rows[0]['ok'] if rows else '?'}")
        except Exception as e:
            await update.message.reply_text(f"DB lỗi: {e}")

        
    app.add_handler(CommandHandler("pingdb", pingdb))
    
    async def createuser(update, context):
        """/createuser Ten|NgaySinh|CCCD|Phong|NgayDK [; Ten|...]
        Hỗ trợ thêm nhiều ngườii, cách nhau bằng dấu ;
        VD: /createuser NguyenVanA|20/5/1995|012345678901|101|1/9/2024 ; TranVanB|15/3/1990|098765432109|102|1/9/2024"""
        raw = " ".join(context.args)
        if not raw or '|' not in raw:
            await update.message.reply_text(
                "Dùng: /createuser Ten|NgaySinh(D/M/Y)|CCCD|Phong|NgayDangKy(D/M/Y)\n"
                "VD: /createuser NguyenVanA|20/5/1995|012345678901|101|1/9/2024\n\n"
                "Thêm nhiều ngườii (cách nhau bởi dấu ;):\n"
                "/createuser NguoiA|...|...|...|... ; NguoiB|...|...|...|... ; NguoiC|..."
            )
            return
        
        # Tách nhiều ngườii bằng dấu ;
        entries = [e.strip() for e in raw.split(';') if e.strip()]
        if not entries:
            await update.message.reply_text("Không có dữ liệu hợp lệ")
            return
        
        results = []
        success_count = 0
        
        for entry in entries:
            parts = [p.strip() for p in entry.split('|')]
            if len(parts) != 5:
                results.append(f"❌ Sai định dạng (cần 5 phần tử): {entry[:50]}...")
                continue
            
            name, birth_s, cccd, room, reg_s = parts
            
            try:
                birth = datetime.strptime(birth_s, "%d/%m/%Y").date()
                reg = datetime.strptime(reg_s, "%d/%m/%Y").date()
            except ValueError:
                results.append(f"❌ Ngày không hợp lệ: {name}")
                continue
            
            # Tính expiry +2 năm (xử lý 29/2)
            try:
                expiry = reg.replace(year=reg.year + 2)
            except ValueError:
                expiry = reg.replace(month=2, day=28, year=reg.year + 2)
            
            sql = ("INSERT INTO users (name, birth_date, cccd, room_number, registration_date, expiry_date) "
                   "VALUES (%s, %s, %s, %s, %s, %s)")
            try:
                await asyncio.to_thread(DBM.execute, sql, (name, birth, cccd, room, reg, expiry), True)
                results.append(f"✅ {name} (hết hạn {expiry.strftime('%d/%m/%Y')})")
                success_count += 1
            except Exception as e:
                error_msg = str(e)
                if "Duplicate entry" in error_msg or "UNIQUE" in error_msg.upper():
                    results.append(f"❌ {name}: CCCD {cccd} đã tồn tại")
                else:
                    results.append(f"❌ {name}: {error_msg[:100]}")
        
        # Gửi kết quả
        summary = f"📊 Kết quả: {success_count}/{len(entries)} thành công\n\n"
        await update.message.reply_text(summary + "\n".join(results))

    app.add_handler(CommandHandler("createuser", createuser))

    async def deleteuser(update, context):
        """/deleteuser CCCD"""
        if not context.args:
            await update.message.reply_text("Dùng: /deleteuser CCCD")
            return
        cccd = context.args[0].strip()
        if not cccd:
            await update.message.reply_text("CCCD rỗng")
            return
        sql = "DELETE FROM users WHERE cccd=%s LIMIT 1"
        try:
            affected = await asyncio.to_thread(DBM.execute, sql, (cccd,), True)
            if affected:
                await update.message.reply_text(f"Đã xóa user CCCD {cccd}")
            else:
                await update.message.reply_text("Không tìm thấy CCCD đó")
        except Exception as e:
            await update.message.reply_text(f"Lỗi: {e}")

    app.add_handler(CommandHandler("deleteuser", deleteuser))

    async def capnhat(update, context):
        """/capnhat CCCD NgayDangKy(D/M/Y) -> cập nhật registration_date & expiry_date"""
        if len(context.args) < 2:
            await update.message.reply_text("Dùng: /capnhat CCCD NgayDangKy(D/M/Y)\nVD: /capnhat 012345678901 15/9/2025")
            return
        cccd = context.args[0].strip()
        date_str = context.args[1].strip()
        try:
            reg = datetime.strptime(date_str, "%d/%m/%Y").date()
        except ValueError:
            await update.message.reply_text("Ngày không hợp lệ. Dạng D/M/Y")
            return
        # tính expiry +2 năm (xử lý 29/2)
        try:
            expiry = reg.replace(year=reg.year + 2)
        except ValueError:
            expiry = reg.replace(month=2, day=28, year=reg.year + 2)
        sql = "UPDATE users SET registration_date=%s, expiry_date=%s WHERE cccd=%s LIMIT 1"
        try:
            affected = await asyncio.to_thread(DBM.execute, sql, (reg, expiry, cccd), True)
            if affected:
                await update.message.reply_text(f"Đã cập nhật {cccd}: đăng ký {reg.strftime('%d/%m/%Y')} hết hạn {expiry.strftime('%d/%m/%Y')}")
            else:
                await update.message.reply_text("Không tìm thấy CCCD đó")
        except Exception as e:
            await update.message.reply_text(f"Lỗi: {e}")

    app.add_handler(CommandHandler("capnhat", capnhat))

    async def kiemtra(update, context):
        """/kiemtra [CCCD]
        - Không tham số: liệt kê đã hết hạn và còn <=15 ngày
        - Có CCCD: trả về trạng thái 1 người
        """
        today = date.today()
        # Trường hợp kiểm tra 1 CCCD
        if context.args:
            cccd = context.args[0].strip()
            if not cccd:
                await update.message.reply_text("CCCD rỗng")
                return
            sql_one = "SELECT name, cccd, room_number, registration_date, expiry_date FROM users WHERE cccd=%s LIMIT 1"
            try:
                rows = await asyncio.to_thread(DBM.query, sql_one, (cccd,))
            except Exception as e:
                await update.message.reply_text(f"Lỗi: {e}")
                return
            if not rows:
                await update.message.reply_text("Không tìm thấy CCCD đó")
                return
            r = rows[0]
            exp = r['expiry_date'] if isinstance(r['expiry_date'], date) else datetime.strptime(str(r['expiry_date']), "%Y-%m-%d").date()
            reg = r['registration_date'] if isinstance(r['registration_date'], date) else datetime.strptime(str(r['registration_date']), "%Y-%m-%d").date()
            delta = (exp - today).days
            if delta < 0:
                status = f"ĐÃ HẾT HẠN {-delta} ngày"
            elif delta == 0:
                status = "Hôm nay hết hạn"
            else:
                status = f"Còn {delta} ngày"
            msg = (
                f"Tên: {r['name']}\n"
                f"CCCD: {r['cccd']}\n"
                f"Phòng: {r['room_number']}\n"
                f"Đăng ký: {reg.strftime('%d/%m/%Y')}\n"
                f"Hết hạn: {exp.strftime('%d/%m/%Y')}\n"
                f"Trạng thái: {status}"
            )
            await update.message.reply_text(msg)
            return
        # Trường hợp liệt kê chung
        limit = today + timedelta(days=15)
        sql = "SELECT name, cccd, room_number, expiry_date FROM users WHERE expiry_date <= %s ORDER BY expiry_date ASC"
        try:
            rows = await asyncio.to_thread(DBM.query, sql, (limit,))
        except Exception as e:
            await update.message.reply_text(f"Lỗi: {e}")
            return
        expired = []
        soon = []
        for r in rows:
            exp = r['expiry_date'] if isinstance(r['expiry_date'], date) else datetime.strptime(str(r['expiry_date']), "%Y-%m-%d").date()
            delta = (exp - today).days
            line = f"{r['name']}|{r['cccd']}|P.{r['room_number']}|{exp.strftime('%d/%m/%Y')}"
            if delta < 0:
                expired.append(f"HẾT HẠN {-delta}d: " + line)
            else:
                soon.append(f"Còn {delta}d: " + line)
        if not expired and not soon:
            await update.message.reply_text("Không có ai hết hạn hay trong 15 ngày tới.")
            return
        parts = []
        if expired:
            parts.append("-- ĐÃ HẾT HẠN --\n" + "\n".join(expired[:100]))
            if len(expired) > 100:
                parts.append(f"... còn {len(expired)-100} hết hạn nữa")
        if soon:
            parts.append("-- SẮP HẾT (<=15 ngày) --\n" + "\n".join(soon[:100]))
            if len(soon) > 100:
                parts.append(f"... còn {len(soon)-100} sắp hết hạn nữa")
        await update.message.reply_text("\n\n".join(parts))

    app.add_handler(CommandHandler("kiemtra", kiemtra))
    
    async def job_kiemtra(context: ContextTypes.DEFAULT_TYPE):
    # Không log info trạng thái, chỉ log lỗi nếu có
        if not CHAT_ID:
            logging.warning("CHAT_ID trống - không gửi được job kiemtra")
            return
        today = date.today()
        limit = today + timedelta(days=15)
        sql = "SELECT name, cccd, room_number, expiry_date FROM users WHERE expiry_date <= %s ORDER BY expiry_date ASC"
        try:
            rows = await asyncio.to_thread(DBM.query, sql, (limit,))
        except Exception as e:
            await context.bot.send_message(chat_id=CHAT_ID, text=f"[kiemtra job] Lỗi: {e}")
            logging.error("Job lỗi: %s", e)
            return
        expired = []
        soon = []
        for r in rows:
            exp = r['expiry_date'] if isinstance(r['expiry_date'], date) else datetime.strptime(str(r['expiry_date']), "%Y-%m-%d").date()
            delta = (exp - today).days
            line = f"{r['name']}|{r['cccd']}|P.{r['room_number']}|{exp.strftime('%d/%m/%Y')}"
            if delta < 0:
                expired.append(f"HẾT HẠN {-delta}d: " + line)
            else:
                soon.append(f"Còn {delta}d: " + line)
        if not expired and not soon:
            await context.bot.send_message(chat_id=CHAT_ID, text="[kiemtra job] Không ai hết hạn hay trong 15 ngày.")
            pass
            return
        parts = []
        if expired:
            parts.append("-- ĐÃ HẾT HẠN --\n" + "\n".join(expired[:60]))
            if len(expired) > 60:
                parts.append(f"... còn {len(expired)-60} hết hạn nữa")
        if soon:
            parts.append("-- SẮP HẾT (<=15 ngày) --\n" + "\n".join(soon[:60]))
            if len(soon) > 60:
                parts.append(f"... còn {len(soon)-60} sắp hết hạn nữa")
        await context.bot.send_message(chat_id=CHAT_ID, text="\n\n".join(parts))
    pass

    # Lên lịch chạy mỗi 12 giờ kể từ lúc bot start
    app.job_queue.run_repeating(job_kiemtra, interval=60*60*12, first=10, name="kiemtra_12h")
    pass
    app.run_polling(poll_interval=1.0)


if __name__ == "__main__":
    main()
