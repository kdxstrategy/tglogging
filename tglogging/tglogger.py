import io
import time
import asyncio
import nest_asyncio
import weakref
import threading
from logging import StreamHandler
from aiohttp import ClientSession, FormData, ClientTimeout

nest_asyncio.apply()

DEFAULT_PAYLOAD = {"disable_web_page_preview": True, "parse_mode": "Markdown"}


class TelegramLogHandler(StreamHandler):
    """
    Handler для отправки логов в Telegram (синхронный код).
    Логи копятся и отправляются автоматически, даже после floodwait.
    """
    _handlers = weakref.WeakSet()

    def __init__(
        self,
        token: str,
        log_chat_id: int,
        topic_id: int = None,
        update_interval: int = 5,
        minimum_lines: int = 1,
        pending_logs: int = 200000,
    ):
        StreamHandler.__init__(self)
        self.token = token
        self.log_chat_id = int(log_chat_id)
        self.topic_id = int(topic_id) if topic_id else None
        self.wait_time = update_interval
        self.minimum = minimum_lines
        self.pending = pending_logs
        self.message_buffer = []
        self.floodwait = 0
        self.message_id = 0
        self.lines = 0
        self.last_update = 0
        self.base_url = f"https://api.telegram.org/bot{token}"
        self.initialized = False
        self.last_sent_content = ""
        self._handlers.add(self)

        # создаём отдельный event loop в отдельном потоке
        self.loop = asyncio.new_event_loop()
        t = threading.Thread(target=self._run_loop, daemon=True)
        t.start()

        # запускаем воркер внутри этого event loop
        asyncio.run_coroutine_threadsafe(self._background_worker(), self.loop)

    def _run_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def emit(self, record):
        msg = self.format(record)
        self.lines += 1
        self.message_buffer.append(msg)

    async def _background_worker(self):
        """Фоновая задача — проверяет буфер и отправляет логи"""
        while True:
            try:
                if self.floodwait > 0:
                    self.floodwait -= 1
                elif (
                    self.message_buffer
                    and (
                        time.time() - self.last_update >= self.wait_time
                        or self.lines >= self.minimum
                        or len("\n".join(self.message_buffer)) >= 3000
                    )
                ):
                    await self.handle_logs()
                    self.lines = 0
                    self.last_update = time.time()
            except Exception as e:
                print(f"TGLogger worker error: {e}")
            await asyncio.sleep(1)

    async def handle_logs(self, force_send=False):
        if not self.message_buffer or self.floodwait:
            return

        snapshot = self.message_buffer[:]
        self.message_buffer.clear()

        full_message = '\n'.join(snapshot)
        if not full_message.strip():
            return

        try:
            if not self.initialized:
                success = await self.initialize_bot()
                if not success:
                    # вернуть всё в очередь
                    self.message_buffer = snapshot + self.message_buffer
                    return

            if self.message_id and len(self.last_sent_content + '\n' + full_message) <= 4096:
                combined_message = self.last_sent_content + '\n' + full_message
                ok = await self.edit_message(combined_message)
                if ok:
                    self.last_sent_content = combined_message
                else:
                    self.message_buffer = snapshot + self.message_buffer
            else:
                chunks = self._split_into_chunks(full_message)
                for chunk in chunks:
                    if not chunk.strip():
                        continue
                    ok = await self.send_message(chunk)
                    if not ok:
                        # вернуть неотправленный кусок
                        self.message_buffer.insert(0, chunk)
                        # вернуть оставшиеся части
                        self.message_buffer = self.message_buffer + chunks[chunks.index(chunk)+1:]
                        break
        except Exception as e:
            print(f"Handle logs error: {e}")
            # всегда возвращаем в очередь
            self.message_buffer = snapshot + self.message_buffer

    def _split_into_chunks(self, message):
        chunks = []
        current_chunk = ""
        lines = message.split('\n')

        for line in lines:
            if line == "":
                line = " "

            while len(line) > 4096:
                split_pos = line[:4096].rfind(' ')
                if split_pos <= 0:
                    split_pos = 4096
                part = line[:split_pos]
                line = line[split_pos:].lstrip()

                if current_chunk:
                    current_chunk += '\n' + part
                else:
                    current_chunk = part

                if len(current_chunk) >= 4096:
                    chunks.append(current_chunk)
                    current_chunk = ""

            if len(current_chunk) + len(line) + 1 <= 4096:
                if current_chunk:
                    current_chunk += '\n' + line
                else:
                    current_chunk = line
            else:
                if current_chunk:
                    chunks.append(current_chunk)
                current_chunk = line

        if current_chunk:
            chunks.append(current_chunk)

        return chunks

    async def initialize_bot(self):
        uname, is_alive = await self.verify_bot()
        if not is_alive:
            print("TGLogger: [ERROR] - Invalid bot token")
            return False
        self.initialized = True
        return True

    async def send_request(self, url, payload):
        timeout = ClientTimeout(total=3)  # таймаут 3 секунды
        async with ClientSession(timeout=timeout) as session:
            async with session.post(url, json=payload) as response:
                return await response.json()

    async def verify_bot(self):
        res = await self.send_request(f"{self.base_url}/getMe", {})
        if res.get("error_code") == 401:
            return None, False
        return res.get("result", {}).get("username"), True

    async def send_message(self, message):
        if not message.strip():
            return False

        payload = DEFAULT_PAYLOAD.copy()
        payload["chat_id"] = self.log_chat_id
        payload["text"] = f"```k-server\n{message}```"
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        try:
            res = await self.send_request(f"{self.base_url}/sendMessage", payload)
            if res.get("ok"):
                self.message_id = res["result"]["message_id"]
                self.last_sent_content = message
                return True
            await self.handle_error(res)
            return False
        except Exception as e:
            print(f"Send message error: {e}")
            return False

    async def edit_message(self, message):
        if not message.strip() or not self.message_id:
            return False

        if message == self.last_sent_content:
            return True

        payload = DEFAULT_PAYLOAD.copy()
        payload["chat_id"] = self.log_chat_id
        payload["message_id"] = self.message_id
        payload["text"] = f"```k-server\n{message}```"
        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        try:
            res = await self.send_request(f"{self.base_url}/editMessageText", payload)
            if res.get("ok"):
                self.last_sent_content = message
                return True
            await self.handle_error(res)
            return False
        except Exception as e:
            print(f"Edit message error: {e}")
            return False

    async def handle_error(self, resp: dict):
        error = resp.get("parameters", {})
        error_code = resp.get("error_code")
        description = resp.get("description", "")

        if description == "message thread not found":
            print(f"Thread {self.topic_id} not found - resetting")
            self.message_id = 0
            self.initialized = False
        elif error_code == 429:
            retry_after = error.get("retry_after", 30)
            print(f'Floodwait: {retry_after} seconds')
            self.floodwait = retry_after
        elif "message to edit not found" in description:
            print("Message to edit not found - resetting")
            self.message_id = 0
            self.initialized = False
        elif "message is not modified" in description:
            print("Message not modified (no changes), skipping")
        else:
            print(f"Telegram API error: {description}")

    def flush_and_close(self, timeout: int = 100):
        """Ждём пока все логи отправятся"""
        start = time.time()
        while time.time() - start < timeout:
            if not self.message_buffer and self.floodwait == 0:
                break
            time.sleep(1)
        self.loop.call_soon_threadsafe(self.loop.stop)
