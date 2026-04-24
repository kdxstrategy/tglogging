import io
import time
import asyncio
import nest_asyncio
import weakref
import threading
import html
from logging import StreamHandler
from aiohttp import ClientSession, FormData, ClientTimeout, TCPConnector

nest_asyncio.apply()

DEFAULT_PAYLOAD = {
    "disable_web_page_preview": True,
    "parse_mode": "HTML",
}

PRE_WRAPPER_OVERHEAD = 20
TELEGRAM_HARD_LIMIT = 4096


class TelegramLogHandler(StreamHandler):
    """
    Handler для отправки логов в Telegram (синхронный код).
    Логи копятся и отправляются автоматически, даже после floodwait.
    """
    _handlers = weakref.WeakSet()
    MAX_MESSAGE_LEN = 3500

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
        self.floodwait_until = 0.0
        self.message_id = 0
        self.lines = 0
        self.last_update = 0
        self.base_url = f"https://api.telegram.org/bot{token}"
        self.initialized = False
        self.last_sent_content = ""
        self._handlers.add(self)

        self._lock = threading.Lock()

        self.loop = asyncio.new_event_loop()
        t = threading.Thread(target=self._run_loop, daemon=True)
        t.start()

        # ждём пока loop реально поднимется в треде
        # иначе nest_asyncio падает с "_ident is None"
        while not self.loop.is_running():
            time.sleep(0.05)

        asyncio.run_coroutine_threadsafe(self._background_worker(), self.loop)

    @property
    def floodwait(self):
        """Сколько секунд осталось ждать. 0 — можно слать."""
        remaining = self.floodwait_until - time.time()
        return max(0, int(remaining))

    @floodwait.setter
    def floodwait(self, seconds):
        if seconds <= 0:
            self.floodwait_until = 0
        else:
            self.floodwait_until = time.time() + seconds

    def _run_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def emit(self, record):
        msg = self.format(record)
        with self._lock:
            self.lines += 1
            self.message_buffer.append(msg)

    def _escaped_fits(self, content):
        """True если content после html.escape + обёртки влезет в лимит TG."""
        return len(html.escape(content)) + PRE_WRAPPER_OVERHEAD <= TELEGRAM_HARD_LIMIT

    async def _background_worker(self):
        while True:
            try:
                if self.floodwait == 0:
                    with self._lock:
                        has_messages = bool(self.message_buffer)

                    if has_messages and (
                        time.time() - self.last_update >= self.wait_time
                        or self.lines >= self.minimum
                    ):
                        await self.handle_logs()
                        with self._lock:
                            if not self.message_buffer:
                                self.last_update = time.time()
                            else:
                                self.last_update = time.time() - self.wait_time / 2
            except Exception as e:
                print(f"TGLogger worker error: {e}")
            await asyncio.sleep(1)

    async def handle_logs(self, force_send=False):
        if self.floodwait:
            return

        with self._lock:
            if not self.message_buffer:
                return
            snapshot = self.message_buffer[:]
            self.message_buffer.clear()
            self.lines = 0

        returned = False

        def return_to_buffer(items):
            nonlocal returned
            if returned or not items:
                returned = True
                return
            with self._lock:
                self.message_buffer[0:0] = items
            returned = True

        try:
            full_message = '\n'.join(snapshot)
            if not full_message.strip():
                returned = True
                return

            if not self.initialized:
                success = await self.initialize_bot()
                if not success:
                    return_to_buffer(snapshot)
                    return

            combined_message = self.last_sent_content + '\n' + full_message
            can_edit = (
                self.message_id
                and self.last_sent_content
                and self._escaped_fits(combined_message)
            )

            if can_edit:
                ok = await self.edit_message(combined_message)
                if ok:
                    returned = True
                else:
                    return_to_buffer(snapshot)
            else:
                chunks = self._split_into_chunks(full_message)
                for idx, chunk in enumerate(chunks):
                    if not chunk.strip():
                        continue
                    if not self.initialized or self.floodwait:
                        remaining = [c for c in chunks[idx:] if c.strip()]
                        return_to_buffer(remaining)
                        break
                    ok = await self.send_message(chunk)
                    if not ok:
                        remaining = [c for c in chunks[idx:] if c.strip()]
                        return_to_buffer(remaining)
                        break
                else:
                    returned = True
        except BaseException as e:
            print(f"Handle logs error: {e}")
            return_to_buffer(snapshot)
            raise
        finally:
            if not returned:
                return_to_buffer(snapshot)

    def _split_into_chunks(self, message):
        """
        Делит message так, чтобы каждый чанк после html.escape+обёртки влезал в TG.
        """
        chunks = []
        current_chunk = ""
        lines = message.split('\n')

        fits = self._escaped_fits

        for line in lines:
            if line == "":
                line = " "

            while not fits(line):
                cut = min(len(line), self.MAX_MESSAGE_LEN)
                while cut > 0 and not fits(line[:cut]):
                    cut -= 100
                if cut <= 0:
                    cut = 1

                split_pos = line[:cut].rfind(' ')
                if split_pos <= 0:
                    split_pos = cut
                part = line[:split_pos]
                line = line[split_pos:]

                candidate = (current_chunk + '\n' + part) if current_chunk else part
                if fits(candidate):
                    current_chunk = candidate
                else:
                    if current_chunk:
                        chunks.append(current_chunk)
                    current_chunk = part

            candidate = (current_chunk + '\n' + line) if current_chunk else line
            if fits(candidate):
                current_chunk = candidate
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
        timeout = ClientTimeout(total=15, connect=5)
        connector = TCPConnector(force_close=True)
        async with ClientSession(connector=connector, timeout=timeout) as session:
            async with session.post(url, json=payload) as response:
                return await response.json()

    async def verify_bot(self):
        try:
            res = await self.send_request(f"{self.base_url}/getMe", {})
            print(f"TGLogger verify_bot response: {res}")
        except Exception as e:
            print(f"verify_bot error: {e}")
            return None, False
        if res.get("error_code") == 401:
            return None, False
        if not res.get("ok"):
            return None, False
        return res.get("result", {}).get("username"), True

    async def send_message(self, message):
        if not message.strip():
            return True

        payload = DEFAULT_PAYLOAD.copy()
        payload["chat_id"] = self.log_chat_id

        escaped = html.escape(message)
        payload["text"] = f"<pre>k-server\n{escaped}</pre>"

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
            self.message_id = 0
            self.last_sent_content = ""
            return False

    async def edit_message(self, message):
        if not message.strip() or not self.message_id:
            return False

        if message == self.last_sent_content:
            return True

        payload = DEFAULT_PAYLOAD.copy()
        payload["chat_id"] = self.log_chat_id
        payload["message_id"] = self.message_id

        escaped = html.escape(message)
        payload["text"] = f"<pre>k-server\n{escaped}</pre>"

        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        try:
            res = await self.send_request(f"{self.base_url}/editMessageText", payload)
            if res.get("ok"):
                self.last_sent_content = message
                return True
            description = res.get("description", "")
            if "message is not modified" in description:
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
            print(f"Thread {self.topic_id} not found - pausing for 5 min")
            self.message_id = 0
            self.last_sent_content = ""
            self.floodwait = 300
        elif error_code == 429:
            retry_after = error.get("retry_after", 30)
            print(f"Floodwait: {retry_after} seconds (+2 запас)")
            self.floodwait = retry_after + 2
        elif "message to edit not found" in description:
            print("Message to edit not found - resetting message_id")
            self.message_id = 0
            self.last_sent_content = ""
        elif "message is not modified" in description:
            print("Message not modified (no changes), skipping")
        elif "message is too long" in description:
            print("Message too long - resetting message_id to force re-chunk")
            self.message_id = 0
            self.last_sent_content = ""
        else:
            print(f"Telegram API error: {description}")
            self.message_id = 0
            self.last_sent_content = ""

    def flush_and_close(self, timeout: int = 100):
        """Ждём пока все логи отправятся, включая in-flight запросы."""
        start = time.time()
        stable_empty_count = 0
        while time.time() - start < timeout:
            with self._lock:
                empty = not self.message_buffer
            if empty and self.floodwait == 0:
                stable_empty_count += 1
                if stable_empty_count >= 3:
                    break
            else:
                stable_empty_count = 0
            time.sleep(1)
        self.loop.call_soon_threadsafe(self.loop.stop)        log_chat_id: int,
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
        # timestamp, до которого действует floodwait
        self.floodwait_until = 0.0
        self.message_id = 0
        self.lines = 0
        self.last_update = 0
        self.base_url = f"https://api.telegram.org/bot{token}"
        self.initialized = False
        self.last_sent_content = ""
        self._handlers.add(self)

        self._lock = threading.Lock()

        self.loop = asyncio.new_event_loop()
        t = threading.Thread(target=self._run_loop, daemon=True)
        t.start()

        asyncio.run_coroutine_threadsafe(self._background_worker(), self.loop)

    @property
    def floodwait(self):
        """Сколько секунд осталось ждать. 0 — можно слать."""
        remaining = self.floodwait_until - time.time()
        return max(0, int(remaining))

    @floodwait.setter
    def floodwait(self, seconds):
        if seconds <= 0:
            self.floodwait_until = 0
        else:
            self.floodwait_until = time.time() + seconds

    def _run_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def emit(self, record):
        msg = self.format(record)
        with self._lock:
            self.lines += 1
            self.message_buffer.append(msg)

    def _escaped_fits(self, content):
        """True если content после html.escape + обёртки влезет в лимит TG."""
        return len(html.escape(content)) + PRE_WRAPPER_OVERHEAD <= TELEGRAM_HARD_LIMIT

    async def _background_worker(self):
        while True:
            try:
                if self.floodwait == 0:
                    with self._lock:
                        has_messages = bool(self.message_buffer)

                    if has_messages and (
                        time.time() - self.last_update >= self.wait_time
                        or self.lines >= self.minimum
                    ):
                        await self.handle_logs()
                        # last_update двигаем по-разному в зависимости от успеха
                        with self._lock:
                            if not self.message_buffer:
                                # всё отправлено — стандартный интервал до следующей отправки
                                self.last_update = time.time()
                            else:
                                # остались логи — сократим ожидание до wait_time/2,
                                # чтобы скорее повторить, но не спамить
                                self.last_update = time.time() - self.wait_time / 2
            except Exception as e:
                print(f"TGLogger worker error: {e}")
            await asyncio.sleep(1)

    async def handle_logs(self, force_send=False):
        if self.floodwait:
            return

        with self._lock:
            if not self.message_buffer:
                return
            snapshot = self.message_buffer[:]
            self.message_buffer.clear()
            self.lines = 0

        # флаг: True = snapshot уже обработан (отправлен или возвращён)
        returned = False

        def return_to_buffer(items):
            """Возвращает items в начало буфера, помечая snapshot как обработанный."""
            nonlocal returned
            if returned or not items:
                returned = True
                return
            with self._lock:
                self.message_buffer[0:0] = items
            returned = True

        try:
            full_message = '\n'.join(snapshot)
            if not full_message.strip():
                returned = True
                return

            if not self.initialized:
                success = await self.initialize_bot()
                if not success:
                    return_to_buffer(snapshot)
                    return

            # решение edit vs send принимаем по фактической длине после escape+обёртки
            combined_message = self.last_sent_content + '\n' + full_message
            can_edit = (
                self.message_id
                and self.last_sent_content
                and self._escaped_fits(combined_message)
            )

            if can_edit:
                ok = await self.edit_message(combined_message)
                if ok:
                    returned = True
                else:
                    return_to_buffer(snapshot)
            else:
                chunks = self._split_into_chunks(full_message)
                for idx, chunk in enumerate(chunks):
                    if not chunk.strip():
                        continue
                    # если bot разлогинили / floodwait посреди цикла — вернуть остаток
                    if not self.initialized or self.floodwait:
                        remaining = [c for c in chunks[idx:] if c.strip()]
                        return_to_buffer(remaining)
                        break
                    ok = await self.send_message(chunk)
                    if not ok:
                        remaining = [c for c in chunks[idx:] if c.strip()]
                        return_to_buffer(remaining)
                        break
                else:
                    # нормальный выход из for (без break) — всё отправлено
                    returned = True
        except BaseException as e:
            print(f"Handle logs error: {e}")
            return_to_buffer(snapshot)
            # не подавляем CancelledError / KeyboardInterrupt
            raise
        finally:
            # страховка на случай любого пути выхода без установки флага
            if not returned:
                return_to_buffer(snapshot)

    def _split_into_chunks(self, message):
        """
        Делит message так, чтобы каждый чанк после html.escape+обёртки влезал в TG.
        """
        chunks = []
        current_chunk = ""
        lines = message.split('\n')

        fits = self._escaped_fits

        for line in lines:
            if line == "":
                line = " "

            # если строка одна не влезает — режем её
            while not fits(line):
                # находим максимальный cut, при котором line[:cut] влезает
                cut = min(len(line), self.MAX_MESSAGE_LEN)
                while cut > 0 and not fits(line[:cut]):
                    cut -= 100
                if cut <= 0:
                    cut = 1  # защита от зацикливания

                # стараемся резать по пробелу
                split_pos = line[:cut].rfind(' ')
                if split_pos <= 0:
                    split_pos = cut
                part = line[:split_pos]
                line = line[split_pos:]  # отступы сохраняем

                # добавляем part в current_chunk или сбрасываем текущий и начинаем новый
                candidate = (current_chunk + '\n' + part) if current_chunk else part
                if fits(candidate):
                    current_chunk = candidate
                else:
                    if current_chunk:
                        chunks.append(current_chunk)
                    current_chunk = part

            # line теперь гарантированно влезает сама по себе
            candidate = (current_chunk + '\n' + line) if current_chunk else line
            if fits(candidate):
                current_chunk = candidate
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
        timeout = ClientTimeout(total=15, connect=5)
        # TCPConnector с force_close=True гарантирует что соединение
        # не привязывается к глобальному event loop (актуально при nest_asyncio)
        connector = TCPConnector(force_close=True)
        async with ClientSession(connector=connector, timeout=timeout) as session:
            async with session.post(url, json=payload) as response:
                return await response.json()

    async def verify_bot(self):
        try:
            res = await self.send_request(f"{self.base_url}/getMe", {})
            print(f"TGLogger verify_bot response: {res}")
        except Exception as e:
            print(f"verify_bot error: {e}")
            return None, False
        if res.get("error_code") == 401:
            return None, False
        if not res.get("ok"):
            return None, False
        return res.get("result", {}).get("username"), True

    async def send_message(self, message):
        if not message.strip():
            # пустое сообщение не отправляем, но и не считаем ошибкой
            return True

        payload = DEFAULT_PAYLOAD.copy()
        payload["chat_id"] = self.log_chat_id

        escaped = html.escape(message)
        payload["text"] = f"<pre>k-server\n{escaped}</pre>"

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
            # сетевая ошибка — состояние message_id неизвестно, сбрасываем
            self.message_id = 0
            self.last_sent_content = ""
            return False

    async def edit_message(self, message):
        if not message.strip() or not self.message_id:
            return False

        if message == self.last_sent_content:
            return True

        payload = DEFAULT_PAYLOAD.copy()
        payload["chat_id"] = self.log_chat_id
        payload["message_id"] = self.message_id

        escaped = html.escape(message)
        payload["text"] = f"<pre>k-server\n{escaped}</pre>"

        if self.topic_id:
            payload["message_thread_id"] = self.topic_id

        try:
            res = await self.send_request(f"{self.base_url}/editMessageText", payload)
            if res.get("ok"):
                self.last_sent_content = message
                return True
            description = res.get("description", "")
            if "message is not modified" in description:
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
            # Топик удалён/закрыт. Сбрасывать initialized нельзя —
            # это вызовет бесконечный цикл getMe + sendMessage каждые несколько секунд.
            # Ставим длинный floodwait, чтобы дать админу починить.
            print(f"Thread {self.topic_id} not found - pausing for 5 min")
            self.message_id = 0
            self.last_sent_content = ""
            self.floodwait = 300
        elif error_code == 429:
            retry_after = error.get("retry_after", 30)
            # +2 сек запас на рассинхрон часов и сетевую задержку
            print(f"Floodwait: {retry_after} seconds (+2 запас)")
            self.floodwait = retry_after + 2
        elif "message to edit not found" in description:
            print("Message to edit not found - resetting message_id")
            self.message_id = 0
            self.last_sent_content = ""
        elif "message is not modified" in description:
            print("Message not modified (no changes), skipping")
        elif "message is too long" in description:
            # наша оценка длины подвела — форсируем пересборку через send
            print("Message too long - resetting message_id to force re-chunk")
            self.message_id = 0
            self.last_sent_content = ""
        else:
            print(f"Telegram API error: {description}")
            self.message_id = 0
            self.last_sent_content = ""

    def flush_and_close(self, timeout: int = 100):
        """Ждём пока все логи отправятся, включая in-flight запросы."""
        start = time.time()
        stable_empty_count = 0
        # нужно 3 подряд проверки с пустым буфером, чтобы убедиться,
        # что handle_logs не в процессе await (иначе он ещё может вернуть snapshot)
        while time.time() - start < timeout:
            with self._lock:
                empty = not self.message_buffer
            if empty and self.floodwait == 0:
                stable_empty_count += 1
                if stable_empty_count >= 3:
                    break
            else:
                stable_empty_count = 0
            time.sleep(1)
        self.loop.call_soon_threadsafe(self.loop.stop)
