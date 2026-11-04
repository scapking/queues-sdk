import sys, os, time, asyncio, uuid, traceback, threading, collections, json, shutil, datetime, pathlib
from typing import Callable, Generic, TypeVar

from queues_sdk.watermarks import default_watermarks
from queues_sdk.errors import QueuesError

T = TypeVar("T")




class Queues(Generic[T]):
    """零丢失异步有界队列：等待者直递、满载背压、水位压力门控溢出落盘（FIFO 回灌）。

    用法::

        q = Queues()
        await q.publish(message)                  # 未命名：内存快路径，永不丢弃
        await q.publish(message, name="audit")    # 命名：过水位压力门
        message = await q.consume()               # 背压感知接收
        message = await q.consume(name="audit")   # 自动先回灌盘上溢出
    """

    def __init__(self, maxsize: int = 1024*512, memory_max: int = 99, disk_max: int = 99, spill_path: str = "~/.local/queues_sdk/spill/", spill_batch: int = 1024, watermarks: Callable[[], tuple[int, int]] | None = None):
        """构造即全部配置，永不读外部状态文件。

        Args:
            maxsize: 内存缓冲容量（deque maxlen）。满时 publish 挂起等待消费腾位（背压），不拒绝不丢。
            memory_max: 内存软线（0-100）。命名消息在水位 ≥ 软线时溢出落盘为 batch-*.jsonl。
            disk_max: 磁盘硬线（0-100）。命名消息在水位 ≥ 硬线时滞留内存不写盘，回落自动续传。
            spill_path: 溢出根目录，每个 name 一个子目录。
            spill_batch: 每个溢出文件的行数（攒批落盘粒度）。
            watermarks: 水位提供者，零参返回 (memory_pct, disk_pct)；None 时用内置 /proc + statvfs 读取器。
        """
        self.pending = dict()
        self.draining = dict()
        self.checked = set()
        self.pressure_memory_max = int(memory_max)
        self.pressure_disk_max = int(disk_max)
        self.pressure_spill_path = str(spill_path)
        self.pressure_spill_batch = int(spill_batch)
        self._watermarks = watermarks if watermarks is not None else default_watermarks
        self.db = collections.deque(maxlen=int(maxsize))
        self.db_append = self.db.append
        self.db_popleft = self.db.popleft
        self.event = asyncio.Event()
        self.waiters = collections.deque()
        self.waiters_append = self.waiters.append
        self.waiters_popleft = self.waiters.popleft
        self.space_waiters = collections.deque()
        self.space_waiters_append = self.space_waiters.append
        self.space_waiters_popleft = self.space_waiters.popleft
        self.almightydb_service_logger_warning = asyncio.Queue(maxsize=1024*4)
        self.almightydb_service_logger_info    = asyncio.Queue(maxsize=1024*4)
        self.almightydb_service_logger_debug   = asyncio.Queue(maxsize=1024*4)
        self.almightydb_service_logger_error   = asyncio.Queue(maxsize=1024*4)
        self.almightydb_service_logger_store   = asyncio.Queue(maxsize=1024*4)





    def state(self) -> str:
        """当前压力态：normal / soft（内存软线）/ hard（磁盘硬线）。读取失败退 normal。"""
        try:
            # 磁盘过硬线 → hard；内存过硬线 → soft；否则 normal。
            memory, disk = self._watermarks()
            if int(disk) >= int(self.pressure_disk_max): return "hard"
            if int(memory) >= int(self.pressure_memory_max): return "soft"
            return "normal"

        except BaseException: return "normal"





    async def gate(self, name: str | None, message: T) -> str:
        """写侧压力门：命名消息按水位分流 pass（内存路径）/ spill（压力缓冲）；未命名直接 pass。"""
        try:
            # 写侧统一入口：'spill' 压力缓冲（硬线滞留内存不写盘/软线溢出落盘）/ 'pass' 内存路径。
            memory, disk = self._watermarks()
            hard = int(disk) >= int(self.pressure_disk_max)
            if name is None: return "pass"
            if name not in self.checked:
                self.checked.add(name)
                directory = self._spill_directory(name)
                if directory.is_dir() and any(directory.glob("*.jsonl")): self.draining[name] = True
            if hard or int(memory) >= int(self.pressure_memory_max) or self.draining.get(name):
                lines = self.pending.setdefault(name, list())
                lines.append(json.dumps(message, ensure_ascii=False, default=str))
                # 攒批到界且磁盘未硬线时落盘一个 batch-*.jsonl，行缓冲随即清空。
                if len(lines) >= int(self.pressure_spill_batch) and not hard:
                    directory = self._spill_directory(name)
                    directory.mkdir(parents=True, exist_ok=True)
                    path = directory / "batch-{}.jsonl".format(time.monotonic_ns())
                    with open(path, "w", encoding="utf-8") as handle: handle.write("\n".join(lines) + "\n")
                    lines.clear()
                return "spill"
            return "pass"

        except BaseException as error: await self.__log_error("gate", error); return "pass"





    def _spill_directory(self, name: str) -> pathlib.Path:
        return pathlib.Path(self.pressure_spill_path).expanduser() / str(name)





    async def drain(self, name: str, target: list, limit: int | None = None) -> int:
        """有界回灌：盘上旧 batch 文件在前 FIFO，pending 就地落成最新文件在后；返回回灌条数。"""
        try:
            # 有界回灌：盘上旧 batch 文件在前 FIFO，pending 就地落成最新文件在后，顺序自然正确。
            limit = int(self.pressure_spill_batch) if limit is None else int(limit)
            pulled = int(0); lines = self.pending.get(name); directory = self._spill_directory(name)
            if lines:
                directory.mkdir(parents=True, exist_ok=True)
                path = directory / "batch-{}.jsonl".format(time.monotonic_ns())
                with open(path, "w", encoding="utf-8") as handle: handle.write("\n".join(lines) + "\n")
                lines.clear()
            if directory.is_dir():
                for path in sorted(directory.glob("*.jsonl")):
                    if pulled >= limit: break
                    try:
                        with open(path, encoding="utf-8") as handle: batch = [line.strip() for line in handle if line.strip()]
                        target.extend(json.loads(line) for line in batch); pulled = pulled + len(batch)
                        path.unlink()
                    except BaseException:
                        # 毒文件改名 .bad 隔离，不阻塞 FIFO。
                        os.replace(path, directory / (path.name + ".bad"))
            self.checked.add(name)
            if self.pending.get(name) or (directory.is_dir() and any(directory.glob("*.jsonl"))): self.draining[name] = True
            else: self.draining.pop(name, None)
            return pulled

        except BaseException as error: await self.__log_error("drain", error); return int(0)





    async def discard(self, name: str) -> int:
        """清盘指定 name 并返回丢弃条数（pending + 盘上 batch 对账）。"""
        try:
            # 清盘并整盘对账：丢弃条数 = pending 行数 + 盘上 batch 行数。
            dropped = len(self.pending.pop(name, list()))
            self.draining.pop(name, None)
            self.checked.discard(name)
            directory = self._spill_directory(name)
            if directory.is_dir():
                for path in directory.glob("*.jsonl"):
                    with open(path, encoding="utf-8") as handle: dropped = dropped + len([line for line in handle if line.strip()])
            shutil.rmtree(directory, ignore_errors=True)
            return int(dropped)

        except BaseException as error: await self.__log_error("discard", error); return int(0)





    async def publish(self, message: T, name: str | None = None) -> None:
        """入队。未命名走内存快路径（零依赖）；容量满时挂起等待消费腾位（零丢失背压）。"""
        try:
            # 未命名消息直走内存路径（零依赖快路径）；命名消息过压力门。
            action = "pass" if name is None else await self.gate(name, message)
            if action == "spill": return None
            if self.waiters and not self.waiters[0].done(): self.waiters_popleft().set_result(message); return None
            if self.waiters: self.waiters_popleft()
            # 容量满 → 背压等待消费腾位，不拒绝不挤队首（零丢失）。
            while self.db.maxlen is not None and len(self.db) >= self.db.maxlen:
                producer = asyncio.get_running_loop().create_future()
                self.space_waiters_append(producer)
                try:
                    await producer
                except asyncio.CancelledError:
                    if producer in self.space_waiters: self.space_waiters.remove(producer)
                    raise
            # 腾位后发现已有等待者：直递不落 db（保 FIFO 与唤醒链），并把让出的空位链式传给下一个等位生产者。
            if self.waiters and not self.waiters[0].done():
                self.waiters_popleft().set_result(message)
                if self.space_waiters and self.db.maxlen is not None and len(self.db) < self.db.maxlen: self.space_waiters_popleft().set_result(None)
                return None
            self.db_append(message)

        except BaseException as error:
            # 取消必须上抛：吸收会让 producer 误以为消息已入队（丢失语义）。
            if isinstance(error, asyncio.CancelledError): raise
            await self.__log_error("publish", error); return None





    async def consume(self, name: str | None = None) -> T:
        """出队。缓冲空时挂起等待直递；命名消息自动先回灌盘上溢出再取内存。"""
        try:
            if len(self.db) > 0:
                message = self.db_popleft()
                if self.space_waiters and self.db.maxlen is not None and len(self.db) < self.db.maxlen: self.space_waiters_popleft().set_result(None)
                return message
            if name and (self.draining.get(name) or self.pending.get(name)): await self.drain(name, self.db)
            if len(self.db) > 0: return self.db_popleft()
            waiter = asyncio.get_running_loop().create_future()
            self.waiters_append(waiter)
            # 取消时清理等待者并上抛（吸收取消会让消费端拿到 None 混淆空值语义）。
            try:
                message = await waiter
            except asyncio.CancelledError:
                if waiter in self.waiters: self.waiters.remove(waiter)
                raise
            return message

        except BaseException as error:
            # 取消必须上抛：吸收会让消费循环把 None 当消息、teardown 永久等待。
            if isinstance(error, asyncio.CancelledError): raise
            await self.__log_error("consume", error); return None





    async def main(self) -> None:
        try:
            pass
        except BaseException as error: await self.__log_error("main", error)





    async def __log_error(self, method: str, error: BaseException) -> None:
        try:
            import logging
            frames = traceback.extract_tb(error.__traceback__)
            frame = frames[-1] if frames else None
            detail = {
                "error_id": uuid.uuid4().hex,
                "error_time": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "error_method": "{}.{}".format(type(self).__name__, method),
                "error_type": type(error).__name__,
                "error_value": str(error),
                "error_file": os.path.abspath(frame.filename) if frame else "unknown",
                "error_line": frame.lineno if frame else 0,
                "error_pid": os.getpid(),
                "error_thread": threading.current_thread().name,
                "error_traceback": "".join(traceback.format_exception(type(error), error, error.__traceback__)),
            }
            logging.getLogger("queues_sdk").error("[queues_sdk error] [{}]".format(detail))

        except BaseException: pass
