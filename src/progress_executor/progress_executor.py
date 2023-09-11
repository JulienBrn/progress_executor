from __future__ import annotations
from typing import Dict, Any, List, Callable, Literal, Optional, Tuple, Set, TypedDict, NoReturn, TYPE_CHECKING
import concurrent, threading, time, asyncio, time, functools, signal

if TYPE_CHECKING:
    import tqdm

class ProgressInfo(TypedDict):
    n: int | float
    total: int | float
    status: str

class Updater:
    n: int | float
    total: int | float
    status: str
    last_time: float
    min_amount_percentage: float
    last_amount: int | float

    def __init__(self, n=0, total=0):
        self.n = n
        self.total = total
        self.status = "running"

        self.last_time = time.time()
        self.min_interval = 0.1
        self.min_amount_percentage = 0.005
        self.last_amount = self.n

    def refresh(self): raise NotImplementedError
    def update(self, amount=1):
        self.n+=amount
        if self.total <= 0 or (self.n - self.last_amount)/self.total > self.min_amount_percentage:
            now = time.time()
            if now - self.last_time > self.min_interval:
                self.refresh()
                self.last_time = now
                self.last_amount = self.n


    def __call__(self, iterable):
        self.iterable = iterable
        self.total = len(iterable)
        self.n = 0
        return self
    
    def __iter__(self):
        for obj in self.iterable:
            yield obj
            self.update(1)

    def close(self): pass

class ProgressFuture(concurrent.futures.Future):
    old_progress_info: ProgressInfo
    progress_info: ProgressInfo
    progress_ev: threading.Event
    cancel_ev: threading.Event

    progress_callbacks: Callable[[progress_info], None]

    def _child_init(self, progress_info: ProgressInfo, progress_ev: threading.Event, cancel_ev: threading.Event):
        self.progress_info = progress_info
        self.old_progress_info = dict(n=0, total=0, status="pending")
        self.progress_ev = progress_ev
        self.cancel_ev = cancel_ev
        self.progress_callbacks = []

    def cancel(self):
        self.cancel_ev.set()
        self.progress_info["status"] = "cancelled"
        self._process_progress()
        super().cancel()

    def add_progress_callback(self, fn):
        self.progress_callbacks.append(fn)

    def remove_progress_callback(self, fn):
        self.progress_callbacks.remove(fn)
    
    def _process_progress(self):
        for c in self.progress_callbacks:
            c(self.old_progress_info, self.progress_info)

    async def check_for_progress(self, sleep_duration=0.1):
        try:
            first = True
            while not self.done():
                if self.progress_ev.is_set() or first:
                    self.old_progress_info["n"], self.old_progress_info["total"], self.old_progress_info["status"] = self.progress_info["n"], self.progress_info["total"], self.progress_info["status"]
                    self._process_progress()
                    self.progress_ev.clear()
                await asyncio.sleep(sleep_duration)
                first = False
            return self.result()
        except asyncio.CancelledError:
            self.cancel()
            raise


    def add_tqdm_callback(self, tqdm_cls, init_kwargs={}, triggers: Set[Literal["now", "running", "cancelled"]] = {"now"}):
        instance = []
        def callback(old, new):
            nonlocal instance
            if new["status"] in triggers and instance == []:
                instance = [tqdm_cls(**init_kwargs)]
            if len(instance) > 0:
                [tqdm_instance] = instance
                tqdm_instance.n = new["n"]
                tqdm_instance.total = new["total"]
                tqdm_instance.set_postfix_str(new["status"])

        if "now" in triggers:
            instance = [tqdm_cls(**init_kwargs)]
        self.add_progress_callback(callback)
        self.add_done_callback(lambda r: instance[0].close() if len(instance) > 0 else None)
            

class ProgressExecutor(concurrent.futures.Executor):
    def submit(self, f, *args, **kwargs) -> ProgressFuture:
        raise NotImplementedError("Abstract submit method")

class PoolUpdater(Updater):
    def __init__(self, cancel_ev, progress_ev, progress_info):
        super().__init__(progress_info["n"], progress_info["total"])
        self.cancel_ev = cancel_ev
        self.progress_ev = progress_ev
        self.progress_info = progress_info
        
        self.refresh()
        
    def refresh(self, *args, **kwargs):
        self.progress_ev.set()
        (self.progress_info["n"], self.progress_info["total"], self.progress_info["status"]) = (self.n, self.total, self.status)
        if self.cancel_ev.is_set():
            raise asyncio.CancelledError() from None
        
    def close(self):
        self.refresh()
        while self.progress_ev.is_set():
            time.sleep(0.1)


def make_f(f, *args, cancel_ev, progress_ev, progress_info, **kwargs):
    updater = PoolUpdater(cancel_ev, progress_ev, progress_info)
    try:
        res = f(*args, progress = updater, **kwargs)
    except:
        raise
    finally:
        updater.close()
    return res


class ThreadPoolProgressExecutor(concurrent.futures.ThreadPoolExecutor, ProgressExecutor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def submit(self, f, *args, **kwargs) -> ProgressFuture:
        cancel_ev = threading.Event()
        progress_ev = threading.Event()
        progress_info: ProgressInfo = dict(n=0, total=0, status="pending")
            
        t = super().submit(make_f, f, *args, cancel_ev = cancel_ev, progress_ev=progress_ev, progress_info=progress_info , **kwargs)

        t.__class__ = ProgressFuture
        t: ProgressFuture
        t._child_init(progress_info, progress_ev, cancel_ev)
        
        return t
    
class ProcessPoolProgressExecutor(concurrent.futures.ProcessPoolExecutor, ProgressExecutor):
    def __init__(self, *args,**kwargs):
        super().__init__(*args, **kwargs)

    def submit(self, f, *args, progress_init_args=(), **kwargs) -> ProgressFuture:
        cancel_ev = self.manager.Event()
        progress_ev = self.manager.Event()
        progress_info: ProgressInfo = self.manager.dict(n=0, total=0, status="pending")
            
        t = super().submit(make_f, f, *args, cancel_ev = cancel_ev, progress_ev=progress_ev, progress_info=progress_info , **kwargs)

        t.__class__ = ProgressFuture
        t: ProgressFuture
        t._child_init(progress_info, progress_ev, cancel_ev)

        return t
    
    def __enter__(self, *args, **kwargs):
        import multiprocessing
        super().__enter__(*args, **kwargs)
        self.manager = multiprocessing.Manager()

    def __exit__(self, *args, **kwargs):
        super().__exit__(*args, **kwargs)
        self.manager.shutdown()

