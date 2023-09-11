from __future__ import annotations
from typing import Dict, Any, List, Callable, Literal, Optional, Tuple, Set, TypedDict, NoReturn
import concurrent, threading, time, asyncio, tqdm,time, functools, signal


class ProgressInfo(TypedDict):
    n: int | float
    total: int | float
    status: str


class ProgressFuture(concurrent.futures.Future):
    progress_info: ProgressInfo
    progress_ev: threading.Event
    cancel_ev: threading.Event

    progress_callbacks: Callable[[progress_info], None]

    def _child_init(self, progress_info: ProgressInfo, progress_ev: threading.Event, cancel_ev: threading.Event):
        self.progress_info = progress_info
        self.progress_ev = progress_ev
        self.cancel_ev = cancel_ev
        self.progress_callbacks = []

    def cancel(self):
        super().cancel()
        self.cancel_ev.set()

    def add_progress_callback(self, fn):
        self.progress_callbacks.append(fn)

    def remove_progress_callback(self, fn):
        self.progress_callbacks.remove(fn)

    def _process_progress(self, n: float, total: float):
        for c in self.progress_callbacks:
            c(self.progress_info)

    async def check_for_progress(self, sleep_duration=0.1):
        try:
            while not self.done():
                has_progress = False
                if self.progress_info["status"] != self._state:
                    self.progress_info["status"] = self._state
                    self.has_progress=True

                if self.progress_ev.is_set():
                    self.progress_ev.clear()
                    has_progress = True
                    

                if has_progress:
                    self._process_progress()
                await asyncio.sleep(sleep_duration)
            return self.result()
        except asyncio.CancelledError:
            self.cancel()
            raise

class ProgressExecutor(concurrent.futures.Executor):
    def submit(self, f, *args, **kwargs) -> ProgressFuture:
        raise NotImplementedError("Abstract submit method")

class CustomUpdater:
    def __init__(self, cancel_ev, progress_ev, progress_info):
        self.n=0
        self.total=0
        self.cancel_ev = cancel_ev
        self.progress_ev = progress_ev
        self.progress_info = progress_info
        
        self.last_time = time.time()
        self.min_interval = 0.1
        self.min_amount_percentage = 0.005
        self.last_amount = self.n

        self.progress_info["status"] = "Running"
        self.refresh()
        
    def refresh(self, *args, **kwargs):
        self.progress_ev.set()
        (self.progress_info["n"], self.progress_info["total"]) = (self.n, self.total)
        if self.cancel_ev:
            raise asyncio.CancelledError() from None
        


    def update(self, amount):
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
    
    def close(self):
        self.refresh()
        while self.progress_ev.is_set():
            time.sleep(0.1)


def make_f(f, *args, cancel_ev, progress_ev, progress_info, **kwargs):
    updater = CustomUpdater(cancel_ev, progress_ev, progress_info)
    res = f(*args, progress = updater, **kwargs)
    updater.close()
    return res


class ThreadPoolProgressExecutor(concurrent.futures.ThreadPoolExecutor, ProgressExecutor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def submit(self, f, *args, **kwargs) -> ProgressFuture:
        cancel_ev = threading.Event()
        progress_ev = threading.Event()
        progress_info = dict(n=0, tot=0, status="pending")
            
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
        progress_info = self.manager.dict(n=0, tot=0, status="pending")
            
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