from __future__ import annotations
from typing import Dict, Any, List, Callable, Literal, Optional, Tuple, Set, TypedDict, NoReturn, TYPE_CHECKING
import concurrent, threading, time, asyncio, time, functools, signal
from progress_executor.progress_executor_base import Updater, ProgressExecutor, ProgressFuture, ProgressInfo


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



class PoolProgressFuture(ProgressFuture):
    old_progress_info: ProgressInfo
    progress_info: ProgressInfo
    progress_ev: threading.Event
    cancel_ev: threading.Event

    progress_callbacks: Callable[[progress_info], None]
    def __init__(self):
        super().__init__()
        
    def _child_init(self, progress_info: ProgressInfo, progress_ev: threading.Event, cancel_ev: threading.Event):
        super()._child_init(progress_info)
        self.progress_ev = progress_ev
        self.cancel_ev = cancel_ev

    def cancel(self):
        self.cancel_ev.set()
        super().cancel()

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


def make_f(f, *args, cancel_ev, progress_ev, progress_info, **kwargs):
    updater = PoolUpdater(cancel_ev, progress_ev, progress_info)
    try:
        res = f(*args, progress = updater, **kwargs)
        updater.status = "done"
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

        t.__class__ = PoolProgressFuture
        t: PoolProgressFuture
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

        t.__class__ = PoolProgressFuture
        t: PoolProgressFuture
        t._child_init(progress_info, progress_ev, cancel_ev)

        return t
    
    def __enter__(self, *args, **kwargs):
        import multiprocessing
        super().__enter__(*args, **kwargs)
        self.manager = multiprocessing.Manager()

    def __exit__(self, *args, **kwargs):
        super().__exit__(*args, **kwargs)
        self.manager.shutdown()

