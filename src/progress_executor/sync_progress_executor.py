from __future__ import annotations
from typing import Dict, Any, List, Callable, Literal, Optional, Tuple, Set, TypedDict, NoReturn, TYPE_CHECKING
import concurrent, threading, time, asyncio, time, functools, signal
from progress_executor.progress_executor_base import Updater, ProgressExecutor, ProgressFuture, ProgressInfo
from progress_executor.pool_progress_executor import PoolUpdater

class SyncUpdater(Updater):
    def __init__(self, fut):
        super().__init__()
        self.fut = fut
    def refresh(self):
        (self.fut.old_progress_info["n"], self.fut.old_progress_info["total"], self.fut.old_progress_info["status"]) = (self.fut.progress_info["n"], self.fut.progress_info["total"], self.fut.progress_info["status"])
        (self.fut.progress_info["n"], self.fut.progress_info["total"], self.fut.progress_info["status"]) = (self.n, self.total, self.status)
        self.fut._process_progress()

class SyncProgressFuture(ProgressFuture):

    def __init__(self, f, args, kwargs):
        super().__init__()
        super()._child_init(progress_info=dict(n=0, total=0, status="pending"))
        self.f = f
        self.args= args
        self.kwargs = kwargs
        

    async def check_for_progress(self, sleep_duration=0.1):
        if self.cancelled():
            raise asyncio.CancelledError() from None
        self.status = "running"
        old_handler = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, signal.default_int_handler)
        progress=SyncUpdater(self)
        try:
            try:
                res = self.f(*self.args, progress=progress , **self.kwargs)
            except Exception as e:
                res = e
                is_exception =True
            except KeyboardInterrupt:
                is_exception=False
                raise
            else:
                is_exception =False
            finally:
                progress.status = f"done, exception={is_exception}"
        except KeyboardInterrupt:
            self.cancel()
            progress.refresh()
            old_handler(None, None)
            raise
        else:
            progress.refresh()
            if is_exception:
                self.set_exception(res)
                return res
            else:
                self.set_result(res)
        finally:
            signal.signal(signal.SIGINT, old_handler)

class SyncProgressExecutor(ProgressExecutor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.shutdowned = False
        self.tasks=[]
        self.handlers =()
    def submit(self, f, *args, **kwargs) -> ProgressFuture:
        if not self.shutdowned:
            t= SyncProgressFuture(ProgressExecutor.add_progress_arg(f), args, kwargs)
            self.tasks.append(t)
            return t
        else:
            raise asyncio.CancelledError()
    
    def declare_handlers(self, default_handlers, loop_handler):
        self.handlers= (default_handlers, loop_handler)

    def shutdown(self, wait=True, *, cancel_futures=False):
        for t in self.tasks:
            t.cancel()
    def __enter__(self, *args, **kwargs):pass

    def __exit__(self, *args, **kwargs):
        self.shutdown()