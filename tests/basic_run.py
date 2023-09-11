
import logging, beautifullogger
import sys, time, asyncio, tqdm
from progress_executor import *

logger = logging.getLogger(__name__)


def long_compute(n):
    tot = 17
    for i in range(int(n*25000000)):
        tot = tot//2 if tot % 2 ==0 else 3*tot+1
    return tot


def f(n, progress: Updater):
    progress.total = n
    for i in progress(range(2*n)):
        if i %2 ==0:
            long_compute(0.1)
        else:
            time.sleep(0.1)
    return n

tp = ThreadPoolProgressExecutor(max_workers =3)
pp = ProcessPoolProgressExecutor(max_workers =3)
se = SyncProgressExecutor()
executor = tp


async def main():
    vals = [30, 40, 35, 60, 20, 50, 38, 27]*2
    try:
        with executor:
            async with asyncio.TaskGroup() as tg:
                tasks=[]
                for i, val in enumerate(vals):
                    fut = executor.submit(f, val)
                    fut.add_tqdm_callback(tqdm.tqdm, dict(desc=f"Task {i}"), triggers=["now", "running", "cancelled"])
                    tasks.append(tg.create_task(fut.check_for_progress()))
                await asyncio.sleep(2)
                tasks[0].cancel()
    except asyncio.CancelledError: pass
    finally:
        print("FINISHED")
        for i, (val,task) in enumerate(zip(vals, tasks)):
            print(f"Task {i} with val={val} has result {'cancelled' if task.cancelled() else task.result()}")
    

if __name__ == "__main__":
    beautifullogger.setup(warning_level_modules=["asyncio"])
    logger.info("Running start")
    asyncio.run(main())
    logger.info("Running end")