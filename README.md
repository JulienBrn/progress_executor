# Motivation

While Concurrent.futures enables one to seemlessly launch with different executors ([threadpool and processpool](https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ProcessPoolExecutor)) the same code,
it is non trivial to add the two following features:
1. Handling cancellation of a task currently executing
2. Handling progress of a task from elsewhere (i.e. one can print a tqdm bar within the task, but what if one wishes to aggregate progress from different tasks?)

Furthermore, we additionally add a new executor that behaves in "sync" (no threads, no processes) for testing purposes


# Problem

Correctly handling cancellation is more complicated than can be expected: 
a thread cannot be cancelled from the outside without harm.

As for progress, as soon as one wishes to do computation on the progress of different tasks one needs to retrieve the data from the different processes.
The synchronization aspects are not extremelly difficult but are still error prone.

# Solution

## The big picture

While cancellation of processes could probably (see alternatives) be handled by a reimplementation of the [concurrent.futures.ProcessPoolExecutor]((https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ProcessPoolExecutor)), we do not see a good way to do so for the ThreadPoolExecutor. As we wish to have a uniform API with similar semantics, we opted for another solution.

This solution has the drawback of requiring the programmer to give regurlar progress information, but we believe this is a relatively low drawback as the programmer should already be doing so. 
We then use the regurlar progress calls to check for cancellation and cancel from within the task if necessary.


## A limitation for progress calls

As we wish progress updates to happen in the main thread, and yet not block whatever else the main thread is doing, we implemented it as a async coroutine.
This means that the main thread should be running as asyncio loop (this is probably already the case if you wanted to use concurrent.futures) where fairness is respected.


# API



## Using ProgressExecutors 

Our executors inherit for the concurrent.futures.Executor. We provide three executors that can be created (for example) with the following lines:

```python
from progress_executor import ThreadPoolProgressExecutor, ProcessPoolProgressExecutor, SyncProgressExecutor

tp = ThreadPoolProgressExecutor(max_workers =3)
pp = ProcessPoolProgressExecutor(max_workers =3)
se = SyncProgressExecutor()
```


To create tasks, one should use the submit function of concurrent.futures.Executor, and we highly recommand creating a task from that future (as suggested by modern asyncio).
The use of asyncio.TaskGroup in an async with block is highly recommanded (see asyncio docs) 

```python
with executor:
    async with asyncio.TaskGroup() as tg:
        future1 = executor.submit(f1, *args, **kwargs)
        task1 = tg.create_task(future1)

        future2 = executor.submit(f2, *args, **kwargs)
        task2 = tg.create_task(future2)
```

The following lines runs the task, but without any changes compared to using the executors of concurrent.futures (except for cancellation). This is because we have not taken advantage that the futures returned by a the submit method of ProgressExecutor are ProgressFuture.

## ProgressFuture Method

To correctly use ProgressFuture, there three modifications that are required:

1. Use  `tg.create_task(future.check_for_progress())` instead of `tg.create_task(future)`. This adds the progress callbacks to the asyncio loop.
2. Add callbacks on progress. You can either use `future.add_progress_callback(old_state, new_state)`, where state is a dictionary with items `n, total, status`,
or use `future.add_tqdm_callback(tqdm_cls=tqdm.tqdm, init_kwargs = {}, trigger: Set[Literal["now", "running", "cancelled"]] = {"now"} )` where `tqdm_cls` should be a class/function (not instance)
that has similar API to tqdm.tqdm, `init_kwargs` is the initializer arguments for `tqdm_cls` and `trigger` states when `tqdm_cls(**init_kwargs)` is called.
3. Add a progress argument to your function.

## Full Example

```python
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
    for i in progress(range(2*n)): #you can use progress directly on an iterator
        if i %2 ==0:
            long_compute(0.1)
        else:
            time.sleep(0.1)
    return n

tp = ThreadPoolProgressExecutor(max_workers =3)
pp = ProcessPoolProgressExecutor(max_workers =3)
se = SyncProgressExecutor()
executor = tp #Change here to see the differences


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
                #See what happends if you uncomment these two lines
                # await asyncio.sleep(2) 
                # tasks[0].cancel()
    finally:
        print("FINISHED")
        for i, (val,task) in enumerate(zip(vals, tasks)):
            print(f"Task {i} with val={val} has result {'cancelled' if task.cancelled() else task.result()}")
    

if __name__ == "__main__": #Necessary due to multiprocessing
    beautifullogger.setup(warning_level_modules=["asyncio"]) #Just for pretty printing
    logger.info("Running start")
    asyncio.run(main())
    logger.info("Running end")
```