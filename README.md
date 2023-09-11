# Motivation

While Concurrent.futures enables one to seemlessly launch with different executors (threadpool and processpool) the same code,
it is non trivial to add the two following features:
1. Handling cancellation
2. Handling progress of a task from elsewhere (i.e. one can print a tqdm bar within the task, but what if one wishes to aggregate progress from different tasks?)

Furthermore, we additionally add executor that behaves in "sync" (no threads, no processes) for testing purposes


# Problem

Correctly handling cancellation is more complicated than can be expected: 
a thread cannot be cancelled from the outside without harm.

As for progress, as soon as one wishes to do computation on the progress of different tasks one needs to retrieve the data from the different processes.
The synchronization aspects are not extremelly difficult but are still error prone.

# Solution

## The big picture

While cancellation of processes could be handled by a reimplementation of the concurrent.futures.ProcessPoolExecutor 
such that a synchronized dictionary of which process is executing which task is saved and then by simply killing the relevant process,
this is not the case for concurrent.futures.ThreadPoolExecutor. 

Furthermore, as we wish to encourage tasks to give progress information regurlarly, we have taken a different approach:
we assume that tasks can check for cancellation/give progress information regurlarly and we automatically raise asyncio.CancelledError from within the task,
should cancellation be requested. This finishes the task and thus releases the ThreadPool/ProcessPool.

## A limitation for progress calls

As we wish progress updates to happen in the main thread, and yet not block whatever else the main thread is doing, we implemented it as a async coroutine.
This means that the main thread should be running as asyncio loop (this is probably already the case if you wanted to use concurrent.futures) where fairness is respected.

## The case of the "sync" executor

# API

## API for submitted functions tasks

## API to submit these functions as tasks

