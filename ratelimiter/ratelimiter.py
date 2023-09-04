"""
On the basis of the thread pool, the operation of the function is controlled at a single point through the semaphore.

If no semaphore control is registered, it is not limited,
and the function execution is only limited by the number of threads in the thread pool
"""
from concurrent.futures import ThreadPoolExecutor, Future
from threading import Semaphore
from typing import Dict, List, Callable, Optional, Union
import functools
import queue
try:
    from ratelimiter.logging import logger
except ModuleNotFoundError:
    import logging
    logger = logging.getLogger('RateLimiter')


def result_callback(result_data: dict, future: Future):
    try:
        result = future.result()
    except Exception as error:
        result = {'exception': error}
    if result:
        if isinstance(result, dict):
            result_data.update(result)
        else:
            result_data["result"] = result
    

class RateLimiter:
    """
    Task processing throttler, controlled based on thread pool and semaphores
    """
    def __init__(
        self,
        max_workers: Optional[int] = 0
    ):
        if max_workers:
            self.pool = ThreadPoolExecutor(max_workers=max_workers)
        else:
            self.pool = ThreadPoolExecutor()
        
        self.process_queue: Dict[str, queue.Queue] = {}
        self.semaphore: Dict[str, Semaphore] = {}
        self.tasks_futures: Dict[str, List[Future, Dict]] = {}
        
    def register_semaphore(self, job_id: Union[str, Callable], semaphore: int = 0, cover: bool = False):
        """Register semaphore to control concurrency"""
        if semaphore == 0:
            return
        
        if callable(job_id):
            job_id = job_id.__name__
        if job_id in self.semaphore and not cover:
            return
        
        self.semaphore[job_id] = Semaphore(semaphore)
    
    def submit(
        self,
        task_id: str,
        fn: Callable,
        callbacks: Union[List[Callable], None] = None,
        *args,
        **kwargs
    ):
        """
        In the case of non-blocking, determine whether the semaphore can be acquired.
            Example Code: sem.acquire(blocking=False)
            
        * If it can be acquired, it is pushed into the thread pool for execution,
        and semaphores and other work are released in the thread pool by adding callbacks
        * If it cannot be acquired, it is queued and the queue data is processed in the form of FIFO each time
        
        Thread scan task status or judge whether the task needs to process conversion through callback
        """
        if callbacks is None:
            callbacks = []
            
        job_type = fn.__name__
        job_semaphore = self.semaphore.get(job_type)
        
        if job_semaphore:
            self._submit_with_sem(task_id, fn, callbacks, job_semaphore, *args, **kwargs)
        else:
            self._submit_direct(task_id, fn, *args, **kwargs)
            
        self._attach_callback(task_id, job_type, callbacks, with_semaphore=job_semaphore)
        
    def _submit_with_sem(
        self,
        task_id: str,
        fn: Callable,
        callbacks: List[Callable],
        job_semaphore: Semaphore,
        *args, **kwargs
    ):
        job_type = fn.__name__
        semaphore_acquired = job_semaphore.acquire(blocking=False)
        
        if semaphore_acquired:
            self._submit_direct(task_id, fn, *args, **kwargs)
        else:
            logger.debug(
                f'<Task Type: {job_type}> The semaphore cannot be obtained, '
                f'and the storage cache is waiting for task distribution'
            )
            process_queue = self.process_queue.get(job_type, queue.Queue())
            process_queue.put((task_id, fn, callbacks, args, kwargs))
            self.process_queue[job_type] = process_queue
    
    def _submit_direct(
        self,
        task_id: str,
        fn: Callable,
        *args, **kwargs
    ):
        logger.info(f'<Task: {task_id}>: Push tasks to workers')
        self.tasks_futures[str(task_id)] = [
            self.pool.submit(fn, *args, **kwargs),
            {},
        ]

    def _fn2worker_callback(self, fn_name: str, future: Future):
        process_queue = self.process_queue.get(fn_name)
        if not process_queue or process_queue.empty():
            return
        
        task_id, fn, callbacks, args, kwargs = process_queue.get()
        self.submit(task_id, fn, callbacks, *args, **kwargs)
            
    def _attach_callback(
        self,
        task_id: str,
        job_type: str,
        callbacks: List[Callable],
        with_semaphore: Union[Semaphore, None] = None
    ):
        """The callback of optional parameters is not supported temporarily
        
        if additional parameters are required, use function. partial creates a partial function
        """
        task_future_pair = self.tasks_futures.get(str(task_id), None)
        if task_future_pair is None:
            logger.info(f'<Task: {task_id}> did not enter the queue in the worker')
            return
        
        future = task_future_pair[0]
        # register callback function
        future.add_done_callback(
            functools.partial(result_callback, task_future_pair[1])
        )
        future.add_done_callback(
            lambda x: logger.info(f'<Task: {task_id}> execute done')
        )
        
        if with_semaphore:
            future.add_done_callback(
                lambda x: self.semaphore[job_type].release()
            )
            future.add_done_callback(
                functools.partial(self._fn2worker_callback, job_type)
            )
            
        for _callback in callbacks:
            future.add_done_callback(
                _callback
            )
            



