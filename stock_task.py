from celery import Celery
import config
import multiprocessing
import gevent
from gevent.pool import Pool as gPool
import os
import time


broker = config.REDIS_URL+"5"
backend = config.REDIS_URL+"6"
# celery -A stock_task worker --loglevel=info --logfile=./logs/celery_stock_task.log
app = Celery('stock_task', broker=broker, backend=backend)
# app.worker_main(argv=[''
#      '--loglevel=info',
#      '--logfile=./logs/celery_stock_task.log'
#  ])

def set_part(size:int):
    bit = len(str(size))
    if bit > 3:
        return 10**(bit - 3)
    else:
        return 1

def compelete_per(s:int, c:int):
    print("task running : %.2f %%" % (c/s*100))

def time_sleep(second = 1):
    time.sleep(second)

@app.task
def test():
    print("celery work!")

@app.task
def run_task_process(fun:object, fun_kwargs:list, processes:int=2, prefun:object=None, postfun:object=None):
    size = len(fun_kwargs)
    price = set_part(size)
    pool = multiprocessing.Pool(processes=processes)
    if prefun is not None:
        for i in range(0, processes):
            pool.apply(func=prefun)
    fun_threads = []
    for i, kwargs in enumerate(fun_kwargs):
        res = pool.apply_async(func=fun, kwds=kwargs)
        if i % price == 0:
            pool.apply_async(compelete_per, kwds={"s":size, "c":i})
        fun_threads.append(res)
    if postfun is not None:
        for i in range(0, processes):
            pool.apply(func=postfun)
    pool.close()
    pool.join()
    results = []
    for thread in fun_threads:
        if thread.successful():
            results.append(thread.get())
    return results

@app.task
def run_task_gevent(fun:object, fun_kwargs:list, processes:int=2, prefun:object=None, postfun:object=None):
    size = len(fun_kwargs)
    price = set_part(size)
    pool = gPool(processes)
    if prefun is not None:
        pool.apply(prefun)
    gevent_threads = []
    for i, kwargs in enumerate(fun_kwargs):
        res = pool.apply_async(fun, kwds = kwargs)
        gevent_threads.append(res)
        if i % price == 0:
            pool.apply_async(compelete_per, kwds={"s":size, "c":i})
    pool.join()
    results = []
    for thread in gevent_threads:
        if thread.successful():
            results.append(thread.value)
    if postfun is not None:
        pool.apply(postfun)
    return results
