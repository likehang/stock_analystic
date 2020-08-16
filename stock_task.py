from celery import Celery
import config
import multiprocessing
import gevent
from gevent.pool import Pool as gPool
import os


broker = config.REDIS_URL+"5"
backend = config.REDIS_URL+"6"
# celery -A stock_task worker --loglevel=info --logfile=./log_celery_stock_task.log
app = Celery('stock_task', broker=broker, backend=backend)
app.debug = True
# app.worker_main(argv=[''
#     '--loglevel=info',
#     '--logfile=./log_celery_stock_task.log'
# ])

# gevent.config.set("GEVENT_SUPPORT",True)

def compelete_per(s:int, c:int):
    print("task running :", str(c/s*100),"%")


@app.task
def test():
    print("celery work!")

@app.task
def run_task_process(fun:object, fun_kwargs:list, processes:int=2, prefun:object=None, postfun:object=None):
    pool = multiprocessing.Pool(processes=processes)
    if prefun is not None:
        for i in range(0, processes):
            pool.apply_async(func=prefun)
    fun_threads = []
    for kwargs in fun_kwargs:
        res = pool.apply_async(func=fun, kwds=kwargs)
        fun_threads.append(res)
    if postfun is not None:
        for i in range(0, processes):
            pool.apply_async(func=postfun)
    pool.close()
    pool.join()
    results = []
    for thread in fun_threads:
        if thread.successful():
            results.append(thread.get())
    return results

@app.task
def run_task_gevent(fun:object, fun_kwargs:list, processes:int=2, prefun:object=None, postfun:object=None):
    pool = gPool(processes)
    if prefun is not None:
        pool.apply_async(prefun)
    gevent_threads = []
    for kwargs in fun_kwargs:
        gevent_threads.append(pool.apply_async(fun, kwds = kwargs))
    gevent.joinall(gevent_threads)
    results = []
    for thread in gevent_threads:
        if thread.successful():
            results.append(thread.value)
    if postfun is not None:
        pool.apply_async(postfun)
    return results
