import os
import time
import functools
from benchmark.basic import client
from decorator import decorator
from benchmark.basic.my_logger import logger

TIMER = os.environ.get("TIMER", "0") == "1"


@decorator
def wait_task(func, callback=None, timeout=120, task_type="ML", *args, **kw):
    res = func(*args, **kw)
    if "task_id" not in res:
        return res
    task_id = res["task_id"]
    if is_complete(res):
        return res

    uri = None
    if task_type == "ML":
        uri = "/_plugins/_ml/tasks/{}".format(task_id)
    startTime = time.time()
    while True:
        task_res = client.http.get(uri)
        if is_complete(task_res):
            if callback is not None:
                callback_obj = args[0]
                actual_callback = getattr(callback_obj, callback.__name__, None)
                if actual_callback is not None:
                    actual_callback(task_res)
            break
        time.sleep(5)
        elapsedTime = time.time() - startTime
        if elapsedTime > timeout:
            break
    return res

def is_complete(res):
    if "status" in res and res["status"] == "COMPLETED":
        return True 
    if "state" in res and res["state"] == "COMPLETED":
        return True
    return False


@decorator
def trace(func, *args, **kw):
    logger.debug(f"before run: {func.__module__}:{func.__qualname__}")
    startTime = time.time()
    res = func(*args, **kw)
    elapsedTime = time.time() - startTime
    if TIMER:
        logger.info(f"{func.__qualname__} elapsed time: {elapsedTime:.2f} seconds")
    logger.debug(f"after run: {func.__module__}:{func.__qualname__}")
    logger.debug(f"result: {res}")
    return res
   

def parser(obj, keys):
    def get(obj, key):
        if isinstance(obj, list):
            ret = []
            for o in obj:
                try:
                    ret.append(get(o, key))
                except KeyError:
                    pass
            if len(ret) == 0:
                raise KeyError
            return ret
        else:
            if key == "*":
                return list(obj.values())
            if key not in obj:
                raise KeyError
            return obj[key]

    for key in keys:
        obj = get(obj, key)

    # flatten
    return flatten(obj)

def flatten(obj):
    if isinstance(obj, list):
        ret = []
        for i in obj:
            r = flatten(i)
            if isinstance(r, list):
                ret = ret + r
            else:
                ret.append(flatten(i))
        return ret
    else:
        return obj


@decorator
def swallow_exceptions(func, exceptions=None, *args, **kw):
    """
    Decorator to swallow exceptions.
    
    Args:
        func: The function to decorate
        exceptions: List of exception types to catch. If None, catches all exceptions.
    
    Returns:
        The result of the function or None if an exception occurred
    """
    try:
        return func(*args, **kw)
    except Exception as e:
        if exceptions is None or any(isinstance(e, exc) for exc in exceptions):
            logger.error(f"Swallowed exception in {func.__qualname__}: {str(e)}")
            return None
        else:
            raise

