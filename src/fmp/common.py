import time
import inspect
from urllib.error import HTTPError
from concurrent import futures
from functools import wraps
import typing as t

import polars as pl
import tqdm

def multithread_concat(
    worker_funcs: t.Iterable[t.Callable[[], pl.DataFrame]],
) -> pl.DataFrame:
    caller_name = inspect.currentframe().f_back.f_code.co_name
    frame = None
    with futures.ThreadPoolExecutor(8) as executor:
        workers = [executor.submit(worker_func) for worker_func in worker_funcs]
        for worker in tqdm.tqdm(
            futures.as_completed(workers), caller_name, len(worker_funcs)
        ):
            result = worker.result()

            if frame is None:
                frame = result
            else:
                frame = pl.concat([frame, result])

    return frame


def ignore_rate_limit(func: t.Callable) -> t.Callable:
    @wraps(func)
    def __func_ignoring_rate_limit(*args, **kwargs) -> pl.DataFrame:
        while True:
            try:
                result = func(*args, **kwargs)
                return result
            except HTTPError as err:
                if err.code == 429:
                    time.sleep(10)
                else:
                    raise err
            except Exception as err:
                raise err

    return __func_ignoring_rate_limit
    