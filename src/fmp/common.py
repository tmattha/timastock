import time
import inspect
from urllib.error import HTTPError
from concurrent import futures
from functools import wraps, partial
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
            try:
                result = worker.result()
            except Exception as e:
                print(f"Exception {e} on worker {worker}.")
                raise e

            if frame is None:
                frame = result
            else:
                try:
                    if result is not None:
                        frame = pl.concat([frame, result])
                except Exception as e:
                    print(f"Error {e} when applying result:")
                    print(result)
                    raise e

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

ParameterType = t.TypeVar('ParameterType')
ReturnType = t.TypeVar('ReturnType')
def convert_exceptions_to_none(func: t.Callable[ParameterType, ReturnType]) -> t.Callable[ParameterType, ReturnType | None]:
    @wraps(func)
    def __func_converting_exceptions(*args, **kwargs) -> ReturnType | None:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            trace_log(f"Exception {e} encountered and converted to None in function {func.__qualname__} with parameters {args} and {kwargs}.")
            return None
    
    return __func_converting_exceptions

def multi_dataframe(func):
    @wraps(func)
    def __wrapped_function(symbols: list[str], *args, **kwargs):
        return multithread_concat([partial(func, symbol, *args, **kwargs) for symbol in symbols])
    return __wrapped_function

def trace_log(str):
    now = time.strftime("%H:%M:%S")
    caller = inspect.currentframe().f_back.f_code.co_name
    print(f"[{now}] {caller}: {str}")