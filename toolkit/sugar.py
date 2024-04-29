import time
import numpy as np
import pandas as pd
import subprocess as sp

from io import StringIO
from contextlib import contextmanager
from typing import List, Union, Tuple, Iterable, Any, Callable, Optional, cast, TypeVar, Dict, Set
from collections import defaultdict

__all__ = ['clock_it', 'run_cmd', 'filter_from_gather', 'grain_separator',
           'group_by_attrib', 'count_uniq_attrib', 'check_last', 'diff_sets',
           'load_output']


@contextmanager
def clock_it(action: str, method: Callable = time.time, unit: str = 's',
             print_func: Callable = print, fmt: str = '{action} takes {sec:.3f}{unit}'):
    start = method()
    try:
        yield
    finally:
        end = method()
        print_func(fmt.format(action=action, sec=end - start, unit=unit))


def run_cmd(cmd: Union[List[str], str], expect: int = 0, **kwargs) -> Tuple[str, str, int]:

    p = sp.run(cmd, capture_output=True, text=True, shell=isinstance(cmd, str), **kwargs)
    if p.returncode == expect:
        return p.stdout, p.stderr, p.returncode
    else:
        raise RuntimeError(f'Failed to execute {cmd}, retval = {p.returncode}, '
                           f'stderr = {p.stderr}')


def filter_from_gather(iters: Iterable[Any], mask: Iterable[Any], *, truthy: bool = False,
                       custom_test: Optional[Callable] = None, fill_none: bool = False):
    def _test(m) -> bool:
        """Check if m is an exception, if not, depending on whether truthy is True,
        we may further test the truthness of the mask
        """
        if callable(custom_test): return custom_test(m)
        if not isinstance(m, BaseException):
            return (truthy and bool(m)) or (not truthy)
        else:
            return False

    for elem, m in zip(iters, mask):
        if _test(m):
            yield elem
        elif fill_none:
            yield None


_T = TypeVar('_T')
GrainType = Tuple[List[_T], List[_T]]

def grain_separator(iters: Iterable[_T], *, filter_func: Optional[Callable[[_T], bool]] = None,
                    mask: Optional[Iterable[bool]] = None) -> Tuple[List[_T], List[_T]]:
    """Split a sequences into two lists based on the filter_func

    Args:
        iters (Iterable[Any]): Sequence to be splitted
        filter_func (Optional[Callable]): A filter function that returns a boolean val
        mask (Optional[Iterable[Any]]): Sequence of True or False values

    Returns:
        Two lists: the first list has elements that are tested false,
        the second has elements that are tested true
    """
    if not filter_func and not mask:
        raise ValueError('Need to specify at least filter_func or mask')

    F, T = [], []
    mask_iter = iter(mask) if mask else None
    for e in iters:
        is_true = next(mask_iter) if mask_iter else cast(Callable, filter_func)(e)
        if is_true: T.append(e)
        else: F.append(e)
    return F, T


def group_by_attrib(iters: Iterable[_T], *, attrib: Optional[str] = None,
                    func: Optional[Callable] = None) -> Dict[Any, List[_T]]:
    if not attrib and not func:
        raise ValueError('Need to specify at least attrib or func')

    group = defaultdict(list)
    for elem in iters:
        if attrib:
            key = getattr(elem, attrib)
        else:
            key = cast(Callable, func)(elem)
        group[key].append(elem)
    return group


def count_uniq_attrib(iters: Iterable[Any], *, attrib: Optional[str] = None,
                      func: Optional[Callable] = None) -> int:
    if not attrib and not func:
        raise ValueError('Need to specify at least attrib or func')

    _res = set()
    for elem in iters:
        if attrib:
            val = getattr(elem, attrib)
        else:
            val = cast(Callable, func)(elem)
        _res.add(val)
    return len(_res)


def check_last(it: Iterable[Any]) -> Iterable[Tuple[bool, Any]]:
    it = iter(it)
    try:
        cur = next(it)
    except StopIteration:
        return
    else:
        for next_elem in it:
            yield False, cur
            cur = next_elem
        yield True, cur


def diff_sets(s1: Set[Any], s2: Set[Any], print_func: Callable = print):
    new = len(s2 - s1)
    rm = len(s1 - s2)
    print(f'S1: {len(s1)}; S2: {len(s2)}; New: {new}; Deleted: {rm}')
    return new, rm


def load_output(out: str, delimiter: str = ' ') -> np.ndarray:
    return pd.read_csv(StringIO(out), header=None, index_col=None,
                       delimiter=delimiter).to_numpy()
