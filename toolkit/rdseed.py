import time
import asyncio
import numpy as np
import pandas as pd

from io import StringIO
from typing import Optional, Union, Sequence, List, cast

from . import gcp_logger
from .sugar import load_output
from .tstest import TsResp, TsConnection, TsSocketIO

__all__ = ['rdseed_support', 'SeedTestRes', 'RdseedEmitter', 'RdseedFlooder']


def rdseed_support(brand: str) -> bool:
    unsupported_brands = {'Intel(R) Xeon(R) CPU E5-2696 v3 @ 2.30GHz'}
    return brand not in unsupported_brands


class SeedTestRes(np.ndarray):
    def __new__(cls, results: Sequence[np.ndarray], desync_idx: int = 0,
                ts_idx: int = 1, signal_idx: int = 2, **kwargs):
        if not results:
            raise ValueError(f'SeedTestRes gets an empty list')

        if len({r.shape for r in results}) != 1:
            raise ValueError(f'Cannot merge results with differet shapes')

        obj = np.asarray(np.column_stack(results), **kwargs).view(cls)

        ncols = obj.shape[1]
        res_width = signal_idx + 1

        obj.cnt = len(results)
        obj.res_width = res_width
        obj._desync_idx = desync_idx
        obj._ts_idx = ts_idx
        obj._signal_idx = signal_idx
        obj._desync_cols = np.arange(ncols) % res_width == desync_idx
        obj._ts_cols = np.arange(ncols) % res_width == ts_idx
        obj._signal_cols = np.arange(ncols) % res_width == signal_idx
        return obj

    def __array_finalize__(self, obj):
        if obj is None: return
        if not isinstance(obj, self.__class__): return

        self.cnt: int = getattr(obj, 'cnt')
        self.res_width: int = getattr(obj, 'res_width')
        self._desync_idx: int = getattr(obj, '_desync_idx')
        self._ts_idx: int = getattr(obj, '_ts_idx')
        self._signal_idx: int = getattr(obj, '_signal_idx')
        self._desync_cols: np.ndarray = getattr(obj, '_desync_cols')
        self._ts_cols: np.ndarray = getattr(obj, '_ts_cols')
        self._signal_cols: np.ndarray = getattr(obj, '_signal_cols')

    @property
    def desyncs(self) -> np.ndarray: return self[:, self._desync_cols]

    @property
    def timestamps(self) -> np.ndarray: return self[:, self._ts_cols]

    @property
    def signals(self) -> np.ndarray: return self[:, self._signal_cols]

    @property
    def valid_rows(self) -> np.ndarray: return np.all(self.desyncs == 0, axis=1)

    @property
    def valid_signals(self) -> np.ndarray: return self.signals[self.valid_rows]

    @property
    def num_pos(self): return np.sum(self.valid_signals > 0, axis=0)

    @property
    def num_neg(self): return np.sum(self.valid_signals == 0, axis=0)

    @property
    def num_valid(self): return cast(int, np.sum(self.valid_rows))

    @property
    def num_desync(self): return len(self) - self.num_valid

    def valid_rows_among(self, idxs: Sequence[int]) -> np.ndarray:
        desyncs = self.desyncs[:, idxs]
        return np.all(desyncs == 0, axis=1)

    def valid_signals_among(self, idxs: Sequence[int]) -> np.ndarray:
        signals = self.signals[self.valid_rows_among(idxs)]
        return signals[:, idxs]

    def results(self, idx: int) -> np.ndarray:
        if idx >= self.cnt:
            raise IndexError(f'There are only {self.cnt} results but idx is {idx}')

        ncols_per_res = self._signal_idx + 1
        mask = (np.arange(self.shape[1]) // ncols_per_res) == idx
        return self[:, mask]

    def merge(self, __o: 'SeedTestRes') -> 'SeedTestRes':
        stacked = np.vstack((self, __o))
        return self.__class__([stacked], desync_idx=self._desync_idx,
                              ts_idx=self._ts_idx, signal_idx=self._signal_idx)

    def __len__(self) -> int: return self.shape[0]

    def mark_desyncs(self, active_ranges: np.ndarray):
        tss = self.timestamps
        valid_mask = None
        for start, end in active_ranges:
            _mask = np.logical_and(tss >= start, tss <= end)
            if valid_mask is None:
                valid_mask = _mask
            else:
                valid_mask = np.logical_or(valid_mask, _mask)

        if valid_mask is not None:
            invalid_mask = np.logical_not(valid_mask)
            self[:, self._desync_cols] = np.logical_or(self.desyncs, invalid_mask)


class RdseedEmitter:
    def __init__(self, interval: int = 200_000, err_bound: int = 500,
                 gadget_size: int = 5, num_channels: int = 1,
                 bin_name: str = 'bin/rdseed') -> None:
        self.interval = interval
        self.err_bound = err_bound
        self.gadget_size = gadget_size
        self.num_channels = num_channels
        self.bin = bin_name
        self.ch_queue = asyncio.Queue()
        for ch in range(num_channels):
            self.ch_queue.put_nowait(ch)

    @property
    def env_config(self):
        return dict(INTERVAL=self.interval, ERR_BOUND=self.err_bound,
                    GADGET_SIZE=self.gadget_size, NUM_CHANNELS=self.num_channels)

    def get_cmd(self, n_emits: int, start_ns: int):
        return f'{self.bin} emit {n_emits} {start_ns}'

    async def _emit_channel(self, conns: Sequence[TsConnection], channel: int,
                            n_emits: int, defer: Optional[float] = None,
                            start_ns: Optional[int] = None):
        if defer is None and start_ns is None:
            raise ValueError('Need to specify at least defer or start_ns')

        if not start_ns:
            start_ns = int((time.time() + defer) * 1e9) if defer else 0

        env_config = self.env_config
        env_config['BROADCAST_CHANNEL'] = channel
        cmd = self.get_cmd(n_emits, start_ns)

        tasks = [c.run_cmd(cmd, env=env_config) for c in conns]
        if len(tasks) > 1:
            resps = await asyncio.gather(*tasks, return_exceptions=True)
        else:
            resps = [await t for t in tasks]

        return resps

    def process_resps(self, resps: Sequence[Union[Optional[TsResp], BaseException]]):
        if isinstance(resps, BaseException):
            gcp_logger.error(f'Responses contain exceptions: {resps}')
            return None

        emit_res = []
        for resp in resps:
            if not resp or isinstance(resp, BaseException):
                gcp_logger.error(f'rdseed cmd received no response')
                return None
            elif resp.errored or resp.returncode != 0:
                gcp_logger.error(f'Failed to run rdseed channel!\n'
                                 f'retval={resp.returncode}; '
                                 f'stderr={resp.stderr}')
                return None
            else:
                data = pd.read_csv(StringIO(resp.stdout), sep=' ',
                                   header=None, index_col=None).to_numpy()
                emit_res.append(data)

        return SeedTestRes(emit_res)

    async def test(self, conns: Sequence[TsConnection], n_emits: int = 500,
                   defer: float = 0.5):
        ch = await self.ch_queue.get() # get a channel
        resps = await self._emit_channel(conns, ch, n_emits, defer)
        await self.ch_queue.put(ch) # release the channel
        return self.process_resps(resps)

    @property
    def interval_ns(self) -> int:
        if self.bin.endswith('-rt'):
            return self.interval
        else:
            return self.interval // 2


class RdseedFlooder:
    def __init__(self, emitter: RdseedEmitter, flood_gadget_size: int = 5) -> None:
        assert(emitter.bin == 'bin/rdseed-rt')
        self.emitter = emitter
        self.flood_gadget_size = flood_gadget_size

    async def flood(self, flooders: Sequence[TsConnection],
                    testers: Sequence[TsConnection], min_duration: float = .5,
                    n_emits: int = 100, defer: float = 0.1, max_retry: int = 10,
                    n_valid_thresh: Optional[int] = None, at_once: bool = False):
        emit_duration = self.emitter.interval_ns * (n_emits + 50)
        calc_duration = np.ceil((emit_duration * (len(testers) + 5) / 1e9 + defer) * 10) / 10
        duration = max(min_duration, calc_duration) if not at_once else min_duration

        async def _flooding():
            duration_ms = int(duration * 1e3)
            cmd = ' '.join([self.emitter.bin, 'loop', str(duration_ms)])
            env = dict(GADGET_SIZE=self.flood_gadget_size)
            resps = await asyncio.gather(*[c.run_cmd(cmd, env=env) for c in flooders],
                                                   return_exceptions=True)
            return resps

        async def _testing(_conns_to_test: Sequence[TsConnection], _defer: float,
                           channel: int):
            if at_once:
                tasks = [self.emitter._emit_channel([c], channel, n_emits, defer=_defer)
                        for c in _conns_to_test]
            else:
                emit_nss = np.arange(len(_conns_to_test)) * emit_duration
                emit_nss += time.time_ns() + int(_defer * 1e9)
                # print(f'Start nss: {emit_nss}')
                tasks = [self.emitter._emit_channel([c], channel, n_emits, start_ns=ns)
                         for ns, c in zip(emit_nss, _conns_to_test)]

            resps = await asyncio.gather(*tasks, return_exceptions=True)
            results = [self.emitter.process_resps(r) for r in resps]
            return results

        retry, not_enough = 0, True
        tester_order = {t: i for i, t in enumerate(testers)}
        tester_res: List[Optional[SeedTestRes]] = [None for _ in range(len(testers))]
        while retry < max_retry and not_enough:
            not_enough = False

            channel = await self.emitter.ch_queue.get()
            f_res, t_res = await asyncio.gather(_flooding(),
                                                _testing(testers, defer, channel))
            await self.emitter.ch_queue.put(channel)
            retry += 1

            ranges = []
            for resp in f_res:
                if resp is None or isinstance(resp, BaseException):
                    gcp_logger.error(f'Flooder failed with exception {resp}')
                    return None
                elif resp.returncode != 0 or resp.errored or not resp.stdout:
                    gcp_logger.error(f'Failed to run flooder; stderr={resp.stderr}; '
                                     f'stdout={resp.stdout}; retval={resp.returncode}')
                    return None
                else:
                    ranges.append(load_output(resp.stdout))

            remains = []
            for c, res in zip(testers, t_res):
                if res is not None:
                    if n_valid_thresh is not None:
                        gcp_logger.debug(f'Pre desync marking: {res.num_valid} valid; '
                                            f'{res.num_desync} desync; thresh: {n_valid_thresh}')

                        for r in ranges:
                            res.mark_desyncs(r)

                        gcp_logger.debug(f'Post desync marking: {res.num_valid} valid; '
                                            f'{res.num_desync} desync; thresh: {n_valid_thresh}')

                    idx = tester_order[c]
                    old_res = tester_res[idx]
                    if old_res is None:
                        tester_res[idx] = res
                    else:
                        tester_res[idx] = old_res.merge(res)

                    merged_res = tester_res[idx]
                    if n_valid_thresh and merged_res is not None and \
                       merged_res.num_valid < n_valid_thresh:
                        not_enough = True
                        remains.append(c)
            # gcp_logger.info(f'Flooder {len(testers)} testers to {len(remains)}')
            testers = remains

        if n_valid_thresh and not_enough:
            gcp_logger.warning(f'Tried {max_retry} times but some conns still '
                               f'cannot reach {n_valid_thresh} records; '
                               f'Flooders: {len(flooders)}; '
                               f'Tester: {len(testers)}')

        return tester_res
