import asyncio
import numpy as np

from collections import defaultdict
from typing import Optional, List, Union, Set, Tuple, Iterable, Any, Dict, Sequence, Callable
from . import gcp_logger
from .fingerprint import GCPMachine, GCPGen2Machine
from .tstest import disconnect_ts_sockets, TsConnection, TsSocketIO
from .rdseed import RdseedEmitter, RdseedFlooder, SeedTestRes
from .sugar import grain_separator, GrainType, group_by_attrib, clock_it

__all__ = ['FinpValidator', 'Gen2FinpValidator']


_GrpResult = Optional[Sequence[Set[TsConnection]]]
_ValidationResult = Sequence[Set[TsConnection]]
_GrpResultGather = Sequence[Union[_GrpResult, BaseException]]


class RetryExceeded(Exception): pass


class RetryEmitterWrapper:
    def __init__(self, emitter: RdseedEmitter, max_retries: int = 20,
                 max_fails: int = 3) -> None:
        self.max_retries, self.max_fails = max_retries, max_fails
        self._retries = 0
        self._emitter = emitter

    async def test(self, conns: Sequence[TsConnection], **kwargs):
        if self._retries == self.max_retries:
            raise RetryExceeded(f'Exceeded {self.max_retries} retries')

        _fails = 0
        while _fails < self.max_fails:
            emit_res = await self._emitter.test(conns, **kwargs)
            if emit_res is None:
                _fails += 1
            else:
                self._retries += 1
                return emit_res
        raise RetryExceeded(f'Exceeded {self.max_fails} test failures')


class FinpValidator:
    _default_model = '_default'

    def __init__(self, emitters: Dict[str, RdseedEmitter], n_valid_thresh: int = 60,
                 positive_ratio: float = .5,
                 emitter_factory: Callable[[], RdseedEmitter] = RdseedEmitter) -> None:
        self.emitters: Dict[str, RdseedEmitter] = defaultdict(emitter_factory)
        self.emitters.update(emitters)
        self.n_valid_thresh = n_valid_thresh
        self.positive_ratio = positive_ratio
        self._lock = asyncio.Lock()

    async def get_emitter(self, brand: str) -> RdseedEmitter:
        async with self._lock:
            return self.emitters[brand]

    async def _screen_negatives(self, conns: Sequence[TsConnection], retries: int = 50,
                                **kwargs) -> GrainType[TsConnection]:
        """ Returns a list of must-negative conns and **may-positive** conns
        Testing a long list of TsConnection is tricky, due to desyncs.
        For example, if I test four connection 0, 1, 2, and 3,
        and connections 1 and 2 are co-located, I may get:
        ----------
        0  1  2  3
        ----------
        N  D  N  N
        D  N  D  N
        D  P  P  N
        N  N  D  D
        ----------
        where N -> negative; P -> positive; D -> desynchronized.

        Recall that our goal here is to filter out connections that are
        always negative w.r.t. any other connection under test.
        Here's the algorithm:
        1. find positives: for each connection, if it has more than N positives,
        we consider it as positive w.r.t. some connections (although we don't
        know which ones) and do not consider it in the subsequent tests.
        2. test all the remaining pairs: for each remaining connection,
        we examine whether it has enough valid signals w.r.t. any other connection,
        and whether we can conclude that these two are negative to each other
        """
        def _test_among(emit_res: SeedTestRes, idxs: Sequence[int]):
            vsignals = emit_res.valid_signals_among(idxs)
            if vsignals.shape[0] < self.n_valid_thresh:
                # gcp_logger.debug(f'idx={idxs}, too many desyncs {vsignals.shape}')
                return None
            else:
                pos_rate = np.sum(vsignals > 0, axis=0) / vsignals.shape[0]
                # gcp_logger.debug(f'idx={idxs}, vsignal_shape: {vsignals.shape}, pos: {np.sum(vsignals > 0, axis=0)}, '
                #                  f'rate: {pos_rate}, res: {pos_rate >= self.positive_ratio}')
                return pos_rate >= self.positive_ratio

        if not conns:
            return [], []
        elif len(conns) == 1:
            return list(conns), [] # no need to test if we have one conn

        brands = {c.greeting.brand for c in conns}
        if len(brands) > 1:
            # WARNING: the default emitter is not safe, it may clash with other emitters!
            gcp_logger.warning(f'Observed {len(brands)} brands in screen neg!'
                               'Fallback to the default emitter')
            emitter = await self.get_emitter(self._default_model)
        else:
            brand = next(iter(brands))
            emitter = await self.get_emitter(brand)

        emit_wrapper = RetryEmitterWrapper(emitter, max_retries=retries)
        try:
            emit_res = await emit_wrapper.test(conns, **kwargs)
        except RetryExceeded as e:
            gcp_logger.warning(f'Screen unlikely initial try failed due to {e}')
            return [], list(conns)  # conservatively assume all conns need refined test

        if emit_res.num_valid >= self.n_valid_thresh:
            # fast path for the case that we have enough all-synced records
            mask = list(emit_res.num_pos / emit_res.num_valid >= self.positive_ratio)
            gcp_logger.info(f'Screen unlikely initial try got {emit_res.num_desync} desyncs; '
                            f'{emit_res.num_valid} valid')
        else:
            # slow path due to many desyncs
            gcp_logger.info(f'Screen unlikely initial try had too many desyncs. '
                            f'{emit_res.num_desync} desyncs; {emit_res.num_valid} valid; '
                            'fallback to pair-wise')

            # None -> no verification records yet;
            # Set[int] -> remaining cids to check; and
            # bool -> completed
            _veri_status = List[Union[None, Set[int], bool]]
            veri_results: _veri_status = [None for _ in range(len(conns))]
            _num_verified, _is_fst = 0, True
            while _num_verified < len(conns):
                if not _is_fst:
                    try:
                        emit_res = await emit_wrapper.test(conns, **kwargs)
                    except RetryExceeded as e:
                        gcp_logger.warning(f'Screen unlikely pair-wise try failed due to {e}')
                        break
                else:
                    _is_fst = False

                for cid in range(len(conns)):
                    vres = veri_results[cid]
                    if isinstance(vres, bool):
                        continue # has verified

                    cid_pos = _test_among(emit_res, [cid])
                    if cid_pos is not None:
                        if cid_pos[0]:
                            veri_results[cid] = True
                            _num_verified += 1
                        else:
                            unverified = {i for i in range(len(conns))
                                          if i != cid} if vres is None else vres
                            _still_unveri = set()
                            for c2 in unverified:
                                pair = [cid, c2]
                                pos = _test_among(emit_res, pair)
                                if pos is None: # not enough valid signals
                                    _still_unveri.add(c2)
                                else:
                                    if pos[0]: # positive w.r.t. c2
                                        veri_results[cid] = True
                                        _num_verified += 1
                                        break

                            if veri_results[cid] is not True: # not positive during the last test
                                if _still_unveri:
                                    veri_results[cid] = _still_unveri
                                else: # every pair is verified, negative
                                    veri_results[cid] = False
                                    _num_verified += 1
                    else:
                        pass # not enough valid rows

                gcp_logger.info(f'{_num_verified}/{len(conns)} conns are pre-screened')

            # generate mask
            gcp_logger.debug(veri_results)
            mask = [r if isinstance(r, bool) else True for r in veri_results]

        return grain_separator(conns, mask=mask)

    async def _group_likely(self, conns: Sequence[TsConnection], min_pos_conns: int = 2,
                            retries: int = 10, **kwargs) -> _GrpResult:
        """
        Algorithm assumes that when we test (min_pos_conns + 1) connections,
        all positives ones are co-located with each other.
        Therefore, we test (min_pos_conns + 1) connections each time and
        merge positive connections into the same set.
        """
        if not conns:
            return []
        elif len(conns) == 1:
            return [set(conns)] # one connection in its own group

        brands = {c.greeting.brand for c in conns}
        if len(brands) > 1:
            # WARNING: the default emitter is not safe, it may clash with other emitters!
            gcp_logger.warning(f'Observed {len(brands)} brands in group likely!'
                               'Fallback to the default emitter factory')
            emitter = await self.get_emitter(self._default_model)
        else:
            brand = next(iter(brands))
            emitter = await self.get_emitter(brand)

        async def _run_test_among(idxs: Sequence[int]):
            """Test if conns[idxs] with rdseed

            Args:
                idxs (Sequence[int]): idxs of conns to test

            Returns:
                Optional[List[bool]]: A list of whether the conn is tested positive
            """
            old_emit_res = None
            _conns = [conns[idx] for idx in idxs]
            emit_wrapper = RetryEmitterWrapper(emitter, max_retries=retries)
            while True:
                try:
                    emit_res = await emit_wrapper.test(_conns, **kwargs)
                except RetryExceeded as e:
                    gcp_logger.warning(f'"Group likely" failed due to {e}')
                    break
                else:
                    if old_emit_res is not None:
                        emit_res = old_emit_res.merge(emit_res)
                        gcp_logger.debug(f'merged emit res {emit_res.shape[0]}')

                    if emit_res.num_valid >= self.n_valid_thresh:
                        return list(emit_res.num_pos / emit_res.num_valid >= self.positive_ratio)
                    else:
                        old_emit_res = emit_res

            gcp_logger.error(f'Failed to test likely conns')
            return None

        def _next_n(seq: Iterable[Any], n: int = 2):
            it = iter(seq)
            buf = []
            for i, elem in enumerate(it):
                buf.append(elem)
                if i % n == (n - 1):
                    yield buf
                    buf = []
            if buf:
                yield buf

        # connections are grouped by the current co-location info
        # one connection represents a group of co-located conns
        under_tests = {cid: {cid} for cid in range(len(conns))}
        groups = []
        step = (min_pos_conns * 2 - 1) - 1 # can test up to min_pos_conns*2-1, minus base
        while under_tests:
            base_cid, *remains = list(under_tests)
            for _conn_cids in _next_n(remains, n=step):
                _conn_cids.append(base_cid) # base_cid must be the last for later processing
                test_res = await _run_test_among(_conn_cids)
                if test_res is None:
                    return None

                _, _pos_cids = grain_separator(_conn_cids, mask=test_res)
                if _pos_cids: # _pos_cids are the co-located conns
                    if len(_pos_cids) < min_pos_conns:
                        gcp_logger.error(f'We expect at least {min_pos_conns} positive conns (if any). '
                                         f'But we got {len(_pos_cids)} positive connections instead!')
                        return None

                    # if base_cid is positive, then we merge to it
                    *_to_merge, _merge_to = _pos_cids
                    for _cid in _to_merge:
                        under_tests[_merge_to].update(under_tests[_cid])
                        under_tests.pop(_cid)

            # now, all conns that are co-located with base_cid are found
            groups.append(under_tests[base_cid])
            under_tests.pop(base_cid)

        return [{conns[cid] for cid in grp} for grp in groups]

    async def group_conns(self, conns: Sequence[TsConnection],
                          likely_positive: bool = False, **kwargs) -> _GrpResult:
        if not conns:
            return gcp_logger.info(f'No conns to group')
        elif len(conns) == 1:
            # gcp_logger.info(f'{len(conns)} conns are grouped to {len(conns)} hosts')
            return [set(conns)]

        if not likely_positive:
            screen_res = await self._screen_negatives(conns, **kwargs)
            must_negs, may_poss = screen_res
        else:
            must_negs, may_poss = [], conns
        grps = [{n} for n in must_negs]

        if may_poss:
            pos_grps = await self._group_likely(may_poss, **kwargs)
            if not pos_grps:
                gcp_logger.error(f'Failed to group {len(conns)} connections')
                return None
            else:
                grps.extend(pos_grps)

        gcp_logger.info(f'{len(conns)} conns are grouped to {len(grps)} hosts')
        return grps

    async def validate(self, conns: Sequence[TsConnection], stage1_retries: int = 5,
                       stage2_retries: int = 30, stage1_defer: float = .05,
                       stage2_defer: float = .5, early_close: bool = False,
                       **kwargs) -> Tuple[_ValidationResult, Sequence[TsConnection]]:
        with clock_it(f'Validating {len(conns)} fingerprints', print_func=gcp_logger.info):
            mgroups: Dict[GCPMachine, List[TsConnection]]
            mgroups = group_by_attrib(conns, func=lambda c: c.greeting.machine)

            tasks = [self.group_conns(_conns, likely_positive=True, retries=stage1_retries,
                                      defer=stage1_defer, **kwargs)
                    for _conns in mgroups.values()]
            stage1: _GrpResultGather = await asyncio.gather(*tasks, return_exceptions=True)

            failed_conns, to_close = [], []
            under_tests = defaultdict(set) # each conn represents a set of co-located conns
            for res, _conns in zip(stage1, mgroups.values()):
                if res is None or isinstance(res, BaseException):
                    failed_conns.append(_conns)
                else:
                    for grp in res:
                        if grp:
                            leader = next(iter(grp))
                            under_tests[leader] = grp
                            if early_close:
                                to_close.extend(grp - {leader})

            if early_close:
                await disconnect_ts_sockets([c for c in to_close if isinstance(c, TsSocketIO)])

            stage2_conns: Dict[str, List[TsConnection]]
            stage2_conns = group_by_attrib(list(under_tests),
                                        func=lambda c: c.greeting.brand)
            for k, v in stage2_conns.items():
                gcp_logger.info(f'Model {k} has {len(v)} conns to group')

            tasks = [self.group_conns(_conns, likely_positive=False, retries=stage2_retries,
                                      defer=stage2_defer, **kwargs)
                    for _conns in stage2_conns.values()]
            stage2: _GrpResultGather = await asyncio.gather(*tasks, return_exceptions=True)
            if early_close:
                for _conns in stage2_conns.values():
                    await disconnect_ts_sockets([c for c in _conns if isinstance(c, TsSocketIO)])

            for res, (b, _conns) in zip(stage2, stage2_conns.items()):
                if res is None or isinstance(res, BaseException):
                    failed_conns.append(_conns)
                    gcp_logger.error(f'{b}: failed to test {len(_conns)} connections')
                else:
                    for grp in res:
                        if grp:
                            merge_to = grp.pop()
                            for _conn in grp:
                                under_tests[merge_to].update(under_tests[_conn])
                                under_tests.pop(_conn)

            return list(under_tests.values()), failed_conns


class Gen2FinpValidator:
    def __init__(self, n_valid_thresh: int = 60, positive_ratio: float = .5,
                 uncertain_ratio: float = .06, flood_gadget_size: int = 5,
                 emitter_factory: Callable[[], RdseedEmitter] =
                 lambda: RdseedEmitter(interval=10_000, bin_name='bin/rdseed-rt')) -> None:
        self.emitters: Dict[Union[GCPGen2Machine, str], RdseedEmitter] = defaultdict(emitter_factory)
        self.n_valid_thresh = n_valid_thresh
        self.positive_ratio = positive_ratio
        self.uncertain_ratio = uncertain_ratio
        self.flood_gadget_size = flood_gadget_size
        self.emitter_factory = emitter_factory
        self._lock = asyncio.Lock()
        self._global_emitter = emitter_factory()

    async def get_emitter(self, mach: Union[GCPGen2Machine, str]) -> RdseedEmitter:
        async with self._lock:
            return self.emitters[mach]

    async def _screen_negatives(self, conns: Sequence[TsConnection], _emitter: RdseedEmitter,
                                concurrency: int = 30, retries: int = 10, **kwargs):
        async def _binary_test(conns: Sequence[TsConnection], _flooder: RdseedFlooder,
                               _sema: asyncio.Semaphore) -> GrainType[TsConnection]:
            if not conns: return [], []
            elif len(conns) == 1: return list(conns), []

            if conns[0].greeting.parsed_freq > 3_000_000_000:
                half = 1
            else:
                half = min(len(conns) // 2, 5)

            conns_l = conns[:half]
            conns_r = conns[half:]

            retry = 0
            pos_conns, remain_l, remain_r = [], [], []
            not_enough_l, not_enough_r = conns_l, conns_r
            while retry < retries and (not_enough_l or not_enough_r):
                async with _sema:
                    left_res = await _flooder.flood(conns_r, not_enough_l,
                                                    n_valid_thresh=self.n_valid_thresh,
                                                    at_once=True, n_emits=500, max_retry=20,
                                                    **kwargs)
                    right_res = await _flooder.flood(conns_l, not_enough_r,
                                                    n_valid_thresh=self.n_valid_thresh,
                                                    at_once=True, n_emits=500, max_retry=20,
                                                    **kwargs)

                if left_res is None or right_res is None:
                    gcp_logger.warning(f'Failed to screen negatives')
                    return [], list(conns)  # conservatively assume all conns may positive

                new_not_enough_l, new_not_enough_r = [], []
                for r, c in zip(left_res, not_enough_l):
                    if r is None:
                        gcp_logger.warning(f'Failed to screen a conn, treat it as may positive')
                        pos_conns.append(c)
                    elif r.num_valid < self.n_valid_thresh:
                        new_not_enough_l.append(c)
                    else:
                        if r.num_pos / r.num_valid >= self.positive_ratio:
                            pos_conns.append(c)
                        elif r.num_pos / r.num_valid >= self.uncertain_ratio:
                            new_not_enough_l.append(c)
                        else:
                            remain_l.append(c)

                for r, c in zip(right_res, not_enough_r):
                    if r is None:
                        gcp_logger.warning(f'Failed to screen a conn, treat it as may positive')
                        pos_conns.append(c)
                    elif r.num_valid < self.n_valid_thresh:
                        new_not_enough_r.append(c)
                    else:
                        if r.num_pos / r.num_valid >= self.positive_ratio:
                            pos_conns.append(c)
                        elif r.num_pos / r.num_valid >= self.uncertain_ratio:
                            new_not_enough_r.append(c)
                        else:
                            remain_r.append(c)

                not_enough_l, not_enough_r = new_not_enough_l, new_not_enough_r
                if not_enough_l or not_enough_r:
                    gcp_logger.debug(f'Try {retry}: no enough: left: {len(not_enough_l)}; right: {len(not_enough_r)}')

            if not_enough_l or not_enough_r:
                gcp_logger.warning(f'Failed to screen negatives after {retries} tries')
                return [], list(conns)

            tasks = [_binary_test(remain_l, RdseedFlooder(self.emitter_factory()), _sema),
                     _binary_test(remain_r, RdseedFlooder(self.emitter_factory()), _sema)]
            (neg_l, pos_l), (neg_r, pos_r) = await asyncio.gather(*tasks)
            return neg_l + neg_r, pos_conns + pos_l + pos_r

        semaphore = asyncio.Semaphore(concurrency)
        flooder = RdseedFlooder(_emitter, self.flood_gadget_size)
        return await _binary_test(conns, flooder, semaphore)

    async def _group_likely(self, conns: Sequence[TsConnection], emitter: RdseedEmitter,
                            retries: int = 10, failed_conns: Optional[List[TsConnection]] = None,
                            **kwargs) -> _GrpResult:
        if not conns:
            return []
        elif len(conns) == 1:
            return [set(conns)]

        flooder = RdseedFlooder(emitter, self.flood_gadget_size)
        retries *= len(conns) // 2

        retry = 0
        groups = defaultdict(set)
        while conns and retry < retries:
            base, *tester_conns = conns
            groups[base].add(base)
            remains = []
            if tester_conns:
                while retry < retries:
                    res = await flooder.flood([base], tester_conns,
                                              n_valid_thresh=self.n_valid_thresh,
                                              **kwargs)
                    retry += 1
                    if res is None:
                        gcp_logger.error('Failed to run flooder')
                        if failed_conns is not None:
                            failed_conns.append(base)
                            groups.pop(base)
                            remains = tester_conns
                            break
                        else:
                            return None

                    need_retry = []
                    for r, c in zip(res, tester_conns):
                        if r is None:
                            gcp_logger.error('One connection failed to run flooder test')
                            if failed_conns is not None:
                                failed_conns.append(c)
                                continue
                            else:
                                return None
                        elif r.num_valid < self.n_valid_thresh:
                            need_retry.append(c)
                        else:
                            if r.num_pos / r.num_valid >= self.positive_ratio:
                                groups[base].add(c)
                            elif r.num_pos / r.num_valid >= self.uncertain_ratio:
                                need_retry.append(c)
                            else:
                                remains.append(c)

                    if not need_retry:
                        break
                    else:
                        tester_conns = need_retry
            conns = remains

        if conns:
            gcp_logger.warning(f'Failed to group likely after {retries} retires')
            if failed_conns is not None:
                failed_conns.extend(conns)
            else:
                return None

        return list(groups.values())

    async def group_conns(self, conns: Sequence[TsConnection], emitter: RdseedEmitter,
                          likely_positive: bool = True, sema: Optional[asyncio.Semaphore] = None,
                          failed_conns: Optional[List[TsConnection]] = None, **kwargs) -> _GrpResult:
        if not conns:
            return gcp_logger.info(f'No conns to group')
        elif len(conns) == 1:
            return [set(conns)]

        if sema: await sema.acquire()

        if not likely_positive:
            screen_res = await self._screen_negatives(conns, emitter, **kwargs)
            must_negs, may_poss = screen_res
        else:
            must_negs, may_poss = [], conns
        grps = [{n} for n in must_negs]
        # gcp_logger.info(f'{len(must_negs)} must negative connections')

        if may_poss:
            # gcp_logger.info(f'{len(may_poss)} may positive connections')
            pos_grps = await self._group_likely(may_poss, emitter, failed_conns=failed_conns, **kwargs)
            if not pos_grps:
                gcp_logger.error(f'Failed to group {len(conns)} connections')
                if sema: sema.release()
                return None
            else:
                grps.extend(pos_grps)

        if sema: sema.release()

        gcp_logger.info(f'{len(conns)} conns are grouped to {len(grps)} hosts')
        # cnts = sum([1 for grp in grps for c in grp])
        # gcp_logger.info(f'{cnts} conns after grouping')
        return grps

    async def validate(self, conns: Sequence[TsConnection], trust_no_fn: bool = False,
                       stage1_likely_positive: bool = True,
                       stage1_concurrency: int = 20, stage1_retries: int = 5,
                       run_stage2: bool = True, double_pos: bool = True,
                       stage2_retries: int = 30, stage1_defer: float = .05,
                       stage2_defer: float = .1, early_close: bool = False,
                       debug_info: Optional[Dict]=None,
                       **kwargs) -> Tuple[_ValidationResult, Sequence[TsConnection]]:
        with clock_it(f'Validating {len(conns)} fingerprints', print_func=gcp_logger.info):
            mgroups: Dict[GCPGen2Machine, List[TsConnection]]
            mgroups = group_by_attrib(conns, func=lambda c: c.greeting.machine)

            async def _get_stage1_emitter(m: GCPGen2Machine, conns: Sequence[TsConnection]):
                if trust_no_fn:
                    return await self.get_emitter(m)
                else:
                    return await self.get_emitter(conns[0].greeting.brand)
            emitters = [await _get_stage1_emitter(m, conns) for m, conns in mgroups.items()]
            fails: List[List[TsConnection]] = [[] for _ in mgroups]

            semaphore = asyncio.Semaphore(stage1_concurrency)
            tasks = [self.group_conns(_conns, _e, likely_positive=stage1_likely_positive, retries=stage1_retries,
                                      defer=stage1_defer, sema=semaphore, failed_conns=_fc, **kwargs)
                    for _e, _conns, _fc in zip(emitters, mgroups.values(), fails)]
            stage1: _GrpResultGather = await asyncio.gather(*tasks, return_exceptions=True)
            if debug_info is not None:
                debug_info['stage1'] = list(stage1)

            failed_conns, to_close = [], []
            for f in fails:
                failed_conns.extend(f)

            under_tests = defaultdict(set) # each conn represents a set of co-located conns
            for res, _conns in zip(stage1, mgroups.values()):
                if res is None or isinstance(res, BaseException):
                    failed_conns.extend(_conns)
                else:
                    for grp in res:
                        if grp:
                            leader = next(iter(grp))
                            under_tests[leader] = grp
                            if early_close:
                                to_close.extend(grp - {leader})

            if early_close:
                await disconnect_ts_sockets([c for c in to_close if isinstance(c, TsSocketIO)])

            if not run_stage2:
                if early_close:
                    await disconnect_ts_sockets([c for c in under_tests if isinstance(c, TsSocketIO)])
                return list(under_tests.values()), failed_conns

            stage2_conns: Dict[str, List[TsConnection]]
            if not double_pos:
                stage2_conns = group_by_attrib(list(under_tests), func=lambda c: c.greeting.brand)
                for k, v in stage2_conns.items():
                    gcp_logger.info(f'Model {k} has {len(v)} conns to group')

                fails = [[] for _ in stage2_conns]
                tasks = [self.group_conns(_conns, await self.get_emitter(k),
                                          likely_positive=False,
                                          retries=stage2_retries,
                                          defer=stage2_defer,
                                          failed_conns=_fc, **kwargs)
                        for (k, _conns), _fc in zip(stage2_conns.items(), fails)]
            else:
                stage2_conns = group_by_attrib(list(under_tests), func=lambda c: c.greeting.machine)
                fails = [[] for _ in stage2_conns]
                tasks = [self.group_conns(_conns, await self.get_emitter(k),
                                          likely_positive=True,
                                          retries=stage1_retries,
                                          defer=stage1_defer,
                                          failed_conns=_fc, **kwargs)
                        for (k, _conns), _fc in zip(stage2_conns.items(), fails)]

            stage2: _GrpResultGather = await asyncio.gather(*tasks, return_exceptions=True)
            if early_close:
                for _conns in stage2_conns.values():
                    await disconnect_ts_sockets([c for c in _conns if isinstance(c, TsSocketIO)])

            if debug_info is not None:
                debug_info['under_tests'] = {l: set(grp) for l, grp in under_tests.items()}
                debug_info['bad_merges'] = []
                debug_info['stage2'] = [[set(grp) if grp else None for grp in grps]
                                        for grps in stage2 if grps and not isinstance(grps, BaseException)]

            for f in fails:
                if f:
                    failed_conns.extend(f)
                    gcp_logger.warning(f'Removing {len(f)} failed conns from the second stage')
                    for c in failed_conns:
                        if c in under_tests:
                            under_tests.pop(c)

            for res, (b, _conns) in zip(stage2, stage2_conns.items()):
                if res is None or isinstance(res, BaseException):
                    failed_conns.append(_conns)
                    gcp_logger.error(f'{b}: failed to test {len(_conns)} connections')
                else:
                    for grp in res:
                        if grp:
                            if len(grp) > 1 and debug_info is not None:
                                debug_info['bad_merges'].append(set(grp))

                            merge_to = grp.pop()
                            for _conn in grp:
                                under_tests[merge_to].update(under_tests[_conn])
                                under_tests.pop(_conn)

            return list(under_tests.values()), failed_conns
