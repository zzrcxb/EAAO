import numpy as np

from typing import Set
from itertools import chain
from collections import Counter
from . import gcp_logger

__all__ = ['GCPMachine', 'GCPGen1Machine', 'GCPGen2Machine']


class GCPMachine:
    def __init__(self, brand: str, rec_ts: float) -> None:
        self.brand = brand
        self.rec_ts = rec_ts

    @property
    def gen(self) -> str:
        raise NotImplemented


class GCPGen1Machine(GCPMachine):
    def __init__(self, brand: str, rec_ts: float, raw_tscs: np.ndarray,
                 tsc_freq: int, boot_rounding: int = 0, freq_rounding = 5,
                 use_numa: bool = False) -> None:
        super().__init__(brand, rec_ts)

        if tsc_freq < 0:
            gcp_logger.warning(f'Abnormal tsc frequency: {tsc_freq}! Brand={brand}')

        self._rounded_freq = round(tsc_freq, -freq_rounding)
        self._boot_ns_err = -9 - boot_rounding

        self.n_measures = raw_tscs.shape[0]
        tscs, nss, diffs, auxs = raw_tscs.transpose()
        boot_nss = nss - (tscs / self._rounded_freq) * 1e9
        self.boot_ns = int(np.mean(boot_nss))
        self.boot_ns_std = np.std(boot_nss)
        self.m_lats = diffs
        numa_cnts = Counter(auxs >> 12)
        if len(numa_cnts) > 1 and use_numa:
            gcp_logger.warning(f'A fingerprint migrated between sockets {numa_cnts}')
            numa = sorted(numa_cnts, key=lambda a: numa_cnts[a], reverse=True)[0]
        else:
            numa = list(numa_cnts)[0]
        self.numa = numa
        self.use_numa = use_numa

    @property
    def gen(self): return 'gen1'

    @property
    def rounded_boot_ns(self):
        return int(round(self.boot_ns, self._boot_ns_err))

    def __hash__(self) -> int:
        return hash(self.rounded_boot_ns)

    def __eq__(self, __o: 'GCPGen1Machine') -> bool:
        if self.use_numa != __o.use_numa:
            raise ValueError('Fingerprints\' definition mismatch on use_numa')

        if isinstance(__o, GCPGen1Machine):
            return hash(self) == hash(__o)
        else:
            return False

    def __ne__(self, __o: object) -> bool:
        return not self == __o

    def __repr__(self) -> str:
        return f'GCPGen1Machine({self.brand}, {self.boot_ns}) -> {self.rounded_boot_ns}'

    def __str__(self) -> str:
        return repr(self)

    @staticmethod
    def subtract(mach1: Set['GCPGen1Machine'], mach2: Set['GCPGen1Machine'], error: int = 1):
        mach2_ns = np.array([m.boot_ns for m in mach2])
        remains = {m for m in mach1 - mach2
                   if np.min(np.abs(m.boot_ns - mach2_ns) >= error * 1e9)}
        return remains

    @staticmethod
    def union(mach1: Set['GCPGen1Machine'], mach2: Set['GCPGen1Machine'], error: int = 1):
        union: Set[GCPGen1Machine] = set()
        for m in mach1.union(mach2):
            if not union or min((abs(m.boot_ns - o.boot_ns) for o in union)) >= error * 1e9:
                union.add(m)
        return union

    @staticmethod
    def intersection(mach1: Set['GCPGen1Machine'], mach2: Set['GCPGen1Machine'], error: int = 1):
        intersection: Set[GCPGen1Machine] = mach1.intersection(mach2)
        remains = {m for m in chain(mach1, mach2)}

        for m in list(remains):
            if len(remains) > 1:
                diff = min((abs(m.boot_ns - m2.boot_ns) for m2 in remains if m != m2))
                if diff < error * 1e9:
                    intersection.add(m)
                    remains.remove(m)
        return intersection


class GCPGen2Machine(GCPMachine):
    def __init__(self, brand: str, rec_ts: float, kernel_tsc) -> None:
        super().__init__(brand, rec_ts)
        self.kernel_tsc = kernel_tsc

    @property
    def gen(self) -> str: return 'gen2'

    def __hash__(self) -> int:
        return hash((self.brand, self.kernel_tsc))

    def __eq__(self, __value: object) -> bool:
        if isinstance(__value, GCPGen2Machine):
            return hash(self) == hash(__value)
        else:
            return False

    def __ne__(self, __value: object) -> bool:
        return not self == __value

    def __repr__(self) -> str:
        return f'GCPGen2Machine({self.brand}, {int(self.kernel_tsc / 1e3)} kHz)'

    def __str__(self) -> str:
        return repr(self)
