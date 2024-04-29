import time
import json
import asyncio
import aiohttp
import socketio
import numpy as np

from aiohttp_socks import ProxyConnector
from socketio.exceptions import ConnectionError as SocketConnectionError
from socketio.exceptions import TimeoutError as SocketTimeoutError
from typing import Optional, List, Dict, Any, Iterable, Callable, Sequence, \
                   Tuple, TypeVar, Set
from . import gcp_logger
from .sugar import clock_it
from .fingerprint import GCPMachine, GCPGen1Machine, GCPGen2Machine

__all__ = ['TsResp', 'TsConnection', 'TsSocketIO', 'TsHttpSession',
           'ts_http_burst', 'connect_ts_sockets', 'disconnect_ts_sockets',
           'prune_connections', 'prune_connections_by_group']


class TsResp:
    def __init__(self, raw, query: Dict[str, Any]) -> None:
        self.raw = raw
        self.query = query
        self.resp_ts = time.time()

    @property
    def account(self) -> str: return self.raw['account']

    @property
    def region(self) -> str: return self.raw['region']

    @property
    def service(self) -> str: return self.raw['service']

    @property
    def exec_env(self) -> str: return self.raw['exec_env']

    @property
    def instance(self) -> str: return self.raw['uuid']

    @property
    def brand(self) -> str: return self.raw['brand_name']

    @property
    def parsed_freq(self) -> int: return self.raw['parsed_freq']

    @property
    def tsc_freq(self) -> int: return int(self.raw['tsc_freq_avg'])

    @property
    def kernel_freq(self) -> Optional[int]: return self.raw.get('kernel_freq')

    @property
    def tsc_freq_std(self) -> int: return int(self.raw['tsc_freq_std'])

    @property
    def boot_ns(self) -> int: return int(self.raw['host_boot_ns'])

    @property
    def arrival_ts(self) -> float: return self.raw['time']

    @property
    def stdout(self) -> Optional[str]: return self.raw.get('stdout')

    @property
    def stderr(self) -> Optional[str]: return self.raw.get('stderr')

    @property
    def returncode(self) -> Optional[int]: return self.raw.get('retval')

    @property
    def cmd_id(self) -> Optional[str]: return self.raw.get('cmd_id')

    @property
    def raw_tsc_res(self) -> np.ndarray:
        return np.array(self.raw['raw_tscs'])

    @property
    def errored(self) -> bool: return not self.raw['succ']

    @property
    def rdseed_support(self) -> bool: return self.raw['rdseed_support']

    @property
    def is_amd(self) -> bool: return 'amd' in self.brand.lower()

    @property
    def is_intel(self) -> bool: return 'intel' in self.brand.lower()

    def get_machine(self, use_m_freq: bool = False, hybrid: bool = False,
                    hybrid_thresh: int = 1000, **kwargs) -> GCPMachine:
        if self.errored:
            raise RuntimeError('Cannot get machine from an errored response')

        if self.exec_env == 'gen1':
            if not use_m_freq and hybrid and self.tsc_freq_std < hybrid_thresh:
                use_m_freq = True

            freq = self.tsc_freq if use_m_freq else self.parsed_freq
            if not use_m_freq:
                kwargs['freq_rounding'] = 0
            return GCPGen1Machine(self.brand, self.arrival_ts, self.raw_tsc_res, freq, **kwargs)
        elif self.exec_env == 'gen2':
            return GCPGen2Machine(self.brand, self.arrival_ts, self.kernel_freq)
        else:
            raise ValueError(f'Unknown execution env {self.exec_env}')

    @property
    def machine(self) -> GCPMachine:
        return self.get_machine()


class TsConnection:
    def __init__(self, url, proxy: Optional[str] = None) -> None:
        self.url = url
        self.resps: List[Optional[TsResp]] = []
        self.connector = ProxyConnector.from_url(proxy) if proxy else None

    async def query(self, timeout: int = 30, **kwargs) -> Optional[TsResp]:
        raise NotImplementedError

    async def run_cmd(self, cmd: str, env: Dict[str, Any] = dict(),
                      blocking: bool = True, **kwargs) -> Optional[TsResp]:
        env = {str(k): str(v) for k, v in env.items()}
        return await self.query(cmd=cmd, env=json.dumps(env),
                                blocking=int(blocking), **kwargs)

    @property
    def resp(self) -> Optional[TsResp]:
        return self.resps[-1] if self.resps else None

    @property
    def greeting(self) -> TsResp:  # first valid response
        resp = ([r for r in self.resps if r] + [None])[0]
        if not resp:
            raise ValueError(f'An invalid TsConnection without greeting info!')
        return resp

    @property
    def greeted(self) -> bool:
        try:
            _ = self.greeting
        except ValueError:
            return False
        else:
            return True


class TsSocketIO(TsConnection):
    def __init__(self, url, proxy: Optional[str] = None) -> None:
        super().__init__(url, proxy)
        self._inited = False
        self._lock = asyncio.Lock()
        self._start_ts, self._end_ts = None, None

    async def connect(self, timeout: int = 5, **kwargs) -> bool:
        self.session = aiohttp.ClientSession(connector=self.connector)
        self.sio = socketio.AsyncClient(reconnection=False,
                                        http_session=self.session)
        self._inited = True
        try:
            task = self.sio.connect(self.url, transports='websocket',
                                    wait_timeout=timeout, **kwargs)
            await asyncio.wait_for(task, timeout=timeout + .1)
            assert(self.connected)
        except (SocketConnectionError, SocketTimeoutError, asyncio.TimeoutError, AssertionError):
            await self.disconnect()
            self._start_ts, self._end_ts = None, None
            return False
        else:
            self._start_ts, self._end_ts = time.time(), None
            return self.connected

    async def disconnect(self) -> None:
        if self._inited:
            await self.sio.disconnect()
            await self.session.close()
            self._inited = False
            self._end_ts = time.time()

    async def query(self, timeout: int = 30, **kwargs) -> Optional[TsResp]:
        if not self.connected:
            raise RuntimeError('Trying to query a disconnected socket!')

        async with self._lock:
            try:
                raw = await self.sio.call('query', kwargs, timeout=timeout)
            except (SocketConnectionError, SocketTimeoutError):
                self.resps.append(None)
            else:
                resp = TsResp(raw, kwargs)
                self.resps.append(resp)
                if self.greeted and self.greeting.instance != resp.instance:
                    gcp_logger.warning(f'TsSocket switched from instance '
                                       f'{self.greeting.instance} to {resp.instance}')
                    self.resps = [resp]
            finally:
                return self.resp

    async def kill(self) -> None:
        resp = await self.query()
        if resp:
            await self.sio.emit('query', dict(kill=resp.instance))
        await self.disconnect()

    async def reset(self):
        await self.kill()
        self.resps = []
        self._start_ts, self._end_ts = None, None

    @property
    def duration(self) -> Optional[float]:
        if self._start_ts is None:
            return None
        else:
            if self._end_ts is None:
                return time.time() - self._start_ts
            else:
                return self._end_ts - self._start_ts

    @property
    def connected(self):
        return self._inited and self.sio.connected


class TsHttpSession(TsConnection):
    def __init__(self, url, proxy: Optional[str] = None) -> None:
        super().__init__(url, proxy)
        self.cookie_jar = aiohttp.CookieJar()

    async def query(self, timeout: int = 30, **kwargs) -> Optional[TsResp]:
        async with aiohttp.ClientSession(connector=self.connector,
                                         cookie_jar=self.cookie_jar) as session:
            async with session.get(self.url, params=kwargs, timeout=timeout) as r:
                if r.status != 200:
                    self.resps.append(None)
                else:
                    raw = await r.json()
                    self.resps.append(TsResp(raw, kwargs))
        return self.resp


async def ts_http_burst(url: str, num_reqs: int, proxy: Optional[str] = None, **kwargs):
    sessions = [TsHttpSession(url, proxy) for _ in range(num_reqs)]

    tasks = [s.query(**kwargs) for s in sessions]
    with clock_it(f'A burst of {num_reqs} HTTP requests to {url}', print_func=gcp_logger.info):
        resps = await asyncio.gather(*tasks, return_exceptions=True)

    return resps, sessions


async def connect_ts_sockets(url: str, num_conns: int, strict: bool = True,
                             max_retries: int = 10, timeout: int = 10,
                             proxy: Optional[str] = None, query: Dict[str, Any] = dict(),
                             init_burst: bool = True, burst_factor: float = 1.0,
                             interval: int = 3, concurrency: Optional[int] = None,
                             extra_filter: Optional[Callable[[TsSocketIO], bool]] = None,
                             **kwargs) -> List[TsSocketIO]:
    def _health_check(_sock: TsSocketIO) -> bool:
        return _sock.connected and _sock.greeted

    def _integrity_check(_sock: TsSocketIO) -> bool:
        if _health_check(_sock):
            greeting = _sock.greeting
            _res = (not greeting.errored) and greeting.parsed_freq != 0
            if _res and callable(extra_filter):
                _res &= extra_filter(_sock)
            return _res
        else:
            return True

    async def _handshake_task(_sock: TsSocketIO, sema: asyncio.Semaphore) -> None:
        async with sema:
            if not _sock.connected: await _sock.connect(timeout, **kwargs)

            if _sock.connected and not _sock.greeted:
                await _sock.query(timeout=timeout, **query)

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as r:
            if r.status != 200:
                text = await r.text()
                gcp_logger.warning(f'Cannot access {url}; Status = {r.status}; '
                                   f'Resp = {text}')
                return []

    if init_burst:
        await ts_http_burst(url, int(num_conns * burst_factor), proxy)

    concurrency = num_conns if not concurrency else concurrency
    semaphore = asyncio.Semaphore(concurrency)
    with clock_it(action=f'Trying to connect {num_conns} sockets',
                  print_func=gcp_logger.info):
        socks = [TsSocketIO(url, proxy) for _ in range(num_conns)]
        num_healthy, retry = 0, 0
        while num_healthy < num_conns and retry < max_retries:
            if retry > 0:
                await asyncio.sleep(interval)

            tasks = [_handshake_task(s, semaphore) for s in socks if not _health_check(s)]
            await asyncio.gather(*tasks, return_exceptions=True)

            resets = [s.reset() for s in socks if not _integrity_check(s)]
            if resets:
                await asyncio.gather(*resets, return_exceptions=True)
                gcp_logger.info(f'Filtered out {len(resets)} SocketIO connections')

            num_healthy = sum((_health_check(s) for s in socks))
            gcp_logger.info(f'{url}: {num_healthy} healthy SocketIO connections so far.')

            if not strict:
                break
            else:
                retry += 1

    return [s for s in socks if _health_check(s)]


async def disconnect_ts_sockets(socks: Iterable[TsSocketIO]) -> None:
    tasks = [s.disconnect() for s in socks if s.connected]
    await asyncio.gather(*tasks, return_exceptions=True)

_T = TypeVar('_T', bound=TsConnection)
_PrunedType = Tuple[Sequence[_T], Sequence[_T]]
def prune_connections(conns: Sequence[_T], **kwargs) -> _PrunedType[_T]:
    hosts_so_far = set()
    preserve, to_dc = [], []
    for c in conns:
        mch = c.greeting.get_machine(**kwargs)
        if mch in hosts_so_far:
            to_dc.append(c)
        else:
            hosts_so_far.add(mch)
            preserve.append(c)
    return preserve, to_dc


def prune_connections_by_group(grps: Sequence[Set[_T]]) -> _PrunedType[_T]:
    preserve, to_dc = [], []
    for grp in grps:
        if grp:
            preserve.append(grp.pop())
            to_dc.extend(grp)
    return preserve, to_dc
