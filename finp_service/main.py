import os
import re
import sys
import time
import uuid
import json
import signal
import numpy as np
import subprocess as sp
import requests

from pathlib import Path
from threading import Lock, Thread
from collections import namedtuple
from flask import jsonify, Flask, request
from typing import List, Dict, Optional, Any, Callable, Union, cast
from dataclasses import dataclass, asdict, field
from flask_socketio import SocketIO


# Some configurations
DEFAULT_FREQ_MEASURE_REPEAT = 10
DEFAULT_FREQ_MEASURE_SLEEP = 0.1
DEFAULT_TBOOT_MEASURE_REPEAT = 5


app = Flask(__name__)
socketio = SocketIO(app, ping_timeout=120, monitor_clients=True, async_mode='gevent')
app_dir: Path = Path(__file__).parent


# The following AMD CPUs do not report their TSC frequencies in CPUID.
# As a result, we hardcode their TSC frequencies.
# Note that at the time of writing this paper,
# we did not observe the use of AMD CPUs on Google Cloud Run.
_extra_cpu_list = {
    'AMD EPYC 7B12 64-Core Processor': 2250000000,
    'AMD EPYC 7B13 64-Core Processor': 2450000000,
    'AMD EPYC 9B14 96-Core Processor': 2600000000,
}

# Basic host CPU info:
# (brand name: str, nominal TSC frequency: int, RDSEED support: bool)
CPUIDInfo = namedtuple('CPUIDInfo', 'brand labeled_freq rdseed')

def get_cpuid_info() -> CPUIDInfo:
    """Return basic host CPU information via CPUID instruction

    Returns:
        CPUIDInfo: Detected host CPU info
    """

    def _find_info(line: str, name: str):
        # Parse individual lines of the "cpuid" program
        normalized = ' '.join(line.strip().split())
        splitted = normalized.split('=')
        if len(splitted) == 2:
            k, v = splitted
            if k.strip().lower() == name.lower():
                return v.strip()

    p = sp.run(['cpuid', '-1'], capture_output=True, text=True)
    brand, parsed_freq, rdseed  = 'Unknown', -1, False
    if p.returncode == 0:
        for line in p.stdout.split('\n'):
            _brand = _find_info(line, 'brand')
            if _brand:
                brand = _brand.replace('"', '').strip()

            _rdseed = _find_info(line, 'RDSEED instruction')
            if _rdseed:
                rdseed = (_rdseed.lower() == 'true')

        parsed_freq = re.findall(r'(\d+\.\d+)GHz', brand)
        if parsed_freq:
            # For Intel CPUs, their model names contain the base frequency
            parsed_freq = int(float(parsed_freq[0]) * 1e9)
        elif brand in _extra_cpu_list:
            # Search in the list of AMD CPUs that are observed so far.
            # Note that AMD CPUs on GCR are observed only after around June 2023
            parsed_freq = _extra_cpu_list[brand]
        else:
            # Failed to find any frequency info
            parsed_freq = -1

    return CPUIDInfo(brand, parsed_freq, rdseed)


def detect_gen2_tsc_freq() -> Optional[int]:
    """Detect TSC frequency in the gen2 environment via kernel boot

    Returns:
        Optional[int]: TSC frequency refined by the host kernel
    """
    if os.getenv('EXEC_ENV') == 'gen2':
        p = sp.run(['dmesg'], capture_output=True, text=True)
        if p.returncode == 0:
            matches = re.findall(r'Detected (\d+\.\d+) MHz', p.stdout)
            if matches:
                return int(float(matches[0]) * 1e6) if matches else None
            else:
                return None
        else:
            return None
    else:
        return None


# Container instance-global information, preserved across invocations
@dataclass
class InstanceInfo:
    # Service related info
    account: Optional[str]
    service: Optional[str]
    revision: Optional[str]
    region: Optional[str]
    exec_env: Optional[str]

    # Instance related info
    uuid: str  # UUID of the container
    brand_name: str  # CPU brand name
    parsed_freq: int  # Nominal TSC frequency found in the brand name
    kernel_freq: Optional[int]  # Refined host TSC frequency
    rdseed_support: bool  # Whether the host CPU supports RDSEED
    created_ns: int  # Creation time of the instance in UNIX epoch with ns-resolution
    terminated_ns: int = 0  # Termination time of the instance in UNIX epoch
    num_reqs: int = 0  # Number of requests the instance has served
    num_websockets: int = 0  # Number of *live* websocket connections

    # Boot time (using parsed/labeled tsc frequency)
    host_boot_ns: int = 0  # average boot time
    host_boot_std: float = 0
    host_boot_measures: int = 0

    # Measured TSC freq
    tsc_freq_avg: int = 0  # average measured TSC frequency
    tsc_freq_std: float = 0
    tsc_freq_measures: int = 0

    # Sanity metrics
    bad_freq_measures: int = 0
    bad_tsc_measures: int = 0


# Now initialize the global instance information
_cpuid_info = get_cpuid_info()
os.environ['TSC_FREQ'] = str(_cpuid_info.labeled_freq)
ginfo = InstanceInfo(account=os.getenv('ACCOUNT'),  # User-supplied variable
                     service=os.getenv('K_SERVICE'),  # Cloud Run-supplied variable
                     revision=os.getenv('K_REVISION'),  # Cloud Run-supplied variable
                     region=os.getenv('REGION'),  # User-supplied variable
                     exec_env=os.getenv('EXEC_ENV'),  # user-supplied variable
                     uuid=str(uuid.uuid4()),
                     brand_name=_cpuid_info.brand,
                     parsed_freq=_cpuid_info.labeled_freq,
                     kernel_freq=detect_gen2_tsc_freq(),
                     rdseed_support=_cpuid_info.rdseed,
                     created_ns=time.time_ns())


# Measuring boot time and timestamps
# Fields:
#   tsc: the value of the TSC
#   now_ns: the real-world time when the TSC is read (UNIX epoch timestamp in nano seconds)
#   lat: latency of retrieving time from clock_gettime(CLOCK_REALTIME, ...); measured in TSC cycles
#   core_id: the value of the TSC_AUX MSR when the TSC is read.
#            TSC_AUX is usually configured to represent logical core ID.
TSCRec = namedtuple('TSCRec', 'tsc now_ns lat core_id')

# This class accumulates all the TSC frequency and boot time measurements
class InstanceMeasures:
    def __init__(self) -> None:
        self.freqs: List[int] = []
        self.boot_nss: List[int] = []
        self.raw_tscs: List[TSCRec] = []

    def measure_freq(self, repeat: int, sleep: float, replace: bool = False) -> bool:
        """Measure the TSC frequency by reading TSC twice
        with some wait time in between the two reads

        Args:
            repeat (int): Number of repeated measurements
            sleep (float): The wait time between two TSC reads in seconds
            replace (bool, optional): Reset the past results. Defaults to False.

        Returns:
            bool: Whether the measurement succeeds
        """
        if sleep <= 0 or repeat <= 0:
            return False

        def _m_freq_c() -> Optional[List[int]]:
            sleep_us = int(sleep * 1e6)
            cmd = ['bin/timer', 'freq', str(repeat), str(sleep_us)]
            p = sp.run(cmd, cwd=app_dir, capture_output=True, text=True)
            if p.returncode != 0:
                return None
            else:
                return list(map(int, p.stdout.strip().split()))

        measures = _m_freq_c()
        if not measures:
            ginfo.bad_freq_measures += 1
            return False

        if replace:
            self.freqs = measures
        else:
            self.freqs.extend(measures)

        ginfo.tsc_freq_avg = int(np.mean(self.freqs))
        ginfo.tsc_freq_std = float(np.std(self.freqs))
        ginfo.tsc_freq_measures = len(self.freqs)
        return True

    def measure_boot_ns(self, repeat: int, replace: bool = False) -> bool:
        """Measure the host's boot time in UNIX epoch timestamp with ns resolution.
        The measurements are used to generate host_boot_ns with the labeled TSC frequency

        Args:
            repeat (int): Number of repeated measurements
            replace (bool, optional): Reset the past results. Defaults to False.

        Returns:
            bool: Whether the measurement succeeds
        """
        if repeat <= 0: return False

        def _tsc_and_now(n_measures: int) -> Optional[List[TSCRec]]:
            p = sp.run(['bin/timer', 'time', f'{n_measures}'],
                       cwd=app_dir, capture_output=True, text=True)
            if p.returncode == 0:
                res: List[TSCRec] = []
                for line in p.stdout.splitlines():
                    tsc, now_ns, diff, core_id = list(map(int, line.strip().split()))
                    res.append(TSCRec(tsc, now_ns, diff, core_id))
                return res

            ginfo.bad_tsc_measures += 1
            return None

        tsc_recs = _tsc_and_now(repeat)
        if tsc_recs is None:
            return False

        if replace:
            self.boot_nss = []
            self.raw_tscs = []

        for tsc_rec in tsc_recs:
            boot_ns = int(tsc_rec.now_ns - (tsc_rec.tsc / ginfo.parsed_freq) * 1e9)
            self.boot_nss.append(boot_ns)
            self.raw_tscs.append(tsc_rec)

        ginfo.host_boot_ns = int(np.mean(self.boot_nss))
        ginfo.host_boot_std = float(np.std(self.boot_nss))
        ginfo.host_boot_measures = len(self.boot_nss)
        return True

# Instantiate the measurement helper class as a global varaible
gmeasures = InstanceMeasures()


# Results of running a user-supplied command
CmdResult = namedtuple('CmdResult', 'stdout stderr retval')

class CmdRunner:
    _lock: Lock = Lock()  # the lock is always acquired when the subprocess is running
    _pending_buf: Dict[str, CmdResult] = dict()

    @classmethod
    def run(cls, cmd: str, env: Optional[Dict[str, str]] = None,
            blocking: bool = True) -> Union[CmdResult, str]:
        def _run_cmd(save_to: Optional[str] = None) -> CmdResult:
            proc = sp.run(cmd, shell=True, cwd=app_dir, env=env,
                          capture_output=True, text=True)
            cls._lock.release()

            res = CmdResult(proc.stdout, proc.stderr, proc.returncode)
            if save_to:
                cls._pending_buf[save_to] = res
            return res

        if not env:
            env = dict(os.environ)
        else:
            os_env = dict(os.environ)
            os_env.update(env)
            env = os_env

        cls._lock.acquire()
        if blocking:
            return _run_cmd()
        else:
            cmd_id = str(uuid.uuid4())
            thread = Thread(target=_run_cmd, name='CmdRunner', args=(cmd_id, ))
            thread.start()
            return cmd_id

    @classmethod
    def is_busy(cls) -> bool:
        return cls._lock.locked()

    @classmethod
    def query_res(cls, cmd_id: str) -> Optional[CmdResult]:
        return cls._pending_buf.pop(cmd_id, None)


def get_param(payload: Dict[str, Any], name: str, default: Any = None,
              dtype: Optional[Callable] = None):
    if name in payload:
        data = payload[name]
        return dtype(data) if callable(dtype) else data
    else:
        return default


def run_query(payload: Dict[str, Any]) -> Dict[str, Any]:
    resp = dict(time=time.time(),
                remote_ip=request.environ.get('HTTP_X_FORWARDED_FOR'))
    resp_succ = True
    ginfo.num_reqs += 1

    # TSC frequency and boot time measurements
    # It is at least measured once during the initial query
    replace = bool(get_param(payload, 'replace', 0, int)) # Discard old results
    m_freq_sleep = get_param(payload, 'm_freq', None, float)
    m_freq_repeat = get_param(payload, 'm_freq_repeat', None, int)
    if not gmeasures.freqs or m_freq_sleep or m_freq_repeat:
        if not m_freq_sleep: m_freq_sleep = DEFAULT_FREQ_MEASURE_SLEEP
        if not m_freq_repeat: m_freq_repeat = DEFAULT_FREQ_MEASURE_REPEAT
        succ = gmeasures.measure_freq(m_freq_repeat, m_freq_sleep, replace=replace)
        resp_succ &= succ

    m_boot_ns = get_param(payload, 'm_boot_ns', None, int)
    if not gmeasures.boot_nss or m_boot_ns:
        if not m_boot_ns: m_boot_ns = DEFAULT_TBOOT_MEASURE_REPEAT
        succ = gmeasures.measure_boot_ns(m_boot_ns, replace=replace)
        resp_succ &= succ

    # handle cmd running
    cmd = get_param(payload, 'cmd')
    if cmd:
        try:
            env = get_param(payload, 'env', None, json.loads)
        except json.JSONDecodeError:
            resp_succ &= False
        else:
            blocking = bool(get_param(payload, 'blocking', 1, int))
            if CmdRunner.is_busy():
                resp_succ &= False
            else:
                resp['cmd_start_ts'] = time.time()
                res = CmdRunner.run(cmd, env, blocking)
                resp['cmd'] = cmd
                if blocking:
                    resp['cmd_end_ts'] = time.time()
                    resp.update(cast(CmdResult, res)._asdict())
                else:
                    resp['cmd_id'] = res

    cmd_res = get_param(payload, 'cmd_res')
    if cmd_res:
        res = CmdRunner.query_res(cmd_res)
        if res:
            resp.update(res._asdict())
        else:
            resp_succ &= False

    # further population of the response
    resp.update(asdict(ginfo))
    resp['raw_tscs'] = gmeasures.raw_tscs
    resp['succ'] = resp_succ

    if bool(get_param(payload, 'return_measures', 0, int)):
        resp.update(dict(freq_history=gmeasures.freqs,
                         boot_ns_history=gmeasures.boot_nss))

    if bool(get_param(payload, 'debug', 0, int)):
        resp['payload'] = payload

    return resp


# actual connection handlers
# socketio handlers
@socketio.on('connect')
def handle_sock_connect(): ginfo.num_websockets += 1

@socketio.on('disconnect')
def handle_sock_disconnect(): ginfo.num_websockets -= 1

@socketio.on('query')
def handle_query(payload): return run_query(payload)

# Flask handler
@app.route('/', methods=['GET'])
def handle_http_get(): return jsonify(run_query(request.args))


# register shutdown handler
def shutdown_handler(*_) -> None:
    # will report to a server on shutdown if 'LIFETRACKER_URL' is present
    report_url = os.getenv('LIFETRACKER_URL')
    if not report_url:
        sys.exit(0)

    ginfo.terminated_ns = time.time_ns()
    # the server on the receiving end needs to support this "POST" method
    r = requests.post(report_url, data=asdict(ginfo))
    if r.status_code == 200:
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == '__main__':
    # Cloud Run sends a SIGTERM before it terminates a container instance
    signal.signal(signal.SIGTERM, shutdown_handler)
    socketio.run(app, host='0.0.0.0', port=8080)  # type: ignore
