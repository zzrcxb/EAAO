# adapted from https://gist.github.com/awesomebytes/a1e8b613c6c53fd2ff39bf6b130cbc5e

import os
import sys
import signal
import docker
import logging

from typing import List, Optional
from docker.errors import NotFound as ContainerNotFound


class DockerExecWrapper:
    def __init__(self, container: str, cmd: List[str], no_sig: bool = False, **kwargs) -> None:
        self.client = docker.from_env(timeout=10)

        try:
            self.container = self.client.containers.get(container)
        except ContainerNotFound as e:
            logging.error(e)
            raise e

        if not no_sig:
            self.setup_signal()

        self.cmd = cmd
        self._extra_args = kwargs
        self.exec_id = None

    def __del__(self):
        if self.exec_id and self.client.api.exec_inspect(self.exec_id).get('Running'):
            self.signal_exec(signal.SIGKILL)

    def run(self, stdout=sys.stdout, stderr=sys.stderr) -> int:
        exec_crt = self.client.api.exec_create(self.container.id, self.cmd,
                                               stdout=True, stderr=True,
                                               **self._extra_args)
        self.exec_id = exec_crt['Id']
        exec_output = self.client.api.exec_start(self.exec_id, stream=True,
                                                 socket=False, demux=True)

        for out, err in exec_output:
            if out:
                print(out.decode(), file=stdout, end='')
            if err:
                print(err.decode(), file=stderr, end='')

        inspect = self.client.api.exec_inspect(self.exec_id)
        return inspect['ExitCode']

    def signal_exec(self, signal):
        if self.pid:
            os.kill(self.pid, signal)

    def fwd_signal(self, signal, _):
        self.signal_exec(signal)

    def setup_signal(self):
        signal.signal(signal.SIGHUP, self.fwd_signal)
        signal.signal(signal.SIGINT, self.fwd_signal)
        signal.signal(signal.SIGQUIT, self.fwd_signal)
        signal.signal(signal.SIGILL, self.fwd_signal)
        signal.signal(signal.SIGTRAP, self.fwd_signal)
        signal.signal(signal.SIGABRT, self.fwd_signal)
        signal.signal(signal.SIGBUS, self.fwd_signal)
        signal.signal(signal.SIGFPE, self.fwd_signal)
        signal.signal(signal.SIGUSR1, self.fwd_signal)
        signal.signal(signal.SIGUSR2, self.fwd_signal)
        signal.signal(signal.SIGSEGV, self.fwd_signal)
        signal.signal(signal.SIGPIPE, self.fwd_signal)
        signal.signal(signal.SIGALRM, self.fwd_signal)
        signal.signal(signal.SIGTERM, self.fwd_signal)

    @property
    def pid(self) -> Optional[int]:
        if self.exec_id:
            return self.client.api.exec_inspect(self.exec_id).get('Pid')
