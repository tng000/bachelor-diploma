import contextlib
import logging
import paramiko

from plugins.vm_scanner.cfg import TEXT_ENC, TIMEOUT_SSH_INIT
from plugins.vm_scanner.errs import ExecutionFailure, HvScanBaseException
from plugins.vm_scanner.remote_base import AbstractRemoteShell

_logger = logging.getLogger(__name__)

class SshRunner(AbstractRemoteShell):
    def __init__(self, ip_addr: str, login: str, secret: str, tcp_port: int):
        self._ip = ip_addr
        self._login = login
        self._secret = secret
        self._port = tcp_port

    @contextlib.contextmanager
    def _create_session(self):
        ssh_instance = paramiko.SSHClient()
        ssh_instance.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            ssh_instance.connect(
                hostname=self._ip,
                port=self._port,
                username=self._login,
                password=self._secret,
                timeout=TIMEOUT_SSH_INIT,
                auth_timeout=TIMEOUT_SSH_INIT,
            )
            yield ssh_instance
        except Exception as err:
            raise HvScanBaseException(f"Сбой SSH на {self._ip}: {err}") from err
        finally:
            ssh_instance.close()

    def trigger_script(self, script_text: str, max_wait: int = 600) -> str:
        with self._create_session() as session:
            i, o, e = session.exec_command(script_text, timeout=max_wait)
            
            out_str = o.read().decode(TEXT_ENC, errors="ignore").strip()
            err_str = e.read().decode(TEXT_ENC, errors="ignore").strip()
            
            status_code = o.channel.recv_exit_status()

            if status_code != 0:
                raise ExecutionFailure(f"SSH Код {status_code}", status_code, err_str)

            return out_str