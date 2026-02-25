import logging
import winrm

from plugins.vm_scanner.cfg import RM_ENC
from plugins.vm_scanner.errs import ExecutionFailure
from plugins.vm_scanner.remote_base import AbstractRemoteShell

_logger = logging.getLogger(__name__)

class WinRmRunner(AbstractRemoteShell):
    def __init__(self, ip_addr: str, login: str, secret: str):
        self._ip = ip_addr
        self._login = login
        self._secret = secret

    def trigger_script(self, script_text: str, max_wait: int = 600) -> str:
        ps_session = winrm.Session(
            self._ip,
            auth=(self._login, self._secret),
            transport="ntlm",
            server_cert_validation="ignore",
            read_timeout_sec=max_wait + 30,
            operation_timeout_sec=max_wait,
        )

        try:
            resp = ps_session.run_ps(script_text)
            text_out = resp.std_out.decode(RM_ENC).strip()
            return text_out
        except Exception as err:
            raise ExecutionFailure(f"WinRM Ошибка: {err}") from err