import json
import base64
import gzip
import logging
from typing import Any, Optional

from plugins.vm_scanner.cfg import DEF_SSH_PORT
from plugins.vm_scanner.errs import ExecutionFailure, HvScanBaseException
from plugins.vm_scanner.modules import extractor
from plugins.vm_scanner.remote_base import AbstractRemoteShell
from plugins.vm_scanner.transports.ssh_impl import SshRunner
from plugins.vm_scanner.transports.winrm_impl import WinRmRunner

_logger = logging.getLogger(__name__)


class OperationsEngine:
    def __init__(
        self,
        node_ip: str,
        user_id: str,
        secret: str,
        net_port: int = DEF_SSH_PORT,
        os_usr: Optional[str] = None,
        os_pwd: Optional[str] = None,
    ):
        self._target_ip = node_ip
        self._port = net_port
        self._login = user_id
        self._password = secret
        self._in_guest_usr = os_usr
        self._in_guest_pwd = os_pwd
        self._driver: Optional[AbstractRemoteShell] = None

    def initialize_transport(self, mode: str):
        if mode == "ssh":
            self._driver = SshRunner(
                self._target_ip, self._login, self._password, self._port
            )
        elif mode == "winrm":
            self._driver = WinRmRunner(self._target_ip, self._login, self._password)
        else:
            raise ValueError(f"Неизвестный тип транспорта: {mode}")

    def fetch_all_vms_on_host(self) -> list[dict[str, Any]]:
        cmd_text = extractor.gen_host_full_scan_script(
            usr=self._in_guest_usr, pwd=self._in_guest_pwd
        )

        try:
            raw_b64 = self._driver.trigger_script(cmd_text, max_wait=1800)
            raw_b64 = "".join(raw_b64.split())
            if not raw_b64:
                return []

            compressed_data = base64.b64decode(raw_b64)
            json_str = gzip.decompress(compressed_data).decode("utf-8")
            parsed_data = json.loads(json_str)

            if isinstance(parsed_data, dict):
                parsed_data = [parsed_data]

            return parsed_data

        except (HvScanBaseException, ExecutionFailure) as exc:
            _logger.error("Ошибка сети/скрипта на хосте %s: %s", self._target_ip, exc)
            raise exc
        except Exception as exc:
            _logger.error(
                "Сбой декодирования Gzip/JSON на %s: %s", self._target_ip, exc
            )
            raise exc
