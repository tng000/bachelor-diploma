import json
import gzip
import base64
import pytest
from unittest.mock import MagicMock, patch
from airflow.models import DagBag

from plugins.vm_scanner.operations import (
    fetch_target_ips_op,
    update_host_op,
    process_single_host_op,
)
from plugins.vm_scanner.engine import OperationsEngine
from plugins.vm_scanner.transports.ssh_impl import SshRunner
from plugins.vm_scanner.modules.probe import extract_names_from_text
from plugins.vm_scanner.errs import HvScanBaseException

# 1. Тестирование корректности DAG (Airflow)


def test_dag_loads_without_errors():
    """
    Проверяет, что DAG парсится Airflow без синтаксических ошибок
    и содержит ожидаемые задачи.
    """
    dag_bag = DagBag(dag_folder="dags/", include_examples=False)
    dag = dag_bag.get_dag(dag_id="vm_scanner_optimized")

    assert dag_bag.import_errors == {}, "Ошибки импорта при парсинге DAG-ов"
    assert dag is not None

    assert dag.has_task("fetch_target_ips")
    assert dag.has_task("process_hosts_batch")


# 2. Тестирование модуля Operations (Работа с БД)


@patch("plugins.vm_scanner.operations.yield_pg_cursor")
def test_fetch_target_ips_op(mock_yield_cursor):
    """Тест: Выборка целевых IP адресов из БД."""
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        {"ip_address": "10.0.0.10"},
        {"ip_address": "10.0.0.11"},
    ]
    mock_yield_cursor.return_value.__enter__.return_value = mock_cursor

    ips = fetch_target_ips_op()

    assert ips == ["10.0.0.10", "10.0.0.11"]
    mock_cursor.execute.assert_called_once()
    assert "SELECT ip_address" in mock_cursor.execute.call_args[0][0]


@patch("plugins.vm_scanner.operations.yield_pg_cursor")
def test_update_host_op_success(mock_yield_cursor):
    """Тест: Обновление статуса хоста в БД при успехе."""
    mock_cursor = MagicMock()
    mock_yield_cursor.return_value.__enter__.return_value = mock_cursor

    update_host_op("192.168.1.5", success=True)

    mock_cursor.execute.assert_called_once()
    query, params = mock_cursor.execute.call_args[0]
    assert "UPDATE hosts" in query
    assert params == ("success", "192.168.1.5")


@patch("plugins.vm_scanner.operations.OperationsEngine")
@patch("plugins.vm_scanner.operations.update_host_op")
def test_process_single_host_op_success(mock_update_host, mock_engine_class):
    """Тест: Главная функция обработки одного хоста (Успешный сценарий)."""
    mock_engine_instance = MagicMock()
    mock_engine_class.return_value = mock_engine_instance

    mock_engine_instance.fetch_all_vms_on_host.return_value = [{"name": "Test-VM"}]

    node_creds = {"login": "admin", "pwd": "123", "port": 22}
    guest_creds = {"login": "guest", "pwd": "456"}

    result = process_single_host_op("10.0.0.5", node_creds, guest_creds)

    mock_engine_instance.initialize_transport.assert_called_once()
    mock_engine_instance.fetch_all_vms_on_host.assert_called_once()
    mock_update_host.assert_called_with(ip_address="10.0.0.5", success=True)
    assert result == [{"name": "Test-VM"}]


# 3. Тестирование OperationsEngine (Декодирование и парсинг)


def test_engine_fetch_all_vms_on_host():
    """
    Тест: Движок OperationsEngine должен корректно отправлять скрипт,
    принимать Base64-GZIP-JSON и разжимать его.
    """
    engine = OperationsEngine("10.0.0.2", "usr", "pwd")
    engine._driver = MagicMock()

    fake_vm_data = [{"name": "VM-01", "cpu": 4, "ram_mb": 8192}]
    json_str = json.dumps(fake_vm_data)
    compressed_data = gzip.compress(json_str.encode("utf-8"))
    b64_str = base64.b64encode(compressed_data).decode("utf-8")

    engine._driver.trigger_script.return_value = b64_str

    vms = engine.fetch_all_vms_on_host()

    assert len(vms) == 1
    assert vms[0]["name"] == "VM-01"
    assert vms[0]["cpu"] == 4
    engine._driver.trigger_script.assert_called_once()


# 4. Тестирование SSH-драйвера (Paramiko)


@patch("plugins.vm_scanner.transports.ssh_impl.paramiko.SSHClient")
def test_ssh_runner_trigger_script_success(mock_ssh_client_class):
    """Тест: Выполнение скрипта по SSH при успешном коде ответа (0)."""
    mock_ssh_instance = MagicMock()
    mock_ssh_client_class.return_value = mock_ssh_instance

    mock_stdout = MagicMock()
    mock_stderr = MagicMock()

    mock_stdout.read.return_value = b"base64_encoded_payload_string\n"
    mock_stderr.read.return_value = b""
    mock_stdout.channel.recv_exit_status.return_value = 0  # Успешный код

    mock_ssh_instance.exec_command.return_value = (
        MagicMock(),
        mock_stdout,
        mock_stderr,
    )

    runner = SshRunner("192.168.0.1", "root", "secret", 22)
    output = runner.trigger_script("echo 'test'")

    assert output == "base64_encoded_payload_string"
    mock_ssh_instance.connect.assert_called_once_with(
        hostname="192.168.0.1",
        port=22,
        username="root",
        password="secret",
        timeout=10,
        auth_timeout=10,
    )
    mock_ssh_instance.close.assert_called_once()


@patch("plugins.vm_scanner.transports.ssh_impl.paramiko.SSHClient")
def test_ssh_runner_trigger_script_failure(mock_ssh_client_class):
    """Тест: Выбрасывание исключения при ненулевом коде возврата SSH (ошибка)."""
    mock_ssh_instance = MagicMock()
    mock_ssh_client_class.return_value = mock_ssh_instance

    mock_stdout = MagicMock()
    mock_stderr = MagicMock()

    mock_stdout.read.return_value = b""
    mock_stderr.read.return_value = b"Command not found"
    mock_stdout.channel.recv_exit_status.return_value = 127

    mock_ssh_instance.exec_command.return_value = (
        MagicMock(),
        mock_stdout,
        mock_stderr,
    )

    runner = SshRunner("192.168.0.1", "root", "secret", 22)

    with pytest.raises(HvScanBaseException) as exc:
        runner.trigger_script("invalid_command")

    assert "SSH Код 127" in str(exc.value)


# 5. Тестирование модуля Probe (Утилиты)


def test_extract_names_from_text_valid():
    """Тест: Парсинг списка имён ВМ из ответа хоста."""
    raw_text = """
    
    VM-Web-01
    VM-DB-02
    
    VM-App-03
    """
    names = extract_names_from_text(raw_text)

    assert len(names) == 3
    assert names == ["VM-Web-01", "VM-DB-02", "VM-App-03"]


def test_extract_names_from_text_empty():
    """Тест: Парсинг списка имён ВМ при пустом ответе."""
    assert extract_names_from_text("") == []
    assert extract_names_from_text(None) == []


# 6. Тестирование масштабируемости (200 хостов / 10000 ВМ)


def test_dag_concurrency_for_200_hosts():
    """
    Тест: Проверка конфигурации DAG для обеспечения параллелизма.
    При чанке по умолчанию (HOSTS_PER_WORKER_CHUNK = 10),
    для обработки 200 хостов потребуется 20 параллельных задач.
    Проверяем, что DAG имеет достаточный пул (max_active_tasks >= 20).
    """
    dag_bag = DagBag(dag_folder="dags/", include_examples=False)
    dag = dag_bag.get_dag(dag_id="vm_scanner_optimized")

    assert (
        dag.max_active_tasks >= 20
    ), "DAG должен разрешать минимум 20 активных тасок для параллельной обработки 200 хостов"

    process_task = dag.get_task("process_hosts_batch")
    assert (
        process_task.is_mapped
    ), "Задача process_hosts_batch должна использовать маппинг (expand) для распараллеливания"


@patch("plugins.vm_scanner.modules.pg_upsert.yield_pg_cursor")
def test_db_upsert_supports_10000_vms(mock_yield_cursor):
    """
    Тест: Проверка, что модуль записи в БД способен переварить
    10 000 виртуальных машин с 200 хостов за один запуск без падения
    (обход лимита в ~65535 параметров PostgreSQL благодаря chunk_list).
    """
    from plugins.vm_scanner.modules.pg_upsert import write_batch_hosts_records

    massive_batch = []
    for h_idx in range(200):
        vms = []
        for v_idx in range(50):
            vms.append(
                {
                    "name": f"vm-{h_idx}-{v_idx}",
                    "guid": f"10000000-0000-0000-0000-{h_idx:04d}{v_idx:04d}",
                    "cpu": 4,
                    "ram_mb": 8192,
                    "storage": 50000,
                    "power_state": "running",
                    "ip_address": f"10.1.{h_idx}.{v_idx}",
                    "mac_address": "00:11:22:33:44:55",
                    "domain": "test.local",
                    "os": "Windows Server 2019",
                    "software": [
                        {"name": f"App A v{h_idx}", "version": "1.0"},
                        {"name": f"App B v{h_idx}", "version": "2.1"},
                    ],
                }
            )
        massive_batch.append({"host_ip": f"192.168.100.{h_idx}", "vms": vms})

    class SmartFakeCursor:
        def __init__(self):
            self.last_query = ""
            self.last_params = []

        def execute(self, query, params=None):
            self.last_query = query
            self.last_params = params or []

        def fetchall(self):
            if "FROM hosts" in self.last_query:
                return [
                    {"ip_address": f"192.168.100.{i}", "id": f"host-uuid-{i}"}
                    for i in range(200)
                ]

            elif "INSERT INTO vms" in self.last_query:
                guids = self.last_params[2::12]
                ids = self.last_params[0::12]
                return [{"guid": g, "id": i} for g, i in zip(guids, ids)]

            elif "SELECT id, name, version FROM software" in self.last_query:
                return []

            elif "INSERT INTO software" in self.last_query:
                ids = self.last_params[0::3]
                names = self.last_params[1::3]
                versions = self.last_params[2::3]
                return [
                    {"id": i, "name": n, "version": v}
                    for i, n, v in zip(ids, names, versions)
                ]

            return []

    mock_cursor = MagicMock(wraps=SmartFakeCursor())
    mock_yield_cursor.return_value.__enter__.return_value = mock_cursor

    write_batch_hosts_records(massive_batch)

    execute_calls = mock_cursor.execute.call_args_list

    insert_vm_calls = [
        call for call in execute_calls if "INSERT INTO vms" in call[0][0]
    ]
    assert (
        len(insert_vm_calls) >= 10
    ), f"Для 10 000 ВМ должно быть сгенерировано минимум 10 батч-запросов (по 1000 шт), а было {len(insert_vm_calls)}"

    insert_soft_calls = [
        call for call in execute_calls if "INSERT INTO software" in call[0][0]
    ]
    assert (
        len(insert_soft_calls) > 0
    ), "Должны быть вызовы сохранения программного обеспечения"


# 7. Дополнительные тесты (Edge cases, Ошибки, WinRM)


@patch("plugins.vm_scanner.operations.OperationsEngine")
@patch("plugins.vm_scanner.operations.update_host_op")
def test_process_single_host_op_failure(mock_update_host, mock_engine_class):
    """
    Тест: Если движок выбрасывает исключение при сборе данных,
    статус хоста в БД должен обновиться на failed, а исключение проброшено дальше.
    """
    mock_engine_instance = MagicMock()
    mock_engine_class.return_value = mock_engine_instance

    mock_engine_instance.fetch_all_vms_on_host.side_effect = Exception(
        "Connection lost"
    )

    node_creds = {"login": "admin", "pwd": "123", "port": 22}
    guest_creds = {"login": "guest", "pwd": "456"}

    with pytest.raises(Exception, match="Connection lost"):
        process_single_host_op("10.0.0.99", node_creds, guest_creds)

    mock_update_host.assert_called_with(ip_address="10.0.0.99", success=False)


def test_engine_init_unknown_transport():
    """Тест: Инициализация OperationsEngine с неизвестным протоколом."""
    engine = OperationsEngine("10.0.0.2", "usr", "pwd")

    with pytest.raises(ValueError, match="Неизвестный тип транспорта: telnet"):
        engine.initialize_transport("telnet")


@patch("plugins.vm_scanner.transports.winrm_impl.winrm.Session")
def test_winrm_runner_trigger_script_success(mock_winrm_session_class):
    """Тест: Выполнение скрипта по WinRM при успешном ответе."""
    from plugins.vm_scanner.transports.winrm_impl import WinRmRunner

    mock_session_instance = MagicMock()
    mock_winrm_session_class.return_value = mock_session_instance

    mock_response = MagicMock()
    mock_response.std_out = b"winrm_success_output \n"
    mock_session_instance.run_ps.return_value = mock_response

    runner = WinRmRunner("10.10.10.10", "admin", "secret")
    output = runner.trigger_script("Get-Process")

    assert output == "winrm_success_output"
    mock_winrm_session_class.assert_called_once_with(
        "10.10.10.10",
        auth=("admin", "secret"),
        transport="ntlm",
        server_cert_validation="ignore",
        read_timeout_sec=630,
        operation_timeout_sec=600,
    )


def test_chunk_list_unit():
    """Тест: Проверка алгоритма разбиения списка на чанки."""
    from plugins.vm_scanner.modules.pg_upsert import chunk_list

    data = list(range(10))

    chunks = list(chunk_list(data, 3))

    assert len(chunks) == 4
    assert chunks[0] == [0, 1, 2]
    assert chunks[1] == [3, 4, 5]
    assert chunks[2] == [6, 7, 8]
    assert chunks[3] == [9]
