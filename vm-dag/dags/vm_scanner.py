from __future__ import annotations

import logging
import concurrent.futures
from pendulum import datetime, duration

from airflow.sdk import dag, task, Variable
from airflow.exceptions import AirflowException
from airflow.sdk.bases.hook import BaseHook

from plugins.vm_scanner.cfg import NODE_CREDS_CONN, GUEST_CREDS_CONN
from plugins.vm_scanner.operations import fetch_target_ips_op, process_single_host_op
from plugins.vm_scanner.modules.pg_upsert import write_batch_hosts_records

_logger = logging.getLogger(__name__)

HOSTS_PER_WORKER_CHUNK = Variable.get("HOSTS_PER_WORKER_CHUNK", default=10)
MAX_THREADS_PER_WORKER = Variable.get("MAX_THREADS_PER_WORKER", default=10)


@dag(
    dag_id="vm_scanner_optimized",
    max_active_tasks=20,
    start_date=datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args={
        "retries": 5,
        "retry_delay": duration(minutes=1),
    },
)
def virtual_machine_scanner_dag():

    @task(task_id="fetch_target_ips")
    def get_nodes() -> list[list[str]]:
        ips = fetch_target_ips_op()
        _logger.info(
            "Получено %d целевых хостов. Разбиваем на батчи по %d.",
            len(ips),
            HOSTS_PER_WORKER_CHUNK,
        )
        return [
            ips[i : i + HOSTS_PER_WORKER_CHUNK]
            for i in range(0, len(ips), HOSTS_PER_WORKER_CHUNK)
        ]

    @task(task_id="process_hosts_batch")
    def process_hosts_batch(host_ips_chunk: list[str]):
        _logger.info(
            "Воркер запустил обработку батча из %d хостов.", len(host_ips_chunk)
        )

        node_conn = BaseHook.get_connection(NODE_CREDS_CONN)
        guest_conn = BaseHook.get_connection(GUEST_CREDS_CONN)

        node_creds = {
            "login": node_conn.login,
            "pwd": node_conn.password,
            "port": node_conn.port or 22,
        }
        guest_creds = {"login": guest_conn.login, "pwd": guest_conn.password}

        batch_results = []
        errors_count = 0

        threads_count = min(MAX_THREADS_PER_WORKER, len(host_ips_chunk))

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=threads_count
        ) as executor:
            future_to_ip = {
                executor.submit(process_single_host_op, ip, node_creds, guest_creds): ip
                for ip in host_ips_chunk
            }

            for future in concurrent.futures.as_completed(future_to_ip):
                ip = future_to_ip[future]
                try:
                    host_data = future.result()
                    if host_data:
                        batch_results.append({"host_ip": ip, "vms": host_data})
                except Exception as exc:
                    _logger.error("Сбой при полной обработке хоста %s: %s", ip, exc)
                    errors_count += 1

        if batch_results:
            _logger.info(
                "Успешно собраны данные с %d хостов. Запуск массовой записи в БД.",
                len(batch_results),
            )
            write_batch_hosts_records(batch_results)

        if errors_count > 0:
            raise AirflowException(
                f"Обработка {errors_count} хостов завершилась ошибкой."
            )

    ip_chunks = get_nodes()
    process_hosts_batch.expand(host_ips_chunk=ip_chunks)


virtual_machine_scanner_dag()
