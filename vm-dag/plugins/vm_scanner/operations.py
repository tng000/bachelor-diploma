import logging
from airflow.sdk import Variable

from plugins.vm_scanner.cfg import DB_CONN, GLB_TRANSPORT_MODE_VAR
from plugins.vm_scanner.engine import OperationsEngine
from utils.pg_provider import yield_pg_cursor

_logger = logging.getLogger(__name__)
GLB_TRANSPORT_MODE = Variable.get(GLB_TRANSPORT_MODE_VAR, default="ssh")


def fetch_target_ips_op() -> list[str]:
    try:
        with yield_pg_cursor(pg_target_id=DB_CONN) as pointer:
            query = """
                SELECT ip_address 
                FROM hosts
                WHERE 
                    status IS NULL
                    OR status = 'new'
                    OR last_scan IS NULL
                    
                    OR (status = 'failed' AND last_scan >= NOW() - INTERVAL '1 hour')
                    
                    OR (last_scan < NOW() - INTERVAL '1 hour');
            """
            pointer.execute(query)
            db_records = pointer.fetchall()

        return [item["ip_address"] for item in db_records]
    except Exception as exc:
        _logger.error("Ошибка получения списка хостов из БД: %s", exc)
        raise exc
    
def update_host_op(ip_address: str, success: bool) -> None:
    try:
        status_value = "success" if success else "failed"
        
        with yield_pg_cursor(pg_target_id=DB_CONN) as pointer:
            query = """
                UPDATE hosts 
                SET 
                    status = %s,
                    last_scan = NOW()
                WHERE ip_address = %s;
            """
            pointer.execute(query, (status_value, ip_address))
            
    except Exception as exc:
        _logger.error("Ошибка обновления статуса хоста %s: %s", ip_address, exc)
        raise exc

            
    except Exception as exc:
        _logger.error("Ошибка обновления статуса хоста %s на failed: %s", ip_address, exc)
        raise exc


def process_single_host_op(host_ip: str, node_creds: dict, guest_creds: dict) -> list[dict]:
    manager = OperationsEngine(
        node_ip=host_ip,
        user_id=node_creds["login"],
        secret=node_creds["pwd"],
        net_port=node_creds["port"],
        os_usr=guest_creds["login"],
        os_pwd=guest_creds["pwd"]
    )
    manager.initialize_transport(GLB_TRANSPORT_MODE)

    try:
        all_vms_data = manager.fetch_all_vms_on_host()
    except Exception as exc:
        update_host_op(ip_address=host_ip, success=False)
        raise exc
    
    update_host_op(ip_address=host_ip, success=True)

    if not all_vms_data:
        _logger.warning("На хосте %s не найдено ВМ.", host_ip)
        return []

    return all_vms_data