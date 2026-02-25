import logging
import uuid
from plugins.vm_scanner.cfg import DB_CONN
from utils.pg_provider import yield_pg_cursor

_logger = logging.getLogger(__name__)

def chunk_list(data: list, chunk_size: int):
    """Разбивает список на чанки для обхода лимита параметров Postgres (~65535)."""
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]

def write_batch_hosts_records(batch_data: list[dict]):
    if not batch_data:
        return

    with yield_pg_cursor(pg_target_id=DB_CONN) as cur:
        host_ips = [r["host_ip"] for r in batch_data]
        placeholders = ",".join(["%s"] * len(host_ips))
        cur.execute(f"SELECT ip_address, id FROM hosts WHERE ip_address IN ({placeholders})", host_ips)
        host_map = {row['ip_address']: row['id'] for row in cur.fetchall()}

        vm_params =[]
        for host_record in batch_data:
            ref_host_id = host_map.get(host_record["host_ip"])
            if not ref_host_id:
                continue
            
            for vm in host_record.get("vms",[]):
                vm_params.append((
                    str(uuid.uuid4()), vm['name'], vm['guid'], ref_host_id,
                    vm.get('domain'), vm.get('os'), vm.get('power_state'),
                    vm.get('ip_address'), vm.get('mac_address'),
                    vm.get('cpu'), vm.get('ram_mb'), vm.get('storage')
                ))

        vm_params.sort(key=lambda x: x[2])
        guid_to_vmid = {}
        for vm_chunk in chunk_list(vm_params, 1000):
            flat_params =[item for sublist in vm_chunk for item in sublist]
            vm_placeholders = ",".join(["(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"] * len(vm_chunk))
            
            vm_query = f"""
                INSERT INTO vms (
                    id, name, guid, host_id, domain, os, power_state, 
                    ip_address, mac_address, cpu, ram_mb, storage
                ) VALUES {vm_placeholders}
                ON CONFLICT (guid) DO UPDATE SET 
                    host_id = EXCLUDED.host_id, domain = EXCLUDED.domain, name = EXCLUDED.name, 
                    power_state = EXCLUDED.power_state, ip_address = EXCLUDED.ip_address,
                    os = EXCLUDED.os, mac_address = EXCLUDED.mac_address,
                    cpu = EXCLUDED.cpu, ram_mb = EXCLUDED.ram_mb, storage = EXCLUDED.storage
                RETURNING guid, id;
            """
            cur.execute(vm_query, flat_params)
            guid_to_vmid.update({row['guid']: row['id'] for row in cur.fetchall()})

        all_apps_set = set()
        for host_record in batch_data:
            for vm in host_record.get("vms", []):
                for app in vm.get('software',[]):
                    if app.get('name'):
                        all_apps_set.add((app['name'].strip(), app.get('version') or ''))

        soft_map = {}
        if all_apps_set:
            unique_apps = list(all_apps_set)
            unique_apps.sort(key=lambda x: (x[0], x[1]))
            
            for soft_chunk in chunk_list(unique_apps, 5000):
                flat_soft =[item for sublist in soft_chunk for item in sublist]
                
                l_placeholders = ",".join(["(%s,%s)"] * len(soft_chunk))
                cur.execute(f"""
                    SELECT id, name, version FROM software 
                    WHERE (name, version) IN ({l_placeholders});
                """, flat_soft)
                
                for row in cur.fetchall():
                    soft_map[(row['name'], row['version'])] = row['id']

                to_insert =[item for item in soft_chunk if item not in soft_map]
                
                if to_insert:
                    s_placeholders = ",".join(["(%s,%s,%s)"] * len(to_insert))
                    insert_params = []
                    for name, ver in to_insert:
                        insert_params.extend([str(uuid.uuid4()), name, ver])

                    cur.execute(f"""
                        INSERT INTO software (id, name, version) VALUES {s_placeholders}
                        ON CONFLICT (name, version) DO NOTHING
                        RETURNING id, name, version;
                    """, insert_params)
                    
                    for row in cur.fetchall():
                        soft_map[(row['name'], row['version'])] = row['id']
                        
                    missing =[item for item in to_insert if item not in soft_map]
                    if missing:
                        m_placeholders = ",".join(["(%s,%s)"] * len(missing))
                        flat_missing =[item for sublist in missing for item in sublist]
                        cur.execute(f"""
                            SELECT id, name, version FROM software 
                            WHERE (name, version) IN ({m_placeholders});
                        """, flat_missing)
                        for row in cur.fetchall():
                            soft_map[(row['name'], row['version'])] = row['id']

        all_db_vm_ids = list(guid_to_vmid.values())
        if all_db_vm_ids:
            for ids_chunk in chunk_list(all_db_vm_ids, 5000):
                ids_placeholders = ",".join(["%s"] * len(ids_chunk))
                cur.execute(f"DELETE FROM vm_software WHERE vm_id IN ({ids_placeholders})", ids_chunk)

        links_params =[]
        for host_record in batch_data:
            for vm in host_record.get("vms",[]):
                db_vm_id = guid_to_vmid.get(vm['guid'])
                if not db_vm_id:
                    continue
                for app in vm.get('software',[]):
                    sid = soft_map.get((app.get('name', '').strip(), app.get('version') or ''))
                    if sid:
                        links_params.append((db_vm_id, sid))

        if all_db_vm_ids:
            cur.execute("CREATE TEMP TABLE tmp_actual_links (vm_id UUID, software_id UUID) ON COMMIT DROP;")
            if links_params:
                for links_chunk in chunk_list(links_params, 5000):
                    flat_links =[item for sublist in links_chunk for item in sublist]
                    link_ph = ",".join(["(%s,%s)"] * len(links_chunk))
                    cur.execute(f"INSERT INTO tmp_actual_links VALUES {link_ph}", flat_links)

            cur.execute(f"""
                DELETE FROM vm_software vs
                USING unnest(ARRAY[{','.join(["%s"] * len(all_db_vm_ids))}]) AS target_vms(vid)
                WHERE vs.vm_id = target_vms.vid
                AND NOT EXISTS (
                    SELECT 1 FROM tmp_actual_links t 
                    WHERE t.vm_id = vs.vm_id AND t.software_id = vs.software_id
                )
            """, all_db_vm_ids)

            cur.execute("""
                INSERT INTO vm_software (vm_id, software_id)
                SELECT vm_id, software_id FROM tmp_actual_links
                ON CONFLICT DO NOTHING;
            """)

        _logger.info("Батч успешно записан: загружено %d ВМ.", len(vm_params))