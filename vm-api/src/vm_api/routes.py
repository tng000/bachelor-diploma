from fastapi import Depends, HTTPException
from vm_api.models import Host, VM, Software
from vm_api.schemas import HostCreate, HostRead, SoftwareRead, VMRead, VMCreate, VMUpdate, SoftwareCreate
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from typing import List
from vm_api.db import get_db
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import APIRouter
from uuid import UUID


# --- HOSTS ---
host_router = APIRouter(tags=["Hosts"])

@host_router.post("/hosts/", response_model=HostRead, status_code=201)
async def create_host(host: HostCreate, db: AsyncSession = Depends(get_db)):
    db_host = Host(**host.model_dump())
    db.add(db_host)
    await db.commit()
    await db.refresh(db_host)
    return db_host

@host_router.get("/hosts/", response_model=List[HostRead])
async def read_hosts(skip: int = 0, limit: int = 100, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Host).offset(skip).limit(limit))
    return result.scalars().all()

@host_router.get("/hosts/{host_id}", response_model=HostRead)
async def read_host(host_id: UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Host).filter(Host.id == host_id))
    host = result.scalars().first()
    if not host:
        raise HTTPException(status_code=404, detail="Host not found")
    return host

@host_router.put("/hosts/{host_id}", response_model=HostRead)
async def update_host(host_id: UUID, host_data: HostCreate, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Host).filter(Host.id == host_id))
    host = result.scalars().first()
    if not host:
        raise HTTPException(status_code=404, detail="Host not found")
    for k, v in host_data.model_dump().items():
        setattr(host, k, v)
    await db.commit()
    await db.refresh(host)
    return host

@host_router.delete("/hosts/{host_id}", status_code=204)
async def delete_host(host_id: UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Host).filter(Host.id == host_id))
    host = result.scalars().first()
    if not host:
        raise HTTPException(status_code=404, detail="Host not found")
    await db.delete(host)
    await db.commit()

# --- SOFTWARE ---
software_router = APIRouter(tags=["Softwares"])
@software_router.post("/software/", response_model=SoftwareRead, status_code=201)
async def create_software(soft: SoftwareCreate, db: AsyncSession = Depends(get_db)):
    db_soft = Software(**soft.model_dump())
    db.add(db_soft)
    await db.commit()
    await db.refresh(db_soft)
    return db_soft

@software_router.get("/software/", response_model=List[SoftwareRead])
async def read_software(skip: int = 0, limit: int = 100, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Software).offset(skip).limit(limit))
    return result.scalars().all()

@software_router.put("/software/{soft_id}", response_model=SoftwareRead)
async def update_software(soft_id: UUID, soft_data: SoftwareCreate, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Software).filter(Software.id == soft_id))
    soft = result.scalars().first()
    if not soft:
        raise HTTPException(status_code=404, detail="Host not found")
    for k, v in soft_data.model_dump().items():
        setattr(soft, k, v)
    await db.commit()
    await db.refresh(soft)
    return soft

@software_router.delete("/software/{soft_id}", status_code=204)
async def delete_software(soft_id: UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Software).filter(Software.id == soft_id))
    soft = result.scalars().first()
    if not soft:
        raise HTTPException(status_code=404, detail="Software not found")
    await db.delete(soft)
    await db.commit()

# --- VMS ---
vm_router = APIRouter(tags=["Virtual Machines"])
@vm_router.post("/vms/", response_model=VMRead, status_code=201)
async def create_vm(vm: VMCreate, db: AsyncSession = Depends(get_db)):
    host_result = await db.execute(select(Host).filter(Host.id == vm.host_id))
    if not host_result.scalars().first():
        raise HTTPException(status_code=404, detail="Host not found")

    vm_data = vm.model_dump(exclude={"software_ids"})
    db_vm = VM(**vm_data)

    if vm.software_ids is not None:
        soft_result = await db.execute(select(Software).filter(Software.id.in_(vm.software_ids)))
        soft_list = soft_result.scalars().all()
        if len(soft_list) != len(vm.software_ids):
            raise HTTPException(status_code=404, detail="One or more software IDs not found")
        db_vm.software = soft_list

    db.add(db_vm)
    await db.commit()
    stmt = (
        select(VM)
        .options(selectinload(VM.host), selectinload(VM.software))
        .where(VM.id == db_vm.id)
    )
    result = await db.execute(stmt)
    return result.scalar_one()

@vm_router.get("/vms/", response_model=List[VMRead])
async def read_vms(skip: int = 0, limit: int = 100, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(VM)
        .options(selectinload(VM.host), selectinload(VM.software))
        .offset(skip)
        .limit(limit)
    )
    return result.scalars().all()

@vm_router.get("/vms/{vm_id}", response_model=VMRead)
async def read_vm(vm_id: UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(VM)
        .options(selectinload(VM.host), selectinload(VM.software))
        .filter(VM.id == vm_id)
    )
    vm = result.scalars().first()
    if not vm:
        raise HTTPException(status_code=404, detail="VM not found")
    return vm

@vm_router.put("/vms/{vm_id}", response_model=VMRead)
async def update_vm(vm_id: UUID, vm_data: VMUpdate, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(VM)
        .options(selectinload(VM.host), selectinload(VM.software))
        .filter(VM.id == vm_id)
    )
    db_vm = result.scalars().first()
    
    if not db_vm:
        raise HTTPException(status_code=404, detail="VM not found")

    for k, v in vm_data.model_dump(exclude={"software_ids"}).items():
        setattr(db_vm, k, v)

    if vm_data.software_ids is not None:
        soft_result = await db.execute(select(Software).filter(Software.id.in_(vm_data.software_ids)))
        soft_list = soft_result.scalars().all()
        if len(soft_list) != len(vm_data.software_ids):
            raise HTTPException(status_code=404, detail="One or more software IDs not found")
        db_vm.software = soft_list

    await db.commit()
    final_result = await db.execute(
        select(VM)
        .options(selectinload(VM.host), selectinload(VM.software))
        .filter(VM.id == vm_id)
    )
    return final_result.scalar_one()

@vm_router.delete("/vms/{vm_id}", status_code=204)
async def delete_vm(vm_id: UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(VM).filter(VM.id == vm_id))
    vm = result.scalars().first()
    if not vm:
        raise HTTPException(status_code=404, detail="VM not found")
    await db.delete(vm)
    await db.commit()