from datetime import datetime
from pydantic import BaseModel, ConfigDict
from typing import Optional, List, Literal
from uuid import UUID


class SoftwareBase(BaseModel):
    name: str
    version: str


class SoftwareCreate(SoftwareBase):
    pass


class SoftwareRead(SoftwareBase):
    id: UUID
    model_config = ConfigDict(from_attributes=True)


class HostBase(BaseModel):
    ip_address: str
    status: Optional[Literal["new", "success", "failed"]] = "new"
    last_scan: Optional[datetime] = None


class HostCreate(HostBase):
    pass


class HostRead(HostBase):
    id: UUID
    model_config = ConfigDict(from_attributes=True)


class VMBase(BaseModel):
    host_id: UUID
    guid: Optional[str] = None
    domain: Optional[str] = None
    os: Optional[str] = None
    power_state: Optional[str] = None
    ip_address: Optional[str] = None
    mac_address: Optional[str] = None
    cpu: Optional[int] = None
    ram_mb: Optional[int] = None
    storage: Optional[int] = None


class VMCreate(VMBase):
    software_ids: Optional[List[UUID]] = None


class VMUpdate(VMCreate):
    pass


class VMRead(VMBase):
    id: UUID
    host: Optional[HostRead] = None
    software: List[SoftwareRead] = []
    model_config = ConfigDict(from_attributes=True)
