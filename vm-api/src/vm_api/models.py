from vm_api.db import Base
from sqlalchemy import (
    Column,
    DateTime,
    Enum,
    Integer,
    String,
    ForeignKey,
    UniqueConstraint,
    BigInteger,
)
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID
import uuid


class Host(Base):
    __tablename__ = "hosts"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    ip_address = Column(String, nullable=False, unique=True)
    status = Column(
        Enum("new", "success", "failed", name="host_status_enum"),
        nullable=True,
        default="new",
    )
    last_scan = Column(DateTime(timezone=True), nullable=True)

    vms = relationship("VM", back_populates="host", cascade="all, delete-orphan")


class VM(Base):
    __tablename__ = "vms"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    guid = Column(String, unique=True, nullable=False)
    name = Column(String, nullable=True)
    host_id = Column(UUID(as_uuid=True), ForeignKey("hosts.id"))
    domain = Column(String, nullable=True)
    os = Column(String, nullable=True)
    power_state = Column(String, nullable=True)
    ip_address = Column(String, nullable=True)
    mac_address = Column(String, nullable=True)
    cpu = Column(Integer, nullable=True)
    ram_mb = Column(Integer, nullable=True)
    storage = Column(BigInteger, nullable=True)

    host = relationship("Host", back_populates="vms")
    software = relationship("Software", secondary="vm_software", back_populates="vms")


class Software(Base):
    __tablename__ = "software"
    __table_args__ = (
        UniqueConstraint("name", "version", name="_software_name_version_uc"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    name = Column(String, nullable=False)
    version = Column(String, nullable=False)
    vms = relationship("VM", secondary="vm_software", back_populates="software")


class VMSoftware(Base):
    __tablename__ = "vm_software"
    vm_id = Column(UUID(as_uuid=True), ForeignKey("vms.id"), primary_key=True)
    software_id = Column(
        UUID(as_uuid=True), ForeignKey("software.id"), primary_key=True
    )
