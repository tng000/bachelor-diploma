from fastapi import FastAPI
from vm_api.routes import host_router, software_router, vm_router

app = FastAPI(title="Hyper-V Hosts & VMs Manager")
app.include_router(host_router)
app.include_router(software_router)
app.include_router(vm_router)
