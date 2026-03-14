from contextlib import suppress
import uvicorn


if __name__ == "__main__":
    with suppress(KeyboardInterrupt):
        uvicorn.run(
            app="vm_api.main:app",
            host="localhost",
            port=8000,
            reload=True,
        )
