import abc

class AbstractRemoteShell(abc.ABC):
    """Абстрактный интерфейс для удалённого выполнения скриптов на хосте."""

    @abc.abstractmethod
    def trigger_script(self, script_text: str, max_wait: int = 60) -> str:
        """Выполняет переданный скрипт на удалённой машине и возвращает stdout."""
        pass