import logging

_logger = logging.getLogger(__name__)

def build_probe_script() -> str:
    """Возвращает PowerShell-скрипт для получения списка имён всех ВМ на хосте."""
    return """
    $OutputEncoding = [System.Text.Encoding]::UTF8
    [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
    Get-VM | Select-Object -ExpandProperty Name
    """

def extract_names_from_text(raw_val: str) -> list[str]:
    """Разбирает текстовый вывод скрипта в список имён виртуальных машин."""
    if not raw_val:
        _logger.warning("Получен пустой ответ от хоста, список ВМ пуст.")
        return []

    try:
        cleaned_lines = [item.strip() for item in raw_val.strip().splitlines() if item.strip()]
        _logger.info("Обнаружено машин на хосте: %d — %s", len(cleaned_lines), cleaned_lines)
        return cleaned_lines
    except Exception as exc:
        _logger.error("Ошибка разбора вывода с именами ВМ: %s", exc)
        return []