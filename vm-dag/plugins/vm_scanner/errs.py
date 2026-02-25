class HvScanBaseException(Exception):
    """Базовое исключение для ошибок взаимодействия с хостом Hyper-V."""
    pass

class ExecutionFailure(Exception):
    """Ошибка выполнения скрипта на удалённой стороне. Содержит детали и код возврата."""
    def __init__(self, details, ret_code=None, err_stream=None):
        super().__init__(details)
        self.ret_code = ret_code
        self.err_stream = err_stream
        self.details = details