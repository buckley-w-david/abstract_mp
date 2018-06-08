class CsvTransformError(Exception):
    pass


class InvalidResourceError(CsvTransformError):
    pass


class ApplyTypeError(CsvTransformError):
    pass
