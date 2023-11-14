class BadJsonError(Exception):
    ...


class PathParameterError(Exception):
    ...


class QueryParameterError(Exception):
    ...


class ParameterError(Exception):
    ...


class QueryParameterMismatch(Exception):
    ...


class WrongNumberOfRecordsUpdated(Exception):
    ...


class QueryDoesntReturnOneRow(Exception):
    ...


class QueryDoesntReturnOneColumn(Exception):
    ...


class QueryDoesntReturnOneValue(Exception):
    ...
