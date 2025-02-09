import datetime as dt


def current_timestamp_utc() -> str:
    """Returns the current timestamp (in UTC).

    Returns:
        str: The current timestamp in UTC.
    """

    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
