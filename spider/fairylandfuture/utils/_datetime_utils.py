# coding: UTF-8
"""
@software: PyCharm
@author: Lionel Johnson
@contact: https://fairy.host
@organization: https://github.com/FairylandFuture
@datetime: 2024-08-18 01:05:53 UTC+08:00
"""

import time
from datetime import datetime, timedelta, timezone
from typing import Optional, Union

from dateutil.relativedelta import relativedelta

from fairylandfuture import logger
from fairylandfuture.enums import DateTimeEnum, TimeZoneEnum


class DateTimeUtils:
    """
    Data and time module
    """

    TIMEZONE: str = TimeZoneEnum.Shanghai.value

    @classmethod
    def date(cls, _format: Optional[str] = None) -> str:
        """
        Get the current date.

        :param _format: Date format.
        :type _format: str
        :return: Current date
        :rtype: str
        """
        if not _format:
            _format = DateTimeEnum.DATE.value

        result = datetime.now().date().strftime(_format)
        logger.debug(f"Current date: {result}")
        return result

    @classmethod
    def date_shanghai(cls, _format: Optional[str] = None) -> str:
        """
        Get the current date in shanghai time zone.

        :param _format: Date format.
        :type _format: str
        :return: Current date in shanghai time zone.
        :rtype: str
        """
        if not _format:
            _format = DateTimeEnum.DATE.value

        result = datetime.now(tz=timezone(timedelta(hours=8), name=cls.TIMEZONE)).date().strftime(_format)
        logger.debug(f"Current date in Shanghai time zone: {result}")
        return result

    @classmethod
    def time(cls, _fromat: Optional[str] = None) -> str:
        """
        Get the current time.

        :param _fromat: Time format.
        :type _fromat: str
        :return: Current time
        :rtype: str
        """
        if not _fromat:
            _fromat = DateTimeEnum.TIME.value

        result = datetime.now().time().strftime(_fromat)
        logger.debug(f"Current time: {result}")
        return result

    @classmethod
    def time_shanghai(cls, _fromat: Optional[str] = None) -> str:
        """
        Get the current time in shanghai time zone.

        :param _fromat: Time format.
        :type _fromat: str
        :return: Current time in shanghai time zone.
        :rtype: str
        """
        if not _fromat:
            _fromat = DateTimeEnum.TIME.value

        result = datetime.now(tz=timezone(timedelta(hours=8), name=cls.TIMEZONE)).time().strftime(_fromat)
        logger.debug(f"Current time in Shanghai time zone: {result}")
        return result

    @classmethod
    def datetime(cls, _format: Optional[str] = None) -> str:
        """
        Get the current datetime_str.

        :param _format: Datetime format.
        :type _format: str
        :return: Current datetime_str
        :rtype: str
        """
        if not _format:
            _format = DateTimeEnum.DATETIME.value

        result = datetime.now().strftime(_format)
        logger.debug(f"Current datetime: {result}")
        return result

    @classmethod
    def datetime_shanghai(cls, _format: Optional[str] = None) -> str:
        """
        Get the current datetime_str in shanghai time zone.

        :param _format: Datetime format.
        :type _format: str
        :return: Current datetime_str in shanghai time zone.
        :rtype: str
        """
        if not _format:
            _format = DateTimeEnum.DATETIME.value

        result = datetime.now(tz=timezone(timedelta(hours=8), name=cls.TIMEZONE)).strftime(_format)
        logger.debug(f"Current datetime in Shanghai time zone: {result}")
        return result

    @classmethod
    def timestamp(cls, ms: bool = False, n: Optional[int] = None) -> int:
        """
        Get the current timestamp.

        :return: Current timestamp.
        :rtype: int
        """

        if ms:
            result = round(time.time() * 1000)
            logger.debug(f"Current timestamp in ms: {result}")
            return result
        if n:
            result = round(time.time()) * (10 ** (n - 10))
            logger.debug(f"Current timestamp with {n} digits: {result}")
            return result

        result = round(time.time())
        logger.debug(f"Current timestamp: {result}")
        return result

    @classmethod
    def timestamp_to_datetime(cls, timestamp: Union[int, float], _format: Optional[str] = None) -> str:
        """
        Convert timestamp to datetime_str.

        :param timestamp: Timestamp.
        :type timestamp: int or float
        :param _format: Datetime format.
        :type _format: str
        :return: Formatted datetime_str string.
        :rtype: str
        """

        if len(str(int(timestamp))) == 13:
            timestamp /= 1000

        if not _format:
            _format = DateTimeEnum.DATETIME.value

        result = datetime.fromtimestamp(timestamp).strftime(_format)
        logger.debug(f"Converted datetime from timestamp {timestamp}: {result}")
        return result

    @classmethod
    def datetime_to_timestamp(cls, dt_string: str, ms: bool = False, n: Optional[int] = None, _format: Optional[str] = None) -> int:
        """
        Convert datetime to timestamp.

        :param dt_string: Datetime string.
        :type dt_string: str
        :param ms: Whether to include mss.
        :type ms: bool
        :param n: Number of decimal places for the timestamp.
        :type n: int or None
        :param _format: Datetime format.
        :type _format: str
        :return: Timestamp.
        :rtype: int
        """

        if not _format:
            _format = DateTimeEnum.DATETIME.value

        timestamp = datetime.strptime(dt_string, _format).timestamp()

        if ms:
            result = int(timestamp * 1000)
            logger.debug(f"Converted timestamp in ms from datetime {dt_string!r}: {result}")
            return result
        if n:
            result = int(timestamp * (10 ** (n - 10)))
            logger.debug(f"Converted timestamp with {n} digits from datetime {dt_string!r}: {result}")
            return result

        result = int(timestamp)
        logger.debug(f"Converted timestamp from datetime {dt_string!r}: {result}")
        return result

    @classmethod
    def yesterday(cls, _format: Optional[str] = None) -> str:
        """
        Get yesterday's date.

        :param _format: Date format.
        :type _format: str
        :return: Yesterday's date.
        :rtype: str
        """
        if not _format:
            _format = DateTimeEnum.DATE.value

        result = (datetime.now() - relativedelta(days=1)).strftime(_format)
        logger.debug(f"Yesterday's date: {result}")
        return result

    @classmethod
    def tomorrow(cls, _format: Optional[str] = None) -> str:
        """
        Get tomorrow's date.

        :param _format: Date format.
        :type _format: str
        :return: Tomorrow's date.
        :rtype: str
        """
        if not _format:
            _format = DateTimeEnum.DATE.value

        result = (datetime.now() + relativedelta(days=1)).strftime(_format)
        logger.debug(f"Tomorrow's date: {result}")
        return result

    @classmethod
    def daysdelta(cls, dt1: Union[str, int, float], dt2: Union[str, int, float], timestamp: bool = False, ms: bool = False, _format: Optional[str] = None) -> int:
        """
        Calculate the number of days between two dates.

        :param dt1: Datetime_str or timestamp of the first date.
        :type dt1: str or int or float
        :param dt2: Datetime_str or timestamp of the second date.
        :type dt2: str or int or float
        :param timestamp: Is timestamp or datetime_str.
        :type timestamp: bool
        :param ms: Is ms or not.
        :type ms: bool
        :param _format: Datetime_str format.
        :type _format: str
        :return: Days delta.
        :rtype: int
        """
        if not _format:
            _format = DateTimeEnum.DATE.value

        if timestamp:
            if ms:
                date1 = datetime.fromtimestamp(dt1 / 1000)
                date2 = datetime.fromtimestamp(dt2 / 1000)
            else:
                date1 = datetime.fromtimestamp(dt1)
                date2 = datetime.fromtimestamp(dt2)
        else:
            date1 = datetime.strptime(dt1, _format)
            date2 = datetime.strptime(dt2, _format)

        result = abs((date2 - date1).days)
        logger.debug(f"Days delta between {dt1!r} and {dt2!r}: {result}")
        return result

    @classmethod
    def unzone_utc(cls) -> "datetime":
        """
        Remove the timezone from the current datetime.

        :return: Unzoned datetime.
        :rtype: datetime
        """
        result = datetime.now(timezone.utc).replace(tzinfo=None)
        logger.debug(f"Unzoned UTC datetime: {result}")
        return result

    @classmethod
    def unzone_cst(cls) -> "datetime":
        """
        Remove the timezone from the current datetime in China.

        :return: Unzoned datetime in China.
        :rtype: datetime
        """
        result = datetime.now(tz=timezone(timedelta(hours=8), name=cls.TIMEZONE)).replace(tzinfo=None)
        logger.debug(f"Unzoned CST datetime: {result}")
        return result
