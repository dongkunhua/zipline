#!coding=utf-8

import os

from pandas_datareader.data import  DataReader
from datetime import time
import pandas as pd
import numpy  as np

from pytz import timezone
from pandas import (
    DataFrame,
    date_range,
    DatetimeIndex,
    DateOffset
)

import zipline
from zipline.utils.calendars.trading_calendar import TradingCalendar, \
    HolidayCalendar, NANOS_IN_MINUTE, lazyval


start_default = pd.Timestamp('1990-12-19', tz="UTC")
end_base = pd.Timestamp('today',  tz="UTC")

# Give an aggressive buffer for logic that needs to use the next trading
# day or minute.
end_default = end_base + pd.Timedelta(days=365)

class SHExchangeCalendar(TradingCalendar):
    """
    上海证券交易所交易日历

    Open Time: 09:15 AM,  Asia/Shanghai
    Close Time: 03:00 PM, Asia/Shanghai

    Regularly-Observed Holidays:
    """

    def __init__(self, start=start_default, end=end_default):
        super(SHExchangeCalendar, self).__init__(start=start, end=end)

    @property
    def name(self):
        return "SH"

    @property
    def tz(self):
        return timezone("Asia/Shanghai")

    @property
    def open_offset(self):
        return 0

    @property
    def close_offset(self):
        return 0

    @property
    def open_time(self):
        return time(9, 31)

    @property
    def before_trading_start_minutes(self):
        return time(9, 0)

    @property
    def close_time(self):
        return time(15, 0)

    @property
    def adhoc_holidays(self):
        ts = DataReader("000001.SS", "yahoo", start_default, end_default).index
        # from net.RPCClient import request
        # data = request(
        #     "123.56.77.52:10030",
        #     "Tdays",
        #     {}
        # )
        # ts = pd.Series(pd.to_datetime(data)).sort_index()
        # ts.name = "Date"
        # ts = pd.DatetimeIndex(ts)
        # print ts
        ts1 = pd.bdate_range(start=ts[0], end=ts[-1]).tz_localize("UTC")
        sh_holidays = []
        for d in ts1.values:
            if d  in ts.values:
                continue
            sh_holidays.append(d)
        return sh_holidays

    @property
    def regular_holidays(self):
        return HolidayCalendar([

        ])

    @property
    def special_opens(self):
        return [
        ]

    @lazyval
    def all_minutes(self):
        """
        Returns a DatetimeIndex representing all the minutes in this calendar.
        """
        opens_in_ns = \
            self._opens.values.astype('datetime64[ns]')

        closes_in_ns = \
            self._closes.values.astype('datetime64[ns]')

        deltas = closes_in_ns - opens_in_ns

        # + 1 because we want 390 minutes per standard day, not 389
        # 美国交易日是390分钟，中国交易日是330分钟，且中间有一个半小时休市
        daily_sizes = (deltas / NANOS_IN_MINUTE) + 1 - 90
        num_minutes = np.sum(daily_sizes).astype(np.int64)

        # One allocation for the entire thing. This assumes that each day
        # represents a contiguous block of minutes.
        all_minutes = np.empty(num_minutes, dtype='datetime64[ns]')

        idx = 0
        for day_idx, size in enumerate(daily_sizes):
            # lots of small allocations, but it's fast enough for now.

            # size is a np.timedelta64, so we need to int it
            size_int = int(size)/2
            all_minutes[idx:(idx + size_int)] = \
                np.arange(
                    opens_in_ns[day_idx],
                    opens_in_ns[day_idx] + 120*NANOS_IN_MINUTE,
                    NANOS_IN_MINUTE
                )
            idx += size_int
            all_minutes[idx:(idx + size_int)] = \
                np.arange(
                    opens_in_ns[day_idx] + 210*NANOS_IN_MINUTE,
                    closes_in_ns[day_idx] + NANOS_IN_MINUTE,
                    NANOS_IN_MINUTE
                )
            idx += size_int
        return DatetimeIndex(all_minutes).tz_localize("UTC")

if __name__ == "__main__":
    calendar_class = SHExchangeCalendar()
    # print calendar_class
