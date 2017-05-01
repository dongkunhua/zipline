import pandas as pd
import numpy as np

from .data_portal import DataPortal, OHLCV_FIELDS, NoDataOnDate


class DataPoratal_async(DataPortal):
    def _get_daily_data(self, asset, column, dt):
        reader = self._get_pricing_reader('daily')
        if column == "last_traded":
            last_traded_dt = reader.get_last_traded_dt(asset, dt)

            if pd.isnull(last_traded_dt):
                return pd.NaT
            else:
                return last_traded_dt
        elif column in OHLCV_FIELDS:
            # don't forward fill
            try:
                return reader.get_value(asset, dt, column)
            except NoDataOnDate:
                return np.nan
        elif column == "price":
            found_dt = dt
            while True:
                try:
                    value = reader.get_value(
                        asset, found_dt, "close"
                    )
                    if not pd.isnull(value):
                        if dt == found_dt:
                            return value
                        else:
                            # adjust if needed
                            return self.get_adjusted_value(
                                asset, column, found_dt, dt, "minute",
                                spot_value=value
                            )
                    else:
                        found_dt -= self.trading_calendar.day
                except NoDataOnDate:
                    return np.nan