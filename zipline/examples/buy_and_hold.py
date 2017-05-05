#!/usr/bin/env python
#
# Copyright 2015 Quantopian, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from zipline.api import order, symbol, symbols
import pytz
stocks = ['600000.SS', '000001.SZ']


def initialize(context):
    # context.set_benchmark(symbol("000001.SZ"))
    context.has_ordered = False
    context.stocks = stocks


def handle_data(context, data):
    print context.datetime.tz_convert(pytz.timezone("Asia/Shanghai"))
    import pandas as pd

    print data.history(symbols("000001.SZ"), ["price","volume"], 10,
                       '1d').iloc[:,1]
    print data.history(symbols("000002.SZ"), "price", 2, '1m').iloc[0]
    print data.history(symbols("600000.SH"), "close", 1, '1d')
    print data.current(symbol("000001.SZ"), 'price')


def _test_args():
    """Extra arguments to use when zipline's automated tests run this example.
    """
    import pandas as pd

    return {
        'start': pd.Timestamp('2008', tz='utc'),
        'end': pd.Timestamp('2013', tz='utc'),
    }
