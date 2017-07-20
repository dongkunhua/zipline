from net.RPCServer import RPCServer
from net.RPCClient import request
import pandas as pd

class handler:
    def get(self, seq, params, session):
        res = request("123.56.77.52:10030", "Kline",
                      {"symbol": "000001.SZ", "period":"m",
                        "s":"20170505","e":"20170506"},)
        df = pd.DataFrame(res)
        df.index.name = "Date"
        df.index = pd.to_datetime(df.index)
        pd.Timestamp("today")
        import time
        for _index in  df.index:
            if session.connected:
                print _index
                time.sleep(10)
                session.sendResponse(seq, [_index])
            else:
                print "connect lost"
                break


RPCServer("0.0.0.0", 9001, handler()).run()
# res = request("123.56.77.52:10030", "Kline", {"symbol": "000001.SZ",
#                                         "s":"20170505","e":"20150506"},)
# df = pd.DataFrame(res)
# df.index.name = "Date"
# df.index = pd.to_datetime(df.index)
# for _index in  df.index:
#     print _index

