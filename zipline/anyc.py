from net.RPCClient import RPCClient
import pandas as pd
import asyncore

import socket
import struct
from net.RPCBase import TTPackage
from net.RPCBase import NET_CMD_RPC


class asynRPCClient(RPCClient):
    def __init__(self, host, port, handler=None):
        RPCClient.__init__(self, host, port, 0)
        self._cb = {}

    def subscribe(self, func, params, cb):
        self.seq += 1
        d = {u"func": func, u"params": params}
        sendPack = TTPackage(self.seq, NET_CMD_RPC, d)
        datas = sendPack.encode()
        self.socket.sendall(datas)
        self._cb[self.seq] = cb
        return self.seq

    def recvPackages(self):
        while True:
            package = self._recvOnePackage()
            if self._cb.has_key(package.seq):
                yield self._cb.get(package.seq)(package.data)

    def _recv_all(self, length):
        data = ''
        while len(data) < length:
            more = self.socket.recv(length - len(data))
            if not more:
                raise EOFError(
                    'socket closed %d bytes into a %d-byte message' % (
                        len(data), length))
            data += more
        return data

    def _recvOnePackage(self):
        strLen = self._recv_all(4)
        packLen, = struct.unpack_from("!I", strLen, 0)
        strContent = self._recv_all(packLen - 4)
        datas = strLen + strContent
        package = TTPackage()
        package.decode(datas)
        return package



if __name__ == "__main__":
    server_ip = "127.0.0.1:9001"
    rpc_client = asynRPCClient(
        host="127.0.0.1",
        port=9001,
    )

    def cb(*args, **kargs):
        print args


    import functools
    rpc_client.subscribe(
        "get", {"symbol": "300133.SZ"},
        functools.partial(cb, "300133.SZ")
    )

    for _dt in rpc_client.recvPackages():
        print _dt