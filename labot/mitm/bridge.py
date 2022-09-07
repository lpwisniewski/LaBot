"""
Classes to take decisions for transmitting the packets

The classes inheriting from BridgeHandler must
implement `handle`.

The classes inheriting from MsgBridgeHandler must
implement `handle_message`.
"""

import requests
import psycopg2
import select
from abc import ABC, abstractmethod
from collections import deque
from random import randrange
import logging
import threading
from threading import Thread
import time

from ..data import Buffer, Msg
from .. import protocol

logger = logging.getLogger("labot")
# TODO: use the logger


def from_client(origin):
    return origin.getpeername()[0] == "127.0.0.1"


def direction(origin):
    if from_client(origin):
        return "Client->Server"
    else:
        return "Server->Client"


class BridgeHandler(ABC):
    """Abstract class for bridging policies.
    You just have to subclass and fill the handle method.

    It implements the proxy_callback that will be called
    when a client tries to connect to the server.
    proxy_callback will call `handle` on every packet.

    To modify the behavior, you have to create subclasses pf
    BridgeHandler.
    """

    def __init__(self, coJeu, coSer):
        self.coJeu = coJeu
        self.coSer = coSer
        self.other = {coJeu: coSer, coSer: coJeu}
        self.conns = [coJeu, coSer]

    @abstractmethod
    def handle(self, data, origin):
        pass

    @classmethod
    def proxy_callback(cls, coJeu, coSer):
        """Callback that can be called by the proxy

        It creates an instance of the class and
        calls `handle` on every packet

        coJeu: socket to the game
        coSer: socket to the server
        """
        bridge_handler = cls(coJeu, coSer)
        bridge_handler.loop()

    def loop(self):
        conns = self.conns
        active = True
        try:
            while active:
                rlist, wlist, xlist = select.select(conns, [], conns)
                if xlist or not rlist:
                    break
                for r in rlist:
                    data = r.recv(8192)
                    if not data:
                        active = False
                        break
                    self.handle(data, origin=r)
        finally:
            for c in conns:
                c.close()


class DummyBridgeHandler(BridgeHandler):
    """Implements a dummy policy
    that forwards all packets"""

    def handle(self, data, origin):
        self.other[origin].sendall(data)


class PrintingBridgeHandler(DummyBridgeHandler):
    """
    Implements a dummy policy that
    forwards and prints all packets
    """

    def handle(self, data, origin):
        super().handle(data, origin)
        print(direction(origin), data.hex())


class MsgBridgeHandler(DummyBridgeHandler, ABC):
    """
    Advanced policy to work with the parsed messages
    instead of the raw packets like BridgeHandler.

    This class implements a generic `handle` that calls
    `handle_message` which acts on the parsed messages
    and that should be implemented by the subclasses.
    """

    def __init__(self, coJeu, coSer):
        super().__init__(coJeu, coSer)
        self.buf = {coJeu: Buffer(), coSer: Buffer()}

    def handle(self, data, origin):

        super().handle(data, origin)
        self.buf[origin] += data
        from_client = origin == self.coJeu
        # print(direction(origin), self.buf[origin].data)
        msg = Msg.fromRaw(self.buf[origin], from_client)
        while msg is not None:
            msgType = protocol.msg_from_id[msg.id]
            parsedMsg = protocol.read(msgType, msg.data)

            assert msg.data.remaining() == 0, (
                    "All content of %s have not been read into %s:\n %s"
                    % (msgType, parsedMsg, msg.data)
            )

            self.handle_message(parsedMsg, origin)
            msg = Msg.fromRaw(self.buf[origin], from_client)

    @abstractmethod
    def handle_message(self, msg, origin):
        pass


class PrintingMsgBridgeHandler(MsgBridgeHandler):
    def handle_message(self, msg, origin):
        print(direction(origin))
        print(msg)
        print()
        print()


class InjectorBridgeHandler(BridgeHandler):
    """Forwards all packets and allows to inject
    packets
    """

    def __init__(self, coJeu, coSer, db_size=100, dumper=None):
        super().__init__(coJeu, coSer)
        self.buf = {coJeu: Buffer(), coSer: Buffer()}
        self.injected_to_client = 0
        self.injected_to_server = 0
        self.counter = 0
        self.countLock = threading.Lock()
        self.db = deque([], maxlen=db_size)
        self.dumper = dumper

    def send_to_client(self, data):
        if isinstance(data, Msg):
            data = data.bytes()
        self.injected_to_client += 1
        self.coJeu.sendall(data)

    def send_to_server(self, data):
        if isinstance(data, Msg):
            self.countLock.acquire()
            data.count = self.counter + 1
            self.countLock.release()
            data = data.bytes()
        self.injected_to_server += 1
        self.coSer.sendall(data)

    def send_message(self, s):
        msg = Msg.from_json(
            {"__type__": "ChatClientMultiMessage", "content": s, "channel": 0}
        )
        self.send_to_server(msg)

    def send_price_request(self, item_id):
        msg = Msg.from_json(
            {"__type__": "ExchangeBidHouseSearchMessage", "objectGID": item_id, "follow": True}
        )
        self.send_to_server(msg)
        time.sleep(randrange(5))
        msg = Msg.from_json(
            {"__type__": "ExchangeBidHouseSearchMessage", "objectGID": item_id, "follow": False}
        )
        self.send_to_server(msg)

    def handle(self, data, origin):
        self.buf[origin] += data
        from_client = origin == self.coJeu

        msg = Msg.fromRaw(self.buf[origin], from_client)

        while msg is not None:
            msgType = protocol.msg_from_id[msg.id]
            parsedMsg = protocol.read(msgType, msg.data)
            writtenMsg = None
            try:
                writtenMsg = Msg.from_json(parsedMsg, 0, False)
            except:
                pass


            assert msg.data.remaining() in [0, 48], (
                    "All content of %s have not been read into %s:\n %s"
                    % (msgType, parsedMsg, msg.data)
            )

            if from_client:
                logger.debug(
                    ("-> [%(count)i] %(name)s (%(size)i Bytes)"),
                    dict(
                        count=msg.count,
                        name=protocol.msg_from_id[msg.id]["name"],
                        size=len(msg.data),
                    ),
                )
                if parsedMsg["__type__"] != "BasicPingMessage":
                    print("Sent:", parsedMsg)
                    print("Sent Raw:", msg)
                    print("Sent Written:", writtenMsg)
                    print("Sent Type:", msgType)
            else:
                logger.debug(
                    ("<- %(name)s (%(size)i Bytes)"),
                    dict(name=protocol.msg_from_id[msg.id]["name"], size=len(msg.data)),
                )
            if from_client:
                msg.count += self.injected_to_server - self.injected_to_client
                self.countLock.acquire()
                self.counter = msg.count
                self.countLock.release()
            else:
                self.countLock.acquire()
                self.counter += 1
                self.countLock.release()
            self.db.append(msg)
            if self.dumper is not None:
                self.dumper.dump(msg)
            self.other[origin].sendall(msg.bytes())

            self.handle_message(parsedMsg, origin)
            msg = Msg.fromRaw(self.buf[origin], from_client)

            time.sleep(0.005)
    def show_message(self, m):
        list = ["GameRole", "Chat", "Interactive", "GameMap", "Map", "Basic", "GameContext", "Guild", "SetCharacter"]
        for el in list:
            if el in m:
                return False
        return True

    def handle_message(self, m, o):
        if m["__type__"] == "InteractiveUseRequestMessage":
            worker = Worker(self)
            worker.start()
        if self.show_message(m["__type__"]):
            print("Received:", m)
        if m["__type__"] == "ExchangeTypesItemsExchangerDescriptionForUserMessage":
            price1 = m["itemTypeDescriptions"][0]["prices"][0]
            price10 = m["itemTypeDescriptions"][0]["prices"][1]
            price100 = m["itemTypeDescriptions"][0]["prices"][2]
            requests.post("http://localhost:3000/dofus/price",
                          json={'itemId': m["objectGID"], 'price1': price1, 'price10': price10, 'price100': price100}
                          )
        pass

class Worker(Thread):
    def __init__(self, bridge):
        Thread.__init__(self)
        self.bridge = bridge
        self.connection = psycopg2.connect(host="localhost", database="postgres", user="postgres", password="postgrespw")

    def run(self):
        while True:
            items = self.getItems()
            print(items)
            for item in items:
                self.bridge.send_price_request(item[0])
                time.sleep(randrange(5))
            time.sleep(randrange(5))
    def getItems(self):
        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM item WHERE selected=true;")
        data = cursor.fetchall()
        cursor.close()
        return data
