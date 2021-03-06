from __future__ import absolute_import, division, print_function, unicode_literals

import zmq
import collections
import threading
import logging
from datetime import datetime

import backtrader as bt
from backtrader.metabase import MetaParams
from backtrader.utils.py3 import queue, with_metaclass
from backtrader import date2num, num2date

from backtradermql5.adapter import OrderAdapter, PositionAdapter

logger = logging.getLogger("MT5Store")


class MTraderError(Exception):
    def __init__(self, *args, **kwargs):
        default = "Meta Trader 5 ERROR"
        if not (args or kwargs):
            args = default
        super(MTraderError, self).__init__(*args, **kwargs)


class ServerConfigError(MTraderError):
    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)


class ServerDataError(MTraderError):
    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)


class TimeFrameError(MTraderError):
    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)


class StreamError(MTraderError):
    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)


class IndicatorError(MTraderError):
    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)


class ChartError(MTraderError):
    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)


class MTraderAPI:
    """
    This class implements Python side for MQL5 JSON API
    See https://github.com/khramkov/MQL5-JSON-API for docs
    """

    # TODO: unify error handling

    def __init__(self, *args, **kwargs):

        self.HOST = kwargs["host"]
        self.SYS_PORT = 15555  # REP/REQ port
        self.DATA_PORT = 15556  # PUSH/PULL port
        self.LIVE_PORT = 15557  # PUSH/PULL port
        self.EVENTS_PORT = 15558  # PUSH/PULL port
        self.INDICATOR_DATA_PORT = 15559  # REP/REQ port
        self.CHART_DATA_PORT = 15560  # PUSH port

        # ZeroMQ timeout in seconds
        sys_timeout = 1
        data_timeout = kwargs["datatimeout"]

        # initialise ZMQ context
        context = zmq.Context()

        # connect to server sockets
        try:
            self.sys_socket = context.socket(zmq.REQ)
            # set port timeout
            self.sys_socket.RCVTIMEO = sys_timeout * 1000
            self.sys_socket.connect("tcp://{}:{}".format(
                self.HOST, self.SYS_PORT))

            self.data_socket = context.socket(zmq.PULL)
            # set port timeout
            self.data_socket.RCVTIMEO = data_timeout * 1000
            self.data_socket.connect("tcp://{}:{}".format(
                self.HOST, self.DATA_PORT))

            self.indicator_data_socket = context.socket(zmq.PULL)
            # set port timeout
            self.indicator_data_socket.RCVTIMEO = data_timeout * 1000
            self.indicator_data_socket.connect("tcp://{}:{}".format(
                self.HOST, self.INDICATOR_DATA_PORT))
            self.chart_data_socket = context.socket(zmq.PUSH)
            # set port timeout
            # TODO check if port is listening and error handling
            self.chart_data_socket.connect("tcp://{}:{}".format(
                self.HOST, self.CHART_DATA_PORT))

        except zmq.ZMQError:
            raise zmq.ZMQBindError("Binding ports ERROR")

    def _send_request(self, data: dict) -> None:
        """Send request to server via ZeroMQ System socket"""
        try:
            self.sys_socket.send_json(data)
            msg = self.sys_socket.recv_string()

            logger.debug("ZMQ SYS REQUEST: {} -> {}".format(data, msg))
            # terminal received the request
            assert msg == "OK", "Something wrong on server side"
        except AssertionError as err:
            raise zmq.NotDone(err)
        except zmq.ZMQError:
            raise zmq.NotDone("Sending request ERROR")

    def _pull_reply(self):
        """Get reply from server via Data socket with timeout"""
        try:
            msg = self.data_socket.recv_json()
        except zmq.ZMQError:
            raise zmq.NotDone("Data socket timeout ERROR")
        logger.debug("ZMQ DATA REPLY: {}".format(msg))
        return msg

    def _indicator_pull_reply(self):
        """Get reply from server via Data socket with timeout"""
        try:
            msg = self.indicator_data_socket.recv_json()
        except zmq.ZMQError:
            raise zmq.NotDone("Indicator Data socket timeout ERROR")
        logger.debug("ZMQ INDICATOR DATA REPLY: {}".format(msg))
        return msg

    def live_socket(self, context=None):
        """Connect to socket in a ZMQ context"""
        try:
            context = context or zmq.Context.instance()
            socket = context.socket(zmq.PULL)
            socket.connect("tcp://{}:{}".format(self.HOST, self.LIVE_PORT))
        except zmq.ZMQError:
            raise zmq.ZMQBindError("Live port connection ERROR")
        return socket

    def streaming_socket(self, context=None):
        """Connect to socket in a ZMQ context"""
        try:
            context = context or zmq.Context.instance()
            socket = context.socket(zmq.PULL)
            socket.connect("tcp://{}:{}".format(self.HOST, self.EVENTS_PORT))
        except zmq.ZMQError:
            raise zmq.ZMQBindError("Data port connection ERROR")
        return socket

    def _push_chart_data(self, data: dict) -> None:
        """Send message for chart control to server via ZeroMQ chart data socket"""

        try:
            logger.debug("ZMQ PUSH CHART DATA: {}".format(data))
            self.chart_data_socket.send_json(data)
        except zmq.ZMQError:
            raise zmq.NotDone("Sending request ERROR")

    def construct_and_send(self, **kwargs) -> dict:
        """Construct a request dictionary from default and send it to server"""

        # default dictionary
        request = {
            "action": None,
            "actionType": None,
            "symbol": None,
            "chartTF": None,
            "fromDate": None,
            "toDate": None,
            "id": None,
            "magic": None,
            "volume": None,
            "price": None,
            "stoploss": None,
            "takeprofit": None,
            "expiration": None,
            "deviation": None,
            "comment": None,
            "chartId": None,
            "indicatorChartId": None,
            "chartIndicatorSubWindow": None,
            # "style": None,
        }

        # update dict values if exist
        for key, value in kwargs.items():
            if key in request:
                request[key] = value
            else:
                raise KeyError("Unknown key in **kwargs ERROR")

        # send dict to server
        self._send_request(request)

        # return server reply
        return self._pull_reply()

    def indicator_construct_and_send(self, **kwargs) -> dict:
        """Construct a request dictionary from default and send it to server"""

        # default dictionary
        request = {
            "action": None,
            "actionType": None,
            "id": None,
            "symbol": None,
            "chartTF": None,
            "fromDate": None,
            "toDate": None,
            "name": None,
            "params": None,
            "linecount": None,
        }

        # update dict values if exist
        for key, value in kwargs.items():
            if key in request:
                request[key] = value
            else:
                raise KeyError("Unknown key in **kwargs ERROR")

        # send dict to server
        self._send_request(request)

        # return server reply
        return self._indicator_pull_reply()

    def chart_data_construct_and_send(self, **kwargs) -> dict:
        """Construct a request dictionary from default and send it to server"""

        # default dictionary
        message = {
            "action": None,
            "actionType": None,
            "chartId": None,
            "indicatorChartId": None,
            "indicatorBufferId": None,
            "style": None,
            "data": None,
        }

        # update dict values if exist
        for key, value in kwargs.items():
            if key in message:
                message[key] = value
            else:
                raise KeyError("Unknown key in **kwargs ERROR")

        # send dict to server
        self._push_chart_data(message)


class MetaSingleton(MetaParams):
    """Metaclass to make a metaclassed class a singleton"""
    def __init__(cls, name, bases, dct):
        super(MetaSingleton, cls).__init__(name, bases, dct)
        cls._singleton = None

    def __call__(cls, *args, **kwargs):
        if cls._singleton is None:
            cls._singleton = super(MetaSingleton,
                                   cls).__call__(*args, **kwargs)

        return cls._singleton


class MTraderStore(with_metaclass(MetaSingleton, object)):
    """
    Singleton class wrapping to control the connections to MetaTrader.

    Balance update occurs at the beginning and after each
    transaction registered by '_t_streaming_events'.
    """

    # TODO: implement stop_limit
    # TODO: Check position ticket

    BrokerCls = None  # broker class will autoregister
    DataCls = None  # data class will auto register

    params = (("host", "localhost"), ("datatimeout", 10))

    _DTEPOCH = datetime(1970, 1, 1)

    # MTrader supported granularities
    _GRANULARITIES = {
        (bt.TimeFrame.Ticks, 1): "TICK",
        (bt.TimeFrame.Minutes, 1): "M1",
        (bt.TimeFrame.Minutes, 5): "M5",
        (bt.TimeFrame.Minutes, 15): "M15",
        (bt.TimeFrame.Minutes, 30): "M30",
        (bt.TimeFrame.Minutes, 60): "H1",
        (bt.TimeFrame.Minutes, 120): "H2",
        (bt.TimeFrame.Minutes, 180): "H3",
        (bt.TimeFrame.Minutes, 240): "H4",
        (bt.TimeFrame.Minutes, 360): "H6",
        (bt.TimeFrame.Minutes, 480): "H8",
        (bt.TimeFrame.Minutes, 720): "H12",
        (bt.TimeFrame.Days, 1): "D1",
        (bt.TimeFrame.Weeks, 1): "W1",
        (bt.TimeFrame.Months, 1): "MN1",
    }

    # Order type matching with MetaTrader 5
    _ORDEREXECS = {
        (bt.Order.Market, "buy"): "ORDER_TYPE_BUY",
        (bt.Order.Market, "sell"): "ORDER_TYPE_SELL",
        (bt.Order.Limit, "buy"): "ORDER_TYPE_BUY_LIMIT",
        (bt.Order.Limit, "sell"): "ORDER_TYPE_SELL_LIMIT",
        (bt.Order.Stop, "buy"): "ORDER_TYPE_BUY_STOP",
        (bt.Order.Stop, "sell"): "ORDER_TYPE_SELL_STOP",
        # (bt.Order.StopLimit, 'buy'): 'ORDER_TYPE_BUY_STOP_LIMIT',
        # (bt.Order.StopLimit, 'sell'): 'ORDER_TYPE_SELL_STOP_LIMIT',
    }

    @classmethod
    def getdata(cls, *args, **kwargs):
        """Returns `DataCls` with args, kwargs"""
        return cls.DataCls(*args, **kwargs)

    @classmethod
    def getbroker(cls, *args, **kwargs):
        """Returns broker with *args, **kwargs from registered `BrokerCls`"""
        return cls.BrokerCls(*args, **kwargs)

    def __init__(self, *args, **kwargs):
        super(MTraderStore, self).__init__()

        self.notifs = collections.deque()  # store notifications for cerebro

        self._env = None  # reference to cerebro for general notifications
        self.broker = None  # broker instance
        self.datas = list()  # datas that have registered over start

        self._orders = collections.OrderedDict()  # map order.ref to oid
        self._ordersrev = collections.OrderedDict()  # map oid to order.ref
        self._orders_type = dict()  # keeps order types

        kwargs.update({
            "host": self.params.host,
            "datatimeout": self.params.datatimeout,
        })
        self.oapi = MTraderAPI(*args, **kwargs)

        self._cash = 0.0
        self._value = 0.0

        self.q_livedata = queue.Queue()

        self._cancel_flag = False

        # Clear any previous subscribed Symbols
        self.reset_server()

    def start(self, data=None, broker=None):
        # Datas require some processing to kickstart data reception
        if data is None and broker is None:
            self.cash = None
            return

        if data is not None:
            self._env = data._env
            # For datas simulate a queue with None to kickstart co
            self.datas.append(data)

            # if self.broker is not None:
            #     self.broker.data_started(data)

        elif broker is not None:
            self.broker = broker
            self.broker_threads()
            self.streaming_events()

    def live(self):
        self.broker.live()

    def rebuild_order(self, order, oid):
        side = "buy" if order.isbuy() else "sell"
        order_type = self._ORDEREXECS.get((order.exectype, side), None)
        if order_type is None:
            raise ValueError("Wrong order type: %s or side: %s" %
                             (order.exectype, side))
        oref = order.ref
        self._orders[oref] = oid
        # keeps orders types
        self._orders_type[oref] = order_type
        # maps ids to backtrader order
        self._ordersrev[oid] = oref

    def refresh(self):
        """
        Check if order is alive when server is disconnect and reconnect
        """
        # TODO Using order ref insteal of order id
        serverorders = dict((o.id, o) for o in self.o.get_orders())
        for order in self.broker.orders:
            if not order.alive():
                continue

            # TODO Using order ref insteal of order id
            oid = self._orders[order.ref]
            if not oid:
                continue

            sorder = serverorders.pop(oid, None)
            if sorder is None:
                logger.warn("Order ref(%d) not found on server", order.ref)
                continue

            state = sorder["state"]
            price = float(sorder["price"])
            size = float(sorder["volume"])
            if "SELL" in sorder["type"]:
                size = -size
            self._process_order(oref=order.ref,
                                state=state,
                                size=size,
                                price=price)

    def stop(self):
        # signal end of thread
        if self.broker is not None:
            self.q_ordercreate.put(None)
            self.q_orderclose.put(None)

    def put_notification(self, msg, *args, **kwargs):
        self.notifs.append((msg, args, kwargs))

    def get_notifications(self):
        """Return the pending "store" notifications"""
        self.notifs.append(None)  # put a mark / threads could still append
        return [x for x in iter(self.notifs.popleft, None)]

    def get_positions(self):
        response = self.oapi.construct_and_send(action="POSITIONS")
        # Error handling
        if response.get("error", False):
            raise ServerDataError(response)

        positions = response.get("positions", [])
        logger.debug("Open positions: {}".format(positions))
        return [PositionAdapter(o) for o in positions]

    def get_orders(self):
        response = self.oapi.construct_and_send(action="ORDERS")
        # Error handling
        if response.get("error", False):
            raise ServerDataError(response)

        orders = response.get("orders", [])
        logger.debug("Open orders: {}".format(orders))
        return [OrderAdapter(o) for o in orders]

    def get_order(self, ref=None, oid=None):
        if oid is None:
            if ref is None:
                raise Exception("Order ref or oid must provice")
            oid = self._orders[ref]
            if not oid:
                raise Exception("Order ref %d found" % ref)

        response = self.oapi.construct_and_send(action="ORDER", id=oid)
        # Error handling
        if response.get("error", False):
            raise ServerDataError(response)

        order = response.get("order", None)
        logger.debug("Get order: {}".format(order))
        return OrderAdapter(order)

    def get_granularity(self, frame, compression):
        granularity = self._GRANULARITIES.get((frame, compression), None)
        if granularity is None:
            raise ValueError(
                "Metatrader 5 doesn't support frame %s with \
                compression %s" %
                (bt.TimeFrame.getname(frame, compression), compression))
        return granularity

    def get_cash(self):
        return self._cash

    def get_value(self):
        return self._value

    def get_balance(self):
        try:
            bal = self.oapi.construct_and_send(action="BALANCE")
        except Exception as e:
            self.put_notification(e)
            return

        # if bal.get('error', False):
        #     self.put_notification(bal)
        #     raise ServerDataError(bal)

        try:
            self._cash = float(bal["balance"])
            self._value = float(bal["equity"])
        except KeyError:
            pass

    def streaming_events(self):
        t = threading.Thread(target=self._t_livedata, daemon=True)
        t.start()

        t = threading.Thread(target=self._t_streaming_events, daemon=True)
        t.start()

    def _t_livedata(self):
        # create socket connection for the Thread
        socket = self.oapi.live_socket()
        while True:
            try:
                last_data = socket.recv_json()
                logger.debug("ZMQ LIVE DATA: {}".format(last_data))
            except zmq.ZMQError:
                raise zmq.NotDone("Live data ERROR")

            self.q_livedata.put(last_data)

    def _t_streaming_events(self):
        # create socket connection for the Thread
        socket = self.oapi.streaming_socket()
        while True:
            try:
                transaction = socket.recv_json()
                logger.debug(
                    "ZMQ STREAMING TRANSACTION: {}".format(transaction))
            except zmq.ZMQError:
                raise zmq.NotDone("Streaming data ERROR")

            self._transaction(transaction)

    def broker_threads(self):
        self.q_ordercreate = queue.Queue()
        t = threading.Thread(target=self._t_order_create, daemon=True)
        t.start()

        self.q_orderclose = queue.Queue()
        t = threading.Thread(target=self._t_order_cancel, daemon=True)
        t.start()

    def order_create(self, order, stopside=None, takeside=None, **kwargs):
        """Creates an order"""
        okwargs = dict()
        okwargs["action"] = "TRADE"

        side = "buy" if order.isbuy() else "sell"
        order_type = self._ORDEREXECS.get((order.exectype, side), None)
        if order_type is None:
            raise ValueError("Wrong order type: %s or side: %s" %
                             (order.exectype, side))

        okwargs["actionType"] = order_type
        okwargs["symbol"] = order.data._dataname
        okwargs["volume"] = abs(order.created.size)

        if order.exectype != bt.Order.Market:
            okwargs["price"] = format(order.created.price)

        if order.valid is None:
            okwargs["expiration"] = 0  # good to cancel
        else:
            okwargs["expiration"] = order.valid  # good to date

        if order.exectype == bt.Order.StopLimit:
            okwargs["price"] = order.created.pricelimit

        # TODO: implement StopTrail
        # if order.exectype == bt.Order.StopTrail:
        #     okwargs['distance'] = order.trailamount

        comment = dict(ref=order.ref)

        if stopside is not None and stopside.price is not None:
            okwargs["stoploss"] = stopside.price
            comment["sl"] = stopside.ref

        if takeside is not None and takeside.price is not None:
            okwargs["takeprofit"] = takeside.price
            comment["tp"] = takeside.ref

        # OCO
        if order.oco is not None:
            comment["oco"] = order.oco.ref

        okwargs["comment"] = "|".join(
            ["%s=%s" % (k, v) for k, v in comment.items()])
        # set store backtrader order ref as MT5 order magic number
        # okwargs["magic"] = order.ref

        okwargs.update(**kwargs)  # anything from the user
        self.q_ordercreate.put((
            order.ref,
            okwargs,
        ))

        return order

    def _t_order_create(self):
        while True:
            msg = self.q_ordercreate.get()
            if msg is None:
                break

            oref, okwargs = msg

            try:
                o = self.oapi.construct_and_send(**okwargs)
            except Exception as e:
                self.put_notification(e)
                self.broker._reject(oref)
                continue

            logger.debug(o)

            if o["error"]:
                self.put_notification(o["desription"])
                self.broker._reject(oref)
                continue
            else:
                oid = o["order"]

            self._orders[oref] = oid

            # keeps orders types
            self._orders_type[oref] = okwargs["actionType"]
            # maps ids to backtrader order
            self._ordersrev[oid] = oref

    def order_cancel(self, order):
        self.q_orderclose.put(order.ref)
        return order

    def _t_order_cancel(self):
        while True:
            oref = self.q_orderclose.get()
            if oref is None:
                break

            oid = self._orders.get(oref, None)
            if oid is None:
                logger.debug("Cannot cancel order ref: {}".format(oref))
                continue  # the order is no longer there

            # get symbol name
            order = self.broker.orders[oref]
            symbol = order.data._dataname
            # get order type
            order_type = self._orders_type.get(oref, None)

            try:
                if order_type in ["ORDER_TYPE_BUY", "ORDER_TYPE_SELL"]:
                    self.close_position(oid, symbol)
                else:
                    self.cancel_order(oid, symbol)
            except Exception as e:
                self.put_notification(
                    "Order not cancelled: ref={}, orderid={}, error={}".format(
                        oref, oid, e))
                continue

            self._cancel_flag = True

    def price_data(self,
                   dataname,
                   dtbegin,
                   dtend,
                   timeframe,
                   compression,
                   include_first=False):
        tf = self.get_granularity(timeframe, compression)

        begin = end = None
        if dtbegin:
            begin = int((dtbegin - self._DTEPOCH).total_seconds())
        if dtend:
            end = int((dtbegin - self._DTEPOCH).total_seconds())

        logger.debug("Fetching: {}, Timeframe: {}, Fromdate: {}".format(
            dataname, tf, dtbegin))

        data = self.oapi.construct_and_send(
            action="HISTORY",
            actionType="DATA",
            symbol=dataname,
            chartTF=tf,
            fromDate=begin,
            toDate=end,
        )
        price_data = data["data"]
        # Remove last unclosed candle
        # TODO Is this relevant for ticks?
        if not include_first and tf != "TICK":
            try:
                del price_data[-1]
            except:
                pass

        q = queue.Queue()
        for c in price_data:
            q.put(c)

        q.put({})
        return q

        # TODO live updates
        # self.streaming_events()

        # while True:
        #   try:
        #     msg = self.q_livedata.get()
        #   except queue.Empty:
        #     return None
        #   if msg['type'] == "FLUSH":
        #     logger.debug(msg['data'], end="\r", flush=True)
        #   else:
        #     logger.debug(msg['data'])

        #   if msg['status']=='DISCONNECTED':
        #     return

    def config_server(self, symbol: str, timeframe: str,
                      compression: int) -> None:
        """Set server terminal symbol and time frame"""
        ret_val = self.oapi.construct_and_send(
            action="CONFIG",
            symbol=symbol,
            chartTF=self.get_granularity(timeframe, compression),
        )

        if ret_val["error"]:
            logger.error(ret_val)
            raise ServerConfigError(ret_val["description"])
            self.put_notification(ret_val["description"])

    def check_account(self) -> None:
        """Get MetaTrader 5 account settings"""
        conf = self.oapi.construct_and_send(action="ACCOUNT")

        # Error handling
        if conf["error"]:
            logger.error(conf)
            raise ServerDataError(conf)

        logger.info(conf)

    def close_position(self, oid, symbol):
        logger.debug("Closing position: {}, on symbol: {}".format(oid, symbol))

        conf = self.oapi.construct_and_send(action="TRADE",
                                            actionType="POSITION_CLOSE_ID",
                                            symbol=symbol,
                                            id=oid)
        # Error handling
        if conf["error"]:
            logger.error(conf)
            raise ServerDataError(conf)
        logger.debug(conf)

    def cancel_order(self, oid, symbol):
        logger.debug("Cancelling order: %d, on symbol: %s", (oid, symbol))

        conf = self.oapi.construct_and_send(action="TRADE",
                                            actionType="ORDER_CANCEL",
                                            symbol=symbol,
                                            id=oid)
        # Error handling
        if conf["error"]:
            logger.error(conf)
            raise ServerDataError(conf)
        logger.debug(conf)

    def _transaction(self, transaction):
        oid = transaction["order"]
        if oid in self._orders.values():
            # when an order id exists process transaction
            self._process_transaction(oid, transaction)
            return

        oid = transaction["position"]
        if oid in self._orders.values():
            if transaction["order_state"] not in ["ORDER_STATE_FILLED"]:
                return
            # when an order id exists process transaction
            self._process_transaction(oid, transaction)
            return

        # external order created this transaction
        if self._cancel_flag and transaction[
                "type"] == "TRADE_TRANSACTION_ORDER_ADD":
            self._cancel_flag = False

            size = float(transaction["volume"])
            price = float(transaction["price"])
            if "SELL" in transaction["order_type"]:
                size = -size
            for data in self.datas:
                if data._name == transaction["symbol"]:
                    self.broker._fill_external(data, size, price)
                    break

    def _process_transaction(self, oid, transaction):
        try:
            # get a reference to a backtrader order based on the order id / trade id
            oref = self._ordersrev[oid]
        except KeyError:
            return

        if "order_state" in transaction:
            state = transaction["order_state"]
            price = float(transaction["price"])
            size = float(transaction["volume"])
            if "SELL" in transaction["order_type"]:
                size = -size

            self._process_order(oref=oref, state=state, size=size, price=price)

    def _process_order(self, oref, state, size=0, price=0):
        if state == "ORDER_STATE_STARTED":
            self.broker._submit(oref)
            return
        if state == "ORDER_STATE_PLACED":
            self.broker._accept(oref)
            return
        if state == "ORDER_STATE_CANCELED":
            self.broker._cancel(oref)
            return
        if state == "ORDER_STATE_PARTIAL" or state == "ORDER_STATE_FILLED":
            self.broker._fill(oref,
                              size,
                              price,
                              filled=state == "ORDER_STATE_FILLED")
            return
        if state == "ORDER_STATE_REJECTED":
            self.broker._reject(oref)
            return
        if state == "ORDER_STATE_EXPIRED":
            self.broker._expire(oref)
            return

    def config_chart(self, chartId, dataname, timeframe, compression):
        """Opens a chart window in MT5"""

        tf = self.get_granularity(timeframe, compression)
        # Creating a chart with Ticks is not supported
        if tf == "TICK":
            raise ValueError(
                "Metatrader 5 Charts don't support frame %s with \
                compression %s" %
                (bt.TimeFrame.getname(timeframe, compression), compression))

        ret_val = self.oapi.construct_and_send(
            action="CHART",
            actionType="OPEN",
            chartId=chartId,
            symbol=dataname,
            chartTF=tf,
        )

        if ret_val["error"]:
            logger.error(ret_val)
            raise ChartError(ret_val["description"])
            self.put_notification(ret_val["description"])

        return ret_val

    def chart_add_indicator(
            self,
            chartId,
            indicatorChartId,
            chartIndicatorSubWindow,  # style
    ):
        """Attaches the JsonAPIIndicator to the specified chart window"""

        ret_val = self.oapi.construct_and_send(
            action="CHART",
            actionType="ADDINDICATOR",
            chartId=chartId,
            indicatorChartId=indicatorChartId,
            chartIndicatorSubWindow=chartIndicatorSubWindow,
            # style=style,
        )

        if ret_val["error"]:
            logger.error(ret_val)
            raise ChartError(ret_val["description"])
            self.put_notification(ret_val["description"])

    def push_chart_data(self, chartId, indicatorChartId, indicatorBufferId,
                        data):
        """Pushes backtrader indicator values to be distributed to be drawn by JsonAPIIndicator instances"""

        self.oapi.chart_data_construct_and_send(
            action="PLOT",
            actionType="DATA",
            chartId=chartId,
            indicatorChartId=indicatorChartId,
            indicatorBufferId=indicatorBufferId,
            data=data,
        )

    def chart_indicator_add_buffer(self, chartId, indicatorChartId, style):
        """Add buffer to be drawn by JsonAPIIndicator instances"""

        self.oapi.chart_data_construct_and_send(
            action="PLOT",
            actionType="ADDBUFFER",
            chartId=chartId,
            indicatorChartId=indicatorChartId,
            style=style,
        )

    def chart_add_graphic(self, chartId, indicatorChartId,
                          chartIndicatorSubWindow, style):
        """Add graphical objects to a chart window"""

        ret_val = self.oapi.construct_and_send(
            action="CHART",
            actionType="ADDINDICATOR",
            chartId=chartId,
            indicatorChartId=indicatorChartId,
            chartIndicatorSubWindow=chartIndicatorSubWindow,
            style=style,
        )

        if ret_val["error"]:
            logger.error(ret_val)
            raise ChartError(ret_val["description"])
            self.put_notification(ret_val["description"])

    def config_indicator(self, symbol, timeframe, compression, name, id,
                         params, linecount):
        """Instantiates an indicator in MT5"""

        tf = self.get_granularity(timeframe, compression)
        if tf == "TICK":
            raise ValueError(
                "Metatrader 5 Indicators don't support frame %s with \
                compression %s" %
                (bt.TimeFrame.getname(timeframe, compression), compression))

        ret_val = self.oapi.indicator_construct_and_send(
            action="INDICATOR",
            actionType="ATTACH",
            symbol=symbol,
            name=name,
            linecount=linecount,
            id=id,
            params=params,
            chartTF=tf,
        )

        if ret_val["error"]:
            logger.error(ret_val)
            raise IndicatorError(ret_val["description"])
            self.put_notification(ret_val["description"])

        return ret_val

    def indicator_data(
        self,
        indicatorId,
        fromDate,
    ):
        """Recieves values from a MT5 indicator instance"""

        logger.debug(
            "Req. indicator data with Timestamp: {}, Indicator Id: {}".format(
                datetime.utcfromtimestamp(float(fromDate)), id))

        ret_val = self.oapi.indicator_construct_and_send(
            action="INDICATOR",
            actionType="REQUEST",
            id=indicatorId,
            fromDate=fromDate,
        )

        if ret_val["error"]:
            logger.error(ret_val)
            raise IndicatorError(ret_val["description"])
            self.put_notification(ret_val["description"])
            if ret_val["lastError"] == "4806":
                self.put_notification(
                    "You have probably requested too many lines (MT5 indicator buffers)."
                )

        return ret_val

    def reset_server(self) -> None:
        """Removes all symbol subscritions and clears all indicators"""

        ret_val = self.oapi.construct_and_send(action="RESET")

        if ret_val["error"]:
            logger.error(ret_val)
            raise ServerConfigError(ret_val["description"])
            self.put_notification(ret_val["description"])

    def write_csv(
        self,
        symbol: str,
        timeframe: str,
        compression: int = 1,
        fromdate: datetime = None,
        todate: datetime = None,
    ) -> None:
        """Request MT5 to write history data to CSV a file"""

        if fromdate is None:
            fromdate = float("-inf")
        else:
            fromdate = date2num(fromdate)

        if todate is None:
            todate = float("inf")
        else:
            todate = date2num(todate)

        date_begin = num2date(fromdate) if fromdate > float("-inf") else None
        date_end = num2date(todate) if todate < float("inf") else None

        begin = end = None
        if date_begin:
            begin = int((date_begin - self._DTEPOCH).total_seconds())
        if date_end:
            end = int((date_end - self._DTEPOCH).total_seconds())

        tf = self.get_granularity(timeframe, compression)

        logger.debug(
            "Request CSV write with Fetching: {}, Timeframe: {}, Fromdate: {}".
            format(symbol, tf, date_begin))

        ret_val = self.oapi.construct_and_send(
            action="HISTORY",
            actionType="WRITE",
            symbol=symbol,
            chartTF=tf,
            fromDate=begin,
            toDate=end,
        )

        if ret_val["error"]:
            logger.error(ret_val)
            raise ServerConfigError(ret_val["description"])
            self.put_notification(ret_val["description"])
        else:
            self.put_notification(
                f"Request to write CVS data for symbol {tf} and timeframe {tf} succeeded. Check MT5 EA logging for the exact output location ..."
            )
