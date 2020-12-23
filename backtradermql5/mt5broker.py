from __future__ import absolute_import, division, print_function, unicode_literals

import collections
import itertools
import logging

from backtrader import BrokerBase, Order, BuyOrder, SellOrder
from backtrader.utils.py3 import with_metaclass
from backtrader.comminfo import CommInfoBase
from backtrader.position import Position

from backtradermql5 import mt5store

logger = logging.getLogger("MT5Broker")


class MTraderCommInfo(CommInfoBase):
    def getvaluesize(self, size, price):
        # In real life the margin approaches the price
        return abs(size) * price

    def getoperationcost(self, size, price):
        """Returns the needed amount of cash an operation would cost"""
        # Same reasoning as above
        return abs(size) * price


class MetaMTraderBroker(BrokerBase.__class__):
    def __init__(cls, name, bases, dct):
        """Class has already been created ... register"""
        # Initialize the class
        super(MetaMTraderBroker, cls).__init__(name, bases, dct)
        mt5store.MTraderStore.BrokerCls = cls


class MTraderBroker(with_metaclass(MetaMTraderBroker, BrokerBase)):
    """Broker implementation for MetaTrader 5.

    This class maps the orders/positions from MetaTrader to the
    internal API of `backtrader`.

    Params:

      - `rebuild` (default:`True`): When connecting to the broker
        provider use the existing orders, positions to kickstart the broker.

        Set to `False` during instantiation to disregard any existing
        position
    """

    # TODO: close positions

    params = (("rebuild", True), )

    def __init__(self, **kwargs):
        super(MTraderBroker, self).__init__()
        self.o = mt5store.MTraderStore(**kwargs)

        self.orders = collections.OrderedDict()  # orders by order id
        self.notifs = collections.deque()  # holds orders which are notified

        self.opending = collections.defaultdict(list)  # pending transmission
        self.brackets = dict()  # confirmed brackets

        self._ocos = dict()
        self._ocol = collections.defaultdict(list)

        self.startingcash = self.cash = 0.0
        self.startingvalue = self.value = 0.0
        self.positions = collections.defaultdict(Position)
        self.addcommissioninfo(self, MTraderCommInfo(mult=1.0,
                                                     stocklike=False))

    def start(self):
        super(MTraderBroker, self).start()
        self.addcommissioninfo(self, MTraderCommInfo(mult=1.0,
                                                     stocklike=False))
        self.o.start(broker=self)
        # Check MetaTrader account
        self.o.check_account()
        # Get balance on start
        self.o.get_balance()
        self.startingcash = self.cash = self.o.get_cash()
        self.startingvalue = self.value = self.o.get_value()

    def rebuild_environement(self):
        """
        Rebuild positions and orders when restart strategy
        """
        self.rebuild_positions()
        self.rebuild_orders()

    def rebuild_positions(self):
        datas = dict((d._name, d) for d in self.o.datas)

        for p in self.o.get_positions():
            print('position for instrument:', p)
            size = float(p.volume)
            is_buy = p.type.endswith("_BUY")
            if not is_buy:
                size = -size
            price = float(p.open)
            if not self.positions[p.symbol]:
                self.positions[p.symbol] = Position(size, price)
            else:
                self.positions[p.symbol].update(size, price)

            data = datas.get(p.symbol, None)
            if data is None:
                logger.warn("data %s not found", p.symbol)
                continue

            # Info
            info = p.comment
            if "ref" in info:
                Order.refbasis = itertools.count(info["ref"])

            # Parent order
            PRClass = BuyOrder if is_buy else SellOrder
            order = PRClass(
                data=data,
                size=float(p.volume),
                price=float(p.open),
                exectype=Order.Market,
                transmit=p.stoploss <= 0 and p.takeprofit <= 0,
                simulated=True,
            )
            pref = order.ref
            self.orders[order.ref] = order
            self.brackets[pref] = [order]
            self.o.rebuild_order(order, p.id)

            # Stoploss
            if p.stoploss > 0:
                if "sl" in info:
                    Order.refbasis = itertools.count(info["sl"])

                # opposite with position size
                SLClass = SellOrder if is_buy else BuyOrder

                slorder = SLClass(
                    data=data,
                    size=float(p.volume),
                    price=float(p.stoploss),
                    exectype=Order.Stop,
                    parent=order,
                    transmit=p.takeprofit <= 0,
                    simulated=True,
                )
                self.orders[slorder.ref] = slorder
                self.brackets[pref].append(slorder)

            # Takeprofit
            if p.takeprofit > 0:
                if "tp" in info:
                    Order.refbasis = itertools.count(info["tp"])

                # opposite with position size
                TPClass = SellOrder if is_buy else BuyOrder

                tporder = TPClass(
                    data=data,
                    size=float(p.volume),
                    price=float(p.takeprofit),
                    exectype=Order.Limit,
                    parent=order,
                    transmit=True,
                    simulated=True,
                )
                self.orders[tporder.ref] = tporder
                self.brackets[pref].append(tporder)

            # Submit order will auto submit bracket orders
            self._submit(pref)

            # complete parent order
            order.addcomminfo(self.getcommissioninfo(data))
            order.execute(0, order.size, order.price, 0, 0.0, 0.0, order.size,
                          0.0, 0.0, 0.0, 0.0, order.size, order.price)
            order.completed()
            self.notify(order)
            self._bracketize(order)
            self._ococheck(order)

    def rebuild_orders(self):
        datas = dict((d._name, d) for d in self.o.datas)

        for o in self.o.get_orders():
            if o.state not in [
                    "ORDER_STATE_STARTED",
                    "ORDER_STATE_PLACED",
                    "ORDER_STATE_PARTIAL",
            ]:
                continue

            print('order for instrument:', o)
            data = datas.get(o.symbol, None)
            if not data:
                logger.warn("data %s not found", o.symbol)
                continue

            is_buy = "BUY" in o.type

            # Info
            info = o.comment
            if "ref" in info:
                Order.refbasis = itertools.count(info["ref"])

            # Parent order
            PRClass = BuyOrder if is_buy else SellOrder
            exectype = Order.Market
            if o.type.endswith("_LIMIT"):
                exectype = Order.Limit
            elif o.type.endswith("_STOP"):
                exectype = Order.Stop
            elif o.type.endswith("STOP_LIMIT"):
                exectype = Order.StopLimit

            order = PRClass(
                data=data,
                size=float(o.volume),
                price=float(o.open),
                exectype=exectype,
                transmit=o.stoploss <= 0 and o.takeprofit <= 0,
                simulated=True,
            )
            pref = order.ref
            self.orders[order.ref] = order
            self.brackets[pref] = [order]
            self.o.rebuild_order(order, o.id)

            # Stoploss
            if o.stoploss > 0:
                if "sl" in info:
                    Order.refbasis = itertools.count(info["sl"])
                # opposite with order size
                SLClass = SellOrder if is_buy else BuyOrder

                slorder = SLClass(
                    data=data,
                    size=float(o.volume),
                    price=float(o.stoploss),
                    exectype=Order.Stop,
                    parent=order,
                    transmit=o.takeprofit <= 0,
                    simulated=True,
                )
                self.orders[slorder.ref] = slorder
                self.brackets[pref].append(slorder)

            # Takeprofit
            if o.takeprofit > 0:
                if "tp" in info:
                    Order.refbasis = itertools.count(info["tp"])
                # opposite with order size
                TPClass = SellOrder if is_buy else BuyOrder

                tporder = TPClass(
                    data=data,
                    size=float(o.volume),
                    price=float(o.takeprofit),
                    exectype=Order.Limit,
                    parent=order,
                    transmit=True,
                    simulated=True,
                )
                self.orders[tporder.ref] = tporder
                self.brackets[pref].append(tporder)

            # OCO
            if "oco" in info:
                ocoref = info["oco"]
                oco = self.orders.get(ocoref, None)
                order.oco = oco

                self._ocoize(order)
                if oco is None:  # Canceled
                    self.cancel(order)
                elif not oco.alive():
                    self._ococheck(oco)

            # Submit order will auto submit bracket orders
            self._submit(pref)

    def live(self):
        # First time live data
        if self.p.rebuild:
            self.rebuild_environement()
            self.p.rebuild = False

    def stop(self):
        super(MTraderBroker, self).stop()
        self.o.stop()

    def getcash(self):
        # This call cannot block if no answer is available from MTrader
        self.cash = cash = self.o.get_cash()
        return cash

    def getvalue(self, datas=None):
        self.value = self.o.get_value()
        return self.value

    def getposition(self, data, clone=True):
        # return self.o.getposition(data._dataname, clone=clone)
        pos = self.positions[data._dataname]
        if clone:
            pos = pos.clone()

        return pos

    def orderstatus(self, order):
        o = self.orders[order.ref]
        return o.status

    def _submit(self, oref):
        order = self.orders[oref]
        if order.status == Order.Submitted:
            return

        order.submit(self)
        self.notify(order)
        # submit for stop order and limit order of bracket
        bracket = self.brackets.get(oref, [])
        for o in bracket:
            if o.ref != oref:
                self._submit(o.ref)

    def _reject(self, oref):
        order = self.orders[oref]
        order.reject(self)
        self.notify(order)
        self._bracketize(order, cancel=True)
        self._ococheck(order)

    def _accept(self, oref):
        order = self.orders[oref]
        if order.status == Order.Accepted:
            return

        order.accept()
        self.notify(order)
        # accept for stop order and limit order of bracket
        bracket = self.brackets.get(oref, [])
        for o in bracket:
            if o.ref != oref:
                self._accept(o.ref)

    def _cancel(self, oref):
        order = self.orders[oref]
        if order.status == Order.Canceled:
            return

        order.cancel()
        self.notify(order)
        self._bracketize(order, cancel=True)
        self._ococheck(order)

    def _expire(self, oref):
        order = self.orders[oref]
        order.expire()
        self.notify(order)
        self._bracketize(order, cancel=True)
        self._ococheck(order)

    def _bracketize(self, order, cancel=False):
        pref = getattr(order.parent, "ref", order.ref)  # parent ref or self
        br = self.brackets.pop(pref, None)  # to avoid recursion
        if br is None:
            return

        if not cancel:
            if len(br) == 3:  # all 3 orders in place, parent was filled
                br = br[1:]  # discard index 0, parent
                for o in br:
                    o.activate()  # simulate activate for children
                self.brackets[pref] = br  # not done - reinsert children

            elif len(br) == 2:  # filling a children
                oidx = br.index(order)  # find index to filled (0 or 1)
                self._cancel(br[1 - oidx].ref)  # cancel remaining (1 - 0 -> 1)
        else:
            # Any cancellation cancel the others
            for o in br:
                if o.alive():
                    self._cancel(o.ref)

    def _ococheck(self, order):
        if order.alive():
            raise Exception("Should not be called here")
        ocoref = self._ocos.pop(order.ref, order.ref)  # a parent or self
        ocol = self._ocol.pop(ocoref, None)
        if ocol:
            # cancel all order in oco group
            for oref in ocol:
                o = self.orders.get(oref, None)
                if o is not None and o.ref != order.ref:
                    self.cancel(o)

    def _ocoize(self, order):
        if order.oco is None:
            return

        ocoref = order.oco.ref
        oref = order.ref
        if ocoref not in self._ocos:
            self._ocos[oref] = ocoref
            self._ocol[ocoref].append(ocoref)  # add to group
        self._ocol[ocoref].append(oref)  # add to group

    def _fill_external(self, data, size, price):
        logger.debug("Fill external order: {}, {}, {}".format(
            data, size, price))
        if size == 0:
            return

        pos = self.getposition(data, clone=False)
        pos.update(size, price)

        if size < 0:
            order = SellOrder(data=data,
                              size=size,
                              price=price,
                              exectype=Order.Market,
                              simulated=True)
        else:
            order = BuyOrder(data=data,
                             size=size,
                             price=price,
                             exectype=Order.Market,
                             simulated=True)

        order.addcomminfo(self.getcommissioninfo(data))
        order.execute(0, size, price, 0, 0.0, 0.0, size, 0.0, 0.0, 0.0, 0.0,
                      size, price)
        order.completed()

        self.notify(order)
        self._ococheck(order)
        self.o.get_balance()

    def _fill(self, oref, size, price, filled=False, **kwargs):
        if size == 0 and not filled:
            return
        logger.debug("Fill order: {}, {}, {}".format(oref, size, price,
                                                     filled))

        order = self.orders[oref]
        if not order.alive():  # can be a bracket
            pref = getattr(order.parent, "ref", order.ref)
            if pref not in self.brackets:
                msg = ("Order fill received for {}, with price {} and size {} "
                       "but order is no longer alive and is not a bracket. "
                       "Unknown situation").format(order.ref, price, size)
                self.o.put_notification(msg)
                return

            # [main, stopside, takeside], neg idx to array are -3, -2, -1
            stop_order = self.brackets[pref][-2]
            limit_order = self.brackets[pref][-1]

            # order type BUY, then stop and limit type SELL
            if order.ordtype == Order.Buy:
                if price >= limit_order.price:  # Limit order trigger when bid price over limit price
                    order = limit_order
                else:
                    order = stop_order
            # order type SELL, then stop and limit type BUY
            else:
                if price <= limit_order.price:  # Limit order trigger when ask price under limit price
                    order = limit_order
                else:
                    order = stop_order

        if filled:
            size = order.size

        data = order.data
        pos = self.getposition(data, clone=False)
        psize, pprice, opened, closed = pos.update(size, price)
        # comminfo = self.getcommissioninfo(data)

        closedvalue = closedcomm = 0.0
        openedvalue = openedcomm = 0.0
        margin = pnl = 0.0

        order.addcomminfo(self.getcommissioninfo(data))
        order.execute(data.datetime[0], size, price, closed, closedvalue,
                      closedcomm, opened, openedvalue, openedcomm, margin, pnl,
                      psize, pprice)

        if order.executed.remsize:
            order.partial()
            self.notify(order)
        else:
            order.completed()
            self.notify(order)
            self._bracketize(order)
            self.o.get_balance()

        self._ococheck(order)

    def _transmit(self, order):
        oref = order.ref
        pref = getattr(order.parent, "ref", oref)  # parent ref or self

        if order.transmit:
            if oref != pref:  # children order
                # Put parent in orders dict, but add stopside and takeside
                # to order creation. Return the takeside order, to have 3s
                takeside = order  # alias for clarity
                parent, stopside = self.opending.pop(pref)
                for o in parent, stopside, takeside:
                    self.orders[o.ref] = o  # write them down

                self.brackets[pref] = [parent, stopside, takeside]
                self.o.order_create(parent, stopside, takeside)
                return takeside  # parent was already returned

            else:  # Parent order, which is not being transmitted
                self.orders[order.ref] = order
                return self.o.order_create(order)

        # Not transmitting
        self.opending[pref].append(order)
        return order

    def buy(self,
            owner,
            data,
            size,
            price=None,
            plimit=None,
            exectype=None,
            valid=None,
            tradeid=0,
            oco=None,
            trailamount=None,
            trailpercent=None,
            parent=None,
            transmit=True,
            **kwargs):

        order = BuyOrder(
            owner=owner,
            data=data,
            size=size,
            price=price,
            pricelimit=plimit,
            exectype=exectype,
            valid=valid,
            tradeid=tradeid,
            oco=oco,
            trailamount=trailamount,
            trailpercent=trailpercent,
            parent=parent,
            transmit=transmit,
        )

        order.addinfo(**kwargs)
        # order.addcomminfo(self.getcommissioninfo(data))
        self._ocoize(order)
        return self._transmit(order)

    def sell(self,
             owner,
             data,
             size,
             price=None,
             plimit=None,
             exectype=None,
             valid=None,
             tradeid=0,
             oco=None,
             trailamount=None,
             trailpercent=None,
             parent=None,
             transmit=True,
             **kwargs):

        order = SellOrder(
            owner=owner,
            data=data,
            size=size,
            price=price,
            pricelimit=plimit,
            exectype=exectype,
            valid=valid,
            tradeid=tradeid,
            oco=oco,
            trailamount=trailamount,
            trailpercent=trailpercent,
            parent=parent,
            transmit=transmit,
        )

        order.addinfo(**kwargs)
        # order.addcomminfo(self.getcommissioninfo(data))
        self._ocoize(order)
        return self._transmit(order)

    def cancel(self, order):
        if not self.orders.get(order.ref, False):
            return
        if order.status == Order.Cancelled:  # already cancelled
            return

        return self.o.order_cancel(order)

    def notify(self, order):
        self.notifs.append(order.clone())

    def get_notification(self):
        if not self.notifs:
            return None

        return self.notifs.popleft()

    def next(self):
        self.notifs.append(None)  # mark notification boundary
