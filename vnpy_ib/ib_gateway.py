"""
IB Symbol Rules

SPY-USD-STK   SMART
EUR-USD-CASH  IDEALPRO
XAUUSD-USD-CMDTY  SMART
ES-202002-USD-FUT  GLOBEX
SI-202006-1000-USD-FUT  NYMEX
ES-2020006-C-2430-50-USD-FOP  GLOBEX

ConId is also supported for symbol.
"""


from copy import copy
from datetime import datetime, timedelta
from threading import Thread, Condition
from typing import Optional
from decimal import Decimal
import shelve
from tzlocal import get_localzone_name
from queue import Empty, Queue

from vnpy.event import EventEngine
from ibapi.client import EClient
from ibapi.common import OrderId, TickAttrib, TickerId
from ibapi.contract import Contract, ContractDetails
from ibapi.execution import Execution
from ibapi.order import Order
from ibapi.order_state import OrderState
from ibapi.ticktype import TickType, TickTypeEnum
from ibapi.wrapper import EWrapper
from ibapi.common import BarData as IbBarData

from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    TickData,
    OrderData,
    TradeData,
    PositionData,
    AccountData,
    ContractData,
    BarData,
    OrderRequest,
    CancelRequest,
    SubscribeRequest,
    HistoryRequest
)
from vnpy.trader.constant import (
    Product,
    OrderType,
    Direction,
    Exchange,
    Currency,
    Status,
    OptionType,
    Interval
)
from vnpy.trader.utility import get_file_path, ZoneInfo, round_to
from vnpy.trader.event import EVENT_TIMER
from vnpy.event import Event

# 委托状态映射
STATUS_IB2VT: dict[str, Status] = {
    "ApiPending": Status.SUBMITTING,
    "PendingSubmit": Status.SUBMITTING,
    "PreSubmitted": Status.NOTTRADED,
    "Submitted": Status.NOTTRADED,
    "ApiCancelled": Status.CANCELLED,
    "Cancelled": Status.CANCELLED,
    "Filled": Status.ALLTRADED,
    "Inactive": Status.REJECTED,
}

# 多空方向映射
DIRECTION_VT2IB: dict[Direction, str] = {Direction.LONG: "BUY", Direction.SHORT: "SELL"}
DIRECTION_IB2VT: dict[str, Direction] = {v: k for k, v in DIRECTION_VT2IB.items()}
DIRECTION_IB2VT["BOT"] = Direction.LONG
DIRECTION_IB2VT["SLD"] = Direction.SHORT

# 委托类型映射
ORDERTYPE_VT2IB: dict[OrderType, str] = {
    OrderType.LIMIT: "LMT",
    OrderType.MARKET: "MKT",
    OrderType.STOP: "STP"
}
ORDERTYPE_IB2VT: dict[str, OrderType] = {v: k for k, v in ORDERTYPE_VT2IB.items()}

# 交易所映射
EXCHANGE_VT2IB: dict[Exchange, str] = {
    Exchange.SMART: "SMART",
    Exchange.NYMEX: "NYMEX",
    Exchange.COMEX: "COMEX",
    Exchange.GLOBEX: "GLOBEX",
    Exchange.IDEALPRO: "IDEALPRO",
    Exchange.CME: "CME",
    Exchange.CBOT: "CBOT",
    Exchange.CBOE: "CBOE",
    Exchange.ICE: "ICE",
    Exchange.SEHK: "SEHK",
    Exchange.SSE: "SEHKNTL",
    Exchange.SZSE: "SEHKSZSE",
    Exchange.HKFE: "HKFE",
    Exchange.CFE: "CFE",
    Exchange.TSE: "TSE",
    Exchange.NYSE: "NYSE",
    Exchange.NASDAQ: "NASDAQ",
    Exchange.AMEX: "AMEX",
    Exchange.ARCA: "ARCA",
    Exchange.EDGEA: "EDGEA",
    Exchange.ISLAND: "ISLAND",
    Exchange.BATS: "BATS",
    Exchange.IEX: "IEX",
    Exchange.IBKRATS: "IBKRATS",
    Exchange.OTC: "PINK",
    Exchange.SGX: "SGX",
    Exchange.EUREX: "EUREX",
    Exchange.SNFE: "SNFE",
    Exchange.LMEOTC: "LMEOTC",
    Exchange.NYBOT: "NYBOT",
    Exchange.IPE: "IPE",
    Exchange.TWSE: "TWSE",
}
EXCHANGE_IB2VT: dict[str, Exchange] = {v: k for k, v in EXCHANGE_VT2IB.items()}

# 产品类型映射
PRODUCT_IB2VT: dict[str, Product] = {
    "STK": Product.EQUITY,
    "CASH": Product.FOREX,
    "CMDTY": Product.SPOT,
    "FUT": Product.FUTURES,
    "OPT": Product.OPTION,
    "FOP": Product.OPTION,
    "CONTFUT": Product.FUTURES,
    "IND": Product.INDEX,
    "CFD": Product.CFD
}

# 期权类型映射
OPTION_IB2VT: dict[str, OptionType] = {
    "C": OptionType.CALL,
    "CALL": OptionType.CALL,
    "P": OptionType.PUT,
    "PUT": OptionType.PUT
}

# 货币类型映射
CURRENCY_VT2IB: dict[Currency, str] = {
    Currency.USD: "USD",
    Currency.CAD: "CAD",
    Currency.CNY: "CNY",
    Currency.HKD: "HKD",
}

# 切片数据字段映射
TICKFIELD_IB2VT: dict[int, str] = {
    0: "bid_volume_1",
    1: "bid_price_1",
    2: "ask_price_1",
    3: "ask_volume_1",
    4: "last_price",
    5: "last_volume",
    6: "high_price",
    7: "low_price",
    8: "volume",
    9: "pre_close",
    10: "bid",
    11: "ask",
    12: "last",
    13: "model",
    14: "open_price",
    86: "open_interest"
}

# 账户类型映射
ACCOUNTFIELD_IB2VT: dict[str, str] = {
    "NetLiquidationByCurrency": "balance",
    "NetLiquidation": "balance",
    "UnrealizedPnL": "positionProfit",
    "AvailableFunds": "available",
    "MaintMarginReq": "margin",
}

# 数据频率映射
INTERVAL_VT2IB: dict[Interval, str] = {
    Interval.MINUTE: "1 min",
    Interval.HOUR: "1 hour",
    Interval.DAILY: "1 day",
}

# 其他常量
LOCAL_TZ = ZoneInfo(get_localzone_name())
JOIN_SYMBOL: str = "-"


class IbGateway(BaseGateway):
    """
    VeighNa用于对接IB的交易接口。
    """

    default_name: str = "IB"

    default_setting: dict = {
        "TWS地址": "localhost",
        "TWS端口": 7497,
        "客户号": 1,
        "交易账户": ""
    }

    exchanges: list[str] = list(EXCHANGE_VT2IB.keys())

    def __init__(self, event_engine: EventEngine, gateway_name: str) -> None:
        """构造函数"""
        super().__init__(event_engine, gateway_name)

        self.api: "IbApi" = IbApi(self)
        self.count: int = 0

    def connect(self, setting: dict) -> None:
        """连接交易接口"""
        host: str = setting["TWS地址"]
        port: int = setting["TWS端口"]
        clientid: int = setting["客户号"]
        account: str = setting["交易账户"]

        self.api.connect(host, port, clientid, account)

        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

    def close(self) -> None:
        """关闭接口"""
        self.api.close()

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        self.api.subscribe(req)

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        return self.api.send_order(req)

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        self.api.cancel_order(req)

    def query_account(self) -> None:
        """查询资金"""
        pass

    def query_position(self) -> None:
        """查询持仓"""
        pass

    def query_history(self, req: HistoryRequest) -> list[BarData]:
        """查询历史数据"""
        return self.api.query_history(req)

    def process_timer_event(self, event: Event) -> None:
        """定时事件处理"""
        self.count += 1
        if self.count < 10:
            return
        self.count = 0

        self.api.check_connection()


class IbApi(EWrapper):
    """IB的API接口"""

    data_filename: str = "ib_contract_data.db"
    data_filepath: str = str(get_file_path(data_filename))

    def __init__(self, gateway: IbGateway) -> None:
        """构造函数"""
        super().__init__()

        self.gateway: IbGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.status: bool = False

        self.reqid: int = 0
        self.orderid: int = 0
        self.clientid: int = 0
        self.history_reqid: int = 0
        self.account: str = ""

        self.ticks: dict[int, TickData] = {}
        self.orders: dict[str, OrderData] = {}
        self.accounts: dict[str, AccountData] = {}
        self.contracts: dict[str, ContractData] = {}
        # self.tradinghours: Dict[str, str] = {}
        self.contracts_details: dict[str, ContractDetails] = {}
        self.contract_dict = {}

        self.subscribed: dict[str, SubscribeRequest] = {}
        self.data_ready: bool = False
        self.order_ready: bool = False
        self.subscribeRequest_queue = Queue()

        self.history_req: HistoryRequest = None
        self.history_condition: Condition = Condition()
        self.history_buf: list[BarData] = []

        self.reqid_symbol_map: dict[int, str] = {}

        self.client: EClient = EClient(self)

        self.ib_contracts: dict[str, Contract] = {}

    def connectAck(self) -> None:
        """连接成功回报"""
        self.status = True
        self.gateway.write_log("IB TWS连接成功")

        # 由于加载合约信息后，会发送on_contract事件，该事件会促使类似datarecorder订阅行情，但现在刚连接上，并一定是马上订阅行情的好时机
        self.load_contract_data()

        """  
        Code:2104
        TWS message：Market data farm connection is OK。 
        Additional notes：notification that connection to the market data server is ok. This is a notification and not a true error condition, and is expected on first establishing connection.
        
        Important: The IBApi.EWrapper.nextValidID callback is commonly used to indicate that the connection is completed and other messages can be sent from the API client to TWS. 
        There is the possibility that function calls made prior to this time could be dropped by TWS
        """
        self.data_ready = False
        self.order_ready = False

    def connectionClosed(self) -> None:
        """连接断开回报"""
        self.status = False
        self.gateway.write_log("IB TWS连接断开")

    def nextValidId(self, orderId: int) -> None:
        """下一个有效订单号回报"""
        super().nextValidId(orderId)

        self.client.reqCurrentTime()

        if not self.orderid:
            self.orderid = orderId

        if not self.order_ready:
            self.order_ready = True

        if not self.data_ready:
            self.data_ready = True

        # 断线后重连，需要把订阅过的合约重新订阅
        reqs: list = list(self.subscribed.values())
        self.subscribed.clear()
        for req in reqs:
            self.subscribe(req)

        # 启动负责订阅行情的线程，只在初始化成功后启动，该线程断线后会退出(self.status = False)
        self.subscribeRequest_thread = Thread(target=self.subscribeRunner)
        self.subscribeRequest_thread.start()

    def currentTime(self, time: int) -> None:
        """IB当前服务器时间回报"""
        super().currentTime(time)

        dt: datetime = datetime.fromtimestamp(time)
        time_string: str = dt.strftime("%Y-%m-%d %H:%M:%S.%f")

        msg: str = f"服务器时间: {time_string}"
        self.gateway.write_log(msg)

    def error(self, reqId: TickerId, errorCode: int, errorString: str, advancedOrderRejectJson: str = "") -> None:
        """具体错误请求回报"""
        super().error(reqId, errorCode, errorString)

        # 2000-2999信息通知不属于报错信息
        if reqId == self.history_reqid and errorCode not in range(2000, 3000):
            self.history_condition.acquire()
            self.history_condition.notify()
            self.history_condition.release()

        msg: str = f"信息通知，代码：{errorCode}，内容: {errorString}"
        self.gateway.write_log(msg)

        '''
        1100:Connectivity between IB and the TWS has been lost.Your TWS/IB Gateway has been disconnected from IB servers. This can occur because of an internet connectivity issue, a nightly reset of the IB servers, or a competing session.
        1101:Connectivity between IB and TWS has been restored- data lost.*.The TWS/IB Gateway has successfully reconnected to IB's servers. Your market data requests have been lost and need to be re-submitted.
        1102:Connectivity between IB and TWS has been restored- data maintained.The TWS/IB Gateway has successfully reconnected to IB's servers. Your market data requests have been recovered and there is no need for you to re-submit them.        
        '''
        # TWS与IB服务器已经断线
        if errorCode == 1100:
            self.order_ready = False
            self.data_ready = False

        # TWS与IB服务器已经重连，需要重新订阅行情
        if errorCode == 1101:
            self.order_ready = True
            self.data_ready = True

            reqs: list = list(self.subscribed.values())
            self.subscribed.clear()
            for req in reqs:
                self.subscribe(req)

        # TWS与IB服务器已经重连，不需要做任何事情
        if errorCode == 1102:
            self.order_ready = True
            self.data_ready = True

        '''
        # 行情服务器已连接
        if errorCode == 2104 and not self.data_ready:
            self.data_ready = True

            self.client.reqCurrentTime()

            reqs: list = list(self.subscribed.values())
            self.subscribed.clear()
            for req in reqs:
                self.subscribe(req)
        '''

    def tickPrice(self, reqId: TickerId, tickType: TickType, price: float, attrib: TickAttrib) -> None:
        """tick价格更新回报"""
        super().tickPrice(reqId, tickType, price, attrib)

        if tickType not in TICKFIELD_IB2VT:
            # # 打印tick的所有信息
            # print(self.ticks[reqId].__str__())
            # # 打印传入的所有信息
            # print(f'reqId: {reqId}, tickType: {tickType}, price: {price}, attrib: {attrib}')
            return

        tick: TickData = self.ticks.get(reqId, None)
        if not tick:
            self.gateway.write_log(f"tickPrice函数收到未订阅的推送，reqId：{reqId}")
            return

        name: str = TICKFIELD_IB2VT[tickType]
        setattr(tick, name, price)

        # 更新tick数据name字段
        contract: ContractData = self.contracts.get(tick.vt_symbol, None)
        if contract:
            tick.name = contract.name

        # 本地计算Forex of IDEALPRO和Spot Commodity的tick时间和最新价格
        if tick.exchange == Exchange.IDEALPRO or "CMDTY" in tick.symbol:
            if not tick.bid_price_1 or not tick.ask_price_1:
                return
            price_tick: float = 0.00001
            if contract and contract.pricetick:
                price_tick = contract.pricetick
            tick.last_price = round_to((tick.bid_price_1 + tick.ask_price_1) / 2, price_tick)
            # 处理计算出来的last price的数字位数，因为公式计算出来的last price位数太长

        # datetime是quote update time，不是last trade time，所以每次行情变化，都修改这个time
        tick.datetime = datetime.now(LOCAL_TZ)
        """
        Market data tick price callback. Handles all price related ticks. Every tickPrice callback is followed by a tickSize. 
        A tickPrice value of -1 or 0 followed by a tickSize of 0 indicates there is no data for this field currently available, whereas a tickPrice with a positive tickSize indicates an active quote of 0 (typically for a combo contract).
        """
        # IB API描述中，有tickPrice变化，一定会紧跟一个tickSize变化，所以，tickSize没更新之前，没必要提交on_tick，否则反而会给出错误的tickSize
        # self.gateway.on_tick(copy(tick))

    def tickSize(self, reqId: TickerId, tickType: TickType, size: Decimal) -> None:

        """tick数量更新回报"""
        super().tickSize(reqId, tickType, size)

        if tickType not in TICKFIELD_IB2VT:
            # # 打印tick的所有信息
            # print(self.ticks[reqId].__str__())
            # # 打印传入的所有信息
            # print(f'reqId: {reqId}, tickType: {tickType}, size: {size}')
            return

        tick: TickData = self.ticks.get(reqId, None)
        if not tick:
            self.gateway.write_log(f"tickSize函数收到未订阅的推送，reqId：{reqId}")
            return

        name: str = TICKFIELD_IB2VT[tickType]
        setattr(tick, name, float(size))

        # datetime是quote update time，不是last trade time，所以每次行情变化，都修改这个time
        tick.datetime = datetime.now(LOCAL_TZ)

        self.gateway.on_tick(copy(tick))

    def tickString(self, reqId: TickerId, tickType: TickType, value: str) -> None:
        """tick字符串更新回报"""
        # 因为这里这里只是更新 last trade time，已经没有必要了，我们把datetime改成quote update time了
        return

        """tick字符串更新回报"""
        super().tickString(reqId, tickType, value)

        if tickType != TickTypeEnum.LAST_TIMESTAMP:
            # print(self.ticks[reqId].__str__())
            # print(f'reqId: {reqId}, tickType: {tickType}, value: {value}')
            return

        tick: TickData = self.ticks.get(reqId, None)
        if not tick:
            self.gateway.write_log(f"tickString函数收到未订阅的推送，reqId：{reqId}")
            return

        dt: datetime = datetime.fromtimestamp(int(value))
        tick.datetime = dt.replace(tzinfo=LOCAL_TZ)

        self.gateway.on_tick(copy(tick))

    def tickOptionComputation(
        self,
        reqId: TickerId,
        tickType: TickType,
        tickAttrib: int,
        impliedVol: float,
        delta: float,
        optPrice: float,
        pvDividend: float,
        gamma: float,
        vega: float,
        theta: float,
        undPrice: float
    ):
        """tick期权数据推送"""
        super().tickOptionComputation(
            reqId,
            tickType,
            tickAttrib,
            impliedVol,
            delta,
            optPrice,
            pvDividend,
            gamma,
            vega,
            theta,
            undPrice,
        )

        tick: TickData = self.ticks.get(reqId, None)
        if not tick:
            self.gateway.write_log(f"tickOptionComputation函数收到未订阅的推送，reqId：{reqId}")
            return

        prefix: str = TICKFIELD_IB2VT[tickType]

        tick.extra["underlying_price"] = undPrice

        if optPrice:
            tick.extra[f"{prefix}_price"] = optPrice
            tick.extra[f"{prefix}_impv"] = impliedVol
            tick.extra[f"{prefix}_delta"] = delta
            tick.extra[f"{prefix}_gamma"] = gamma
            tick.extra[f"{prefix}_theta"] = theta
            tick.extra[f"{prefix}_vega"] = vega
        else:
            tick.extra[f"{prefix}_price"] = 0
            tick.extra[f"{prefix}_impv"] = 0
            tick.extra[f"{prefix}_delta"] = 0
            tick.extra[f"{prefix}_gamma"] = 0
            tick.extra[f"{prefix}_theta"] = 0
            tick.extra[f"{prefix}_vega"] = 0

    def tickSnapshotEnd(self, reqId: int) -> None:
        """行情切片查询返回完毕"""
        super().tickSnapshotEnd(reqId)

        tick: TickData = self.ticks.get(reqId, None)
        if not tick:
            self.gateway.write_log(f"tickSnapshotEnd函数收到未订阅的推送，reqId：{reqId}")
            return

        self.gateway.write_log(f"{tick.vt_symbol}行情切片查询成功")

    def orderStatus(
        self,
        orderId: OrderId,
        status: str,
        filled: Decimal,
        remaining: Decimal,
        avgFillPrice: float,
        permId: int,
        parentId: int,
        lastFillPrice: float,
        clientId: int,
        whyHeld: str,
        mktCapPrice: float,
    ) -> None:
        """订单状态更新回报"""
        super().orderStatus(
            orderId,
            status,
            filled,
            remaining,
            avgFillPrice,
            permId,
            parentId,
            lastFillPrice,
            clientId,
            whyHeld,
            mktCapPrice,
        )

        orderid: str = str(orderId)
        order: OrderData = self.orders.get(orderid, None)
        if not order:
            return

        order.traded = float(filled) # convert Decimal to float

        # 过滤撤单中止状态
        order_status: Status = STATUS_IB2VT.get(status, None)
        if order_status:
            order.status = order_status

        self.gateway.on_order(copy(order))

        self.gateway.write_log(f"[orderStatus] {order}")

    def openOrder(
        self,
        orderId: OrderId,
        ib_contract: Contract,
        ib_order: Order,
        orderState: OrderState,
    ) -> None:
        """新订单回报"""
        super().openOrder(orderId, ib_contract, ib_order, orderState)

        orderid: str = str(orderId)

        if ib_order.orderRef:
            dt: datetime = datetime.strptime(ib_order.orderRef, "%Y-%m-%d %H:%M:%S")
        else:
            dt: datetime = datetime.now()

        order: OrderData = OrderData(
            symbol=self.generate_symbol(ib_contract),
            exchange=EXCHANGE_IB2VT.get(ib_contract.exchange, Exchange.SMART),
            type=ORDERTYPE_IB2VT[ib_order.orderType],
            orderid=orderid,
            direction=DIRECTION_IB2VT[ib_order.action],
            volume=ib_order.totalQuantity,
            datetime=dt,
            gateway_name=self.gateway_name,
        )

        if order.type == OrderType.LIMIT:
            order.price = ib_order.lmtPrice
        elif order.type == OrderType.STOP:
            order.price = ib_order.auxPrice

        self.orders[orderid] = order
        # 没必要发送此事件，因为每次OnOrderStatus都会前，都会发送一次OnOpenOrder，而且OnOpenOrder的order中，status一直都是submitting，回干扰策略的逻辑
        #self.gateway.on_order(copy(order))

    def updateAccountValue(self, key: str, val: str, currency: str, accountName: str) -> None:
        """账号更新回报"""
        super().updateAccountValue(key, val, currency, accountName)

        if not currency or key not in ACCOUNTFIELD_IB2VT:
            return

        accountid: str = f"{accountName}.{currency}"
        account: AccountData = self.accounts.get(accountid, None)
        if not account:
            account = AccountData(
                accountid=accountid,
                gateway_name=self.gateway_name
            )
            self.accounts[accountid] = account

        name: str = ACCOUNTFIELD_IB2VT[key]
        setattr(account, name, float(val))

    def updatePortfolio(
        self,
        contract: Contract,
        position: Decimal,
        marketPrice: float,
        marketValue: float,
        averageCost: float,
        unrealizedPNL: float,
        realizedPNL: float,
        accountName: str,
    ) -> None:
        """持仓更新回报"""
        super().updatePortfolio(
            contract,
            position,
            marketPrice,
            marketValue,
            averageCost,
            unrealizedPNL,
            realizedPNL,
            accountName,
        )

        if contract.exchange:
            exchange: Exchange = EXCHANGE_IB2VT.get(contract.exchange, None)
        elif contract.primaryExchange:
            exchange: Exchange = EXCHANGE_IB2VT.get(contract.primaryExchange, None)
        else:
            exchange: Exchange = Exchange.SMART   # Use smart routing for default

        if not exchange:
            msg: str = f"存在不支持的交易所持仓：{self.generate_symbol(contract)} {contract.exchange} {contract.primaryExchange}"
            self.gateway.write_log(msg)
            return

        try:
            ib_size: int = int(contract.multiplier)
        except ValueError:
            ib_size = 1
        price = averageCost / ib_size

        pos: PositionData = PositionData(
            symbol=self.generate_symbol(contract),
            exchange=exchange,
            direction=Direction.NET,
            volume=float(position), # convert Decimal to float
            price=price,
            pnl=unrealizedPNL,
            gateway_name=self.gateway_name,
        )
        self.gateway.on_position(pos)

    def updateAccountTime(self, timeStamp: str) -> None:
        """账号更新时间回报"""
        super().updateAccountTime(timeStamp)
        for account in self.accounts.values():
            self.gateway.on_account(copy(account))

    def contractDetails(self, reqId: int, contractDetails: ContractDetails) -> None:
        """合约数据更新回报"""
        super().contractDetails(reqId, contractDetails)
        # print(contractDetails.__str__())
        # if contractDetails.contract.secType=="CONTFUT":
        #     print(contractDetails.__str__())

        # 提取合约信息
        ib_contract: Contract = contractDetails.contract

        # 处理合约乘数为0的情况
        if not ib_contract.multiplier:
            ib_contract.multiplier = 1

        # 字符串风格的代码，需要从缓存中获取
        if reqId in self.reqid_symbol_map:# 不允许使用数字代码 lance
            symbol: str = self.reqid_symbol_map[reqId]# 不允许使用数字代码 lance
        # 否则默认使用数字风格代码
        else:# 不允许使用数字代码 lance
            # symbol: str = str(ib_contract.conId)# 不允许使用数字代码 lance
            symbol: str = self.generate_symbol(ib_contract)# 不允许使用数字代码 lance

        # 生成合约
        contract: ContractData = ContractData(
            symbol=symbol,
            exchange=EXCHANGE_IB2VT[ib_contract.exchange],
            name=contractDetails.longName,
            product=PRODUCT_IB2VT[ib_contract.secType],
            size=int(ib_contract.multiplier),
            pricetick=contractDetails.minTick,
            min_volume=contractDetails.minSize,
            # min_volume=1,
            net_position=True,
            history_data=True,
            stop_supported=True,
            gateway_name=self.gateway_name,
        )

        # 龙胜自己额外增加 trading hours, time zone， local symbol
        # contract.tradingHours = contractDetails.tradingHours
        # contract.liquidHours = contractDetails.liquidHours
        # contract.timeZoneId = contractDetails.timeZoneId

        if contract.product == Product.OPTION:
            underlying_symbol: str = str(contractDetails.underConId)

            contract.option_portfolio = underlying_symbol + "_O"
            contract.option_type = OPTION_IB2VT.get(ib_contract.right, None)
            contract.option_strike = ib_contract.strike
            contract.option_index = str(ib_contract.strike)
            contract.option_expiry = datetime.strptime(ib_contract.lastTradeDateOrContractMonth, "%Y%m%d")
            contract.option_underlying = underlying_symbol + "_" + ib_contract.lastTradeDateOrContractMonth

        if contract.vt_symbol not in self.contracts:
            self.gateway.on_contract(contract)

            self.contracts[contract.vt_symbol] = contract
            self.ib_contracts[contract.vt_symbol] = ib_contract

        ContractMonth = contractDetails.contract.lastTradeDateOrContractMonth[:6]

        change_flag = False
        if ContractMonth not in contract.vt_symbol:# 有ContractMonth的（FUT、OPT）且contract不是exec的
            vt_symbol_exec = self.generate_symbol(ib_contract, detail=True)
            self.contracts_details[vt_symbol_exec] = contractDetails
            change_flag = True
            # if str(contractDetails.contract.conId) not in contract.vt_symbol:# 不保存数字代码 lance
            if contract.vt_symbol not in self.contract_dict:
                self.contract_dict[contract.vt_symbol] = [vt_symbol_exec]
            elif vt_symbol_exec not in self.contract_dict[contract.vt_symbol]:
                self.contract_dict[contract.vt_symbol].append(vt_symbol_exec)
        else:# 进入这个分支：没有ContractMonth的（STK）、有ContractMonth但contract已经是exec的
            self.contracts_details[contract.vt_symbol] = contractDetails
            change_flag = True

        if change_flag:
            self.save_contract_data()

    def execDetails(self, reqId: int, contract: Contract, execution: Execution) -> None:
        """交易数据更新回报"""
        super().execDetails(reqId, contract, execution)

        time_str: str = execution.time
        time_split: list = time_str.split(" ")
        words_count: int = 3

        if len(time_split) == words_count:
            timezone = time_split[-1]
            time_str = time_str.replace(f" {timezone}", "")
            tz = ZoneInfo(timezone)
        elif len(time_split) == (words_count - 1):
            tz = LOCAL_TZ
        else:
            self.gateway.write_log(f"收到不支持的时间格式：{time_str}")
            return

        dt: datetime = datetime.strptime(time_str, "%Y%m%d %H:%M:%S")
        dt: datetime = dt.replace(tzinfo=tz)

        if tz != LOCAL_TZ:
            dt = dt.astimezone(LOCAL_TZ)

        trade: TradeData = TradeData(
            symbol=self.generate_symbol(contract),
            exchange=EXCHANGE_IB2VT.get(contract.exchange, Exchange.SMART),
            orderid=str(execution.orderId),
            tradeid=str(execution.execId),
            direction=DIRECTION_IB2VT[execution.side],
            price=execution.price,
            volume=float(execution.shares),
            datetime=dt,
            gateway_name=self.gateway_name,
        )

        self.gateway.on_trade(trade)

    def managedAccounts(self, accountsList: str) -> None:
        """所有子账户回报"""
        super().managedAccounts(accountsList)

        if not self.account:
            for account_code in accountsList.split(","):
                if account_code:
                    self.account = account_code

        self.gateway.write_log(f"当前使用的交易账号为{self.account}")
        self.client.reqAccountUpdates(True, self.account)

    def historicalData(self, reqId: int, ib_bar: IbBarData) -> None:
        """历史数据更新回报"""
        # 日级别数据和周级别日期数据的数据形式为%Y%m%d
        time_str: str = ib_bar.date
        time_split: list = time_str.split(" ")
        words_count: int = 3

        if ":" not in time_str:
            words_count -= 1

        if len(time_split) == words_count:
            timezone = time_split[-1]
            time_str = time_str.replace(f" {timezone}", "")
            tz = ZoneInfo(timezone)
        elif len(time_split) == (words_count - 1):
            tz = LOCAL_TZ
        else:
            self.gateway.write_log(f"收到不支持的时间格式：{time_str}")
            return

        if ":" in time_str:
            dt: datetime = datetime.strptime(time_str, "%Y%m%d %H:%M:%S")
        else:
            dt: datetime = datetime.strptime(time_str, "%Y%m%d")
        dt: datetime = dt.replace(tzinfo=tz)

        if tz != LOCAL_TZ:
            dt: datetime = dt.astimezone(LOCAL_TZ)

        bar: BarData = BarData(
            symbol=self.history_req.symbol,
            exchange=self.history_req.exchange,
            datetime=dt,
            interval=self.history_req.interval,
            volume=float(ib_bar.volume),
            open_price=ib_bar.open,
            high_price=ib_bar.high,
            low_price=ib_bar.low,
            close_price=ib_bar.close,
            gateway_name=self.gateway_name
        )
        if bar.volume < 0:
            bar.volume = 0

        self.history_buf.append(bar)

    def historicalDataEnd(self, reqId: int, start: str, end: str) -> None:
        """历史数据查询完毕回报"""
        self.history_condition.acquire()
        self.history_condition.notify()
        self.history_condition.release()

    def connect(self, host: str, port: int, clientid: int, account: str, query_options: bool) -> None:
    def connect(
        self,
        host: str,
        port: int,
        clientid: int,
        account: str
    ) -> None:
        """连接TWS"""
        if self.status:
            return

        self.host = host
        self.port = port
        self.clientid = clientid
        self.account = account

        self.client.connect(host, port, clientid)
        self.thread = Thread(target=self.client.run)
        self.thread.start()

    def check_connection(self) -> None:
        """检查连接"""
        if self.client.isConnected():
            return

        if self.status:
            self.close()

        self.client.connect(self.host, self.port, self.clientid)

        self.thread = Thread(target=self.client.run)
        self.thread.start()

    def close(self) -> None:
        """断开TWS连接"""
        if not self.status:
            return

        self.status = False
        self.client.disconnect()

    def query_option_portfolio(self, underlying: Contract) -> None:
        """查询期权链合约数据"""
        if not self.status:
            return

        # 解析IB期权合约
        ib_contract: Contract = Contract()
        ib_contract.symbol = underlying.symbol
        ib_contract.exchange = underlying.exchange
        ib_contract.currency = underlying.currency

        if underlying.secType == "FUT":
            ib_contract.secType = "FOP"
        else:
            ib_contract.secType = "OPT"

        # 通过TWS查询合约信息
        self.reqid += 1
        self.client.reqContractDetails(self.reqid, ib_contract)

    def subscribe(self, req: SubscribeRequest) -> None:
        """把待请阅的合约放入队列，后面由专门负责订阅行情的线程来处理订阅"""
        self.subscribeRequest_queue.put(req)

    '''
    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅tick数据更新"""
        if not self.status:
            return

        if req.exchange not in EXCHANGE_VT2IB:
            self.gateway.write_log(f"不支持的交易所{req.exchange}")
            return

        if " " in req.symbol:
            self.gateway.write_log("订阅失败，合约代码中包含空格")
            return

        # 过滤重复订阅
        if req.vt_symbol in self.subscribed:
            return
        self.subscribed[req.vt_symbol] = req

        # 解析IB合约详情
        ib_contract: Contract = generate_ib_contract(req.symbol, req.exchange)
        if not ib_contract:
            self.gateway.write_log("代码解析失败，请检查格式是否正确")
            return

        # #TODO 这边暂时强制修改sectype
        # ib_contract.secType = "FUT+CONTFUT" # CONTFUT：连续合约；FUT：期货合约

        # 通过TWS查询合约信息
        self.reqid += 1
        self.client.reqContractDetails(self.reqid, ib_contract)

        # 如果使用了字符串风格的代码，则需要缓存
        if "-" in req.symbol:
            self.reqid_symbol_map[self.reqid] = req.symbol

        #  订阅tick数据并创建tick对象缓冲区
        self.reqid += 1
        # self.client.reqMktData(self.reqid, ib_contract, "", False, False, [])# lance
        self.client.reqMktData(self.reqid, ib_contract, "101", False, False, [])# lance
        # self.client.reqMktData(self.reqid, ib_contract, "101", True, False, [])# lance 取主力合约无需保持订阅

        tick: TickData = TickData(
            symbol=req.symbol,
            exchange=req.exchange,
            datetime=datetime.now(LOCAL_TZ),
            gateway_name=self.gateway_name
        )
        tick.extra = {}

        self.ticks[self.reqid] = tick
    '''

    def subscribeRunner(self) -> None:
        while self.status:
            try:
                # self.gateway.write_log(f"订阅行情队列{self.subscribeRequest_queue.qsize()}")

                if not self.status:
                    continue

                if not self.data_ready:
                    continue

                req: SubscribeRequest = self.subscribeRequest_queue.get(block=True, timeout=1)
                if req.exchange not in EXCHANGE_VT2IB:
                    self.gateway.write_log(f"订阅行情{req.symbol}失败，不支持的交易所{req.exchange}")
                    continue

                if " " in req.symbol and "-" not in req.symbol:
                    self.gateway.write_log("订阅失败，合约代码中包含空格")
                    return

                # 过滤重复订阅
                if req.vt_symbol in self.subscribed:
                    continue
                self.subscribed[req.vt_symbol] = req

                # 解析IB合约详情
                ib_contract: Contract = generate_ib_contract(req.symbol, req.exchange)
                if not ib_contract:
                    self.gateway.write_log(f"订阅行情{req.symbol}失败。代码解析失败，请检查格式是否正确")
                    continue

                # 通过TWS查询合约信息
                self.reqid += 1
                self.client.reqContractDetails(self.reqid, ib_contract)

                # 如果使用了字符串风格的代码，则需要缓存
                if "-" in req.symbol:
                    self.reqid_symbol_map[self.reqid] = req.symbol

                #  订阅tick数据并创建tick对象缓冲区
                self.reqid += 1
                self.gateway.write_log(f"api订阅前reqid：{self.reqid},symbol:{req.symbol}")
                # self.client.reqMktData(self.reqid, ib_contract, "", False, False, [])# lance
                self.client.reqMktData(self.reqid, ib_contract, "101", False, False, [])# lance
                # self.client.reqMktData(self.reqid, ib_contract, "101", True, False, [])# lance 取主力合约无需保持订阅

                tick: TickData = TickData(
                    symbol=req.symbol,
                    exchange=req.exchange,
                    datetime=datetime.now(LOCAL_TZ),
                    gateway_name=self.gateway_name
                )
                tick.extra = {}
                self.ticks[self.reqid] = tick
                self.gateway.write_log(f"api订阅后reqid：{self.reqid},symbol:{req.symbol}")

            except Empty:
                pass

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        if not self.status:
            return ""
        if not self.order_ready:
            self.gateway.write_log(f"API还没有完全初始化完毕,还没有收到nextValidID,暂不能下单。symbol:{req.vt_symbol},direction:{req.direction},price:{req.price},volume:{req.volume}")
            return ""

        if req.exchange not in EXCHANGE_VT2IB:
            self.gateway.write_log(f"不支持的交易所：{req.exchange}")
            return ""

        if req.type not in ORDERTYPE_VT2IB:
            self.gateway.write_log(f"不支持的价格类型：{req.type}")
            return ""

        if " " in req.symbol and "-" not in req.symbol:
            self.gateway.write_log("委托失败，合约代码中包含空格")
            return ""

        self.orderid += 1

        ib_contract: Contract = generate_ib_contract(req.symbol, req.exchange)
        if not ib_contract:
            return ""

        ib_order: Order = Order()
        ib_order.orderId = self.orderid
        ib_order.clientId = self.clientid
        ib_order.action = DIRECTION_VT2IB[req.direction]
        ib_order.orderType = ORDERTYPE_VT2IB[req.type]
        ib_order.totalQuantity = Decimal(req.volume)
        ib_order.account = self.account
        ib_order.orderRef = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 非常规交易时间
        ib_order.outsideRth = True

        if req.type == OrderType.LIMIT:
            ib_order.lmtPrice = req.price
        elif req.type == OrderType.STOP:
            ib_order.auxPrice = req.price

        self.client.placeOrder(self.orderid, ib_contract, ib_order)
        self.client.reqIds(1)

        order: OrderData = req.create_order_data(str(self.orderid), self.gateway_name)
        order.datetime = datetime.now(LOCAL_TZ)

        self.gateway.on_order(order)
        return order.vt_orderid

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        if not self.status:
            return
        if not self.order_ready:
            self.gateway.write_log(f"API还没有完全初始化完毕,还没有收到nextValidID,暂不能撤单。oderid:{req.orderid},symbol:{req.vt_symbol},direction:{req.direction},price:{req.price},volume:{req.volume}")
            return ""

        # IB API 10.9.1在撤单时，新增1个参数，撤单时间
        manualCancelOrderTime:str = datetime.now(LOCAL_TZ).strftime("%Y%m%d-%H:%M:%S")
        self.client.cancelOrder(int(req.orderid), manualCancelOrderTime)

    def query_history(self, req: HistoryRequest) -> list[BarData]:
        """查询历史数据"""
        contract: ContractData = self.contracts[req.vt_symbol]
        if not contract:
            self.gateway.write_log(f"找不到合约：{req.vt_symbol}，请先订阅")
            return []

        self.history_req = req

        self.reqid += 1

        ib_contract: Contract = generate_ib_contract(req.symbol, req.exchange)

        if req.end:
            end: datetime = req.end
        else:
            end: datetime = datetime.now(LOCAL_TZ)

        end_str: str = end.strftime("%Y%m%d %H:%M:%S") + " " + get_localzone_name()

        delta: timedelta = end - req.start
        # days: int = min(delta.days, 180)     # IB 只提供6个月数据
        # duration: str = f"{days} D"
        days: int = delta.days
        if days < 365:
            duration: str = f"{days} D"
        else:
            duration: str = f"{delta.days/365:.0f} Y"

        bar_size: str = INTERVAL_VT2IB[req.interval]

        if contract.product in [Product.SPOT, Product.FOREX]:
            bar_type: str = "MIDPOINT"
        else:
            bar_type: str = "TRADES"

        self.history_reqid = self.reqid
        self.client.reqHistoricalData(
            self.reqid,
            ib_contract,
            end_str,
            duration,
            bar_size,
            bar_type,
            0,
            1,
            False,
            []
        )

        self.history_condition.acquire()    # 等待异步数据返回
        self.history_condition.wait(60)
        self.history_condition.release()

        history: list[BarData] = self.history_buf
        self.history_buf: list[BarData] = []       # 创新新的缓冲列表
        self.history_req: HistoryRequest = None

        return history

    def load_contract_data(self) -> None:
        """加载本地合约数据"""
        f = shelve.open(self.data_filepath)
        self.contracts = f.get("contracts", {})
        self.contracts_details = f.get("contracts_details", {})
        f.close()

        for contract in self.contracts.values():
            self.gateway.on_contract(contract)

        self.gateway.write_log("本地缓存合约信息加载成功")

    def save_contract_data(self) -> None:
        """保存合约数据至本地"""
        # 保存前确保所有合约数据接口名称为IB，避免其他模块的处理影响
        contracts: dict[str, ContractData] = {}
        for vt_symbol, contract in self.contracts.items():
            c: ContractData = copy(contract)
            c.gateway_name = "IB"
            contracts[vt_symbol] = c

        f = shelve.open(self.data_filepath)
        f["contracts"] = contracts
        f["contracts_details"] = self.contracts_details
        f.close()

    def generate_symbol(self, ib_contract: Contract, detail: bool=False) -> str:
        """生成合约代码"""
        # 生成字符串风格代码
        fields: list = [ib_contract.symbol]

        if ib_contract.secType in ["FUT", "OPT", "FOP", "CONTFUT"]:
            ContractMonth = ib_contract.lastTradeDateOrContractMonth[:6]
            fields.append(ContractMonth)

        if ib_contract.secType in ["OPT", "FOP"]:
            fields.append(ib_contract.right)
            fields.append(str(ib_contract.strike))
            fields.append(str(ib_contract.multiplier))

        fields.append(ib_contract.currency)
        fields.append(ib_contract.secType)

        symbol: str = JOIN_SYMBOL.join(fields)
        exchange: Exchange = EXCHANGE_IB2VT.get(ib_contract.exchange, Exchange.SMART)
        vt_symbol: str = f"{symbol}.{exchange.value}"

        # 在合约信息中找不到字符串风格代码，则使用数字代码
        # if vt_symbol not in self.contracts:# 不允许使用数字代码 lance
        #     symbol = str(ib_contract.conId)# 不允许使用数字代码 lance

        if detail:
            return vt_symbol

        return symbol

    def query_tick(self, vt_symbol: str) -> None:
        """查询行情切片"""
        if not self.status:
            return

        contract: ContractData = self.contracts.get(vt_symbol, None)
        if not contract:
            self.gateway.write_log(f"查询行情切片失败，找不到{vt_symbol}对应的合约数据")
            return

        ib_contract: Contract = self.ib_contracts.get(vt_symbol, None)
        if not contract:
            self.gateway.write_log(f"查询行情切片失败，找不到{vt_symbol}对应的IB合约数据")
            return

        self.reqid += 1
        self.client.reqMktData(self.reqid, ib_contract, "", True, False, [])

        tick: TickData = TickData(
            symbol=contract.symbol,
            exchange=contract.exchange,
            datetime=datetime.now(LOCAL_TZ),
            gateway_name=self.gateway_name
        )
        tick.extra = {}

        self.ticks[self.reqid] = tick

    def unsubscribe(self, req: SubscribeRequest) -> None:
        """退订tick数据更新"""
        # 移除订阅记录
        if req.vt_symbol not in self.subscribed:
            return
        self.subscribed.pop(req.vt_symbol)

        # 获取订阅号
        cancel_id: int = 0
        for reqid, tick in self.ticks.items():
            if tick.vt_symbol == req.vt_symbol:
                cancel_id = reqid
                break

        # 发送退订请求
        self.client.cancelMktData(cancel_id)


def generate_ib_contract(symbol: str, exchange: Exchange) -> Optional[Contract]:
    """生产IB合约"""
    # 字符串代码
    if "-" in symbol:
        try:
            fields: list = symbol.split(JOIN_SYMBOL)

            ib_contract: Contract = Contract()
            ib_contract.exchange = EXCHANGE_VT2IB[exchange]
            ib_contract.secType = fields[-1]
            ib_contract.currency = fields[-2]
            ib_contract.symbol = fields[0]

            if ib_contract.secType in ["FUT", "OPT", "FOP"]:
                # ib_contract.lastTradeDateOrContractMonth = fields[1]
                if len(fields) > 3:
                    ib_contract.lastTradeDateOrContractMonth = fields[1]

            if ib_contract.secType == "FUT":
                # ib_contract.secType = "CONTFUT"
                if len(fields) == 5:
                    ib_contract.multiplier = int(fields[2])

            if ib_contract.secType in ["OPT", "FOP"]:
                ib_contract.right = fields[2]
                ib_contract.strike = float(fields[3])
                ib_contract.multiplier = int(fields[4])
        except IndexError:
            ib_contract = None
    # 数字代码（ConId）
    else:
        if symbol.isdigit():
            ib_contract: Contract = Contract()
            ib_contract.exchange = EXCHANGE_VT2IB[exchange]
            ib_contract.conId = symbol
        else:
            ib_contract = None

    return ib_contract
