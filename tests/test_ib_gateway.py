from collections.abc import Callable
from datetime import datetime
from decimal import Decimal
from typing import Any

import pytest

pytest.importorskip("ibapi", reason="缺少 ibapi")

from ibapi.client import EClient  # noqa: E402
from ibapi.contract import Contract, ContractDetails  # noqa: E402
from ibapi.execution import Execution  # noqa: E402
from ibapi.order import Order  # noqa: E402
from ibapi.order_state import OrderState  # noqa: E402

from vnpy.event import EventEngine  # noqa: E402
from vnpy.trader.constant import (  # noqa: E402
    Direction,
    Exchange,
    OptionType,
    OrderType,
    Product,
    Status,
)
from vnpy.trader.object import (  # noqa: E402
    ContractData,
    OrderData,
    OrderRequest,
    SubscribeRequest,
    TradeData,
)

from vnpy_ib.ib_gateway import (  # noqa: E402
    EXCHANGE_IB2VT,
    EXCHANGE_VT2IB,
    LOCAL_TZ,
    IbApi,
    IbGateway,
    generate_ib_contract,
)


MIN_SIZE: Decimal = Decimal("1")


class Sink:
    def __init__(self) -> None:
        self.logs: list[str] = []
        self.orders: list[OrderData] = []
        self.contracts: list[ContractData] = []
        self.trades: list[TradeData] = []

    def attach(self, gateway: IbGateway) -> None:
        gateway.write_log = self.logs.append
        gateway.on_order = self.orders.append
        gateway.on_contract = self.contracts.append
        gateway.on_trade = self.trades.append


class CallRecorder:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def patch(self, monkeypatch: pytest.MonkeyPatch, target: object, names: list[str]) -> None:
        for name in names:
            monkeypatch.setattr(target, name, self.make_stub(name))

    def make_stub(self, name: str) -> Callable[..., None]:
        def stub(*args: Any, **kwargs: Any) -> None:
            self.calls.append((name, args))

        return stub

    def names(self) -> list[str]:
        return [name for name, _args in self.calls]


@pytest.fixture(autouse=True)
def forbid_socket_connect(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_connect(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("offline test must not open a socket")

    monkeypatch.setattr(EClient, "connect", fail_connect)


@pytest.fixture
def sink() -> Sink:
    return Sink()


@pytest.fixture
def recorder() -> CallRecorder:
    return CallRecorder()


@pytest.fixture
def gateway(sink: Sink) -> IbGateway:
    engine: EventEngine = EventEngine()
    ib: IbGateway = IbGateway(engine, "IB")
    sink.attach(ib)
    return ib


@pytest.fixture
def api(gateway: IbGateway) -> IbApi:
    return gateway.api


def order_request(
    symbol: str = "SPY-USD-STK",
    exchange: Exchange = Exchange.SMART,
    direction: Direction = Direction.LONG,
    order_type: OrderType = OrderType.LIMIT,
    volume: float = 2,
    price: float = 450.5,
) -> OrderRequest:
    return OrderRequest(
        symbol=symbol,
        exchange=exchange,
        direction=direction,
        type=order_type,
        volume=volume,
        price=price,
    )


def contract_details(
    sec_type: str = "STK",
    exchange: str = "SMART",
    con_id: int = 756733,
    multiplier: str = "",
    long_name: str = "SPDR",
    right: str = "",
    strike: float = 0.0,
    expiry: str = "",
    under_con_id: int = 0,
) -> ContractDetails:
    details: ContractDetails = ContractDetails()
    details.contract.conId = con_id
    details.contract.symbol = "SPY"
    details.contract.secType = sec_type
    details.contract.exchange = exchange
    details.contract.currency = "USD"
    details.contract.multiplier = multiplier
    details.contract.right = right
    details.contract.strike = strike
    details.contract.lastTradeDateOrContractMonth = expiry
    details.longName = long_name
    details.minTick = 0.01
    details.minSize = MIN_SIZE
    details.underConId = under_con_id
    return details


def ib_order(
    action: str,
    order_type: str,
    quantity: str = "3",
    limit_price: float = 0.0,
    aux_price: float = 0.0,
    order_ref: str = "2026-03-20 09:30:00",
) -> Order:
    order: Order = Order()
    order.action = action
    order.orderType = order_type
    order.totalQuantity = Decimal(quantity)
    order.lmtPrice = limit_price
    order.auxPrice = aux_price
    order.orderRef = order_ref
    return order


def ib_contract(
    con_id: int = 999,
    exchange: str = "GLOBEX",
    sec_type: str = "FUT",
) -> Contract:
    contract: Contract = Contract()
    contract.symbol = "ES"
    contract.secType = sec_type
    contract.exchange = exchange
    contract.currency = "USD"
    contract.conId = con_id
    contract.lastTradeDateOrContractMonth = "202603"
    return contract


def push_order_status(api: IbApi, order_id: int, status: str, filled: str = "1") -> None:
    api.orderStatus(
        order_id,
        status,
        Decimal(filled),
        Decimal("0"),
        0.0,
        0,
        0,
        0.0,
        1,
        "",
        0.0,
    )


def arm_orders(api: IbApi, recorder: CallRecorder, monkeypatch: pytest.MonkeyPatch) -> None:
    api.status = True
    api.clientid = 7
    api.account = "DU123"
    recorder.patch(monkeypatch, api.client, ["placeOrder", "reqIds"])


@pytest.mark.parametrize(
    ("symbol", "exchange", "expected"),
    [
        (
            "SPY-USD-STK",
            Exchange.SMART,
            {
                "symbol": "SPY",
                "secType": "STK",
                "currency": "USD",
                "exchange": "SMART",
                "multiplier": "",
            },
        ),
        (
            "EUR-USD-CASH",
            Exchange.IDEALPRO,
            {
                "symbol": "EUR",
                "secType": "CASH",
                "currency": "USD",
                "exchange": "IDEALPRO",
                "multiplier": "",
            },
        ),
        (
            "XAUUSD-USD-CMDTY",
            Exchange.SMART,
            {
                "symbol": "XAUUSD",
                "secType": "CMDTY",
                "currency": "USD",
                "exchange": "SMART",
                "multiplier": "",
            },
        ),
        (
            "ES-202002-USD-FUT",
            Exchange.GLOBEX,
            {
                "symbol": "ES",
                "secType": "FUT",
                "currency": "USD",
                "exchange": "GLOBEX",
                "lastTradeDateOrContractMonth": "202002",
                "multiplier": "",
            },
        ),
        (
            "SI-202006-1000-USD-FUT",
            Exchange.NYMEX,
            {
                "symbol": "SI",
                "secType": "FUT",
                "currency": "USD",
                "exchange": "NYMEX",
                "lastTradeDateOrContractMonth": "202006",
                "multiplier": 1000,
            },
        ),
        (
            "ES-2020006-C-2430-50-USD-FOP",
            Exchange.GLOBEX,
            {
                "symbol": "ES",
                "secType": "FOP",
                "currency": "USD",
                "exchange": "GLOBEX",
                "lastTradeDateOrContractMonth": "2020006",
                "right": "C",
                "strike": 2430.0,
                "multiplier": 50,
            },
        ),
        (
            "600000-CNY-STK",
            Exchange.SSE,
            {
                "symbol": "600000",
                "secType": "STK",
                "currency": "CNY",
                "exchange": "SEHKNTL",
                "multiplier": "",
            },
        ),
        (
            "000001-CNY-STK",
            Exchange.SZSE,
            {
                "symbol": "000001",
                "secType": "STK",
                "currency": "CNY",
                "exchange": "SEHKSZSE",
                "multiplier": "",
            },
        ),
        (
            "ABC-USD-STK",
            Exchange.OTC,
            {
                "symbol": "ABC",
                "secType": "STK",
                "currency": "USD",
                "exchange": "PINK",
                "multiplier": "",
            },
        ),
        (
            "CAD-USD-CASH",
            Exchange.LME,
            {
                "symbol": "CAD",
                "secType": "CASH",
                "currency": "USD",
                "exchange": "LMEOTC",
                "multiplier": "",
            },
        ),
    ],
)
def test_generate_ib_contract_string_symbols(
    symbol: str,
    exchange: Exchange,
    expected: dict[str, Any],
) -> None:
    contract: Contract | None = generate_ib_contract(symbol, exchange)

    assert contract is not None
    for field, value in expected.items():
        assert getattr(contract, field) == value


def test_generate_ib_contract_con_id() -> None:
    contract: Contract | None = generate_ib_contract("265598", Exchange.NASDAQ)

    assert contract is not None
    assert contract.conId == 265598
    assert contract.exchange == "NASDAQ"
    assert contract.symbol == ""


@pytest.mark.parametrize("symbol", ["BAD", "", "ES-20260320-OPT"])
def test_generate_ib_contract_invalid_returns_none(symbol: str) -> None:
    assert generate_ib_contract(symbol, Exchange.SMART) is None


def test_exchange_maps_are_inverse() -> None:
    vt_exchange: Exchange
    ib_name: str
    for vt_exchange, ib_name in EXCHANGE_VT2IB.items():
        assert EXCHANGE_IB2VT[ib_name] == vt_exchange


def test_generate_symbol_prefers_cached_string(api: IbApi) -> None:
    stock: Contract = Contract()
    stock.symbol = "SPY"
    stock.secType = "STK"
    stock.currency = "USD"
    stock.exchange = "SMART"
    stock.conId = 756733
    assert api.generate_symbol(stock) == "756733"

    cached_stock: ContractData = ContractData(
        symbol="SPY-USD-STK",
        exchange=Exchange.SMART,
        name="SPY",
        product=Product.EQUITY,
        size=1,
        pricetick=0.01,
        gateway_name="IB",
    )
    api.contracts[cached_stock.vt_symbol] = cached_stock
    assert api.generate_symbol(stock) == "SPY-USD-STK"

    future: Contract = Contract()
    future.symbol = "ES"
    future.secType = "FUT"
    future.currency = "USD"
    future.exchange = "GLOBEX"
    future.lastTradeDateOrContractMonth = "202002"
    future.conId = 11
    assert api.generate_symbol(future) == "11"

    cached_future: ContractData = ContractData(
        symbol="ES-202002-USD-FUT",
        exchange=Exchange.GLOBEX,
        name="ES",
        product=Product.FUTURES,
        size=50,
        pricetick=0.25,
        gateway_name="IB",
    )
    api.contracts[cached_future.vt_symbol] = cached_future
    assert api.generate_symbol(future) == "ES-202002-USD-FUT"


@pytest.mark.parametrize(
    ("sec_type", "product"),
    [
        ("STK", Product.EQUITY),
        ("CASH", Product.FOREX),
        ("CMDTY", Product.SPOT),
        ("FUT", Product.FUTURES),
        ("CONTFUT", Product.FUTURES),
        ("IND", Product.INDEX),
        ("CFD", Product.CFD),
    ],
)
def test_contract_details_maps_product(
    api: IbApi,
    sink: Sink,
    sec_type: str,
    product: Product,
) -> None:
    api.contractDetails(1, contract_details(sec_type=sec_type, multiplier="10", con_id=42))

    contract: ContractData = sink.contracts[0]
    assert contract.symbol == "42"
    assert contract.exchange == Exchange.SMART
    assert contract.product == product
    assert contract.size == 10.0
    assert contract.vt_symbol == "42.SMART"
    assert api.contracts[contract.vt_symbol].product == product
    assert api.ib_contracts[contract.vt_symbol].secType == sec_type


@pytest.mark.parametrize(
    ("ib_exchange", "vt_exchange"),
    [
        ("SMART", Exchange.SMART),
        ("IDEALPRO", Exchange.IDEALPRO),
        ("GLOBEX", Exchange.GLOBEX),
        ("NYMEX", Exchange.NYMEX),
        ("SEHKNTL", Exchange.SSE),
        ("SEHKSZSE", Exchange.SZSE),
        ("PINK", Exchange.OTC),
        ("LMEOTC", Exchange.LME),
    ],
)
def test_contract_details_maps_exchange(
    api: IbApi,
    sink: Sink,
    ib_exchange: str,
    vt_exchange: Exchange,
) -> None:
    api.contractDetails(1, contract_details(exchange=ib_exchange, multiplier="1", con_id=7))

    contract: ContractData = sink.contracts[0]
    assert contract.exchange == vt_exchange
    assert contract.vt_symbol == f"7.{vt_exchange.value}"


def test_contract_details_string_symbol(api: IbApi, sink: Sink) -> None:
    api.reqid_symbol_map[5] = "SPY-USD-STK"
    api.contractDetails(5, contract_details(con_id=756733))

    contract: ContractData = sink.contracts[0]
    assert contract.symbol == "SPY-USD-STK"
    assert contract.exchange == Exchange.SMART
    assert contract.product == Product.EQUITY
    assert contract.name == "SPDR"
    assert contract.size == 1.0
    assert contract.pricetick == 0.01
    assert contract.min_volume == MIN_SIZE
    assert contract.net_position is True
    assert contract.history_data is True
    assert contract.stop_supported is True
    assert contract.gateway_name == "IB"
    assert contract.vt_symbol == "SPY-USD-STK.SMART"


@pytest.mark.parametrize("sec_type", ["OPT", "FOP"])
@pytest.mark.parametrize(
    ("right", "option_type"),
    [
        ("C", OptionType.CALL),
        ("CALL", OptionType.CALL),
        ("P", OptionType.PUT),
        ("PUT", OptionType.PUT),
        ("", None),
    ],
)
def test_contract_details_option(
    api: IbApi,
    sink: Sink,
    sec_type: str,
    right: str,
    option_type: OptionType | None,
) -> None:
    api.contractDetails(
        3,
        contract_details(
            sec_type=sec_type,
            con_id=321,
            multiplier="100",
            right=right,
            strike=2430.0,
            expiry="20260320",
            under_con_id=756733,
            long_name="SPY Mar20 2430",
        ),
    )

    contract: ContractData = sink.contracts[0]
    assert contract.symbol == "321"
    assert contract.product == Product.OPTION
    assert contract.size == 100.0
    assert contract.option_type == option_type
    assert contract.option_strike == 2430.0
    assert contract.option_index == "2430.0"
    assert contract.option_expiry == datetime(2026, 3, 20)
    assert contract.option_portfolio == "756733_O"
    assert contract.option_underlying == "756733_20260320"


def test_contract_details_unsupported_product_is_ignored(api: IbApi, sink: Sink) -> None:
    api.contractDetails(1, contract_details(sec_type="BAG"))

    assert sink.contracts == []
    assert api.contracts == {}


def test_contract_details_duplicate_is_not_pushed_again(api: IbApi, sink: Sink) -> None:
    api.contractDetails(1, contract_details(long_name="First", multiplier="1"))
    api.contractDetails(2, contract_details(long_name="Second", multiplier="1"))

    assert len(sink.contracts) == 1
    assert sink.contracts[0].name == "First"
    assert len(api.contracts) == 1


def test_order_status_unknown_id_is_ignored(api: IbApi, sink: Sink) -> None:
    push_order_status(api, 99, "Filled")

    assert sink.orders == []


def test_pending_cancel_keeps_status_and_updates_traded(api: IbApi, sink: Sink) -> None:
    order: OrderData = OrderData(
        symbol="SPY-USD-STK",
        exchange=Exchange.SMART,
        orderid="8",
        direction=Direction.LONG,
        type=OrderType.LIMIT,
        price=400,
        volume=2,
        status=Status.NOTTRADED,
        gateway_name="IB",
    )
    api.orders[order.orderid] = order

    push_order_status(api, 8, "PendingCancel", filled="1.5")

    assert sink.orders[0].status == Status.NOTTRADED
    assert sink.orders[0].traded == 1.5
    assert sink.orders[0].orderid == "8"
    assert sink.orders[0].vt_orderid == "IB.8"
    assert api.orders["8"].status == Status.NOTTRADED


@pytest.mark.parametrize(
    ("ib_status", "vt_status"),
    [
        ("ApiPending", Status.SUBMITTING),
        ("PendingSubmit", Status.SUBMITTING),
        ("PreSubmitted", Status.NOTTRADED),
        ("Submitted", Status.NOTTRADED),
        ("ApiCancelled", Status.CANCELLED),
        ("Cancelled", Status.CANCELLED),
        ("Filled", Status.ALLTRADED),
        ("Inactive", Status.REJECTED),
    ],
)
def test_order_status_mapping(api: IbApi, sink: Sink, ib_status: str, vt_status: Status) -> None:
    initial: Status = Status.SUBMITTING if vt_status == Status.REJECTED else Status.REJECTED
    order: OrderData = OrderData(
        symbol="SPY-USD-STK",
        exchange=Exchange.SMART,
        orderid="8",
        direction=Direction.SHORT,
        type=OrderType.LIMIT,
        price=400,
        volume=2,
        status=initial,
        gateway_name="IB",
    )
    api.orders[order.orderid] = order

    push_order_status(api, 8, ib_status, filled="2")

    assert sink.orders[0].status == vt_status
    assert sink.orders[0].traded == 2
    assert sink.orders[0].orderid == "8"
    assert sink.orders[0].vt_orderid == "IB.8"
    assert api.orders["8"].status == vt_status


@pytest.mark.parametrize(
    ("ib_type", "action", "vt_type", "direction", "limit_price", "aux_price", "expect_price"),
    [
        ("LMT", "BUY", OrderType.LIMIT, Direction.LONG, 12.5, 0.0, 12.5),
        ("STP", "SELL", OrderType.STOP, Direction.SHORT, 0.0, 88.0, 88.0),
        ("MKT", "BUY", OrderType.MARKET, Direction.LONG, 1.0, 2.0, 0.0),
    ],
)
def test_open_order_maps_new_order(
    api: IbApi,
    sink: Sink,
    ib_type: str,
    action: str,
    vt_type: OrderType,
    direction: Direction,
    limit_price: float,
    aux_price: float,
    expect_price: float,
) -> None:
    api.openOrder(
        4,
        ib_contract(con_id=999),
        ib_order(action, ib_type, quantity="3", limit_price=limit_price, aux_price=aux_price),
        OrderState(),
    )

    order: OrderData = sink.orders[0]
    assert order.symbol == "999"
    assert order.exchange == Exchange.GLOBEX
    assert order.orderid == "4"
    assert order.vt_orderid == "IB.4"
    assert order.type == vt_type
    assert order.direction == direction
    assert order.volume == 3
    assert order.price == expect_price
    assert order.datetime == datetime(2026, 3, 20, 9, 30)
    assert api.orders["4"].orderid == "4"


def test_open_order_keeps_cached_symbol_and_exchange(api: IbApi, sink: Sink) -> None:
    cached: OrderData = OrderData(
        symbol="SPY-USD-STK",
        exchange=Exchange.NYSE,
        orderid="3",
        direction=Direction.LONG,
        type=OrderType.LIMIT,
        price=1,
        volume=1,
        gateway_name="IB",
    )
    api.orders[cached.orderid] = cached

    api.openOrder(
        3,
        ib_contract(con_id=1, exchange="SMART", sec_type="STK"),
        ib_order("SELL", "LMT", quantity="9", limit_price=12.25),
        OrderState(),
    )

    order: OrderData = sink.orders[0]
    assert order.symbol == "SPY-USD-STK"
    assert order.exchange == Exchange.NYSE
    assert order.direction == Direction.LONG
    assert order.volume == 1
    assert order.type == OrderType.LIMIT
    assert order.price == 12.25
    assert order.vt_orderid == "IB.3"


def test_send_order_assigns_order_id(
    gateway: IbGateway,
    api: IbApi,
    sink: Sink,
    recorder: CallRecorder,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    arm_orders(api, recorder, monkeypatch)

    vt_orderid: str = gateway.send_order(order_request(direction=Direction.SHORT, order_type=OrderType.STOP, price=451))

    assert vt_orderid == "IB.1"
    assert api.orderid == 1
    assert api.orders["1"].vt_orderid == "IB.1"
    assert sink.orders[0].orderid == "1"
    assert sink.orders[0].direction == Direction.SHORT
    assert sink.orders[0].type == OrderType.STOP
    assert sink.orders[0].volume == 2
    assert recorder.names() == ["placeOrder", "reqIds"]

    order_id, contract, placed = recorder.calls[0][1]
    assert order_id == 1
    assert contract.symbol == "SPY"
    assert contract.secType == "STK"
    assert contract.currency == "USD"
    assert contract.exchange == "SMART"
    assert placed.action == "SELL"
    assert placed.orderType == "STP"
    assert placed.auxPrice == 451
    assert placed.totalQuantity == Decimal("2")
    assert placed.account == "DU123"
    assert placed.clientId == 7
    assert placed.tif == "DAY"
    assert datetime.strptime(placed.orderRef, "%Y-%m-%d %H:%M:%S")
    assert recorder.calls[1] == ("reqIds", (1,))

    push_order_status(api, 1, "Submitted", filled="0")
    assert sink.orders[-1].status == Status.NOTTRADED
    assert sink.orders[-1].vt_orderid == "IB.1"

    push_order_status(api, 1, "Filled", filled="2")
    assert api.orders["1"].status == Status.ALLTRADED
    assert sink.orders[-1].traded == 2


def test_next_valid_id_seeds_following_order(
    gateway: IbGateway,
    api: IbApi,
    recorder: CallRecorder,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    arm_orders(api, recorder, monkeypatch)
    api.nextValidId(50)
    api.nextValidId(80)

    assert api.orderid == 50
    assert gateway.send_order(order_request()) == "IB.51"
    assert recorder.calls[0][1][0] == 51


def test_send_order_rejects_before_assigning_id(api: IbApi, sink: Sink) -> None:
    assert api.send_order(order_request()) == ""

    api.status = True
    assert api.send_order(order_request(exchange=Exchange.SHFE)) == ""
    assert api.send_order(order_request(order_type=OrderType.FAK)) == ""
    assert api.send_order(order_request(symbol="SPY USD-USD-STK")) == ""
    assert api.orderid == 0
    assert api.orders == {}
    assert sink.orders == []


def test_exec_details_uses_cached_order_and_execution_side(api: IbApi, sink: Sink) -> None:
    cached: OrderData = OrderData(
        symbol="SPY-USD-STK",
        exchange=Exchange.NYSE,
        orderid="6",
        direction=Direction.LONG,
        type=OrderType.LIMIT,
        price=10,
        volume=2,
        gateway_name="IB",
    )
    api.orders[cached.orderid] = cached

    execution: Execution = Execution()
    execution.orderId = 6
    execution.execId = "exec-1"
    execution.time = "20260320 09:30:00"
    execution.side = "SLD"
    execution.shares = Decimal("2")
    execution.price = 450.25
    api.execDetails(1, ib_contract(exchange="SMART", sec_type="STK"), execution)

    trade: TradeData = sink.trades[0]
    assert trade.symbol == "SPY-USD-STK"
    assert trade.exchange == Exchange.NYSE
    assert trade.orderid == "6"
    assert trade.vt_orderid == "IB.6"
    assert trade.tradeid == "exec-1"
    assert trade.direction == Direction.SHORT
    assert trade.volume == 2
    assert trade.price == 450.25
    assert trade.datetime == datetime(2026, 3, 20, 9, 30, tzinfo=LOCAL_TZ)


def test_subscribe_without_connection_does_not_request(
    api: IbApi,
    recorder: CallRecorder,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorder.patch(monkeypatch, api.client, ["reqContractDetails", "reqMktData"])

    api.subscribe(SubscribeRequest(symbol="SPY-USD-STK", exchange=Exchange.SMART))

    assert recorder.calls == []
    assert api.subscribed == {}


def test_close_without_connection_does_not_disconnect(
    gateway: IbGateway,
    api: IbApi,
    recorder: CallRecorder,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert api.status is False
    recorder.patch(monkeypatch, api.client, ["disconnect"])
    monkeypatch.setattr(api.client, "exit", recorder.make_stub("exit"), raising=False)

    gateway.close()
    api.close()

    assert recorder.calls == []
    assert api.status is False
