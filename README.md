# vn.py框架的InteractiveBrokers交易接口

<p align="center">
  <img src ="https://vnpy.oss-cn-shanghai.aliyuncs.com/vnpy-logo.png"/>
</p>

<p align="center">
    <img src ="https://img.shields.io/badge/version-9.81.1.0-blueviolet.svg"/>
    <img src ="https://img.shields.io/badge/platform-windows|linux|macos-yellow.svg"/>
    <img src ="https://img.shields.io/badge/python-3.7-blue.svg" />
    <img src ="https://img.shields.io/github/license/vnpy/vnpy.svg?color=orange"/>
</p>

## 说明

基于ibapi的9.81.1.post1版本开发的InteractiveBrokers交易接口。

IbGateway中的合约代码命名规则和举例：

|合约类型|代码规则|代码（symbol）|交易所（exchange）|
|---|---|---|---|
|股票|名称-货币-类别|SPY-USD-STK|SMART|
|外汇|名称-货币-类别|EUR-USD-CASH|IDEALPRO|
|贵金属|名称-货币-类别|XAUUSD-USD-CMDTY|SMART|
|期货|名称-到期年月-货币-类别|ES-202002-USD-FUT|GLOBEX|
|期货（指定乘数）|名称-到期年月-合约乘数-类别|SI-202006-1000-USD-FUT|NYMEX|
|期货期权|名称-到期年月-期权类型-行权价-合约乘数-货币-类别|ES-2020006-C-2430-50-USD-FOP  |GLOBEX|

## 安装

安装需要基于2.7.0版本以上的[VN Studio](https://www.vnpy.com)。

直接使用pip命令：

```
pip install vnpy_ib
```

下载解压后在cmd中运行：

```
python setup.py install
```

## 使用

以脚本方式启动（script/run.py）：

```
from vnpy.event import EventEngine
from vnpy.trader.engine import MainEngine
from vnpy.trader.ui import MainWindow, create_qapp

from vnpy_ib import IbGateway


def main():
    """主入口函数"""
    qapp = create_qapp()

    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    main_engine.add_gateway(IbGateway)

    main_window = MainWindow(main_engine, event_engine)
    main_window.showMaximized()

    qapp.exec()


if __name__ == "__main__":
    main()
```
