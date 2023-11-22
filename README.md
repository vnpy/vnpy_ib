# VeighNa框架的InteractiveBrokers交易接口

<p align="center">
  <img src ="https://vnpy.oss-cn-shanghai.aliyuncs.com/vnpy-logo.png"/>
</p>

<p align="center">
    <img src ="https://img.shields.io/badge/version-10.19.1.2-blueviolet.svg"/>
    <img src ="https://img.shields.io/badge/platform-windows|linux|macos-yellow.svg"/>
    <img src ="https://img.shields.io/badge/python-3.8|3.9|3.10-blue.svg" />
    <img src ="https://img.shields.io/github/license/vnpy/vnpy.svg?color=orange"/>
</p>

## 说明

基于ibapi的10.19.1版本开发的InteractiveBrokers交易接口。

IbGateway中的合约代码支持两种风格：数字代码和字符串代码。

数字代码基于IB平台的ConId，查询方式：在TWS软件中【右键点击任意合约】->【金融产品信息】->【详情】，然后在弹出的网页上即可找到合约的ConId。

字符串代码基于合约的具体描述信息，命名规则和举例：

|合约类型|代码规则|代码（symbol）|交易所（exchange）|
|---|---|---|---|
|股票|名称-货币-类别|SPY-USD-STK|SMART|
|外汇|名称-货币-类别|EUR-USD-CASH|IDEALPRO|
|贵金属|名称-货币-类别|XAUUSD-USD-CMDTY|SMART|
|期货|名称-到期年月-货币-类别|ES-202002-USD-FUT|GLOBEX|
|期货（指定乘数）|名称-到期年月-合约乘数-类别|SI-202006-1000-USD-FUT|NYMEX|
|期货期权|名称-到期年月-期权类型-行权价-合约乘数-货币-类别|ES-2020006-C-2430-50-USD-FOP|GLOBEX|

委托、成交、持仓信息中的合约代码，默认采用数字代码。如果用户使用字符串代码订阅过行情，则使用字符串代码。

## 安装

安装环境推荐基于3.8.0版本以上的【[**VeighNa Studio**](https://www.vnpy.com)】。

### 安装ibapi

在[IB官网](https://interactivebrokers.github.io/#)下载TWS API的msi安装文件，并运行安装。

找到安装目录下的source\pythonclient文件夹，在cmd中运行下述命令安装：

```
python setup.py install
```

### 安装vnpy_ib

直接使用pip命令：

```
pip install vnpy_ib
```

或者下载源代码后，解压后在cmd中运行：

```
pip install .
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
