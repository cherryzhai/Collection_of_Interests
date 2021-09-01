"""
实盘交易相关的代码
"""
import json
import os
from datetime import datetime, timedelta
from urllib.request import urlopen  # python自带爬虫库
import pandas as pd
import requests
import time


pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.max_rows', 5000)  # 最多显示数据的行数

# =====获取持仓信息相关函数
# 函数：获取单个股票的相关信息
def update_one_stock_info(user, stock_info):
    """
    获取单个股票的相关信息。注意，不同券商的某些字段会不一样，
    :param ths:
    :param stock_info:
    :return:  {'股票代码': 'sh601258', '交易代码': '601258', '最大买入资金': 100000, '是否交易': True,
    '涨停价格': 1.56, '跌停价格': 1.28, '可用资金': 2763.76, '股票余额': 300, '可用余额': 0}
    """
    # ===获取可用资金
    #balance_dict = xbx_get_balance(ths)
    balance_dict = pd.DataFrame(user.balance)
    print('\n账户资金状况：')
    print(balance_dict)
    stock_info['可用资金'] = balance_dict['可用金额']  # 可以用来买入股票的资金。不同券商，此处字段'可用金额'不一定相同

    # =====获取持仓
    postion = pd.DataFrame(user.position)
    print(postion)
    # ===获取股票余额，可用余额
    #postion = xbx_get_position(ths)
    time.sleep(0.5)
    # 更新持仓信息
    if postion.empty:
        stock_info['股票余额'] = 0
        stock_info['可用余额'] = 0
    else:
        t = postion[postion['证券代码'] == stock_info['交易代码']]  # 不同券商，此处字段'证券代码'不一定相同
        if t.empty:
            stock_info['股票余额'] = 0
            stock_info['可用余额'] = 0
        else:
            stock_info['股票余额'] = t.iloc[0]['股票余额']  # 不同券商，此处字段'股票余额'不一定相同
            stock_info['可用余额'] = t.iloc[0]['可用余额']  # 不同券商，此处字段'可用余额'不一定相同

    print('股票情况：\n', stock_info, '\n')


# 函数：获取多个股票的相关信息
def update_account_data(user, stock_df):
    """
    获取多个股票的相关信息。注意，不同券商的某些字段会不一样，
    :param ths:
    :param stock_df:
    :return:
             分配仓位      股票代码    交易代码  是否交易  涨停价格  跌停价格  股票余额  可用余额   市值   买入成本    参考盈亏      分配资金
交易代码
601288   0.4  sh601288  601288  True  3.96  3.24   200   200  716  3.023  111.47  1868.088
603077   NaN      None    None  None   NaN   NaN     0     0    0   1.89   -8.69       NaN
    """
    # ===更新股票的相关信息
    # 如果update_columns在stock_df中不存在，创建这些columns
    update_columns = ['股票余额', '可用余额', '市值', '买入成本', '参考盈亏']  # 不同券商，此处字段'股票余额'不一定相同
    for c in update_columns:
        if c not in stock_df.columns:
            stock_df[c] = None

    # 获取最新持仓
    #postion = xbx_get_position(ths)
    postion = pd.DataFrame(user.position)
    time.sleep(0.5)
    # 更新持仓信息
    if postion.empty is False:
        postion.set_index(keys='证券代码', inplace=True)
        # 补齐不监控，但是账户中存在的股票
        for i in list(set(postion.index) - set(stock_df.index)):
            stock_df.loc[i, :] = None
        # 更新stock_df中的数据
        stock_df.update(postion)

    # ===获取账户资金的相关信息
    #balance_dict = xbx_get_balance(ths)
    balance_dict = pd.DataFrame(user.balance)
    time.sleep(0.5)
    stock_df_monitor = stock_df[stock_df['分配仓位'].notnull()]
    balance_dict['监控股票盈亏'] = stock_df_monitor['参考盈亏'].sum()
    balance_dict['初始资金'] = balance_dict['总资产'] - balance_dict['监控股票盈亏']
    stock_df['分配资金'] = balance_dict['初始资金'] * stock_df['分配仓位']

    stock_df['股票余额'].fillna(value=0, inplace=True)
    stock_df['可用余额'].fillna(value=0, inplace=True)

    print('股票持仓情况：\n', stock_df, '\n')
    print('账户资金状况：', balance_dict, '\n')
    return balance_dict, stock_df


# =====功能性函数
# 获取最新的卖出价格
def cal_order_price(side, buy1_price, sell1_price, slippage, up_limit_price, down_limit_price):
    if side == 'sell':
        order_price = buy1_price * (1 - slippage)
        order_price = max(round(order_price, 2), down_limit_price)
    elif side == 'buy':
        order_price = sell1_price * (1 + slippage)
        order_price = min(round(order_price, 2), up_limit_price)

    return order_price

