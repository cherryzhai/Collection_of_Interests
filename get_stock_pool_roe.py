from datetime import datetime, timedelta
#import easytrader
from ztt_Function import *

import re
import requests

pd.set_option('expand_frame_repr', False)  # 当列太多时不换行

strategy_para = [10, 60]  # 策略参数
page_num=6
page_size=60

# 历史数据地址
hist_data_path = r'E:\quantclass\stock_2019_all_1108\data\stock_day_data\stock'
dingding_robot_id = 'f5d96edd6df1950c6e7b6d572b153112634aa7f3bd60a3372dc968fcee6d5f8a'  # 钉钉dd robot id

# =====固定参数
kline_num = max(strategy_para) + 5  # 获取的历史数据的数量
slippage = 1 / 100  # 下单价格偏移量

# =====初始化监控股票相关数据
hist_data_dict = {}  # 存放历史数据的dict
stock_pool={}
dict_sample = {'分配仓位': 0.2}



# =====移动平均线策略
# 用于实盘的简单移动平均线策略
def Trade_simple_moving_average_signal(df, para=[20, 120]):
    """
    简单的移动平均线策略。只能做多。
    当短期均线上穿长期均线的时候，做多，当短期均线下穿长期均线的时候，平仓
    :param df:
    :param para: ma_short, ma_long
    :return: 最终输出的df中，新增字段：signal，记录发出的交易信号
    """
    # ===策略参数
    ma_short = para[0]  # 短期均线。ma代表：moving_average
    ma_long = para[1]  # 长期均线

    # ===计算均线。所有的指标，都要使用复权价格进行计算。
    df['ma_short'] = df['收盘价_复权'].rolling(ma_short).mean()
    df['ma_long'] = df['收盘价_复权'].rolling(ma_long).mean()
    # ===找出做多信号
    if df.iloc[-1]['ma_short'] > df.iloc[-1]['ma_long'] and df.iloc[-2]['ma_short'] <= df.iloc[-2]['ma_long']:
        return 1

    # ===找出做多平仓信号
    if df.iloc[-1]['ma_short'] < df.iloc[-1]['ma_long'] and df.iloc[-2]['ma_short'] >= df.iloc[-2]['ma_long']:
        return 0

    return None

def crawl_stockcode(page = 1):
    rawurl = "http://vip.stock.finance.sina.com.cn/q/go.php/vFinanceAnalyze/kind/profit/index.phtml?num=%s&p=%s"
    url = rawurl %(page_size,page)
    content = requests.get(url)
    pattern = re.compile("<td style.*?q=(\d*?)&contry=stock",re.S)

    stock = pattern.findall(content.text)
    #print(stock)
    for stock_code in stock:
#        print(stock_code)
        if stock_code[0]=='6':
            stock_name = 'sh'+stock_code
        else:
            stock_name = 'sz'+stock_code
        #print(stock_name)
        stock_pool[stock_name] = dict_sample

def gen_stock_list(num):
    for i in range(1,num + 1):
        #print("start to parse page:",i)
        crawl_stockcode(i)

    #print(stock_pool)
    return pd.DataFrame(stock_pool).T

def initial_stock_data(stock_df):
    print("initial_stock_data")
    for stock_code in stock_df.index:
        stock_df.loc[stock_code, '股票代码'] = stock_code
        stock_df.loc[stock_code, '交易代码'] = stock_code[2:]
    # 股票今天是否可以交易
        stock_df.loc[stock_code, '是否交易'] = True
    # 股票的涨跌停价格
        stock_df.loc[stock_code, '涨停价格'], stock_df.loc[stock_code, '跌停价格'] = get_today_limit_from_eastmoney(stock_code)
    # 从本地文件中读取股票历史数据
        #hist_data_dict[stock_code] = get_hist_candle_data(stock_code, kline_num=kline_num, folder_path=hist_data_path)
        df_tmp =  get_hist_candle_data(stock_code, kline_num=kline_num, folder_path=hist_data_path)

        if df_tmp.empty:
        #    print("empty"+stock_code)
            hist_data_dict[stock_code] = df_tmp
        else:
            hist_data_dict[stock_code] = df_tmp

    stock_df.set_index(keys='交易代码', inplace=True, drop=False)


def handle_stock_data(stock_list,buy_list,sell_list):
    #get最新股票数据
    latest_df = get_latest_data(code_list=stock_list)
    #print('最新股票数据：\n', latest_df, '\n')
    for stock_code in stock_list:
        # 合并历史数据数据，获取最近n根k线
        t = latest_df[latest_df['股票代码'] == stock_code]
        df = hist_data_dict[stock_code].append(t, ignore_index=True, sort=False)
        df.drop_duplicates(subset=['交易日期'], keep='last', inplace=True)
        # 计算复权价格
        df = cal_fuquan_price(df, fuquan_type='后复权')
        # 产生交易信号
        signal = Trade_simple_moving_average_signal(df, para=strategy_para)
        # signal = Trade_test_signal()
        #print(signal)
        if signal == 1:
            buy_list.append(stock_code)
        elif signal == 0:
            sell_list.append(stock_code)


# =====主函数
def main():
    print('=' * 5, '本次运行开始', datetime.now())
    if_trade = True

    stock_roe = gen_stock_list(page_num)
    #exit()
    initial_stock_data(stock_roe)
    # ===获取监控股票的最新交易数据
    stock_code_list = list(stock_roe['股票代码'].dropna())

    print("select stock\n")
    # ===遍历所有股票，判单每个股票的交易信号
    buy_stock_list = []
    sell_stock_list = []
    handle_stock_data(stock_code_list, buy_stock_list, sell_stock_list)

    # ===创建丁丁消息
    dd_msg = ''
    dd_msg += '按ROE排名前%s的股票中\n' % str(page_num*page_size)
    dd_msg += '黄金交叉的股票：\n%s\n' % str(buy_stock_list)
    dd_msg += '死亡交叉的股票：\n%s\n' % str(sell_stock_list)
    print(dd_msg)
   # send_dingding(dd_msg, robot_id=dingding_robot_id)


# =====程序执行主体

main()

