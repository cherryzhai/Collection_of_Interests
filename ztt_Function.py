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

def send_dingding_msg(content, robot_id=''):
    try:
        msg = {
            "msgtype": "text",
            "text": {"content": content + '\n' + datetime.now().strftime("%m-%d %H:%M:%S")}}
        headers = {"Content-Type": "application/json;charset=utf-8"}
        url = 'https://oapi.dingtalk.com/robot/send?access_token=' + robot_id
        body = json.dumps(msg)
        requests.post(url, data=body, headers=headers)
        print('成功发送钉钉')
    except Exception as e:
        print("发送钉钉失败:", e)

# =====获取数据相关函数
# 函数：从网页上抓取数据
def get_content_from_internet(url, max_try_num=10, sleep_time=5):
    """
    使用python自带的urlopen函数，从网页上抓取数据
    :param url: 要抓取数据的网址
    :param max_try_num: 最多尝试抓取次数
    :param sleep_time: 抓取失败后停顿的时间
    :return: 返回抓取到的网页内容
    """
    content = None  # 初始化content为空

    # 抓取内容
    for i in range(max_try_num):
        try:
            content = urlopen(url=url, timeout=10).read()  # 使用python自带的库，从网络上获取信息
            break
        except Exception as e:
            print('抓取数据报错，次数：', i + 1, '报错内容：', e)
            time.sleep(sleep_time)

    # 判断是否成功抓取内容
    if content is not None:
        return content
    else:
        raise ValueError('使用urlopen抓取网页数据不断报错，达到尝试上限，停止程序，请尽快检查问题所在')

# 获取指定股票的数据
def get_latest_data(code_list):
    """
    返回一串股票最近一个交易日的相关数据
    从这个网址获取股票数据：http://hq.sinajs.cn/list=sh600000,sz000002,sz300001
    正常网址：https://finance.sina.com.cn/realstock/company/sh600000/nc.shtml,
    :param code_list: 一串股票代码的list，可以多个，例如[sh600000, sz000002, sz300001],
    :return: 返回一个存储股票数据的DataFrame
           股票代码  股票名称       交易日期    开盘价    最高价    最低价    收盘价   前收盘价          成交量           成交额   buy1  sell1
0  sz000002  万 科Ａ 2019-05-08  27.42  28.01  27.26  27.39  27.98   35387944.0  9.767760e+08  27.39  27.40
1  sh601288  农业银行 2019-05-08   3.64   3.64   3.61   3.61   3.66  245611404.0  8.892762e+08   3.61   3.62
    """
    # 构建url
    url = "http://hq.sinajs.cn/list=" + ",".join(code_list)
    print(url)

    # 抓取数据
    content = get_content_from_internet(url)
    content = content.decode('gbk')

    # 将数据转换成DataFrame
    content = content.strip()  # 去掉文本前后的空格、回车等
    data_line = content.split('\n')  # 每行是一个股票的数据
    data_line = [i.replace('var hq_str_', '').split(',') for i in data_line]
    df = pd.DataFrame(data_line, dtype='float')  #

    # 对DataFrame进行整理
    df[0] = df[0].str.split('="')
    df['股票代码'] = df[0].str[0].str.strip()
    df['股票名称'] = df[0].str[-1].str.strip()
    df['交易日期'] = df[30]  # 股票市场的K线，是普遍以当跟K线结束时间来命名的
    df['交易日期'] = pd.to_datetime(df['交易日期'])
    rename_dict = {1: '开盘价', 2: '前收盘价', 3: '收盘价', 4: '最高价', 5: '最低价', 6: 'buy1', 7: 'sell1',
                   8: '成交量', 9: '成交额', 32: 'status'}  # 自己去对比数据，会有新的返现
    # 其中amount单位是股，volume单位是元
    df.rename(columns=rename_dict, inplace=True)
    if len(code_list) > 1:
        #df['status'] = df['status'].str.strip('";')
        df['status'] = str(df['status']).strip('";')
    else:
        df['status'] = str(df['status']).strip('";')
    df = df[['股票代码', '股票名称', '交易日期', '开盘价', '最高价', '最低价', '收盘价', '前收盘价', '成交量',
             '成交额', 'buy1', 'sell1']]
    return df

def get_today_limit_from_eastmoney(stock_code):
    """
    从东方财富网上获取某个股票今天的涨停与跌停价格
    - 正常股票：返回 涨停价，跌停价
    - 停牌股票：返回最近一个交易日的涨跌停价格
    - 退市股票，返回 None，None
    - 不是A股的股票代码，会报 Value Error

    正常网址：http://quote.eastmoney.com/sh600000.html
    :param stock_code: 股票代码
    :return: 涨停价格，跌停价格
    """
    # 针对股票代码进行格式转换
    if stock_code.startswith('sh'):
        code = '1.' + stock_code[2:]
    elif stock_code.startswith('sz'):
        code = '0.' + stock_code[2:]
    else:
        code = stock_code

    # 构建url
    url = 'http://push2.eastmoney.com/api/qt/stock/get?ut=fa5fd1943c7b386f172d6893dbfba10b&invt=2&fltt=2&fields=f43,f57,f58,f169,f170,f46,f44,f51,f168,f47,f164,f163,f116,f60,f45,f52,f50,f48,f167,f117,f71,f161,f49,f530,f135,f136,f137,f138,f139,f141,f142,f144,f145,f147,f148,f140,f143,f146,f149,f55,f62,f162,f92,f173,f104,f105,f84,f85,f183,f184,f185,f186,f187,f188,f189,f190,f191,f192,f107,f111,f86,f177,f78,f110,f262,f263,f264,f267,f268,f250,f251,f252,f253,f254,f255,f256,f257,f258,f266,f269,f270,f271,f273,f274,f275,f127,f199,f128,f193,f196,f194,f195,f197,f80,f280,f281,f282,f284,f285,f286,f287&secid=%s&cb=jQuery112402728471441417022_1578037725347&_=1578037725361' % code

    # 抓取数据
    content = get_content_from_internet(url)
    content = content.decode('utf-8')

    # 处理返回结果
    try:
        up_limit = content.split('"f51":')[1].split(',')[0]
        down_limit = content.split('"f52":')[1].split(',')[0]
    except:
        # 非正常的股票，报错
        raise ValueError('请检查你输入的股票代码')

    # 当股票是退市股票的时候，返回None，None
    if up_limit == '"-"' or down_limit == '"-"':
        print(stock_code, '已经退市')
        return None, None
    else:
        # 正常股票，返回小数类型的涨跌停价格
        return float(up_limit), float(down_limit)

# 函数：获取历史的k线数据
def get_hist_candle_data(stock_code, kline_num=30, folder_path=''):
    """
    根据之前课程5.7中的大作业1，构建了股票数据库。本程序从本地读取指定股票的数据
    :param stock_code: 指定股票代码，例'sh600000'
    :param kline_num: 获取最近K线的数量
    :param folder_path: 数据文件夹；路劲
    :return:
    """
    # 构建存储文件路径
    path = folder_path + '\\' + stock_code + '.csv'

    # 读取数据，数据存在
    if os.path.exists(path):  # 文件存在，不是新股
        df = pd.read_csv(path, encoding='gbk', skiprows=1, parse_dates=['交易日期'])
        df.sort_values(by=['交易日期'], inplace=True)
        df.drop_duplicates(subset=['交易日期'], keep='last', inplace=True)

    # 读取数据，数据不存在
    else:  # 文件不存在，说明是新股
        #raise ValueError('读取%s历史数据失败，该地址%s不存在' % (stock_code, path))
        print("文件不存在，说明是新股")
        df = pd.DataFrame()
        return df

    # 获取最近一段数据的K线
    df = df.iloc[-kline_num:]
    df.reset_index(drop=True, inplace=True)

    return df


# 计算复权价格
def cal_fuquan_price(df, fuquan_type='后复权'):
    """
    用于计算复权价格
    :param df: 必须包含的字段：收盘价，前收盘价，开盘价，最高价，最低价
    :param fuquan_type: ‘前复权’或者‘后复权’
    :return: 最终输出的df中，新增字段：收盘价_复权，开盘价_复权，最高价_复权，最低价_复权
    """

    # 计算复权因子
    df['复权因子'] = (df['收盘价'] / df['前收盘价']).cumprod()

    # 计算前复权、后复权收盘价
    if fuquan_type == '后复权':
        df['收盘价_复权'] = df['复权因子'] * (df.iloc[0]['收盘价'] / df.iloc[0]['复权因子'])
    elif fuquan_type == '前复权':
        df['收盘价_复权'] = df['复权因子'] * (df.iloc[-1]['收盘价'] / df.iloc[-1]['复权因子'])
    else:
        raise ValueError('计算复权价时，出现未知的复权类型：%s' % fuquan_type)

    # 计算复权
    df['开盘价_复权'] = df['开盘价'] / df['收盘价'] * df['收盘价_复权']
    df['最高价_复权'] = df['最高价'] / df['收盘价'] * df['收盘价_复权']
    df['最低价_复权'] = df['最低价'] / df['收盘价'] * df['收盘价_复权']
    df.drop(['复权因子'], axis=1, inplace=True)

    return df


# 函数：发送钉钉消息
def send_dingding(message, robot_id='', max_try_count=5):
    """
    出错会自动重发发送钉钉消息
    :param message: 你要发送的消息内容
    :param robot_id: 你的钉钉机器人ID
    :param max_try_count: 最多重试的次数
    """
    try_count = 0
    while True:
        try_count += 1
        try:
            msg = {
                "msgtype": "text",
                "text": {"content": message + '\n' + datetime.now().strftime("%m-%d %H:%M:%S")}}
            headers = {"Content-Type": "application/json;charset=utf-8"}
            url = 'https://oapi.dingtalk.com/robot/send?access_token=' + robot_id
            body = json.dumps(msg)
            requests.post(url, data=body, headers=headers)
            print('钉钉已发送')
            break
        except Exception as e:
            if try_count > max_try_count:
                print("发送钉钉失败：", e)
                break
            else:
                print("发送钉钉报错，重试：", e)


