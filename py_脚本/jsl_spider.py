# -*- coding:utf-8 -*-

from urllib.error import URLError  # 异常处理模块，捕获错误
from urllib.request import ProxyHandler, build_opener  # 代理IP模块
import json
import pandas as pd
import time
import datetime
import random


def random_user_agent():
    agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36',
        'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)',
        'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Win64; x64; Trident/6.0)',
        'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0)',
        'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; WOW64; Trident/6.0)',
        'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Win64; x64; Trident/6.0)',
        'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; ARM; Trident/6.0)',
        'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30;'
        ' .NET CLR 3.0.04506.648)',
        'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; InfoPath.1',
        'Mozilla/4.0 (compatible; GoogleToolbar 5.0.2124.2070; Windows 6.0; MSIE 8.0.6001.18241)',
        'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; EasyBits Go v1.0; InfoPath.1;'
        ' .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)',
        'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; SLCC1; .NET CLR 2.0.50727; Media Center PC 5.0; '
        '.NET CLR 3.0.04506)'
    ]
    return random.choice(agents)

def random_proxies():
	ports = ['1080', '1081', '1082', '1083']
	random_port = random.choice(ports)
	proxies = {
		'http':'127.0.0.1:%s' % random_port
	}
	return proxies

def jsl_spider(id_card):

    proxy_handler = ProxyHandler({
        'http': '127.0.0.0:4973'
    })
    opener = build_opener(proxy_handler)  # 通过proxy_handler来构建opener
    # 请求网站

    response = opener.open(
    'https://www.jisilu.cn/data/cbnew/cb_list/?___jsl=LST___t=1567040758309')  # 此处的open方法同urllib的urlopen方法

    return response


def fourTypeCB(opener):
    #global str, df, ndarray, list, each, dict, dict2, i, newDf
    global newDf
    # TODO：可转债筛选
    response = opener.open(
        'https://www.jisilu.cn/data/cbnew/cb_list/?___jsl=LST___t=1567040758309')  # 此处的open方法同urllib的urlopen方法
    str = response.read().decode('utf-8')
    json1 = json.loads(str)
    df = pd.DataFrame(json1)
    df = df.drop(['page', 'total'], axis=1)
    ndarray = df['rows'].values
    # 将ndarray的每个cell放入list
    list = []
    for each in ndarray:
        dict = each['cell']
        list.append(dict)
    # 把list放入dict
    dict2 = {}
    for i in range(len(list)):
        dict2[i] = [list[i]['bond_id'], list[i]['bond_nm'], list[i]['price'], list[i]['premium_rt'],
                    list[i]['rating_cd'], list[i]['convert_cd'], list[i]['year_left'], list[i]['ytm_rt_tax'],
                    list[i]['price_tips']]
    # 再将dict放入dataframe
    newDf = pd.DataFrame.from_dict(dict2, orient='index',
                                   columns=['代码', '转债名称', '现价', '溢价率', '评级', '是否转股期',
                                            '剩余年限', '到期税后收益', '是否上市'])
    i = 0
    for each in newDf.values:
        if (each[1].__contains__('EB') or each[8] == '待上市' or (
                each[4] == 'AA' or each[4] == 'AAA' or each[4] == 'AA+') == False):  # 去掉可交换债
            newDf = newDf.drop([i])
        # if(each[8] == '待上市'):
        #     newDf = newDf.drop([i])
        # if((each[4] == 'AA' or each[4] == 'AAA' or each[4] == 'AA+') == False): #根据评级筛选
        #     newDf = newDf.drop([i])
        i = i + 1

    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    # 数据类型转换
    newDf['到期税后收益'] = newDf['到期税后收益'].map(lambda x: float(x[0:-1]) / 100)
    newDf['溢价率'] = newDf['溢价率'].map(lambda x: float(x[0:-1]) / 100)
    newDf['现价'] = newDf['现价'].map(lambda x: float(x))
    # 跟踪可转债
    list = ['113521','113530'] #写入要跟踪的可转债代码
    track = newDf[newDf['代码'].isin(list)]
    print('跟踪:')
    print(track[['代码', '转债名称', '现价', '溢价率', '评级', '剩余年限', '到期税后收益','是否转股期']])
    # 防御型
    defence = newDf.sort_values(by='到期税后收益', axis=0, ascending=False).head(5)
    print('防御型:')
    print(defence[['代码', '转债名称', '现价', '溢价率', '评级', '剩余年限', '到期税后收益','是否转股期']])
    # 宽平衡型
    balance = newDf[newDf['到期税后收益'] > 0]
    balance = balance[balance['溢价率'] < 0.07].head(5)
    print('宽平衡型:')
    print(balance[['代码', '转债名称', '现价', '溢价率', '评级', '剩余年限', '到期税后收益','是否转股期']])
    # 平衡型
    balance = newDf[newDf['到期税后收益'] > 0]
    balance = balance[balance['到期税后收益'] < 0.03]
    balance = balance[balance['溢价率'] < 0.05].head(5)
    print('平衡型:')
    print(balance[['代码', '转债名称', '现价', '溢价率', '评级', '剩余年限', '到期税后收益','是否转股期']])
    # 进攻型
    indexs = newDf.loc[(newDf['是否转股期'] == '未到转股期')].index  # 过滤未到转股期
    attack = newDf.drop(indexs)
    attack = attack[attack['溢价率'] < 0.02].sort_values(by='溢价率', axis=0, ascending=True)#.head(2)
    print('进攻型:')
    print(attack[['代码', '转债名称', '现价', '溢价率', '评级', '剩余年限', '到期税后收益','是否转股期']])

    # TODO:四要素类型   AA级以上的    溢价率＜10%     到期收益率＞0     价格110以下    再加一个是否转股期自行选择
    fourElement = newDf[newDf['到期税后收益'] > 0]    #到期收益率＞0
    fourElement = fourElement[fourElement['溢价率'] < 0.1]  #溢价率＜10%
    fourElement = fourElement[fourElement['现价'] < 110]
    print('四要素类型：')
    print(fourElement[['代码', '转债名称', '现价', '溢价率', '评级', '剩余年限', '到期税后收益','是否转股期']])



def readyToPutCB(opener):
    #global str, df, ndarray, list, each, dict, dict2, i
    # TODO: 筛选即将触发回售： 直接判断time字段回售触及天数就行 待修改
    response2 = opener.open(
        'https://www.jisilu.cn/data/cbnew/huishou_list/?___jsl=LST___t=1567500724887')  # 此处的open方法同urllib的urlopen方法
    str = response2.read().decode('utf-8')
    json2 = json.loads(str)
    df = pd.DataFrame(json2)
    ndarray = df['rows'].values
    # 将ndarray的每个cell放入list
    list = []
    for each in ndarray:
        dict = each['cell']
        list.append(dict)
    # 把list放入dict 将列转为行
    dict2 = {}
    for i in range(len(list)):
        dict2[i] = [list[i]['bond_id'], list[i]['bond_nm'], list[i]['next_put_dt'], list[i]['sprice'],
                    list[i]['put_convert_price'],list[i]['time']]
    # 再将dict放入dataframe
    newDf2 = pd.DataFrame.from_dict(dict2, orient='index',
                                    columns=['代码', '转债名称', '回售起始日', '正股价', '回售触发价','回售触及天数'])
    nowTime_str = datetime.datetime.now().strftime('%Y-%m-%d')
    print('即将触发回售：')
    i = 0
    for each in newDf2.values:
        if (each[5] == '-' or each[1].__contains__('EB') or each[2] > nowTime_str):  # 去掉可交换债
            newDf2 = newDf2.drop([i])
        i = i + 1
    print(newDf2)
    # for each in newDf2.values:
    #     if (each[5].__contains__('-') or each[1].__contains__('EB') or each[2] > nowTime_str):  #过了回售起始日
    #         continue
    #     # if (float(each[3]) < float(each[4])):  #旧的判断方法  正股价sprice<回售触发价put_convert_price & 今天>回售起始日

    # for each in newDf2.values:  #在newDf取出更多信息
    #     for each2 in newDf.values:
    #         if (each[0] == each2[0]):
    #             print(each2)




# 请求网站
try:
    # 设置代理IP
    proxy_handler = ProxyHandler(random_proxies())
    opener = build_opener(proxy_handler)  # 通过proxy_handler来构建opener


    fourTypeCB(opener)


    readyToPutCB(opener)


    # TODO:筛选当日的新债
    # TODO:筛选类似泰晶的转债
    # TODO:博下修
    # TODO:回售期



except URLError as e:
    print(e.reason)