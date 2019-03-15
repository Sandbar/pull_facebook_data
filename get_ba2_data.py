

import requests
import json
import numpy as np
import os
from pymongo import MongoClient
import datetime
import time
import pytz
import log_maker

tz = pytz.timezone('Asia/Shanghai')

outs_roas = dict()
outs_bid_amount = dict()
FB_URL = "https://graph.facebook.com/v3.1/act_{0}/adsets?fields=bid_amount,name,bid_constraints&access_token={1}&limit=300"


def get_adsets_data(fb_url, url):
    res_req = requests.get(fb_url, proxies={'http': url})
    out = json.loads(res_req.text)
    res_req.close()
    if 'data' in out:
        for dt in out['data']:
            platform = 'android'
            group = '000'
            if 'name' in dt:
                if 'IOS' in dt['name']:
                    platform = 'ios'
                for name in ['ROW', 'US', 'ME', 'T1OTHER']:
                    if name in dt['name'].upper():
                        group = name
                        break
                if 'AT' in dt['name']:
                    group = 'ROW'
                if 'GB' in dt['name'] or 'RU' in dt['name'] or 'DE' in dt['name'] or 'FR' in dt['name'] or 'NO' in dt['name'] or 'NO' in dt['name'] or 'CA' in dt['name']:
                    group = 'T1OTHER'
            if group not in outs_roas:
                outs_roas[group] = {}
            if platform not in outs_roas[group]:
                outs_roas[group][platform] = []

            if group not in outs_bid_amount:
                outs_bid_amount[group] = {}
            if platform not in outs_bid_amount[group]:
                outs_bid_amount[group][platform] = []

            if 'bid_amount' in dt and isinstance(dt['bid_amount'], int):
                outs_bid_amount[group][platform].append(dt['bid_amount']/100.0)
            elif 'bid_constraints' in dt and 'roas_average_floor' in dt['bid_constraints'] and isinstance(dt['bid_constraints']['roas_average_floor'], int):
                outs_roas[group][platform].append(dt['bid_constraints']['roas_average_floor']/10000.0)
    if 'paging' in out and 'next' in out['paging']:
        get_adsets_data(out['paging']['next'], url)


def insert_mongo(res_lst):
    client = MongoClient(os.environ['mongo_url'], maxPoolSize=200)
    db = client.get_database("ai_explore_prod_2")
    log_maker.logger.info('insert into roas_bidamount')
    for res in res_lst:
        db.roas_bidamount.insert(res)
    log_maker.logger.info('insert %s data' % (str(len(res_lst))))
    client.close()


def main():
    outs_roas.clear()
    outs_bid_amount.clear()
    log_maker.logger.info('entry...')
    try:
	ACCOUNTS = os.environ['ACCOUNTS'].split(',')
        for actid in ACCOUNTS:
            fb_url = FB_URL.format(actid, os.environ['access_token'])
            get_adsets_data(fb_url, url=os.environ['purl'])
        res_lst = list()
        log_maker.logger.info('pull data is ok...')
        for group in ['ROW', 'US', 'ME', 'T1OTHER']:
            for platform in ['ios', 'android']:
                roas = {}
                bid_amount = {}
                if group in outs_roas and platform in outs_roas[group]:
                    data = np.array(outs_roas[group][platform])
                    roas = {'min': round(np.min(data), 3),
                            'max': round(np.max(data), 3),
                            'median': round(np.median(data), 3),
                            'mean': round(np.mean(data), 3),
                            '1_quantile': round(np.percentile(data, 25), 3),
                            '3_quantile': round(np.percentile(data, 75), 3)
                            }
                if group in outs_bid_amount and platform in outs_bid_amount[group]:
                    data = np.array(outs_bid_amount[group][platform])
                    bid_amount = {'min': round(np.min(data), 2),
                                  'max': round(np.max(data), 2),
                                  'median': round(np.median(data), 2),
                                  'mean': round(np.mean(data), 2),
                                  '1_quantile': round(np.percentile(data, 25), 2),
                                  '3_quantile': round(np.percentile(data, 75), 2)
                                  }
                res_lst.append({
                    'group': group,
                    'platform': platform,
                    'roas': roas,
                    'bid_amount': bid_amount,
                    'weekday': datetime.datetime.fromtimestamp(int(time.time()), pytz.timezone('Asia/Shanghai')).weekday()+1,
                    'date': datetime.datetime.fromtimestamp(int(time.time()), pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d'),
                    'create_at': datetime.datetime.fromtimestamp(int(time.time()), pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')
                })
        log_maker.logger.info('pull data over...')
        insert_mongo(res_lst)
        log_maker.logger.info('waiting next...')
        print('OK...')
    except Exception as e:
        print(str(e))
        log_maker.logger.info(str(e))


if __name__ == '__main__':
  main()
