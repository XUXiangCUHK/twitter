# -*- coding: utf-8 -*-
"""
Created on Thu Aug 08 16:08:28 2019

@author: Neal Long
"""

import tweepy
import pytz
from pytz import timezone
import csv
from contextlib import closing
from datetime import datetime
import pymysql
import re
import time

localtz = timezone('Asia/Shanghai')

consumer_key="uSFYWOZMzAVXKWQtjwwfWG3C7"
consumer_secret = "yU9rFO4fSFIJSvehvACjvax5Bq5XUSNvKzjxJ4aRXlh9ckqnFH"
access_token = '1165139632914337793-o32IFjH5ldXh59Rgraq9EGtESmDgXb'
access_token_secret = 'jF9HDPlKLywvelKMT79Xc2bpz6aFNoyGqAsWcM82tdMdn'

#提交你的Key和secret
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

#获取类似于内容句柄的东西
api = tweepy.API(auth)

mysql_host = 'rm-wz9lh12zwnbo4b457ro.mysql.rds.aliyuncs.com'
mysql_port = 3306
mysql_user = 'aiden_restricted'
mysql_password = 'j5!eHbW_PNJ0lf)PwpwZqoc@i8tw#UmE'

hkStockRe = re.compile(r'[$(（\s](\d{3,})[:：\s.\-—]{0,3}hk|[$( ]hk[:：\s.\-—]{0,3}(\d{3,})',re.IGNORECASE)

def get_user_tweets(uname="Ashillumination"):
    tweets=list()
    for tweet in tweepy.Cursor(api.user_timeline,id=uname,tweet_mode="extended").items():
        create_at = str(tweet.created_at.replace(tzinfo=pytz.utc).astimezone(localtz))
        post_time = datetime.strptime(create_at[:19],'%Y-%m-%d %H:%M:%S')
        tweets.append({'post_time': post_time, 'platform': 'twitter', 'source_name': uname, 'post_id': str(tweet.id_str), 'url': None, 'content': tweet.full_text})
    return tweets

def push_to_mysql(news_collection):
    if news_collection:
        mysql_connection = pymysql.connect(host=mysql_host, port=mysql_port, user=mysql_user, passwd=mysql_password)
        for news in news_collection:
            with closing(mysql_connection.cursor()) as cursor:
                cursor.execute('''INSERT IGNORE INTO `streaming`.`twitter_short_raw`
                    (`post_time`, `platform`, `source_name`, `post_id`, `url`, `content`)
                    VALUES
                    (%(post_time)s, %(platform)s, %(source_name)s, %(post_id)s, %(url)s, %(content)s);''', news)
                mysql_connection.commit()
        mysql_connection.close()

def push_to_second(result):
    if result:
        mysql_connection = pymysql.connect(host=mysql_host, port=mysql_port, user=mysql_user, passwd=mysql_password)
        with closing(mysql_connection.cursor()) as cursor:
            cursor.executemany('''INSERT IGNORE INTO `streaming`.`twitter_short`
                (`post_time`, `post_id`, `agent`, `stock_id`, `market`, `is_initial_short`, `second_since_initial_short`)
                VALUES
                (%(post_time)s, %(post_id)s, %(agent)s, %(stock_id)s, %(market)s, %(is_initial_short)s, %(second_since_initial_short)s);''', result)
            mysql_connection.commit()
        mysql_connection.close()

def push_to_test(result):
    if result:
        mysql_connection = pymysql.connect(host=mysql_host, port=mysql_port, user=mysql_user, passwd=mysql_password)
        with closing(mysql_connection.cursor()) as cursor:
            cursor.executemany('''INSERT IGNORE INTO `streaming`.`twitter_us_market_test`
                (`post_time`, `post_id`, `agent`, `stock_id`, `market`, `is_initial_short`, `second_since_initial_short`)
                VALUES
                (%(post_time)s, %(post_id)s, %(agent)s, %(stock_id)s, %(market)s, %(is_initial_short)s, %(second_since_initial_short)s);''', result)
            mysql_connection.commit()
        mysql_connection.close()

def push_to_short_test(result):
    if result:
        mysql_connection = pymysql.connect(host=mysql_host, port=mysql_port, user=mysql_user, passwd=mysql_password)
        with closing(mysql_connection.cursor()) as cursor:
            cursor.executemany('''INSERT IGNORE INTO `streaming`.`twitter_short_test`
                (`post_time`, `post_id`, `agent`, `stock_id`, `market`)
                VALUES
                (%(post_time)s, %(post_id)s, %(agent)s, %(stock_id)s, %(market)s);''', result)
            mysql_connection.commit()
        mysql_connection.close()

def get_hk(agent = 'gmtresearch'):
    records=set()
    coverDict=dict()
    result=list()

    mysql_connection = pymysql.connect(host=mysql_host, port=mysql_port, user=mysql_user, passwd=mysql_password)
    with closing(mysql_connection.cursor()) as cursor:
        cursor.execute("SELECT * FROM streaming.twitter_short_raw WHERE source_name = '{}';".format(agent))
        select_query_result = cursor.fetchall()
        
        for item in select_query_result:
            text = item[7]
            hkStks = list()
            for groups in hkStockRe.finditer(text):
                if groups.group(1):
                    hkStks.append(int(groups.group(1)))
                if groups.group(2):
                    hkStks.append(int(groups.group(2)))
            for stk in hkStks:
                post_id = item[5]
                user_id = item[4]
                post_time = item[2]

                records.add((user_id,post_id,post_time,stk))
                curDt = coverDict.get(stk,None)
                if not curDt or post_time<curDt:
                    coverDict[stk]=post_time

        for user_id,post_id,post_time,stk in records:
            lag_secs = (post_time-coverDict[stk]).total_seconds()
            Is_intialShort = 1 if lag_secs==0 else 0
            stock = '{:05d}'.format(stk)
            result.append({'post_time': post_time, 'post_id': post_id, 'agent': user_id, 'stock_id': stock, 'market': 'HK', 'is_initial_short': Is_intialShort, 'second_since_initial_short': lag_secs})
    mysql_connection.close()
    return result

def get_US(agent):
    records=set()
    coverDict=dict()
    result=list()
    USStockRe = re.compile(r'\$([a-zA-Z]{1,5})[^a-zA-Z]{0,1}')
    LongRe = re.compile(r'(?i)long[a-z]* \$([a-zA-Z]{1,5})')

    mysql_connection = pymysql.connect(host=mysql_host, port=mysql_port, user=mysql_user, passwd=mysql_password)
    with closing(mysql_connection.cursor()) as cursor:
        cursor.execute("SELECT * FROM streaming.twitter_short_raw WHERE source_name = '{}';".format(agent))
        select_query_result = cursor.fetchall()
        
        for item in select_query_result:
            text = item[7]
            USStks = list()
            LongStks = list()
            ShortStks = list()
            for groups in USStockRe.finditer(text):
                if groups.group(1):
                    USStks.append(groups.group(1).upper())
            for groups in LongRe.finditer(text):
                if groups.group(1):
                    LongStks.append(groups.group(1).upper())
            ShortStks = [x for x in USStks if x not in LongStks]
    
            for stk in ShortStks:
                post_id = item[5]
                user_id = item[4]
                post_time = item[2]

                records.add((user_id,post_id,post_time,stk.upper()))
                curDt = coverDict.get(stk.upper(),None)
                if not curDt or post_time<curDt:
                    coverDict[stk.upper()]=post_time

        for user_id,post_id,post_time,stk in records:
            lag_secs = (post_time-coverDict[stk.upper()]).total_seconds()
            Is_intialShort = 1 if lag_secs==0 else 0
            result.append({'post_time': post_time, 'post_id': post_id, 'agent': user_id, 'stock_id': stk.upper(), 'market': 'US', 'is_initial_short': Is_intialShort, 'second_since_initial_short': lag_secs})
    mysql_connection.close()
    return result

def get_US_test():
    records=set()
    coverDict=dict()
    result=list()
    USStockRe = re.compile(r'\$([a-zA-Z]{1,5})[^a-zA-Z]{0,1}')

    mysql_connection = pymysql.connect(host=mysql_host, port=mysql_port, user=mysql_user, passwd=mysql_password)
    with closing(mysql_connection.cursor()) as cursor:
        cursor.execute('SELECT * FROM streaming.twitter_short_raw;')
        select_query_result = cursor.fetchall()
        
        for item in select_query_result:
            text = item[7]
            USStks = list()
            for groups in USStockRe.finditer(text):
                if groups.group(1):
                    USStks.append(groups.group(1))
            for stk in USStks:
                post_id = item[5]
                user_id = item[4]
                post_time = item[2]

                records.add((user_id,post_id,post_time,stk))
                curDt = coverDict.get(stk,None)
                if not curDt or post_time<curDt:
                    coverDict[stk]=post_time

        for user_id,post_id,post_time,stk in records:
            result.append({'post_time': post_time, 'post_id': post_id, 'agent': user_id, 'stock_id': stk, 'market': 'US'})
    mysql_connection.close()
    return result

agents_strict_a = ['muddywatersre', 'citronresearch', 'gmtresearch', 'gothamresearch', 'glaucusresearch', 'blueorcainvest', 'BonitasResearch', 'ActivistShorts', 'anonanalytics', 'icebergresear', 'triamresearch', 'geoinvesting', 'ResearchGrizzly', 'WolfpackReports']
agents_strict_b = ['Ashillumination', 'Jcap_Research']
agents_strict_at = ['PresciencePoint', 'BreakoutPoint', 'HindenburgRes', 'WallStCynic', 'investorsbeware', 'AureliusValue', 'FriendlyBearSA']
agents_strict_follow = ['FuzzyPandaShort', 'viceroyresearch', 'GlassH_Research', 'probesreporter', 'MostShorted', 'BatmanResearch', 'FraudResearch']
agents_strict = agents_strict_a + agents_strict_b + agents_strict_at + agents_strict_follow

for agent in agents_strict:
    result = get_US(agent)
    push_to_second(result)

# more_user = agents_strict_follow + agents_lvl2_follow + agents_lvl3_follow
# for user in more_user:
#     result = get_user_tweets(user)
#     # print(result)
#     push_to_mysql(result)

# result = get_US_test()
# #print(result)
# push_to_short_test(result)

# class following():
#     def __init__(self):
#         self.dict_id = dict()

#         self.consumer_key="uSFYWOZMzAVXKWQtjwwfWG3C7"
#         self.consumer_secret = "yU9rFO4fSFIJSvehvACjvax5Bq5XUSNvKzjxJ4aRXlh9ckqnFH"
#         self.access_token = '1165139632914337793-o32IFjH5ldXh59Rgraq9EGtESmDgXb'
#         self.access_token_secret = 'jF9HDPlKLywvelKMT79Xc2bpz6aFNoyGqAsWcM82tdMdn'

#         self.auth = tweepy.OAuthHandler(self.consumer_key, self.consumer_secret)
#         self.auth.set_access_token(self.access_token, self.access_token_secret)

#         self.api = tweepy.API(self.auth)

#     def get_followings(self, uname="muddywatersre"):
#         # for friend in tweepy.Cursor(api.friends, id=uname, count=50).items():
#         #     print(friend.screen_name)
#         # users = api.friends(id=uname)
#         # for user in users:
#         #     print(user.screen_name)
#         users = self.api.friends_ids(id=uname)
#         #print(users)
#         print(uname, len(users))
#         for user in users:
#             # item = api.get_user(id=user)
#             # print(item.screen_name)
#             count = self.dict_id.get(user, None)
#             if count:
#                 self.dict_id.update({user: count+1})
#             else:
#                 self.dict_id.update({user: 1})
    
# if __name__ == "__main__":
#     f = following()
#     # # user_list = ['muddywatersre', 'Ashillumination']
#     # user_list_a = ['muddywatersre','citronresearch','gmtresearch','gothamresearch','glaucusresearch','blueorcainvest','BonitasResearch','ActivistShorts','anonanalytics','triamresearch','geoinvesting','ResearchGrizzly','WolfpackReports','Ashillumination','Jcap_Research']
#     # user_list_b = ['PresciencePoint','BreakoutPoint','BucephResearch','HindenburgRes','IcebergResear','WallStCynic','investorsbeware','KerrisdaleCap','majgeoinvesting','AlderLaneeggs','sprucepointcap','LongShortTrader','doumenzi','QTRResearch','MoxReports','AureliusValue','TheChinaHustle','FriendlyBearSA','DonutShorts','carnesjon','discountinvestr','seaninasia','ShortSightedCap']
#     # user_list = user_list_a + user_list_b
#     # for item in user_list:
#     #     try:
#     #         f.get_followings(item)
#     #         time.sleep(65)
#     #     except:
#     #         print('First sleeping')
#     #         time.sleep(900)
#     #         f.get_followings(item)
#     #         time.sleep(65)


#     # csv_columns = ['user_id', 'follower_count']
#     # with open('twitter_following.csv', 'wb') as csvfile:
#     #     writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
#     #     writer.writeheader()
#     #     for k in f.dict_id:
#     #         writer.writerow({'user_id': k, 'follower_count': f.dict_id[k]})

#     dict_name = dict()
#     with open('twitter_following.csv', 'r') as csv_file:
#         table = csv.reader(csv_file)
#         for row in table:
#             if int(row[1]) > 10:
#                 try:
#                     name = f.api.get_user(id = row[0]).screen_name
#                     dict_name.update({name: row[1]})
#                 except:
#                     print('there is some problem', row[0])
#         print(dict_name)

#     # csv_columns = ['user_name', 'follower_count']
#     # with open('name_twitter_following.csv') as csvfile:
#     #     writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
#     #     writer.writeheader()
#     #     for k in dict_name:
#     #         writer.writerow({'user_name': k, 'follower_count': dict_name[k]})

#     a_file = open("name_twitter_following.csv", "w", newline = '')
#     writer = csv.writer(a_file)
#     for key, value in dict_name.items():
#         writer.writerow([key, value])

#     a_file.close()

#     # import pandas as pd
#     # excel_file = 'order_followings.xlsx'
#     # df_file = pd.read_excel(excel_file)
#     # #print(df_file.loc[[128]])
#     # df_head = df_file[df_file.follower_count>12]
#     # #print(df_head)
#     # dict_id = df_head.to_dict('records')
#     # #print(dict_id)

#     # dict_name = dict()
#     # for d in dict_id:
#     #     try:
#     #         name = f.api.get_user(id=d['user_id']).screen_name
#     #         dict_name.update({name: d['follower_count']})
#     #     except:
#     #         print('there is some problem', d)

#     # print(dict_name)
