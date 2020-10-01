import elly_func
import json
import random
import tweepy
from sys import argv
from datetime import datetime
from collections import Counter as Counter

output_file = 'task3.csv'

def pick_top(count: list, n):
    temp = []
    for i in count:
        if i[1] not in temp:
            temp.append(i[1])

    result = []
    top_candidate = temp[0:n]
    for i in count:
        if i[1] in top_candidate:
            result.append(i)
    return result


class MyStreamListener(tweepy.StreamListener):
    def __init__(self):
        super(MyStreamListener, self).__init__()
        # description說暫存最多100個tweets
        self.Tweets_you_found = [] # --> 瀏覽過的所有字
        self.hashtag_count = Counter()
        self.tweet_num_received = 0 # --> 接收過幾回合


# on status is the method will be called when a tweet is downloaded
    def on_status(self, status):
        print(status.text)

    def on_data(self, data):
        tags  = json.loads(data)

        # 每單位秒進新值接收之框格
        window = []
        # 先進到hashtags層 篩掉空集合
        if tags['entities']['hashtags'] != []:
        # 篩掉不是英文的，不然太麻煩了，把每個詞丟進框格裡
            for i in tags['entities']['hashtags']:
                if i['text'].encode('UTF-8').isalpha():
                    window.append(i['text'])
        # 有可能甚麼都沒取到
        if window != []:
            self.Tweets_you_found.append(window)
            self.tweet_num_received += 1

        # 若框格中有收到字
        if len(window) > 0:
        # 若框格中的存量達100臨界值，要拋棄一個字，並且存新的字
            if len(self.Tweets_you_found) > 100:
                # 隨機拋棄!
                tweet_to_pop = random.randint(0, 100)
                self.Tweets_you_found.pop(tweet_to_pop)
            # 收到過的所有字
            temp = []
            for i in self.Tweets_you_found:
                for j in i:
                    temp.append(j)
            # 計算所有收過字數
            count = dict(Counter(temp))

            #顯示出排名最高的前三tweets
            order = sorted(count.items(), key=lambda key: (-key[1], key[0]))
            top_3 = pick_top(order, 3)
            print(self.tweet_num_received, top_3)
            output_format(top_3, self.tweet_num_received)

        return 1

# handles errors returned by the API
    def on_error(self, status):
        print(status)
        return True

def output_format(top_3,tweet_num):
    with open(output_file, 'a') as f:
        f.write(f'The number of tweets with tags from the beginning: {tweet_num}\n')
        for hashtags, support in top_3:
            f.write(f'{hashtags} : {support}\n')
        f.write('\n')

# 申請的token and secret
c_token = 'EGpklsC4rB2kYOfVdLqdvAadE'
c_secret = 'rSXggA7R6LB3FmJofgslxISX9X95NwsG0zCLOBuF4Mzdo1bkCf'

access_key = '1252782312413990913-fzc4MCxGwRSNg47eqXizp0ZiS2WV42'
access_secret = 'pmlNvkqrYpvXt11Z5NNapJftqVGCWxWat2bSeERylXUsH'

auth = tweepy.OAuthHandler(c_token, c_secret)
auth.set_access_token(access_key, access_secret)

# Copied from tweepy's documents
api = tweepy.API(auth)
api.configuration()

myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)

myStream.filter(track=['#'])
