import os
import json
import time
import requests
import MySQLdb

import telepot
import urllib3


def my_long_running_process():
        #Telegram Setup
        proxy_url = "http://proxy.server:3128"
        telepot.api._pools = {
                'default': urllib3.ProxyManager(proxy_url=proxy_url, num_pools=3, maxsize=10, retries=False, timeout=30),
        }
        telepot.api._onetime_pool_spec = (urllib3.ProxyManager, dict(proxy_url=proxy_url, num_pools=1, maxsize=1, retries=False, timeout=30))
        token = '506594994:AAG2AY7sTI7wwPtVm-Qm1SIGwOfDCHgH9d8'
        TelegramBot = telepot.Bot(token)

        # dir="/home/nikhilaggarwal/markets/"
        #os.chdir(dir)
        timer = 0
        conn = MySQLdb.connect(host= "nikhilaggarwal.mysql.pythonanywhere-services.com",user="nikhilaggarwal",passwd="nikhil123agg",db="nikhilaggarwal$main")
        DB = conn.cursor()
        while True:
                if ( timer == 360 or timer == 0 ) :
                        timer = 0
                        link1 = "https://api.coinmarketcap.com/v1/ticker/?limit=0"
                        f1 = requests.get(url = link1)
                        data = f1.text
                        data1 = data.replace("null","0")
                        fetchTime = int(time.time())
                        deleteData = fetchTime - 172800
                        #print data1
                        with open('rank.json','a+') as f1:
                                config  = json.loads(data1)
                                config1 = json.loads(data1)
                                myList1 = config1
                                for x in range(0,len(myList1)):
                                        id = config[x]["id"]
                                        name = config[x]["name"]
                                        symbol = config[x]["symbol"]
                                        rank = config[x]["rank"]
                                        price_usd = config[x]["price_usd"]
                                        price_btc = config[x]["price_btc"]
                                        h24_volume_usd = config[x]["24h_volume_usd"]
                                        market_cap_usd = config[x]["market_cap_usd"]
                                        available_supply = config[x]["available_supply"]
                                        total_supply = config[x]["total_supply"]
                                        percent_change_1h = config[x]["percent_change_1h"]
                                        percent_change_24h = config[x]["percent_change_24h"]
                                        percent_change_7d = config[x]["percent_change_7d"]
                                        last_updated = config[x]["last_updated"]
                                        #print config[x]
                                        #print "-----------"
                                        #print id
                                        #print name
                                        #print symbol
                                        #print rank
                                        #print price_usd
                                        #print price_btc
                                        #print h24_volume_usd
                                        #print market_cap_usd
                                        #print available_supply
                                        #print total_supply
                                        #print percent_change_1h
                                        #print percent_change_24h
                                        #print percent_change_7d
                                        #print last_updated
                                        #print "---------------"
                                        DB.execute("""INSERT INTO coinmarketcap VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(id,name,symbol,int(rank),float(price_usd),float(price_btc),float(h24_volume_usd),float(market_cap_usd),float(available_supply),float(total_supply),float(percent_change_1h),float(percent_change_24h),float(percent_change_7d),last_updated,fetchTime))
                                        conn.commit()
                                #Not saving into file.. as loading directly into database
                                #f1.write(data1)
                                f1.close()
                        DB.execute("""Delete from coinmarketcap where fetchTime <= %s""", [deleteData] )
                        conn.commit()

                link2 = "https://bittrex.com/api/v1.1/public/getmarketsummaries"
                f2 = requests.get(url = link2)
                data2 = f2.text
                fetchTime = int(time.time())
                #Delete data older than 2 days
                deleteData = fetchTime - 172800
                #print data2
                with open('bittrex.json','a+') as f2:
                        config  = json.loads(data2)
                        config1 = json.loads(data2)
                        myList2 = config1["result"]
                        for x in range(0,len(myList2)):
                                MarketName = config["result"][x]["MarketName"].encode('utf-8')
                                High = config["result"][x]["High"]
        #dir="/home/nikhilaggarwal/markets/"
                                Low = config["result"][x]["Low"]
                                Volume = config["result"][x]["Volume"]
                                Last = config["result"][x]["Last"]
                                BaseVolume = config["result"][x]["BaseVolume"]
                                TimeStamp = config["result"][x]["TimeStamp"].encode('utf-8')
                                Bid = config["result"][x]["Bid"]
                                Ask = config["result"][x]["Ask"]
                                OpenBuyOrders = config["result"][x]["OpenBuyOrders"]
                                OpenSellOrders = config["result"][x]["OpenSellOrders"]
                                PrevDay = config["result"][x]["PrevDay"]
                                Created = config["result"][x]["Created"].encode('utf-8')
                                DB.execute("""INSERT INTO bittrex VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(MarketName,float(High),float(Low),Volume,Last,BaseVolume,TimeStamp,Bid,Ask,OpenBuyOrders,OpenSellOrders,PrevDay,Created,fetchTime))
                                conn.commit()
                        #Not saving into file.. as loading directly into database
                        #f2.write(data2)
                        f2.close()
                DB.execute("""Delete from bittrex where fetchTime <= %s""", [deleteData] )
                conn.commit()

                lastOffset=0
                DB.execute("SELECT MAX(offSetId) from users")
                lastOffset = DB.fetchall()
                if lastOffset is None:
                        lastOffset=-1
                updates = TelegramBot.getUpdates(lastOffset+1)
                #print con.message
                for update in updates:
                    print update
                    text = update["message"]["text"]
                    chatId = update["message"]["from"]["id"]
                    lastOffset = update["update_id"]
                    if text == "/start" or text == "start":
                        DB.execute("SELECT * from users where chatId=%s",chatId)
                        data = DB.fetchall()
                        if data is None:
                            TelegramBot.sendMessage(chatId,"I have added you in my notification list. \nWhile my father is building my algorithm, I would like you to remain calm if I don't respond to your queries.");
                            DB.execute("""INSERT INTO users (chatId, category, offSetId) VALUES (%s,%s,%s)""", chatId , "g" , lastOffset)
                            conn.commit()
                        else:
                            TelegramBot.sendMessage(chatId,"You are already there in my mind and will be notified whenever a new market is added. \nPlease don't poke me when I am learning new things.");


                DB.execute("SELECT marketname,volume,bid,ask,openbuyorders,opensellorders FROM bittrex group by marketname having count(marketname)=1")
                newMarkets = DB.fetchall()
                market="Bittrex"
                print newMarkets
                DB.execute("SELECT distinct(chatId) from users where category=\"g\"")
                chatIds = DB.fetchall()
                for chatId in chatIds:
                    for newMarket in newMarkets:
                            TelegramBot.sendMessage(chatId,market + "\nNew Market Added\nMarket Name : "+str(newMarket[0])+"\nVolume : "+str(newMarket[1])+"\nBid : "+str(newMarket[2])+"\nAsk : "+str(newMarket[3])+"\nOpen Buy Orders : "+str(newMarket[4])+"\nOpen Sell Orders : "+str(newMarket[5]))

                timer = timer + 1
                time.sleep(60)
my_long_running_process()