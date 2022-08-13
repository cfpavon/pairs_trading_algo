import os
from constants import *
from QuantTrader import *
from IGConnector import *
import credentials
from apscheduler.schedulers.background import BackgroundScheduler, BlockingScheduler




igconnector=IGConnector(credentials.account_id,credentials.acc_password,credentials.api_key,credentials.acc_environment)   
session_details=igconnector.create_ig_session()
print("\t"+str(session_details)+"\n")



def refresh_connection():
    global igconnector
    session_details=igconnector.create_ig_session()
    print("\t"+"UPDATED THE CONNECTION"+"\n")
    print("\t"+str(session_details)+"\n")


if __name__ == "__main__":

    #scheduler = BackgroundScheduler()
    #scheduler.add_job(refresh_connection,trigger='cron',day="*",hour=23,minute=58,second=30,jitter=10,timezone="UTC")
    #scheduler.start()

    crude_trade0=PairsTrader(pair="crude_oil",igconnector=igconnector,days="0,1,2,3,4",hours="*",minutes="*/30",monitor_min="2-59/5",monitoring=True,sec_offset=10)

    #min_list=[*range(1,10,1)]+[*range(11,20,1)]+[*range(21,30,1)]+[*range(31,40,1)]+[*range(41,50,1)]+[*range(51,60,1)]
    #lst = list(map(str, min_list))
    min_string=","

    lst="1,6,11,16,21,26,31,36,41,46,51,56"
    

    narus_arbitrage=PairsTraderIDX(pair="narus",igconnector=igconnector,days="0,1,2,3,4",hours="*",minutes="2",monitor_min="3-59/5",monitoring=True,sec_offset=10)
    #min_list=[*range(1,10,1)]+[*range(11,20,1)]+[*range(21,30,1)]+[*range(31,40,1)]+[*range(41,50,1)]+[*range(51,60,1)]
    #lst = list(map(str, min_list))
    #min_string=","
    #lsd="1,2,3,4,6,7,8,9,11,12,13,14,16,17,18,19,21,22,23,24,26,27,28,29,31,32,33,34,36,37,38,39,41,42,43,44,46,47,48,49,51,52,53,54,56,57,58,59"
    #print(min_string.join(lst))
    btceth_arbitrage=PairsTraderIDX(pair="btceth",igconnector=igconnector,days="*",hours="*/4",minutes="0",monitor_min=lst,monitoring=True,sec_offset=10)


    #ftse_arbitrage=QuantTrader(target="FTSE",igconnector=igconnector,days="0",hours="0",minutes="12",sec_offset=5)

    scheduler = BlockingScheduler()
    scheduler.add_job(refresh_connection,trigger='cron',day_of_week="0",hour="*/8",minute=9,second=30,jitter=10,timezone="UTC")
    scheduler.start()


