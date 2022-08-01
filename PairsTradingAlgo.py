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

    #crude_trade0=PairsTrader(pair="crude_oil",igconnector=igconnector,days="0,1,2,3,4",hours="*",minutes="*/30",monitor_min="*/5",sec_offset=10)

    #narus_arbitrage=PairsTraderIDX(pair="narus",igconnector=igconnector,days="0,1,2,3,4",hours="*",minutes="*/5",monitor_min="*/1",sec_offset=10)

    btceth_arbitrage=PairsTraderIDX(pair="btceth",igconnector=igconnector,days="*",hours="*",minutes="*/5",monitor_min="*/1",sec_offset=10)


    #ftse_arbitrage=QuantTrader(target="FTSE",igconnector=igconnector,days="0",hours="0",minutes="12",sec_offset=5)

    scheduler = BlockingScheduler()
    scheduler.add_job(refresh_connection,trigger='cron',day_of_week="0",hour=0,minute=9,second=30,jitter=10,timezone="UTC")
    scheduler.start()


