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
    crude_trade0=QuantTrader(pair="crude_oil",igconnector=igconnector,days="*",hours="*",minutes="*/10",monitor_min="*/1",sec_offset=10)
    #sp500_arbitrage=QuantTrader(target="SPTRD",igconnector=igconnector,days="0",hours="0",minutes="10",sec_offset=5)

    #ftse_arbitrage=QuantTrader(target="FTSE",igconnector=igconnector,days="0",hours="0",minutes="12",sec_offset=5)

    scheduler = BlockingScheduler()
    scheduler.add_job(refresh_connection,trigger='cron',day_of_week="0",hour=0,minute=9,second=30,jitter=10,timezone="UTC")
    scheduler.start()


