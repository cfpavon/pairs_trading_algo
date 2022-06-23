import os
from constants import *
from QuantTrader import *
from IGConnector import *
import credentials
from apscheduler.schedulers.background import BackgroundScheduler
from DataStreamer import DataStreamer



#igconnector=IGConnector(credentials.account_id,credentials.acc_password,credentials.api_key,credentials.acc_environment)   
#session_details=igconnector.create_ig_session()
#print("\t"+str(session_details)+"\n")

#crude_trade0=QuantTrader(pair="crude_oil",igconnector=igconnector,hours=None,minutes="*/1",sec_offset=10)
#crude_trade1=QuantTrader(pair="narus",igconnector=igconnector,hours=None,minutes="*/1",sec_offset=0)


def update_how_happy(streamer_df):
    print(streamer_df.shape[0])

streamer=DataStreamer()

streamer.bind_to(update_how_happy)

streamer.main1()

















#def refresh_connection():

#     global igconnector
#     #global crude_trade0
#     #global crude_trade1

#     session_details=igconnector.create_ig_session()
#     print("\t"+str(session_details)+"\n")

#     #crude_trade0.igconnector=igconnector
#     #crude_trade1.igconnector=igconnector


#if __name__ == "__main__":

#    igconnector=IGConnector(credentials.account_id,credentials.acc_password,credentials.api_key,credentials.acc_environment)   
    

#    crude_trade0=QuantTrader(pair="crude_oil",igconnector=igconnector,hours=None,minutes="*/1",sec_offset=10)
#    #crude_trade1=QuantTrader(pair="narus",igconnector=igconnector,hours=None,minutes="*/1",sec_offset=0)

#    scheduler = BackgroundScheduler()

#    scheduler.add_job(refresh_connection, trigger='cron',minute='*/4',second=55,jitter=2,timezone="UTC")
#    scheduler.start()

#crude_trade=QuantTrader(pair="narus",hours=None,minutes="*/5",sec_offset=0)

##indices_trade=QuantTrader(pair="narus",hours="*/4",minutes=0,sec_offset=0)



