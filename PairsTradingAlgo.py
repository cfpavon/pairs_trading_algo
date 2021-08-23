import os
import sys
import pandas as pd
import json
# insert at 1, 0 is the script path (or '' in REPL)

##from trading_ig.rest import IGService
from datetime import datetime
from constants import *
from DataReader import DataReader
from constants import marketIds, data_filename, marketinfo_filename, watchlist_id, epics,market_names,open_positions,epics_ids,ids_epics
from constants import trading_parameters
from constants import account_id,acc_password,api_key,acc_environment
from QuantTrader import *
from IGConnector import *
from datetime import timezone
import time

import threading

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.background import BlockingScheduler



###################################################################################

def open_trade_positions():

    data_reader=DataReader(epics,market_names,marketIds)
    trade_df=data_reader.get_prices_df()
    print(trade_df.tail(5))

    igconnector=IGConnector(account_id,acc_password,api_key,acc_environment)
    session_details=igconnector.create_ig_session()
    print(session_details)


    trade_w_df,marketinfo_df=data_reader.make_wide(trade_df)

    print(marketinfo_df.head(2))

    data_pca=trade_w_df[[marketIds[market_names[0]]+"_return",marketIds[market_names[1]]+"_return"]].iloc[-120:]
    data_pca.columns=data_pca.columns.get_level_values(0)
    #print(data_pca.columns)
    print(data_pca.head(2))


    quant_trader= QuantTrader(epics,market_names,marketIds)
    pca_res=quant_trader.calculate_pca(data=data_pca.iloc[-120:])
    pca_res1=quant_trader.calculate_size(pca_res)
    pca_res2=quant_trader.calculate_score(pca_res1)

    print(pca_res2.columns)  
    print(pca_res2)   

    name1=marketIds[market_names[0]]
    print(name1)

    order_size=pca_res2[name1+"_size"].iloc[0]*marketinfo_df[marketinfo_df.marketId==name1].minSize.iloc[0]
    print(pca_res2[name1+"_size"].iloc[0])
    print(order_size)

    #print(float(marketinfo_df[marketinfo_df.marketId==name1].pipValue.iloc[0]))

    stop_distance1=(350.0*marketinfo_df[marketinfo_df.marketId==name1].exchangeRate.iloc[0])/(float(marketinfo_df[marketinfo_df.marketId==name1].pipValue.iloc[0])*order_size)
    my_currency1=marketinfo_df[marketinfo_df.marketId==name1].currency.iloc[0]

    trade_order1={"direction":"SELL","epic":epics[market_names[0]],"size":order_size,"currency":my_currency1,"stop_distance":None}

    name2=marketIds[market_names[1]]
    print(name2)

    order_size=pca_res2[name2+"_size"].iloc[0]*marketinfo_df[marketinfo_df.marketId==name2].minSize.iloc[0]

    print(pca_res2[name2+"_size"].iloc[0])
    print(order_size)

    stop_distance2=(350.0*marketinfo_df[marketinfo_df.marketId==name2].exchangeRate.iloc[0])/(float(marketinfo_df[marketinfo_df.marketId==name2].pipValue.iloc[0])*order_size)
    my_currency2=marketinfo_df[marketinfo_df.marketId==name2].currency.iloc[0]

    trade_order2={"direction":"BUY","epic":epics[market_names[1]],"size":order_size,"currency":my_currency2,"stop_distance":None}

    order_dict={name1:trade_order1,name2:trade_order2}
    print(order_dict)



    igconnector.open_paired_position(marketIds=[name1,name2],positions=order_dict,units=[1,1])

##open_trade_positions()

def write_close_positions(new_data, filename='data.json'):

    time_now=datetime.now()
    datenow=datetime(time_now.year,time_now.month,time_now.day,time_now.hour,time_now.minute,0).strftime("%Y/%m/%d %H:%M:%S")
    
    if os.path.isfile(filename):    
        # First we load existing data into a dict.
        file_data = json.load(open(filename))

        # Join new_data with file_data inside emp_details
        file_data[datenow]=new_data
        
        # convert back to json.
        json.dump(file_data, open(filename, 'w' ), indent = 4)

    else:
        file_data={datenow:new_data}
        json.dump(file_data, open(filename, 'w' ), indent = 4)


def close_trade_positions():

    name1="LCO"
    name2="CL"

    igconnector=IGConnector(account_id,acc_password,api_key,acc_environment)
    session_details=igconnector.create_ig_session()
    print(session_details)

    close_position1,close_position2=igconnector.close_paired_position(marketIds=[name1,name2])
            
                
    if bool(close_position1) and  bool(close_position2):
        if close_position1['status']=="CLOSED" and close_position2['status']=="CLOSED" :

            close_positions={name1:close_position1,name2:close_position2}

            write_close_positions(close_positions,"positions_history.json") 
            ##json.dump(close_positions, open( close_trades_file, 'w' )) 
            json.dump({}, open("open_positions.json", 'w' )) 
            print("Close paired positions")



#open_trade_positions()

#time.sleep(30)
#close_trade_positions()



#data_reader=DataReader(epics,market_names,marketIds)


#pca_df=pd.DataFrame()
#igconnector=None
#open_positions_dict={}
#last_datetime_price=[]

def load_datareader(callback): 

     global data_reader
     trade_df=data_reader.get_prices_df()

     print(trade_df.tail(5))
     callback()









def check_open_positions():

    global open_positions_dict
    global igconnector

    positions_dict = json.load( open("./open_positions.json") )

    ##igconnector=IGConnector(account_id,acc_password,api_key,acc_environment)    
    ##session_details=igconnector.create_ig_session()

    marketId1=marketIds[market_names[0]]
    marketId2=marketIds[market_names[1]]

    #dealId1=positions_dict[marketIds[0]]['dealId']
    #dealId2=positions_dict[marketIds[1]]['dealId']    


    if bool(positions_dict):
        dealId1=positions_dict[marketId1]['dealId']
        dealId2=positions_dict[marketId2]['dealId']
        open_positions_dict0=igconnector.get_open_positions_by_dealId([dealId1,dealId2])
        if not bool(open_positions_dict0):
            marketinfo_df=igconnector.fetch_market_details(epics=list(epics.values()))
            ##open_positions_dict={}
        elif len(open_positions_dict0)>1 :
            marketinfo_df=igconnector.fetch_market_details(epics=list(epics.values()))
            open_positions_dict=open_positions_dict0
            


    else:
         marketinfo_df=igconnector.fetch_market_details(epics=list(epics.values()))
         ##open_positions_dict={}


#open_positions_dict=read_open_positions()
#print(open_positions_dict)




def create_quant_indicators():

    global data_reader
    global open_positions_dict
    global pca_df
    global marketinfo_df

    trade_df=data_reader.get_prices_df()
    trade_w_df,marketinfo_df=data_reader.make_wide(trade_df)

    ##print(marketinfo_df.columns)
    ##print(marketinfo_df.head(2))


    data_pca=trade_w_df[[marketIds[market_names[0]]+"_return",marketIds[market_names[1]]+"_return"]].iloc[-240:]
    data_pca.columns=data_pca.columns.get_level_values(0)
    ##print(data_pca.columns)
    print("\n")
    print(data_pca.head(2))
    print("\n")
    print(data_pca.tail(2))
    print("\n")


    quant_trader= QuantTrader(epics,market_names,marketIds)
    pca_res=quant_trader.calculate_pca(data=data_pca.iloc[-trading_parameters['look_out_window']:])
    pca_res1=quant_trader.calculate_size(pca_res)
    pca_res2=quant_trader.calculate_score(pca_res1)
    pca_df=pca_res2

    pca_df['corr']=trade_w_df['corr'].iloc[-1]

    ##print(pca_df.columns)    
    ##print(pca_df.head(2))
    #print(pca_df.columns)

    


    



def create_open_position_dictionaries(paired_positions,name1,name2):

    open_direction={name1:paired_positions[name1]['position']['direction'],name2:paired_positions[name2]['position']['direction']}
    dealIds={name1:paired_positions[name1]['position']['dealId'],name2:paired_positions[name2]['position']['dealId']}
    positions_size={name1:paired_positions[name1]['position']['dealSize'],name2:paired_positions[name2]['position']['dealSize']}
    open_prices={name1:paired_positions[name1]['position']['openLevel'],name2:paired_positions[name2]['position']['openLevel']}
    close_direction={name1:paired_positions[name2]['position']['direction'],name2:paired_positions[name1]['position']['direction']}

    ##print(open_prices)

    current_price1=(paired_positions[name1]['market']['bid']+paired_positions[name1]['market']['offer'])/2
    current_price2=(paired_positions[name2]['market']['bid']+paired_positions[name2]['market']['offer'])/2

    current_prices={name1:current_price1,name2:current_price2}

    ##print(str(current_price1))
    ##print(str(current_price2))

    spread1=(paired_positions[name1]['market']['offer']-paired_positions[name1]['market']['bid'])
    spread2=(paired_positions[name2]['market']['offer']-paired_positions[name2]['market']['bid'])

    ##print(str(spread1))
    ##print(str(spread2))
    spreads={name1:spread1,name2:spread2}
        

            


    position1={"dealId":paired_positions[name1]['position']['dealId'],"direction":paired_positions[name2]['position']['direction'],'size':paired_positions[name1]['position']['dealSize']}
    position2={"dealId":paired_positions[name2]['position']['dealId'],"direction":paired_positions[name1]['position']['direction'],'size':paired_positions[name2]['position']['dealSize']}

    close_dict={name1:position1,name2:position2}

    return open_prices,open_direction,positions_size,current_prices,spreads,close_dict
    
#data_df,pca_res2,marketinfo_df

def make_paired_trades(open_trades_file="open_positions_history.json",close_trades_file="close_positions_history.json",units=[1,1],SL=25.0,TP=45.0):
    
    #paired_positions = json.load( open(open_trades_file) )

    #global open_positions_dict
    global data_reader
    global open_positions_dict
    global pca_df
    global marketinfo_df
    global igconnector
    
    pca_res2=pca_df.copy()
    paired_positions=open_positions_dict

    dealId_epic={}
    dealIds=[]
    name1=None
    name2=None
    epic1=None
    epic2=None

    profit=[]
    
    open_position1={}
    open_position2={}

    close_position1={}
    close_position2={}

    PnL=0
    isOpen=False
    isLong=False
    isShort=False
    
    score=pca_res2["score"].iloc[0]
    correl=pca_res2["corr"].iloc[0]

    print("\t Scores :\t"+str(score)+"\n")
    print("\t Correlation :\t"+str(correl)+"\n")


    if not bool(paired_positions):
        
        name1=marketIds[market_names[0]]
        name2=marketIds[market_names[1]]

        ##print("\t"+name1+"\t"+name2)
        #print(name2)

        epic1=ids_epics[name1]
        epic2=ids_epics[name2]

        tradeable1=marketinfo_df[marketinfo_df.marketId==name1].marketStatus.iloc[0]
        tradeable2=marketinfo_df[marketinfo_df.marketId==name2].marketStatus.iloc[0]

    
    
    
    elif bool(paired_positions):

        ###paired_positions=open_positions_dict

        #name1=market_names[0]
        #name2=market_names[1]

        #id1=marketIds[market_names[0]]
        #id2=marketIds[market_names[1]]
        isOpen=True 

    
        #i=0

        for key in paired_positions:
            dealId_epic[key]=paired_positions[key]['market']['epic']
            #id0=epics_ids[epic0]
            #paired_positions[id0] = paired_positions.pop(key)
            dealIds.append(key)
            #i+=1

        
        epic01=dealId_epic[dealIds[0]]
        epic02=dealId_epic[dealIds[1]]

        if epics_ids[epic01]==marketIds[market_names[0]]:
            name1=epics_ids[epic01]
            name2=epics_ids[epic02]
        elif epics_ids[epic01]==marketIds[market_names[1]]:
            name2=epics_ids[epic01]
            name1=epics_ids[epic02]
        

        epic1=ids_epics[name1]
        epic2=ids_epics[name2]
        
        ##print("\t"+name1+"\t"+name2)
        #print(name1)
        #print(name2)

        paired_positions[name1] = paired_positions.pop(dealIds[0])
        paired_positions[name2] = paired_positions.pop(dealIds[1])



        if (paired_positions[name1]['market']['marketStatus']=="TRADEABLE") and (paired_positions[name2]['market']['marketStatus']=="TRADEABLE"):   
        
                     
            

            open_prices,open_direction,positions_size,current_prices,spreads,close_dict=create_open_position_dictionaries(paired_positions,name1,name2)
              
                      
        
            if open_direction[name1]=="BUY":

                isLong=True
                profit.append((positions_size[name1]*(current_prices[name1]-open_prices[name1]-spreads[name1]/2))/marketinfo_df[marketinfo_df.marketId==name1].exchangeRate.iloc[0])
                profit.append((positions_size[name2]*(open_prices[name2]-current_prices[name2]-spreads[name2]/2))/marketinfo_df[marketinfo_df.marketId==name2].exchangeRate.iloc[0])

        
            elif open_direction[name1]=="SELL":
                
                isShort=True
                profit.append((positions_size[name2]*(current_prices[name2]-open_prices[name2]-spreads[name2]/2))/marketinfo_df[marketinfo_df.marketId==name2].exchangeRate.iloc[0])
                profit.append((positions_size[name1]*(open_prices[name1]-current_prices[name1]-spreads[name1]/2))//marketinfo_df[marketinfo_df.marketId==name1].exchangeRate.iloc[0])

            PnL=sum(profit)
            print("\t PnL :\t"+str(PnL)+"\n")

        else: 
            print("\t Some Instruments are not TRADEABLE \n")
            print("########################################################### \n")
            return None


              


    if (not isOpen) and (tradeable1=="TRADEABLE") and (tradeable2=="TRADEABLE"):

        order_size1=pca_res2[name1+"_size"].iloc[0]*marketinfo_df[marketinfo_df.marketId==name1].minSize.iloc[0]
        stop_distance1=(650/marketinfo_df[marketinfo_df.marketId==name1].pipValue.iloc[0])*marketinfo_df[marketinfo_df.marketId==name1].exchangeRate.iloc[0]
        my_currency1=marketinfo_df[marketinfo_df.marketId==name1].currency.iloc[0]

           

            
        order_size2=pca_res2[name2+"_size"].iloc[0]*marketinfo_df[marketinfo_df.marketId==name2].minSize.iloc[0]
        stop_distance2=(650/marketinfo_df[marketinfo_df.marketId==name2].pipValue.iloc[0])*marketinfo_df[marketinfo_df.marketId==name2].exchangeRate.iloc[0]
        my_currency2=marketinfo_df[marketinfo_df.marketId==name2].currency.iloc[0]


        if ( score > trading_parameters['short_entry']) and (correl>trading_parameters['min_correl']) :
            trade_order1={"direction":"SELL","epic":epic1,"size":order_size1,"currency":my_currency1,"stop_distance":None}
            trade_order2={"direction":"BUY","epic":epic2,"size":order_size2,"currency":my_currency2,"stop_distance":None}

            order_dict={name1:trade_order1,name2:trade_order2}
            
            open_position1, open_position2=igconnector.open_paired_position(marketIds=[name1,name2],positions=order_dict,units=units)


            if bool(open_position1) and bool(open_position2):
                if open_position1['status']=="OPEN" and open_position2['status']=="OPEN":   
                    print("\t OPEN short paired position \n")
                    print("########################################################### \n")
                
        elif ( score < trading_parameters['long_entry']) and (correl>trading_parameters['min_correl']):

            trade_order1={"direction":"BUY","epic":epic1,"size":order_size1,"currency":my_currency1,"stop_distance":None}
            trade_order2={"direction":"SELL","epic":epic2,"size":order_size2,"currency":my_currency2,"stop_distance":None}

            order_dict={name1:trade_order1,name2:trade_order2}
            
            open_position1, open_position2=igconnector.open_paired_position(marketIds=[name1,name2],positions=order_dict,units=units)

            if bool(open_position1) and bool(open_position2):
                if open_position1['status']=="OPEN" and open_position2['status']=="OPEN":   
                    print("\t OPEN long paired position \n")
                    print("########################################################### \n")
        
        else:
            print("\t Did not open any positions \n")
            print("########################################################### \n")
                
        if bool(open_position1) and bool(open_position2):
            if open_position1['status']=="OPEN" and open_position2['status']=="OPEN" :
                open_positions={name1:open_position1,name2:open_position2}
                write_close_positions(open_positions,"open_position_history.json") 
                ###json.dump(open_positions, open(open_trades_file, 'w' ) )
                ###print("OPEN paired position")
                
    elif  (not isOpen) and ((tradeable1!="TRADEABLE") or (tradeable2!="TRADEABLE")):
         print("\t Some Instruments are not TRADEABLE \n")
         print("########################################################### \n")
         return None

    elif isOpen:
        close_positions={}

        

        if isLong and (score>trading_parameters['close_long']) and (PnL> - (15*units[0]/2.0)):  

            close_position1,close_position2=igconnector.close_paired_position(marketIds=[name1,name2],positions=close_dict)

            
        elif isShort and (score<trading_parameters['close_short']) and (PnL > - (15*units[0]/2.0)):   

            close_position1,close_position2=igconnector.close_paired_position(marketIds=[name1,name2],positions=close_dict)
            
        elif PnL>(TP*units[0]/2):
            
            close_position1,close_position2=igconnector.close_paired_position(marketIds=[name1,name2],positions=close_dict)
            
        elif PnL<(-SL*units[0]/2):
            
            close_position1,close_position2=igconnector.close_paired_position(marketIds=[name1,name2],positions=close_dict)

        else:
            print("########################################################### \n")
            
                
        if bool(close_position1) and  bool(close_position2):
            if close_position1['status']=="CLOSED" and close_position2['status']=="CLOSED" :

                close_positions={name1:close_position1,name2:close_position2}

                write_close_positions(close_positions,"close_positions_history.json") 
                ##json.dump(close_positions, open( close_trades_file, 'w' )) 
                json.dump({}, open("open_positions.json", 'w' )) 
                print("\t Close paired positions \n")
                print("########################################################### \n")

    



def run_trading_functions():

    create_quant_indicators()
    make_paired_trades(open_trades_file="open_positions_history.json",close_trades_file="close_positions_history.json",units=trading_parameters['unit_size'],SL=trading_parameters['stop_loss'],TP=trading_parameters['take_profit'])

    
   
def update_price_data(callback0, callback1):

    global data_reader
    global igconnector
    global open_positions_dict

    sys.stdout=open("output.txt","a")
    
    session_details=igconnector.create_ig_session()
    print("\t"+str(session_details)+"\n")

    callback0()
    ##time.sleep(2)

    
    watchlist_df=igconnector.fetch_watchlist(watchlist_id)

    if watchlist_df.shape[0]>1:
        data_reader.append_prices(watchlist_df=watchlist_df[["epic","offer","bid"]])
        data_reader.write_newprices()
        
    drawdown=session_details['accountInfo']['profitLoss']/session_details['accountInfo']['available']
    if drawdown>0.30:
        open_positions_dict={}
        return None

    callback1()
    #del igconnector
    open_positions_dict={}
    sys.stdout.close()




##def run_price_updater():

##    data_reader=DataReader(epics,market_names,marketIds)
    
##    scheduler = BackgroundScheduler()
    
##    scheduler.add_job(update_price_data,args=[], trigger='cron', minute='*/5',second=0)

##    scheduler.start()


#########################################################################################


if __name__ == "__main__":
    #main()
    #data_reader=DataReader(epics,market_names,marketIds)
    #th = threading.Thread(target=main)
    #th.start()
    

    ##sys.stdout=open("output.txt","w")
    data_reader=DataReader(epics,market_names,marketIds)
    pca_df=pd.DataFrame()
    #igconnector=None
    igconnector=IGConnector(account_id,acc_password,api_key,acc_environment)    
    open_positions_dict={}
    #last_datetime_price=[]

    scheduler = BlockingScheduler()
    
    ##scheduler.add_job(update_price_data,args=[check_open_positions,run_trading_functions],trigger='cron',minute="*/1",second=0,jitter=2,timezone="UTC")
    scheduler.add_job(update_price_data,args=[check_open_positions,run_trading_functions], trigger='cron', hour="*/4",minute=0,second=15,jitter=2,timezone="UTC")


    #Bscheduler = BackgroundScheduler()

    ##scheduler.add_job(main, trigger='cron', minute='*/1',second=1)
   
    # Every 4 hours
    #scheduler.add_job(resample_df,args=[ohlc_dict,'4H','./test4h.csv'], trigger='cron', hour='*/4',minute=0,second=0)

    # Every day at 00:00
    #scheduler.add_job(resample_df,args=[ohlc_dict,'1D','./test1d.csv'], trigger='cron',hour=0, minute=0,second=0)

    scheduler.start()
    ##sys.stdout.close()
    #Bscheduler.start()


#####################################################################################################################
