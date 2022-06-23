
import os
import sys
##from constants import constants
import time
##from constants import constants
import pandas as pd
import json
import numpy as np


sys.path.insert(1, './ig-markets-api-python-library-master/')


from trading_ig.rest import IGService


class IGConnector(object):
    """description of class"""

    ig_service=None
    session_details=None
    name1=None
    name2=None
    ##SessionDetails=create_ig_session()
    ##watchlist_df=fetch_watchlist(wid=watchlist_id)
    ##update_pricedata()

    

    def __init__(self, account_id,acc_password,api_key,acc_environment):

        self.account_id=account_id
        self.acc_password=acc_password
        self.api_key=api_key
        self.acc_environment=acc_environment

        ##self.epic1=epics[market_names[0]]
        ##self.epic2=epics[market_names[1]]

        ##self.name1=marketIds[market_names[0]]
        ##self.name2=marketIds[market_names[1]]
    

    def create_ig_session(self):
    
        self.ig_service = IGService(self.account_id, self.acc_password, self.api_key, self.acc_environment)
        n_trial=0
        session_details=None

        while n_trial<3:
            try:
                session_details=self.ig_service.create_session()
                print('Connected to IG server')
            except Exception as e:
                print(e)
                

            if session_details:
                #print(session_details)
                return session_details

            wait = (5 * ( 2 ** n_trial ))  
            time.sleep(wait)
            n_trial+=1

        ##self.assign_session_details(session_details)

        return None


    def assign_session_details(self,session_details):

        self.session_details=session_details
        self.CurrentBalance=SessionDetails['accountInfo']['balance']
        self.AvailableBalance=SessionDetails['accountInfo']['available']
        self.IsDealing=SessionDetails['dealingEnabled']
        self.IsActive=SessionDetails['hasActiveLiveAccounts']
        

        


    def fetch_watchlist(self,wid="111"):
        
        n_trial=0
        watchlist_df=pd.DataFrame()
        
        while n_trial<2:
            try:
                watchlist_df = self.ig_service.fetch_watchlist_markets(watchlist_id=wid, session=None)
            except Exception as e:
                print(e)
                

            if watchlist_df is not None:
                if not watchlist_df.empty:
                    return watchlist_df

            wait = (3 * ( 2 ** n_trial ))  
            time.sleep(wait)
            n_trial+=1

        return pd.DataFrame()


    def get_open_positions_by_dealId(self,dealIds=[]):
        
        open_positions_df=pd.DataFrame()
        open_positions_dict={}
        
        try:
            open_positions_df = self.ig_service.fetch_open_positions()
        except Exception as e:
            print(e)

        print(open_positions_df)
        print(open_positions_df.columns)

        if open_positions_df is not None: 
            if not open_positions_df.empty: 
        ##print(open_positions_df['position'].iloc[0])
                for i in range(len(dealIds)):

                    for index, row in open_positions_df.iterrows():
                        if row['position']['dealId']==dealIds[i]:
                            open_positions_dict[dealIds[i]]={'position':row['position'],'market':row['market']}
        else:
            return {}
                

        return open_positions_dict



    def get_open_positions_by_epic(self,epics=[]):
        
        open_positions_df=pd.DataFrame()
        matched_epics=[]
        
        try:
            open_positions_df = self.ig_service.fetch_open_positions()
        except Exception as e:
            print(e)

        i=0
        ##print(open_positions_df['market'].iloc[0])

        for index, row in open_positions_df.iterrows():

            if row['market']['epic']==epics[i]:
                 matched_epics.append(i)
            i+=1
                

        return open_positions_df.iloc[matched_epics,:]


    def get_open_positions(self):
        
        open_positions_df=pd.DataFrame()
        
        
        try:
            open_positions_df = self.ig_service.fetch_open_positions()
        except Exception as e:
            print(e)
                

        return open_positions_df

        
     


   
    def fetch_market_details(self,epics=[],filename="MarketInfoPrices.txt",writeFile=True):
    
        if writeFile:
            f = open(filename, "w",buffering=1)

        details_df = pd.DataFrame(columns=["marketId","updateTime","epic","currency","pipValue",
                                           "minSize","exchangeRate","margin","marginFactorUnit","marketStatus","delay"])

        open_positions=None
        ##print(epics[0])
        ##print(epics[1])

        for epic in epics:
            #print(name)
            n_trial=0
        

            while n_trial<3:
                try:
                    open_positions = self.ig_service.fetch_market_by_epic(epic=epic, session=None)
                except Exception as e:
                    print(e)
                    

                if open_positions:
                
                    print("\t"+open_positions.instrument.marketId+"\t"+open_positions.snapshot.updateTime+"\t"+
                          open_positions.instrument.epic+"\t"+open_positions.instrument.currencies[0].code+"\t"+open_positions.instrument.valueOfOnePip+"\t"+
                          str(open_positions.dealingRules.minDealSize.value)+"\t"+str(open_positions.instrument.currencies[0].baseExchangeRate)+"\t"+
                          str(open_positions.instrument.marginFactor)+"\t"+str(open_positions.instrument.marginFactorUnit)+"\t"+
                          open_positions.snapshot.marketStatus+"\t"+str(open_positions.snapshot.delayTime)+"\n")

                    if writeFile:

                        f.write(open_positions.instrument.marketId+"\t"+open_positions.snapshot.updateTime+"\t"+
                              open_positions.instrument.epic+"\t"+open_positions.instrument.currencies[0].code+"\t"+open_positions.instrument.valueOfOnePip+"\t"+
                              str(open_positions.dealingRules.minDealSize.value)+"\t"+str(open_positions.instrument.currencies[0].baseExchangeRate)+"\t"+
                              str(open_positions.instrument.marginFactor)+"\t"+str(open_positions.instrument.marginFactorUnit)+"\t"+
                              open_positions.snapshot.marketStatus+"\t"+str(open_positions.snapshot.delayTime)+"\n")
                
                
                
                    details_df = details_df.append({"marketId":open_positions.instrument.marketId,"updateTime":open_positions.snapshot.updateTime,
                                   "epic":open_positions.instrument.epic,"currency":open_positions.instrument.currencies[0].code,
                                   "pipValue":open_positions.instrument.valueOfOnePip,"minSize":open_positions.dealingRules.minDealSize.value,
                                   "exchangeRate":open_positions.instrument.currencies[0].baseExchangeRate,
                                   "margin":open_positions.instrument.marginFactor,
                                   "marginFactorUnit":str(open_positions.instrument.marginFactorUnit),
                                   "marketStatus": open_positions.snapshot.marketStatus,
                                   "delay":open_positions.snapshot.delayTime}, ignore_index=True)

                    details_df['pipValue'] = details_df['pipValue'].astype(np.float64)
                    #print(details_df.head())
                    #time.sleep()
                    break


                wait = (5 * ( 2 ** n_trial ))  
                time.sleep(wait)
                n_trial+=1

        f.close()
        return details_df
            
        
    
    
    def open_position(self,position={},units=1):
        
        n_trial=0

        #limit_dist=str(round(limit_distance*units,1))
        #stop_dist=str(round(stop_distance*units,1))
        open_pos={}
        
        while n_trial<3:
            try:
                open_pos = self.ig_service.create_open_position(currency_code=position['currency'],direction=position['direction'],epic=position['epic'],
                                                  expiry='-',force_open='true',guaranteed_stop='false',
                                                  order_type='MARKET', size=position['size']*units,level=None,limit_distance=None,
                                                  limit_level=None,quote_id=None,stop_distance=position['stop_distance'],stop_level=None,
                                                  trailing_stop=None,trailing_stop_increment=None)
            except Exception as e:
                print(e)
                

            if bool(open_pos):
                if open_pos['dealStatus']=="ACCEPTED":
                    print("\t"+open_pos['date']+"\t"+open_pos['status']+"\t"+open_pos['epic']+"\t"+str(open_pos['affectedDeals'])+"\t"+str(open_pos['level'])+"\t"+str(open_pos['size'])+"\t"+open_pos['direction']+"\t"+str(open_pos['profit'])+"\n")

                ##print(open_pos['affectedDeals'])
                return open_pos

            wait = (3 * ( 2 ** n_trial ))  
            time.sleep(wait)
            n_trial+=1

        return None


    def close_position(self,position={}):
        
        n_trial=0

        
        close_pos={}

        while n_trial<3:
            try:
                close_pos=self.ig_service.close_open_position(deal_id=position["dealId"],direction=position["direction"],epic=None, expiry="-", size=position['size'],
                                                              order_type="MARKET",level=None,quote_id=None)
            except Exception as e:
                print(e)
                
            if bool(close_pos):
                #print(close_pos)
                print("\t"+close_pos['date']+"\t"+close_pos['status']+"\t"+close_pos['epic']+"\t"+str(close_pos['affectedDeals'])+"\t"+str(close_pos['level'])+"\t"+str(close_pos['size'])+"\t"+close_pos['direction']+"\t"+str(close_pos['profit'])+"\n")
          
                ##print(close_pos['affectedDeals'])
                return close_pos

            wait = (3 * ( 2 ** n_trial ))  
            time.sleep(wait)
            n_trial+=1

        return close_pos

    def open_paired_position(self,marketIds=[],positions={},units=[1,1],open_json="open_positions.json"):

        position1=positions[marketIds[0]]
        position2=positions[marketIds[1]]

        open_position1={}
        open_position2={}

        n_trial=0

        while n_trial<3:
            
            if (not bool(open_position1)) :
                open_position1 = self.open_position(position=position1,units=units[0])

                if open_position1['dealStatus']=='ACCEPTED' and (not bool(open_position2)):
                    open_position2 = self.open_position(position=position2,units=units[1])

                elif open_position1['dealStatus']=='ACCEPTED' and (bool(open_position2)):
                    if (open_position2['dealStatus']=='REJECTED'):
                        open_position2 = self.open_position(position=position2,units=units[1])

            elif open_position1['dealStatus']=='REJECTED' :
                open_position1 = self.open_position(position=position1,units=units[0])

                if open_position1['dealStatus']=='ACCEPTED' and (not bool(open_position2)):
                    open_position2 = self.open_position(position=position2,units=units[1])

                elif open_position1['dealStatus']=='ACCEPTED' and (bool(open_position2)):
                    if (open_position2['dealStatus']=='REJECTED'):
                        open_position2 = self.open_position(position=position2,units=units[1])

            
            if bool(open_position1) and bool(open_position2):
                if open_position1['dealStatus']=='ACCEPTED' and open_position2['dealStatus']=='ACCEPTED':
                    with open(open_json,'w') as f:
                        json.dump({marketIds[0]:open_position1,marketIds[1]:open_position2}, f,indent = 4) 
                    return open_position1, open_position2

            if n_trial<2:
                wait = (15 * ( 2 ** n_trial ))  
                print("Not all positions were open")
                time.sleep(wait)

            n_trial+=1




        if open_position1['dealStatus']=='ACCEPTED' and open_position2['dealStatus']=='REJECTED':

            direction='SELL'
            if open_position1['direction']=='SELL':
                direction='BUY'

            position_c1={"dealId":open_position1['dealId'],"direction":direction,'size':open_position1['size']}
            close_position1=self.close_position(position=position_c1)

        
        
        return None, None




    def close_paired_position(self,marketIds=[],positions={},close_json="close_positions.json",open_json="open_positions.json"):

        if not bool(positions):

            with open(open_json,'r') as f:
                positions=json.load(f) 
            ##positions = json.load( open("open_positions.json") )

            if not bool(positions):
                print("No positions were found open")
                return None

            position1=positions[marketIds[0]]
            position2=positions[marketIds[1]]

            if position1['direction']=="SELL": 

                position1['direction']="BUY"
                position2['direction']="SELL"

            elif position1['direction']=="BUY":

                position1['direction']="SELL"
                position2['direction']="BUY"

        else:
            position1=positions[marketIds[0]]
            position2=positions[marketIds[1]]


        

        close_position1={}
        close_position2={}

        n_trial=0

        while n_trial<5:
            
            if (not bool(close_position1)):
                close_position1 = self.close_position(position=position1)

            elif (not close_position1['status']=='CLOSED'):
                close_position1 = self.close_position(position=position1)

            if (not bool(close_position2)):
                close_position2 = self.close_position(position=position2)

            elif (not close_position2['status']=='CLOSED'):
                close_position2 = self.close_position(position=position2)

            if close_position1['status']=='CLOSED' and close_position2['status']=='CLOSED':

                with open(close_json,'w') as f:
                        json.dump({marketIds[0]:close_position1,marketIds[1]:close_position2}, f,indent = 4) 
                #json.dump({marketIds[0]:close_position1,marketIds[1]:close_position2}, open(close_json, 'w' ),indent = 4) 

                return close_position1, close_position2

            if n_trial<4:
                wait = (30 * ( 2 ** n_trial ))  
                print("Not all positions were closed")
                time.sleep(wait)
            
            n_trial+=1
        
        return close_position1, close_position2


    #def refresh_session(self):
    #    self.ig_service.refresh_session()
    
    

    def __del__(self):
        self.ig_service.logout()
        print('Destructor called, IG connector deleted.')
    

