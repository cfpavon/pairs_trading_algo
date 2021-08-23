import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
##from constants import marketIds, data_filename, marketinfo_filename, watchlist_id, epics,market_names
from constants import data_filename, marketinfo_filename


class DataReader(object):
    """description of class"""
    ##trade_df=None
    trade_df=None   
    watchlist_df=None
    filename=None
    
    

    def __init__(self, epics,market_names,marketIds,filename=data_filename):
        self.epic1=epics[market_names[0]]
        self.epic2=epics[market_names[1]]

        self.name1=marketIds[market_names[0]]
        self.name2=marketIds[market_names[1]]
        self.filename=filename
        self.trade_df=self.read_datafile(filename)

    
       

    def read_datafile(self,filename=data_filename):

        epic1=self.epic1
        epic2=self.epic2

        name1=self.name1
        name2=self.name2
        
        if os.path.isfile(filename):
            crude=pd.read_csv(filename,sep=",",dtype={'datetime':str, 'epic': str, 'offer': np.float64,'bid':np.float64} )
        else:
            print('No datafile found')
            empty_df=pd.DataFrame(columns=['datetime','epic','offer','bid'])
            return empty_df.astype(dtype={'datetime':str, 'epic': str, 'offer': np.float64,'bid':np.float64} ) 
        

        ##crude['name']=[x[5:] for x in  crude['epic']]
        ##crude['name']=[x[0:x.find('.')] for x in  crude['name']]
        ##self.trade_df=crude[["datetime","epic","ask_close","bid_close"]].copy()
        #crude['name']=crude['name'].apply(lambda x: x[0:x.find('.')])

        return crude[["datetime","epic","offer","bid"]]


    def make_wide(self,crude_df,cor_window=120):

        name1=self.name1
        name2=self.name2

        prices_df=self.read_marketinfo()
        CLInfo=prices_df[prices_df.marketId==name1]
        LCOInfo=prices_df[prices_df.marketId==name2]
   
        my_df=pd.pivot_table(crude_df, values=['mid_price','spread'], index=['datetime'],columns=['marketId'])
        my_df.dropna(inplace=True)
        my_df[name1+'_notional']=my_df['mid_price'][name1]*CLInfo['pipValue'].iloc[0]*CLInfo['minSize'].iloc[0]/CLInfo['exchangeRate'].iloc[0]
        my_df[name2+'_notional']=my_df['mid_price'][name2]*LCOInfo['pipValue'].iloc[0]*LCOInfo['minSize'].iloc[0]/LCOInfo['exchangeRate'].iloc[0]
        my_df[name1+'_spread']=my_df['spread'][name1]*CLInfo['pipValue'].iloc[0]*CLInfo['minSize'].iloc[0]/CLInfo['exchangeRate'].iloc[0]
        my_df[name1+'_spread']=my_df['spread'][name2]*LCOInfo['pipValue'].iloc[0]*LCOInfo['minSize'].iloc[0]/LCOInfo['exchangeRate'].iloc[0]
    
    
        #my_df.set_index(['datetime'])

        my_df[[name1+'_return',name2+'_return']]=my_df[[name1+'_notional',name2+'_notional']].apply(lambda x: 1+(x-x.shift(1))/x.shift(1))
        #my_df['CLret']= (1+(my_df['CLValue']-my_df['CLValue'].shift(1))/my_df['CLValue'].shift(1))*100
        #my_df['LCOret']= (1+(my_df['LCOValue']-my_df['LCOValue'].shift(1))/my_df['LCOValue'].shift(1))*100
        my_df['corr']=my_df[name1+'_return'].rolling(window=cor_window).corr(my_df[name2+'_return'])
    
        return my_df, prices_df


    def read_marketinfo(self,filename=marketinfo_filename):

        
        ##D:/Documents/Trading Tutorials/auquan v1/Live Script/DEMOPricesv1.txt
        ##prices_df=pd.read_csv(filename, sep='\t', lineterminator='\n',header=None,names=["marketId","epic0","currency","pipValue","minSize","exchangeRate","req"])
        prices_df=pd.read_csv(filename, sep='\t', lineterminator='\n',header=None,names=["marketId","updateTime","epic","currency","pipValue",
                                           "minSize","exchangeRate","margin","marginFactorUnit","marketStatus","delay"])
        prices_df["minSize"].clip(lower=0.05,inplace=True)
    
        return prices_df


    def append_prices(self,watchlist_df):
    
        #n=watchlist_df.epic.size
        #datetime_list=[datetime.now().strftime("%Y-%m-%d %H:%M:%S")]*n
    
        my_df=watchlist_df.copy()
        time_now=datetime.now()
        my_df["datetime"]=datetime(time_now.year,time_now.month,time_now.day,time_now.hour,time_now.minute,0).strftime("%m/%d/%Y %H:%M:%S")
        ##my_df["datetime"]=my_df["datetime"].round('H')

        print(my_df.tail(4))
        print("\n")
    
        my_df=my_df[["epic","datetime","offer","bid"]]

        ##print(my_df.head(5))

        #watchlist_df.epic.tolist()
        self.watchlist_df=my_df[["datetime","epic","offer","bid"]].copy()
        self.trade_df=self.trade_df.append(my_df,ignore_index=True)

        print(self.trade_df.tail(4))

        return None



    def get_prices_df(self):
        
        trade_df=self.trade_df.copy()
        crude_df=self.calculate_midprice(trade_df)

        return crude_df



    def write_newprices(self):
        
        table=self.watchlist_df

        if not os.path.isfile(self.filename):
            table.to_csv(self.filename,index=False,mode='w',header=True,float_format='%.2f',date_format="%m/%d/%Y %H:%M:%S",columns=['datetime','epic','offer','bid'])
        else:
            table.to_csv(self.filename,index=False,mode='a',header=False,float_format='%.2f',date_format="%m/%d/%Y %H:%M:%S",columns=['datetime','epic','offer','bid'])

        


        
    def calculate_midprice(self, crude_df):

        crude_df['datetime']=pd.to_datetime(crude_df['datetime'])
        crude_df['mid_price']=(crude_df['offer']+crude_df['bid'])/2
        crude_df['spread']=(crude_df['offer']-crude_df['bid'])

        crude_df['marketId'] = np.select([crude_df['epic'].isin([self.epic1]),crude_df['epic'].isin([self.epic2])], 
                                        [self.name1, self.name2], default='Unknown')

        return crude_df[["datetime","marketId","offer","bid","mid_price","spread"]]



    
        
    


    

