import os
import sys
import json
import numpy as np
from sklearn.decomposition import PCA
import pandas as pd
from statsmodels.tsa.stattools import adfuller
from pandas.tseries.offsets import DateOffset
from datetime import datetime
import credentials


from constants import *
from DataReader import DataReader
#from constants import marketIds, data_filename, marketinfo_filename, watchlist_id, epics,market_names,open_positions,epics_ids,ids_epics
#from constants import trading_parameters
#from constants import account_id,acc_password,api_key,acc_environment
from IGConnector import *
import time
from constants import constants
import threading

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.background import BlockingScheduler





class QuantIndicators(object):
    """description of class"""


    trade_df=None
    marketinfo_df=None
    

          

    def __init__(self, constants_dict=constants["crude_oil"]):
        self.epic1=constants_dict["epics"][constants_dict["market_names"][0]]
        self.epic2=constants_dict["epics"][constants_dict["market_names"][1]]

        self.name1=constants_dict["marketIds"][constants_dict["market_names"][0]]
        self.name2=constants_dict["marketIds"][constants_dict["market_names"][1]]

        ##self.trade_df = trade_df
        ##self.marketinfo_df = marketinfo_df

    def calculate_pca(self,data):
    
        pca = PCA(n_components=2,whiten=False)
        principalComponents = pca.fit_transform(data)
        loadings = pd.DataFrame(pca.components_.T, columns=['PC1', 'PC2'], index=data.columns)
    
        #print(loadings)
    
        #loadings_rot=varimax(loadings.to_numpy(), gamma = 1.0, q = 20, tol = 1e-6)
    
        #print(loadings_rot)
        
        scores = pd.DataFrame(data = principalComponents, columns = ['PC1', 'PC2'])
        PC2=loadings['PC2']
        Scores2 =scores['PC2']
        Var=pca.explained_variance_ratio_
        PC2std=scores['PC2'].std()
        result = adfuller(Scores2)
        adf=result[0]
        pvalue=result[1]
    
        id1=data.columns[0][:]+"_wi"
        #epic1=a[0:a.find('.')]
        #print(epic1)
        id2=data.columns[1][:]+"_wi"
        #epic2=b[0:b.find('.')]
        #print(epic2)
    
        res_dict={"datetime":data.index[-1]+pd.Timedelta(hours=0),id1:abs(PC2[0]),id2:abs(PC2[1]),
                  "Mean1":data.iloc[:,0].mean(),"Mean2":data.iloc[:,1].mean(),
                  "Var1":Var[0],"Var2":Var[1],"PC2_std":PC2std,"ADF":adf,"pvalue":pvalue}
    
        df=pd.DataFrame(list(res_dict.items())).transpose()
        cols=df.iloc[0,:]
        df=df.drop(0)
        df.columns=cols
        df['datetime']=pd.to_datetime(df['datetime'])
        df.set_index("datetime",inplace=True)
   
        res_df=pd.concat([data.iloc[-1:],df],axis=1)

        ##print(res_df.columns)
        ##print(res_df.head())

        return res_df


    def calculate_size(self,pca_res):

        id1=self.name1+"_return_wi"
        id2=self.name2+"_return_wi"
    
        pca_res[self.name1+"_size"]=[1.0 if x<y else x/y  for x,y in zip(pca_res[id1], pca_res[id2]) ]
        pca_res[self.name2+"_size"]=[1.0  if y<x else y/x for x,y in zip(pca_res[id1], pca_res[id2]) ]
    
        pca_res[self.name1+"_size"]=pca_res[self.name1+"_size"].apply(lambda x: np.round(x,1))
        pca_res[self.name2+"_size"]=pca_res[self.name2+"_size"].apply(lambda x: np.round(x,1))
    
    
    
    
        return pca_res



    def calculate_score(self,merge_df):

        ret1=self.name1+"_return"
        ret2=self.name2+"_return"
        
        id1=self.name1+"_return_wi"
        id2=self.name2+"_return_wi"
    
        merge_df['score'] =merge_df[id1]*(merge_df[ret1]-merge_df['Mean1'])-merge_df[id2]*(merge_df[ret2]-merge_df['Mean2'])
        merge_df['score']=merge_df['score']/merge_df['PC2_std']
    
    
    
    
        return merge_df


class TradingStatus():

    def __init__(self,pair,constants_dict):
            
        self.pair=pair
        self.constants_dict=constants_dict
        self._open_positions_by_dealId={}
        self._open_positions_by_instrumentId={}
        self._open_positions_by_dealId_for_monitoring={}
        self._isOpen=None
        self._watchlist_df=pd.DataFrame()
        self.marketinfo_df=pd.DataFrame()

        self.isLong=None

        self.dealId_epic={}
        self.dealIds=[]
        self.name1=None
        self.name2=None
        self.epic1=None
        self.epic2=None
        self.profit=[]
        self.PnL=0
    
        #self.open_position1={}
        #self.open_position2={}

        self.close_position1={}
        self.close_position2={}
        self.close_dict={}

        self.singlePosition=False 

        self.marketId1=constants_dict["marketIds"][constants_dict["market_names"][0]]
        self.marketId2=constants_dict["marketIds"][constants_dict["market_names"][1]]

        self.dealId1_infile=None
        self.dealId2_infile=None

        self.tradeable1=None
        self.tradeable2=None




    #def get_open_positions_by_dealId(self):
    #    return self._open_positions_by_dealId

    #def set_open_positions_by_dealId(open_positions_dict):
    #    self._open_positions_by_dealId=new_open_positions_by_dealId


    #def get_watchlist_df(self):
    #    return self._open_positions_by_dealId

    #def set_watchlist_df(self,watchlist_df):
    #    self._watchlist_df=watchlist_df


    def get_open_positions_fromfile(self):

            open_positions={}

            if os.path.isfile(self.constants_dict['open_positions']): 
                open_positions= json.load( open(self.constants_dict['open_positions']) )
                print("Read file open_Positions")
                print(open_positions)

            if bool(open_positions):
                if (self.marketId1 in open_positions.keys()) and (self.marketId2 in open_positions.keys()) :
                    return open_positions
            else:
                return {}
                    #self.dealId1_infile=self._open_positions_by_instrumentId[self.marketId1]['dealId']
                    #self.dealId2_infile=self._open_positions_by_instrumentId[self.marketId2]['dealId']


    def update_open_positions(self,open_positions_dict):

        if  bool(open_positions_dict):   
            self._isOpen=True
            self._open_positions_by_dealId=open_positions_dict.copy()
            #self.trade_status._marketinfo_df=self.marketinfo_df.copy()
            print(open_positions_dict)
        else:
            self._isOpen=False    
            self._open_positions_by_dealId={}
        
    def has_open_positions(self,paired_positions):


        if (not bool(paired_positions)):
        
            ##name1=marketIds[market_names[0]]
            self._isOpen=False
            self.name1=constants_dict['marketIds'][constants_dict['market_names'][0]]
            self.name2=constants_dict['marketIds'][constants_dict['market_names'][1]]

            ##print("\t"+name1+"\t"+name2)
            #print(name2)

            self.epic1=constants_dict['ids_epics'][self.name1]
            self.epic2=constants_dict['ids_epics'][self.name2]

            self.tradeable1=self.marketinfo_df[self.marketinfo_df.marketId==self.name1].marketStatus.iloc[0]
            self.tradeable2=self.marketinfo_df[self.marketinfo_df.marketId==self.name2].marketStatus.iloc[0]

 


    def get_open_positions_by_dealId_info(self,paired_positions,constants_dict,watchlist_df,marketinfo_df,isMonitor=True):
              
        self._watchlist_df=watchlist_df
        self.marketinfo_df=marketinfo_df

        #if not (isMonitor):
        self._open_positions_by_dealId=paired_positions.copy()
        

        self.profit=[]
        self.PnL=0
        self.dealId_epic={}
        self.dealIds=[]

        self.open_position1={}
        self.open_position2={}

        self.close_position1={}
        self.close_position2={}
        self.close_dict={}
        self._isOpen=None
        self.singlePosition=False

        #print(self.marketinfo_df.head())

        if (not bool(paired_positions)):
        
            ##name1=marketIds[market_names[0]]
            self._isOpen=False
            self.name1=constants_dict['marketIds'][constants_dict['market_names'][0]]
            self.name2=constants_dict['marketIds'][constants_dict['market_names'][1]]

            ##print("\t"+name1+"\t"+name2)
            #print(name2)

            self.epic1=constants_dict['ids_epics'][self.name1]
            self.epic2=constants_dict['ids_epics'][self.name2]

            self.tradeable1=self.marketinfo_df[self.marketinfo_df.marketId==self.name1].marketStatus.iloc[0]
            self.tradeable2=self.marketinfo_df[self.marketinfo_df.marketId==self.name2].marketStatus.iloc[0]

            #breakpoint()
            #self.tradeable1=self.marketinfo_df[self.marketinfo_df.marketId==self.name1].marketStatus.iloc[0]
            #self.tradeable2=self.marketinfo_df[self.marketinfo_df.marketId==self.name2].marketStatus.iloc[0]




        elif len(paired_positions)>1:

            self._isOpen=True

            
            for key in paired_positions:
                #breakpoint()
                self.dealId_epic[key]=paired_positions[key]['market']['epic']
                #id0=epics_ids[epic0]
                #paired_positions[id0] = paired_positions.pop(key)
                self.dealIds.append(key)
                #i+=1

    
            epic01=self.dealId_epic[self.dealIds[0]]
            epic02=self.dealId_epic[self.dealIds[1]]

            if self.constants_dict['epics_ids'][epic01]==constants_dict['marketIds'][constants_dict['market_names'][0]]:
                self.name1=constants_dict['epics_ids'][epic01]
                self.name2=constants_dict['epics_ids'][epic02]
            elif self.constants_dict['epics_ids'][epic01]==constants_dict['marketIds'][constants_dict['market_names'][1]]:
                self.name2=constants_dict['epics_ids'][epic01]
                self.name1=constants_dict['epics_ids'][epic02]
    

            self.epic1=constants_dict['ids_epics'][self.name1]
            self.epic2=constants_dict['ids_epics'][self.name2]
    
            ##print("\t"+name1+"\t"+name2)
            #print(name1)
            #print(name2)
            if self.dealIds[0] in paired_positions.keys():
                pos1=paired_positions[self.dealIds[0]]
                paired_positions.pop(self.dealIds[0])
               
                paired_positions[self.name1] = pos1

            if self.dealIds[1] in paired_positions.keys():
                pos2=paired_positions[self.dealIds[1]]
                paired_positions.pop(self.dealIds[1])
               
                paired_positions[self.name2] = pos2




            #paired_positions[self.name2] = paired_positions.pop(self.dealIds[1])



            if isMonitor:

                self.tradeable1=self._watchlist_df[self._watchlist_df['epic']==self.epic1].marketStatus.iloc[0]
                self.tradeable2=self._watchlist_df[self._watchlist_df['epic']==self.epic2].marketStatus.iloc[0]

            else:
                #breakpoint()
                if (self.name1 in paired_positions.keys()) and (self.name2 in paired_positions.keys()):
                    self.tradeable1=paired_positions[self.name1]['market']['marketStatus']
                    self.tradeable2=paired_positions[self.name2]['market']['marketStatus']
                else:
                    return


            if (self.tradeable1=="TRADEABLE") and (self.tradeable2=="TRADEABLE"):   
    
                 
        

                open_prices,open_direction,positions_size,current_prices,spreads,self.close_dict=self.create_open_position_dictionaries(paired_positions,self.name1,self.name2)
          
                  
    
                if open_direction[self.name1]=="BUY":

                    self.isLong=True
                    self.profit.append(((positions_size[self.name1]*(current_prices[self.name1]-open_prices[self.name1]))/self.marketinfo_df[self.marketinfo_df.marketId==self.name1].exchangeRate.iloc[0])*self.marketinfo_df[self.marketinfo_df.marketId==self.name1].pipValue.iloc[0])
                    #breakpoint()
                    self.profit.append(((positions_size[self.name2]*(open_prices[self.name2]-current_prices[self.name2]))/self.marketinfo_df[self.marketinfo_df.marketId==self.name2].exchangeRate.iloc[0])*self.marketinfo_df[self.marketinfo_df.marketId==self.name2].pipValue.iloc[0])
                    #breakpoint()
                    print(self.profit)

    
                elif open_direction[self.name1]=="SELL":
            
                    self.isLong=False
                    self.profit.append(((positions_size[self.name2]*(current_prices[self.name2]-open_prices[self.name2]))/self.marketinfo_df[self.marketinfo_df.marketId==self.name2].exchangeRate.iloc[0])*self.marketinfo_df[self.marketinfo_df.marketId==self.name2].pipValue.iloc[0])
                    self.profit.append(((positions_size[self.name1]*(open_prices[self.name1]-current_prices[self.name1]))/self.marketinfo_df[self.marketinfo_df.marketId==self.name1].exchangeRate.iloc[0])*self.marketinfo_df[self.marketinfo_df.marketId==self.name1].pipValue.iloc[0])
                    print(self.profit)

                self.PnL=sum(self.profit)
                #print("\t PnL :\t"+str(self.PnL)+"\n")

                #return_values=[singlePosition,name1,name2,epic2,epic2,paired_positions,open_prices,open_direction,positions_size,current_prices,spreads,close_dict,tradeable1,tradeable2,PnL,dealIds]

                #return return_values
                return None


                #return name1,name2,epic2,epic2,paired_positions,open_prices,open_direction,positions_size,current_prices,spreads,close_dict 

            


        elif len(paired_positions)==1:
        
            self._isOpen=True
            self.singlePosition=True
     
            
     
            for key in paired_positions:
                #breakpoint()
                self.dealId_epic[key]=paired_positions[key]['market']['epic']
                
                self.dealIds.append(key)
                 #i+=1
     
         
            epic01=self.dealId_epic[self.dealIds[0]]
     
             
            self.name1=constants_dict['epics_ids'][epic01]
                
            name2=constants_dict['marketIds'][constants_dict['market_names'][1]]

            if self.name1!=name2:

                self.name2=name2

            else:

                self.name2=constants_dict['marketIds'][constants_dict['market_names'][0]]


           
            #breakpoint()
            self.epic1=constants_dict['ids_epics'][self.name1]
             
            #breakpoint()         
        
            if self.dealIds[0] in paired_positions.keys():
                pos1=paired_positions[self.dealIds[0]]
                paired_positions.pop(self.dealIds[0])
               
                paired_positions[self.name1] = pos1

  
            #self.paired_positions[self.name1] = paired_positions.pop(dealIds[0])
            
            if isMonitor:
     
                self.tradeable1=self._watchlist_df[self._watchlist_df['epic']==epic1].marketStatus.iloc[0]
     
     
            else:
     
                self.tradeable1=paired_positions[self.name1]['market']['marketStatus']
     
     
     
            if (self.tradeable1=="TRADEABLE"):   
                              
             
     
                open_prices,open_direction,positions_size,current_prices,spreads,self.close_dict=self.create_single_position_dictionaries(paired_positions,self.name1)
               
                #breakpoint()       
         
                if open_direction[self.name1]=="BUY":
     
                    self.isLong=True
                    self.profit.append(((positions_size[self.name1]*(current_prices[self.name1]-open_prices[self.name1]))/self.marketinfo_df[self.marketinfo_df.marketId==self.name1].exchangeRate.iloc[0])*self.marketinfo_df[self.marketinfo_df.marketId==self.name1].pipValue.iloc[0])
                    #profit.append((positions_size[name2]*(open_prices[name2]-current_prices[name2]-spreads[name2]/2))/marketinfo_df[marketinfo_df.marketId==name2].exchangeRate.iloc[0])
     
         
                elif open_direction[self.name1]=="SELL":
                 
                    self.isLong=False
                    #profit.append((positions_size[name2]*(current_prices[name2]-open_prices[name2]-spreads[name2]/2))/marketinfo_df[marketinfo_df.marketId==name2].exchangeRate.iloc[0])
                    self.profit.append(((positions_size[self.name1]*(open_prices[self.name1]-current_prices[self.name1]))/self.marketinfo_df[self.marketinfo_df.marketId==self.name1].exchangeRate.iloc[0])*self.marketinfo_df[self.marketinfo_df.marketId==self.name1].pipValue.iloc[0])
     
                self.PnL=sum(self.profit)
                print("\t PnL :\t"+str(self.PnL)+"\n")
     
     
                #return_values=[singlePosition,name1,None,epic1,None,paired_positions,open_prices,open_direction,positions_size,current_prices,spreads,close_dict,tradeable1,None,PnL]
                #return return_values
                return None
     
     
    def get_open_positions_monitor_info(self,constants_dict,paired_positions,marketinfo_df):
              
        #self._watchlist_df=watchlist_df
        #self.marketinfo_df=marketinfo_df

        
        #paired_positions=self._open_positions_by_dealId.copy()
        

        self.profit=[]
        self.PnL=0
        self.dealId_epic={}
        self.dealIds=[]

        self.open_position1={}
        self.open_position2={}

        self.close_position1={}
        self.close_position2={}
        self.close_dict={}
        self._isOpen=None
        self.singlePosition=False

        #print(self.marketinfo_df.head())

        
        if len(paired_positions)>1:

            self._isOpen=True

            
            for key in paired_positions:
                #breakpoint()
                self.dealId_epic[key]=paired_positions[key]['market']['epic']
                #id0=epics_ids[epic0]
                #paired_positions[id0] = paired_positions.pop(key)
                self.dealIds.append(key)
                #i+=1

    
            epic01=self.dealId_epic[self.dealIds[0]]
            epic02=self.dealId_epic[self.dealIds[1]]

            if self.constants_dict['epics_ids'][epic01]==constants_dict['marketIds'][constants_dict['market_names'][0]]:
                self.name1=constants_dict['epics_ids'][epic01]
                self.name2=constants_dict['epics_ids'][epic02]
            elif self.constants_dict['epics_ids'][epic01]==constants_dict['marketIds'][constants_dict['market_names'][1]]:
                self.name2=constants_dict['epics_ids'][epic01]
                self.name1=constants_dict['epics_ids'][epic02]
    

            self.epic1=constants_dict['ids_epics'][self.name1]
            self.epic2=constants_dict['ids_epics'][self.name2]
    
            ##print("\t"+name1+"\t"+name2)
            #print(name1)
            #print(name2)
            if self.dealIds[0] in paired_positions.keys():
                pos1=paired_positions[self.dealIds[0]]
                paired_positions.pop(self.dealIds[0])
               
                paired_positions[self.name1] = pos1

            if self.dealIds[1] in paired_positions.keys():
                pos2=paired_positions[self.dealIds[1]]
                paired_positions.pop(self.dealIds[1])
               
                paired_positions[self.name2] = pos2




            #paired_positions[self.name2] = paired_positions.pop(self.dealIds[1])



            if (self.name1 in paired_positions.keys()) and (self.name2 in paired_positions.keys()):
                self.tradeable1=paired_positions[self.name1]['market']['marketStatus']
                self.tradeable2=paired_positions[self.name2]['market']['marketStatus']
            else:
                return

           

            #self.tradeable1=self._watchlist_df[self._watchlist_df['epic']==self.epic1].marketStatus.iloc[0]
            #self.tradeable2=self._watchlist_df[self._watchlist_df['epic']==self.epic2].marketStatus.iloc[0]

            


            if (self.tradeable1=="TRADEABLE") and (self.tradeable2=="TRADEABLE"):   
    
                 
        

                open_prices,open_direction,positions_size,current_prices,spreads,self.close_dict=self.create_open_position_dictionaries(paired_positions,self.name1,self.name2)
          
                  
    
                if open_direction[self.name1]=="BUY":

                    self.isLong=True
                    self.profit.append(((positions_size[self.name1]*(current_prices[self.name1]-open_prices[self.name1]))/self.marketinfo_df[self.marketinfo_df.marketId==self.name1].exchangeRate.iloc[0])*self.marketinfo_df[self.marketinfo_df.marketId==self.name1].pipValue.iloc[0])
                    #breakpoint()
                    self.profit.append(((positions_size[self.name2]*(open_prices[self.name2]-current_prices[self.name2]))/self.marketinfo_df[self.marketinfo_df.marketId==self.name2].exchangeRate.iloc[0])*self.marketinfo_df[self.marketinfo_df.marketId==self.name2].pipValue.iloc[0])
                    #breakpoint()
                    #print(self.profit)

    
                elif open_direction[self.name1]=="SELL":
            
                    self.isLong=False
                    self.profit.append(((positions_size[self.name2]*(current_prices[self.name2]-open_prices[self.name2]))/self.marketinfo_df[self.marketinfo_df.marketId==self.name2].exchangeRate.iloc[0])*self.marketinfo_df[self.marketinfo_df.marketId==self.name2].pipValue.iloc[0])
                    self.profit.append(((positions_size[self.name1]*(open_prices[self.name1]-current_prices[self.name1]))/self.marketinfo_df[self.marketinfo_df.marketId==self.name1].exchangeRate.iloc[0])*self.marketinfo_df[self.marketinfo_df.marketId==self.name1].pipValue.iloc[0])
                    #print(self.profit)

                self.PnL=sum(self.profit)
                #print("\t PnL :\t"+str(self.PnL)+"\n")

                #return_values=[singlePosition,name1,name2,epic2,epic2,paired_positions,open_prices,open_direction,positions_size,current_prices,spreads,close_dict,tradeable1,tradeable2,PnL,dealIds]

                #return return_values
                return None


                #return name1,name2,epic2,epic2,paired_positions,open_prices,open_direction,positions_size,current_prices,spreads,close_dict 

            


        elif len(paired_positions)==1:
        
            self._isOpen=True
            self.singlePosition=True
     
            
     
            for key in paired_positions:
                #breakpoint()
                self.dealId_epic[key]=paired_positions[key]['market']['epic']
                
                self.dealIds.append(key)
                 #i+=1
     
         
            epic01=self.dealId_epic[self.dealIds[0]]
     
             
            self.name1=constants_dict['epics_ids'][epic01]
                
            name2=constants_dict['marketIds'][constants_dict['market_names'][1]]

            if self.name1!=name2:

                self.name2=name2

            else:

                self.name2=constants_dict['marketIds'][constants_dict['market_names'][0]]


           
            #breakpoint()
            self.epic1=constants_dict['ids_epics'][self.name1]
             
            #breakpoint()         
        
            if self.dealIds[0] in paired_positions.keys():
                pos1=paired_positions[self.dealIds[0]]
                paired_positions.pop(self.dealIds[0])
               
                paired_positions[self.name1] = pos1

  
            #self.paired_positions[self.name1] = paired_positions.pop(dealIds[0])
            
    
     
            #self.tradeable1=self._watchlist_df[self._watchlist_df['epic']==epic1].marketStatus.iloc[0]
     
     

            if (self.name1 in paired_positions.keys()):
                self.tradeable1=paired_positions[self.name1]['market']['marketStatus']
            else:
                print("name not in dict keys")
                return


            
     
     
     
            if (self.tradeable1=="TRADEABLE"):   
                              
             
     
                open_prices,open_direction,positions_size,current_prices,spreads,self.close_dict=self.create_single_position_dictionaries(paired_positions,self.name1)
               
                #breakpoint()       
         
                if open_direction[self.name1]=="BUY":
     
                    self.isLong=True
                    self.profit.append(((positions_size[self.name1]*(current_prices[self.name1]-open_prices[self.name1]))/self.marketinfo_df[self.marketinfo_df.marketId==self.name1].exchangeRate.iloc[0])*self.marketinfo_df[self.marketinfo_df.marketId==self.name1].pipValue.iloc[0])
                    #profit.append((positions_size[name2]*(open_prices[name2]-current_prices[name2]-spreads[name2]/2))/marketinfo_df[marketinfo_df.marketId==name2].exchangeRate.iloc[0])
     
         
                elif open_direction[self.name1]=="SELL":
                 
                    self.isLong=False
                    #profit.append((positions_size[name2]*(current_prices[name2]-open_prices[name2]-spreads[name2]/2))/marketinfo_df[marketinfo_df.marketId==name2].exchangeRate.iloc[0])
                    self.profit.append(((positions_size[self.name1]*(open_prices[self.name1]-current_prices[self.name1]))/self.marketinfo_df[self.marketinfo_df.marketId==self.name1].exchangeRate.iloc[0])*self.marketinfo_df[self.marketinfo_df.marketId==self.name1].pipValue.iloc[0])
                print("Monitoring PnL")
                self.PnL=sum(self.profit)
                print("\t PnL :\t"+str(self.PnL)+"\n")
     
     
                #return_values=[singlePosition,name1,None,epic1,None,paired_positions,open_prices,open_direction,positions_size,current_prices,spreads,close_dict,tradeable1,None,PnL]
                #return return_values
                return None
     
       
  


    def create_single_position_dictionaries(self,paired_positions,name1):

        open_direction={name1:paired_positions[name1]['position']['direction']}
        dealIds={name1:paired_positions[name1]['position']['dealId']}
        positions_size={name1:paired_positions[name1]['position']['dealSize']}
        #breakpoint()
        open_prices={name1:paired_positions[name1]['position']['openLevel']}
       

        #current_price1=(paired_positions[name1]['market']['bid']+paired_positions[name1]['market']['offer'])/2
        

        #current_prices={name1:current_price1}

      

        spread1=(paired_positions[name1]['market']['offer']-paired_positions[name1]['market']['bid'])
        
        spreads={name1:spread1}
        

            

        if paired_positions[name1]['position']['direction']=="BUY":
            position1={"dealId":paired_positions[name1]['position']['dealId'],"direction":"SELL",'size':paired_positions[name1]['position']['dealSize']}
            current_price1=paired_positions[name1]['market']['bid']
        

        


        else:
            position1={"dealId":paired_positions[name1]['position']['dealId'],"direction":"BUY",'size':paired_positions[name1]['position']['dealSize']}
            current_price1=paired_positions[name1]['market']['offer']
        

        current_prices={name1:current_price1}


        

        close_dict={name1:position1}

        return open_prices,open_direction,positions_size,current_prices,spreads,close_dict    



    def create_open_position_dictionaries(self,paired_positions,name1,name2):

        open_direction={name1:paired_positions[name1]['position']['direction'],name2:paired_positions[name2]['position']['direction']}
        dealIds={name1:paired_positions[name1]['position']['dealId'],name2:paired_positions[name2]['position']['dealId']}
        positions_size={name1:paired_positions[name1]['position']['dealSize'],name2:paired_positions[name2]['position']['dealSize']} 
        open_prices={name1:paired_positions[name1]['position']['openLevel'],name2:paired_positions[name2]['position']['openLevel']}
        close_direction={name1:paired_positions[name2]['position']['direction'],name2:paired_positions[name1]['position']['direction']}

        if paired_positions[name1]['position']['direction']=="BUY":
         
            current_price1=paired_positions[name1]['market']['bid'] 
            current_price2=paired_positions[name2]['market']['offer'] 


        
        else:

            current_price1=paired_positions[name1]['market']['offer']
            current_price2=paired_positions[name2]['market']['bid'] 




        #current_price1=(paired_positions[name1]['market']['bid']+paired_positions[name1]['market']['offer'])/2
        #current_price2=(paired_positions[name2]['market']['bid']+paired_positions[name2]['market']['offer'])/2

        current_prices={name1:current_price1,name2:current_price2}

        

        spread1=(paired_positions[name1]['market']['offer']-paired_positions[name1]['market']['bid'])
        spread2=(paired_positions[name2]['market']['offer']-paired_positions[name2]['market']['bid'])

        spreads={name1:spread1,name2:spread2}
                  
        position1={"dealId":paired_positions[name1]['position']['dealId'],"direction":paired_positions[name2]['position']['direction'],'size':paired_positions[name1]['position']['dealSize']}
        position2={"dealId":paired_positions[name2]['position']['dealId'],"direction":paired_positions[name1]['position']['direction'],'size':paired_positions[name2]['position']['dealSize']}

        close_dict={name1:position1,name2:position2}

        return open_prices,open_direction,positions_size,current_prices,spreads,close_dict
    



class PairsTrader(object):
    """description of class"""  
    
   
    

    def __init__(self,pair="crude_oil",igconnector=None,days="*",hours="*/4",minutes=0,monitor_min="*/5",monitoring=False,sec_offset=15):
       
        self.mean_return=pd.DataFrame()
        self.macd_hist=None

        self.trade_status=TradingStatus(pair,constants[pair])

        self._constants_dict=constants[pair]
        self.data_reader=DataReader(self._constants_dict)
        self.pca_df=pd.DataFrame()
        self.igconnector=igconnector
        self.isOpen=False
        self._open_positions_by_dealId_dict={}
        
        self._current_open_positions_dict={}
        self.marketinfo_df=pd.DataFrame()
        
        self._score_lag1=0.0
        self.canMonitor=False

        self.open_positions_tests=0
        #self.is_above_long_level=False
        #self.is_below_short_level=False
        #self.is_open_for_trades=False

       
        self.run_statarb_algo(days=days,hours=hours,minutes=minutes,sec_offset=sec_offset)
        time.sleep(30)
        
        if monitoring==True:
            self.run_position_monitoring(days='*',hours='*',minutes=monitor_min)

          


    def write_close_positions(self,new_data, filename='data.json'):

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


            
   
    def update_price_data(self,callback0, callback1):

       
        constants_dict=self._constants_dict

       

        callback0()
        if self.trade_status.marketinfo_df.shape[0]<1:
            return None
        

    
        self.trade_status._watchlist_df=self.igconnector.fetch_watchlist(constants_dict['watchlist_id'])

        #self.trade_status.get_open_positions_by_dealId_info(self._open_positions_by_dealId_dict,constants_dict,self.trade_status._watchlist_df,self.trade_status.marketinfo_df,isMonitor=False)




        #breakpoint()
        if self.trade_status._watchlist_df.shape[0]>0:
            self.data_reader.append_prices(watchlist_df=self.trade_status._watchlist_df[["epic","offer","bid"]])
            self.data_reader.write_newprices()
        
        self.trade_status.get_open_positions_by_dealId_info(self.trade_status._open_positions_by_dealId,constants_dict,self.trade_status._watchlist_df,self.trade_status.marketinfo_df,isMonitor=False)

        print("Runinng trading functions")
        callback1()
        self.canMonitor=True
        



    def check_open_positions_by_dealId(self):

        
        constants_dict=self._constants_dict

        open_positions=self.trade_status.get_open_positions_fromfile()


        #dealId1=open_positions[self.marketId1]['dealId']
        #dealId2=open_positions[self.marketId2]['dealId']
        self.open_positions_tests=0

        self.trade_status.marketinfo_df=self.igconnector.fetch_market_details(epics=list(constants_dict["epics"].values()),filename=constants_dict["marketinfo_filename"])

        if bool(open_positions):

            dealId1=open_positions[self.trade_status.marketId1]['dealId']
            dealId2=open_positions[self.trade_status.marketId2]['dealId']


            
            open_positions_dict=self.igconnector.get_open_positions_by_dealId([dealId1,dealId2])
            print("########################################query open positions###########################################################################")
            print(open_positions_dict)

            self.trade_status.update_open_positions(open_positions_dict)
            #breakpoint()   
        else:            
            self.trade_status._isOpen=False    
            self.trade_status._open_positions_by_dealId={}
            #self.open_positions_tests=0
            #self._current_open_positions_dict={}

        #breakpoint()



    def run_statarb_algo(self,days="*",hours=0,minutes=1,sec_offset=2):

          
        scheduler = BackgroundScheduler()

        self.update_price_data(self.check_open_positions_by_dealId,self.run_trading_functions)
    
        #breakpoint()   
        scheduler.add_job(self.update_price_data,args=[self.check_open_positions_by_dealId,self.run_trading_functions], trigger='cron',day_of_week=days, hour=hours,minute=minutes,second=sec_offset,jitter=2,timezone="UTC")
        scheduler.start()




    def create_quant_indicators(self):

       
        trade_df=self.data_reader.get_prices_df()
        trade_w_df,marketinfo_df=self.data_reader.make_wide(trade_df)
        
        #name1=constants_dict['marketIds'][constants_dict["market_names"][0]]
        #name2=constants_dict['marketIds'][constants_dict["market_names"][1]]
        #print(self.trade_status.name1)
        #print(trade_w_df.head())
        
        self.mean_return=trade_w_df[self.trade_status.name1+'_mean_ret']
        self.macd_hist=trade_w_df["macd_hist"].iloc[-1]


        self.mid_price1=trade_w_df["mid_price"][self.trade_status.name1].iloc[-1]
        self.mid_price2=trade_w_df["mid_price"][self.trade_status.name2].iloc[-1]
       

        col1=self._constants_dict['marketIds'][self._constants_dict['market_names'][0]]+"_return"
        col2=self._constants_dict['marketIds'][self._constants_dict['market_names'][1]]+"_return"
        wi=-self._constants_dict['trading_parameters']['look_out_window']
        

        data_pca=trade_w_df[[col1,col2]].iloc[-wi:]
        
        data_pca.columns=data_pca.columns.get_level_values(0)
        
        #print("\n")
        #print(data_pca.head(2))
        #print("\n")
        #print(data_pca.tail(2))
        #print("\n")

        if (data_pca.shape[0]<20):
            return

        quant_trader_ind= QuantIndicators(self._constants_dict)
        pca_res=quant_trader_ind.calculate_pca(data=data_pca)
        pca_res1=quant_trader_ind.calculate_size(pca_res)
        pca_res2=quant_trader_ind.calculate_score(pca_res1)
        self.pca_df=pca_res2.copy()

        self.pca_df['corr']=trade_w_df['corr'].iloc[-1]
    

    def run_trading_functions(self):

        self.create_quant_indicators()
        
       
       
        self.make_paired_trades(units=self._constants_dict['trading_parameters']['unit_size'],SL=self._constants_dict['trading_parameters']['stop_loss'],TP=self._constants_dict['trading_parameters']['take_profit'])
       
   

    def make_paired_trades(self,open_trades_file="open_positions_history.json",close_trades_file="close_positions_history.json",units=[1,1],SL=25.0,TP=45.0):
    
        if self.pca_df.shape[0]==0:
            return       
         
        score=self.pca_df["score"].iloc[0]
        correl=self.pca_df["corr"].iloc[0]
        constants_dict=self._constants_dict.copy()

        print("\t mid_price 1 :\t"+str(self.mid_price1)+"\n")
        print("\t mid_price 2 :\t"+str(self.mid_price2)+"\n")
   

        print("\t Scores :\t"+str(score)+"\n")
        print("\t Scores lag1:\t"+str(self._score_lag1)+"\n")
        print("\t Correlation :\t"+str(correl)+"\n")
        print("\t mean return 12: \t"+str(self.mean_return.iloc[-1])+"\n")
        print("\t macd hist: \t"+str(self.macd_hist)+"\n")
        #print("\t mean return 12: \t"+str(self.mean_return.iloc[-1])+"\n")


        print("\t PnL :\t"+str(self.trade_status.PnL)+"\n")


              


        if (not self.trade_status._isOpen) and (self.trade_status.tradeable1=="TRADEABLE") and (self.trade_status.tradeable2=="TRADEABLE"):
          
                     
            

            if ((self.mean_return.iloc[-1] < constants_dict['trading_parameters']['mean_ret_value'])|(self.mean_return.iloc[-1] > constants_dict['trading_parameters']['mean_ret_value'])) \
             and ((self.macd_hist < constants_dict['trading_parameters']['macd_hist_value']) or (self.macd_hist > constants_dict['trading_parameters']['macd_hist_value'])) :


                if ( score > constants_dict['trading_parameters']['short_entry'])  and (correl>constants_dict['trading_parameters']['min_correl']):

                
                    self.send_market_order(isLong=False,units=units)
                
                
                elif ( score < constants_dict['trading_parameters']['long_entry'])  and (correl>constants_dict['trading_parameters']['min_correl']):

                    self.send_market_order(isLong=True,units=units)
             
        
                else:
                    print("\t Did not open any positions \n")
                    print("########################################################### \n")
            

            self._score_lag1=score

    
            
                
        elif  (not self.trade_status._isOpen) and (( self.trade_status.tradeable1 !="TRADEABLE") or ( self.trade_status.tradeable2 !="TRADEABLE")):

             self._score_lag1=score
  
             print("\t Some Instruments are not TRADEABLE \n")
             print("########################################################### \n")
             return None

        elif self.trade_status._isOpen and (not self.trade_status.singlePosition):

            close_positions={}
            close_position1={}
            close_position2={}

            self._score_lag1=score
      

            if self.trade_status.isLong and (score > constants_dict['trading_parameters']['close_long']) and (self.trade_status.PnL > -15*units[0]):  

                

                close_position1,close_position2=self.igconnector.close_paired_position(marketIds=[self.trade_status.name1,self.trade_status.name2],positions=self.trade_status.close_dict)

            
            elif (not self.trade_status.isLong) and (score < constants_dict['trading_parameters']['close_short']) and (self.trade_status.PnL>-15*units[0]):   

                close_position1,close_position2=self.igconnector.close_paired_position(marketIds=[self.trade_status.name1,self.trade_status.name2],positions=self.trade_status.close_dict)
            
            elif self.trade_status.PnL>(TP*units[0]):
            
                close_position1,close_position2=self.igconnector.close_paired_position(marketIds=[self.trade_status.name1,self.trade_status.name2],positions=self.trade_status.close_dict)
            
            elif self.trade_status.PnL<(-SL*units[0]):
            
                close_position1,close_position2=self.igconnector.close_paired_position(marketIds=[self.trade_status.name1,self.trade_status.name2],positions=self.trade_status.close_dict)

            else:
                print("########################################################### \n")
            
                
            #if bool(close_position1) and  bool(close_position2):
            if (bool(close_position1) and  bool(close_position2)):

                if ( ('status' in close_position1.keys()) and ('status' in close_position2.keys())) :
             
                    if (close_position1['status'] in ["CLOSED","FULLY_CLOSED"]) and (close_position2['status'] in ["CLOSED","FULLY_CLOSED"]) :

                        close_positions={self.trade_status.name1:close_position1,self.trade_status.name2:close_position2}
                        self.trade_status._open_positions_by_dealId={}
                        self._current_open_positions_dict={}
                        self.trade_status._isOpen=False
                        self.write_close_positions(close_positions,constants_dict['close_positions_hist']) 
                        ##json.dump(close_positions, open( close_trades_file, 'w' )) 
                        with open(constants_dict['open_positions'],'w') as f:
                            json.dump({}, f,indent = 4) 
                        ##json.dump({}, open("open_positions.json", 'w' )) 
                        print("\t Close paired positions \n")
                        print("########################################################### \n")


        elif self.trade_status._isOpen and self.trade_status.singlePosition:

            close_positions={}

            self._score_lag1=score

            if (self.trade_status.PnL<-10):        
                close_position1=self.igconnector.close_single_position(marketIds=[self.trade_status.name1],positions=self.trade_status.close_dict)

            elif (self.trade_status.PnL>10):
                close_position1=self.igconnector.close_single_position(marketIds=[self.trade_status.name1],positions=self.trade_status.close_dict)

          
                
                if bool(close_position1) :
                    if  ('status' in close_position1.keys())  :
             
                        if close_position1['status'] in ["CLOSED","FULLY_CLOSED"]:

                            close_positions={self.trade_status.name1:close_position1}
                            self.trade_status._open_positions_by_dealId={}
                            self._current_open_positions_dict={}
                            self.trade_status._isOpen=False
                            self.write_close_positions(close_positions,constants_dict['close_positions_hist']) 
                            ##json.dump(close_positions, open( close_trades_file, 'w' )) 
                            with open(constants_dict['open_positions'],'w') as f:
                                json.dump({}, f,indent = 4) 
                            ##json.dump({}, open("open_positions.json", 'w' )) 
                            print("\t Close paired positions \n")
                            print("########################################################### \n")

    

    def send_market_order(self,isLong,units):

        constants_dict=self._constants_dict
        #breakpoint()
        order_size1=round(self.pca_df[self.trade_status.name1+"_size"].iloc[0]*self.trade_status.marketinfo_df[self.trade_status.marketinfo_df.marketId==self.trade_status.name1].minSize.iloc[0],constants_dict['decimals'][0])
        my_currency1=self.trade_status.marketinfo_df[self.trade_status.marketinfo_df.marketId==self.trade_status.name1].currency.iloc[0]

        stop_distance1=round(self.mid_price1*constants_dict["trading_parameters"]["stop_pct1"],1)
        stop_increment1=None
        limit_distance1=round(self.mid_price1*constants_dict["trading_parameters"]["take_pct1"],1)
        trail_stop1='false'
     

           

            
        order_size2=round(self.pca_df[self.trade_status.name2+"_size"].iloc[0]*self.trade_status.marketinfo_df[self.trade_status.marketinfo_df.marketId==self.trade_status.name2].minSize.iloc[0],constants_dict['decimals'][1])
        my_currency2=self.trade_status.marketinfo_df[self.trade_status.marketinfo_df.marketId==self.trade_status.name2].currency.iloc[0]

        stop_distance2=round(self.mid_price2*constants_dict["trading_parameters"]["stop_pct2"],1)
        stop_increment2=None
        limit_distance2=round(self.mid_price2*constants_dict["trading_parameters"]["take_pct2"],1)
        trail_stop2='false'


        if isLong:
            trade_order1={"direction":"SELL","epic":self.trade_status.epic1,"size":order_size1,"currency":my_currency1,"stop_distance":stop_distance1,"limit_distance":limit_distance1}
            trade_order2={"direction":"BUY","epic":self.trade_status.epic2,"size":order_size2,"currency":my_currency2,"stop_distance":stop_distance2,"limit_distance":limit_distance2}
        else:
            trade_order1={"direction":"BUY","epic":self.trade_status.epic1,"size":order_size1,"currency":my_currency1,"stop_distance":stop_distance1,"limit_distance":limit_distance1}
            trade_order2={"direction":"SELL","epic":self.trade_status.epic2,"size":order_size2,"currency":my_currency2,"stop_distance":stop_distance2,"limit_distance":limit_distance2}


        order_dict={self.trade_status.name1:trade_order1,self.trade_status.name2:trade_order2}
        print(order_dict)
        #breakpoint()
        open_position1, open_position2=self.igconnector.open_paired_position(marketIds=[self.trade_status.name1,self.trade_status.name2],positions=order_dict,units=units,decimals=constants_dict['decimals'],open_json=constants_dict["open_positions"])


        if bool(open_position1) and bool(open_position2):
            if open_position1['status']=="OPEN" and open_position2['status']=="OPEN":   
                self._current_open_positions_dict={self.trade_status.name1:open_position1,self.trade_status.name2:open_position2}
                print("\t OPEN short paired position \n")
                self.isOpen=True
                print("########################################################### \n")


    

    

    def run_position_monitoring(self,days="*",hours=0,minutes="*/5"):


        scheduler=BackgroundScheduler()

        scheduler.add_job(self.monitor_open_positions_by_dealId,trigger='cron',day_of_week=days,hour=hours,minute=minutes,second='20',timezone="UTC")
        scheduler.start()




        

    def monitor_open_positions_by_dealId(self,open_hours=range(0,23,1),filename="file.txt"):

        
        constants_dict=self._constants_dict
        hour_now=datetime.now().hour
        if self.canMonitor==False:
            return

        
        

        #if not (bool(self.trade_status._open_positions_by_instrumentId.copy())):
        #   self.trade_status.get_open_positions_by_dealId_fromfile()

        
        if not (hour_now in open_hours):
           print("Outside monitoring hours")
           return
                        
               


        if not bool(self.trade_status._open_positions_by_dealId):          

            if not bool(self._current_open_positions_dict):            
                            

                print("Nothing to monitor")
                return

            elif bool(self._current_open_positions_dict):

                self.update_trade_status(self._current_open_positions_dict[self.trade_status.name1]['dealId'],self._current_open_positions_dict[self.trade_status.name2]['dealId'])



        elif  bool(self.trade_status._open_positions_by_dealId):

            #self.trade_status.get_open_positions_by_dealId_fromfile()
            #print(self.trade_status._open_positions_by_dealId)
            print(self.trade_status.dealIds)


            if len(self.trade_status._open_positions_by_dealId)==1:
            #dealId1=self.trade_status.dealIds[0]
            #dealId2=None

                self.update_trade_status(self.trade_status.dealIds[0],None)

            elif len(self.trade_status._open_positions_by_dealId)==2:
            #dealId1=self.trade_status.dealIds[0]
            #dealId2=self.trade_status.dealIds[1]
                 if len(self.trade_status.dealIds)>1:

                     self.update_trade_status(self.trade_status.dealIds[0],self.trade_status.dealIds[1])

                 elif len(self.trade_status.dealIds)==1:

                     self.update_trade_status(self.trade_status.dealIds[0],None)


            #self.update_trade_status()
            
            #if boot(self._open_positions_by_instrumentId):

            #     self.update_trade_status(self.trade_status.dealId1_infile,self.trade_status.dealId2_infile)


        

            
            
           

        
    def update_trade_status(self,dealId1=None,dealId2=None):

        #dealId1=None
        #dealId2=None
        #dealId1=self.trade_status.dealIds[0]
        #if len(self.trade_status._open_positions_by_dealId)==1:
        #    dealId1=self.trade_status.dealIds[0]
        #    dealId2=None
        #elif len(self.trade_status._open_positions_by_dealId)==2:
        #    dealId1=self.trade_status.dealIds[0]
        #    dealId2=self.trade_status.dealIds[1]
 

        #breakpoint()

        if (dealId1 is not None) and (dealId2 is not None):
            #breakpoint()
            self._open_positions_by_dealId_dict=self.igconnector.get_open_positions_by_dealId([dealId1,dealId2])


            if not (self._open_positions_by_dealId_dict):

                print("NA")
                self.open_positions_tests+=1
                if self.open_positions_tests>4:
                    self._current_open_positions_dict={}
                    
                return 

            else:            

                self.run_monitoring_updates()

        elif  (dealId1 is not None) and (dealId2 is None):
            #breakpoint()
            self._open_positions_by_dealId_dict=self.igconnector.get_open_positions_by_dealId([dealId1])


            if not (self._open_positions_by_dealId_dict):
                print("NA")
                self.open_positions_tests+=1
                if self.open_positions_tests>4:
                    self._current_open_positions_dict={}
 
                return 

            else:     
                        
                self.run_monitoring_updates()


        elif  (dealId1 is None) and  (dealId2 is not None):
            
            #breakpoint()
            self._open_positions_by_dealId_dict=self.igconnector.get_open_positions_by_dealId([dealId2])


            if not (self._open_positions_by_dealId_dict):
                print("NA")
                self.open_positions_tests+=1
                if self.open_positions_tests>4:
                    self._current_open_positions_dict={}
 
                return 

            else:     
                        
                self.run_monitoring_updates()
                

 
    def run_monitoring_updates(self):

        #self.trade_status._watchlist_df=self.igconnector.fetch_watchlist(self._constants_dict['watchlist_id'])
        #print(self._open_positions_by_dealId_dict)
        #breakpoint()

        if (self.trade_status.marketinfo_df.empty) :
            self.trade_status.marketinfo_df=self.igconnector.fetch_market_details(epics=list(self._constants_dict["epics"].values()),filename=self._constants_dict["marketinfo_filename"])
            
        self.trade_status.get_open_positions_monitor_info(self._constants_dict,self._open_positions_by_dealId_dict,self.trade_status.marketinfo_df)
        self.check_open_positions_by_dealId_value()
        

    def check_open_positions_by_dealId_value(self):         


        #info_list=self.get_open_positions_by_dealId_info(paired_positions,constants_dict,watchlist_df, marketinfo_df)
        
        #PnL=info_list[14]
        print("Profit : \n")
        print("PnL : "+str(round(self.trade_status.PnL,2)))
        print("\n")
        print("Instruments: "+self.trade_status.name1+'\t'+self.trade_status.name2)
        #return_values=[singlePosition,name1,name2,epic2,epic2,paired_positions,open_prices,open_direction,positions_size,current_prices,spreads,close_dict,tradeable1,tradeable2]
        #breakpoint()
        print("singlePosition " + str(self.trade_status.singlePosition) +" tradeable1 " + str(self.trade_status.tradeable1=="TRADEABLE") +" tradeable2 "+ str(self.trade_status.tradeable2=="TRADEABLE"))

        if (not self.trade_status.singlePosition) and (self.trade_status.tradeable1=="TRADEABLE") and (self.trade_status.tradeable2=="TRADEABLE"):
            
            close_positions={}
            close_position1={}
            close_position2={}

            #breakpoint()
            print("Test PnL")
            if self.trade_status.PnL > (10):
                print("Profit close")
                
                close_position1,close_position2=self.igconnector.close_paired_position(marketIds=[self.trade_status.name1,self.trade_status.name2],positions=self.trade_status.close_dict)
            
            elif self.trade_status.PnL < (-20):
                print("Lose close")
            
                close_position1,close_position2=self.igconnector.close_paired_position(marketIds=[self.trade_status.name1,self.trade_status.name2],positions=self.trade_status.close_dict)

            else:
                print("########################################################### \n")
            
                
            if bool(close_position1) and  bool(close_position2):
                if (close_position1['status'] in ["CLOSED","FULLY_CLOSED"]) and (close_position2['status'] in ["CLOSED","FULLY_CLOSED"]) :

                    close_positions={self.trade_status.name1:close_position1,self.trade_status.name2:close_position2}
                    self.trade_status._open_positions_by_dealId={}
                    self.trade_status._isOpen=False
                    self.write_close_positions(close_positions,self.trade_status.constants_dict['close_positions_hist']) 
                    ##json.dump(close_positions, open( close_trades_file, 'w' )) 
                    with open(self.trade_status.constants_dict['open_positions'],'w') as f:
                        json.dump({}, f,indent = 4) 
                    ##json.dump({}, open("open_positions.json", 'w' )) 
                    print("\t Close paired positions \n")
                    print("########################################################### \n")
        

        elif (self.trade_status.singlePosition) and (self.trade_status.tradeable1=="TRADEABLE")  :
            close_position={}
            close_position1={}
 
            if self.trade_status.PnL > 10:
                print("Profit close single")
                close_position1=self.igconnector.close_single_position(marketIds=[self.trade_status.name1],positions=self.trade_status.close_dict)
            
            elif self.trade_status.PnL < (-20):
                print("Lose close single")
            
                close_position1=self.igconnector.close_single_position(marketIds=[self.trade_status.name1],positions=self.trade_status.close_dict)

            else:
                print("########################################################### \n")
            
                
            if bool(close_position1):
                if close_position1['status'] in ["CLOSED","FULLY_CLOSED"]:

                    close_positions={self.trade_status.name1:close_position1}

                    self.write_close_positions(close_positions,self.trade_status.constants_dict['close_positions_hist']) 
                    ##json.dump(close_positions, open( close_trades_file, 'w' )) 
                    with open(self.trade_status.constants_dict['open_positions'],'w') as f:
                        json.dump({}, f,indent = 4) 
                    ##json.dump({}, open("open_positions.json", 'w' )) 
                    print("\t Close paired positions \n")
                    print("########################################################### \n")
 


        



class PairsTraderIDX(PairsTrader):


   

    def make_paired_trades(self,open_trades_file="open_positions_history.json",close_trades_file="close_positions_history.json",units=[1,1],SL=25.0,TP=45.0):
    
        if self.pca_df.shape[0]==0:
            return       
         
         
        score=self.pca_df["score"].iloc[0]
        correl=self.pca_df["corr"].iloc[0]

        constants_dict=self._constants_dict.copy()


        print("\t mid_price 1 :\t"+str(self.mid_price1)+"\n")
        print("\t mid_price 2 :\t"+str(self.mid_price2)+"\n")
   

        print("\t Scores :\t"+str(score)+"\n")
        print("\t Scores lag1:\t"+str(self._score_lag1)+"\n")
        print("\t Correlation :\t"+str(correl)+"\n")
        print("\t mean return 12: \t"+str(self.mean_return.iloc[-1])+"\n")
        print("\t macd hist: \t"+str(self.macd_hist)+"\n")
        

              
        print("\t PnL :\t"+str(self.trade_status.PnL)+"\n")


        if (not self.trade_status._isOpen) and (self.trade_status.tradeable1=="TRADEABLE") and (self.trade_status.tradeable2=="TRADEABLE"):
          
                     
            

           if ((self.mean_return.iloc[-1] < constants_dict['trading_parameters']['mean_ret_value'])|(self.mean_return.iloc[-1] > constants_dict['trading_parameters']['mean_ret_value'])) \
           and ((self.macd_hist < constants_dict['trading_parameters']['macd_hist_value']) or (self.macd_hist > constants_dict['trading_parameters']['macd_hist_value'])) :


                if ( score > constants_dict['trading_parameters']['short_entry']) and (self._score_lag1 > -1) and (correl>constants_dict['trading_parameters']['min_correl']):

                    self.send_market_order(isLong=False,units=units)
                
                
                elif ( score < constants_dict['trading_parameters']['long_entry']) and (self._score_lag1 < 1) and (correl>constants_dict['trading_parameters']['min_correl']):

                    self.send_market_order(isLong=True,units=units)
             
        
                else:
                    print("\t Did not open any positions \n")
                    print("########################################################### \n")
            

           self._score_lag1=score

    
            
                
        elif  (not self.trade_status._isOpen) and (( self.trade_status.tradeable1 !="TRADEABLE") or ( self.trade_status.tradeable2 !="TRADEABLE")):

             self._score_lag1=score
  
             print("\t Some Instruments are not TRADEABLE \n")
             print("########################################################### \n")
             return None

        elif self.trade_status._isOpen and (not self.trade_status.singlePosition):

            close_positions={}
            close_position1={}
            close_position2={}

            self._score_lag1=score
            print("Close positions dict")
            print(self.trade_status.close_dict)
            #breakpoint()
            if self.trade_status.isLong and (score > constants_dict['trading_parameters']['close_long']) and (self.trade_status.PnL > 1*units[0]):  

                

                close_position1,close_position2=self.igconnector.close_paired_position(marketIds=[self.trade_status.name1,self.trade_status.name2],positions=self.trade_status.close_dict,open_json=constants_dict['open_positions'])
            
            elif (not self.trade_status.isLong) and (score < constants_dict['trading_parameters']['close_short']) and (self.trade_status.PnL>1*units[0]):   

                close_position1,close_position2=self.igconnector.close_paired_position(marketIds=[self.trade_status.name1,self.trade_status.name2],positions=self.trade_status.close_dict,open_json=constants_dict['open_positions'])
            
            elif self.trade_status.PnL>(TP*units[0]):
            
                close_position1,close_position2=self.igconnector.close_paired_position(marketIds=[self.trade_status.name1,self.trade_status.name2],positions=self.trade_status.close_dict,open_json=constants_dict['open_positions'])
            
            elif self.trade_status.PnL<(-50):
            
                close_position1,close_position2=self.igconnector.close_paired_position(marketIds=[self.trade_status.name1,self.trade_status.name2],positions=self.trade_status.close_dict,open_json=constants_dict['open_positions'])

            else:
                print("########################################################### \n")
            
                
            if (bool(close_position1) and  bool(close_position2)):
                if ( ('status' in close_position1.keys()) and ('status' in close_position2.keys())) :
                    if (close_position1['status'] in ["CLOSED","FULLY_CLOSED"]) and (close_position2['status'] in ["CLOSED","FULLY_CLOSED"]) :

                        close_positions={self.trade_status.name1:close_position1,self.trade_status.name2:close_position2}
                        self.__open_positions_by_dealId_dict={}
                        self.trade_status._open_positions_by_dealId={}
                        #self.trade_status._isOpen=False
                        self.trade_status._isOpen=False
                        self.write_close_positions(close_positions,constants_dict['close_positions_hist']) 
                        ##json.dump(close_positions, open( close_trades_file, 'w' )) 
                        with open(constants_dict['open_positions'],'w') as f:
                           json.dump({}, f,indent = 4) 
                        ##json.dump({}, open("open_positions.json", 'w' )) 
                        print("\t Close paired positions \n")
                        print("########################################################### \n")


        elif self.trade_status._isOpen and self.trade_status.singlePosition:

            close_positions={}

            self._score_lag1=score
            print("Close positions dict")
            print(self.trade_status.close_dict)
            #
            if (self.trade_status.PnL<-10):
                close_position1=self.igconnector.close_single_position(marketIds=[self.trade_status.name1],positions=self.trade_status.close_dict)
            elif (self.trade_status.PnL>10):
                close_position1=self.igconnector.close_single_position(marketIds=[self.trade_status.name1],positions=self.trade_status.close_dict)


                   
                
                if bool(close_position1):
                    if ('status' in close_position1.keys())  :
                        if close_position1['status'] in ["CLOSED","FULLY_CLOSED"]:

                            close_positions={self.trade_status.name1:close_position1}
                            self.__open_positions_by_dealId_dict={}
                            self.trade_status._open_positions_by_dealId={}
      
                            self.trade_status._isOpen=False
                            self.write_close_positions(close_positions,constants_dict['close_positions_hist']) 
                            ##json.dump(close_positions, open( close_trades_file, 'w' )) 
                            with open(constants_dict['open_positions'],'w') as f:
                                json.dump({}, f,indent = 4) 
                            ##json.dump({}, open("open_positions.json", 'w' )) 
                            print("\t Close paired positions \n")
                            print("########################################################### \n")


