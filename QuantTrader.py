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




class QuantTrader(object):
    """description of class"""  
    
    marketinfo_df=pd.DataFrame()
    current_open_positions={}
    constants_dict={}
    mean_return=pd.DataFrame()


    def __init__(self,pair="crude_oil",igconnector=None,hours="*/4",minutes=0,sec_offset=15):
        #self.epic1=constants_dict["epics"][constants_dict["market_names"][0]]

        self.__constants_dict=constants[pair]
        self.data_reader=DataReader(self.__constants_dict)
        self.pca_df=pd.DataFrame()
        self.igconnector=igconnector
        self.__open_positions_dict={}
        self.run_pairs_algo(hours=hours,minutes=minutes,sec_offset=sec_offset)

        


        ##self.epic2=constants_dict["epics"][constants_dict["market_names"][1]]

        ##self.name1=constants_dict["marketIds"][constants_dict["market_names"][0]]
        ##self.name2=constants_dict["marketIds"][constants_dict["market_names"][1]]

        ##self.trade_df = trade_df
        ##self.marketinfo_df = marketinfo_df
    


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





    def monitor_open_positions(self,filename="file.txt"):

        ##global open_positions_dict
        ##global igconnector
        constants_dict=self.__constants_dict

        positions_dict = json.load( open(self.__constants_dict['open_positions']) )

        ##igconnector=IGConnector(account_id,acc_password,api_key,acc_environment)    
        ##session_details=igconnector.create_ig_session()

        marketId1=constants_dict["marketIds"][constants_dict["market_names"][0]]
        marketId2=constants_dict["marketIds"][constants_dict["market_names"][1]]

        


        if bool(positions_dict):
            dealId1=positions_dict[marketId1]['dealId']
            dealId2=positions_dict[marketId2]['dealId']

            ######################################################################
            ##TO DO 
            ### Read file from Streaming with current prices..............

            ##open_positions_dict0=self.igconnector.get_open_positions_by_dealId([dealId1,dealId2])
            prices_df=pd.read_csv(filename, sep='\t', lineterminator='\n',header=None,names=["updateTime","epic","offer","bid"])

           


    def check_open_positions(self):

        ##global open_positions_dict
        ##global igconnector
        constants_dict=self.__constants_dict

        if os.path.isfile(self.__constants_dict['open_positions']):    
            
            with open(self.__constants_dict['open_positions']) as f: 
                positions_dict = json.load(f)
            print("########################################################file open positions")
            print(positions_dict)
        else:
            positions_dict={}

        ##igconnector=IGConnector(account_id,acc_password,api_key,acc_environment)    
        ##session_details=igconnector.create_ig_session()

        marketId1=constants_dict["marketIds"][constants_dict["market_names"][0]]
        marketId2=constants_dict["marketIds"][constants_dict["market_names"][1]]

        #dealId1=positions_dict[marketIds[0]]['dealId']
        #dealId2=positions_dict[marketIds[1]]['dealId']    


        if bool(positions_dict):
            dealId1=positions_dict[marketId1]['dealId']
            dealId2=positions_dict[marketId2]['dealId']
            open_positions_dict0=self.igconnector.get_open_positions_by_dealId([dealId1,dealId2])
            print("########################################query open positions")
            print(open_positions_dict0)
            if  not bool(open_positions_dict0):
                self.marketinfo_df=self.igconnector.fetch_market_details(epics=list(constants_dict["epics"].values()),filename=constants_dict["marketinfo_filename"])
                ##self.current_open_positions={}
                ##self.__open_positions_dict={}
            else:
                self.marketinfo_df=self.igconnector.fetch_market_details(epics=list(constants_dict["epics"].values()),filename=constants_dict["marketinfo_filename"])
                self.__open_positions_dict=open_positions_dict0
           

            


        else:
             self.marketinfo_df=self.igconnector.fetch_market_details(epics=list(constants_dict["epics"].values()),filename=constants_dict["marketinfo_filename"])
             ##open_positions_dict={}


    #open_positions_dict=read_open_positions()
    #print(open_positions_dict)




    def create_quant_indicators(self):

        ##global data_reader
        ##global open_positions_dict
        ##global pca_df
        ##global marketinfo_df
        constants_dict=self.__constants_dict
        trade_df=self.data_reader.get_prices_df()
        trade_w_df,marketinfo_df=self.data_reader.make_wide(trade_df)

        self.mean_return=trade_w_df[name1+'_mean_ret']

        print(marketinfo_df.columns)
        print(marketinfo_df.dtypes)
        ##print(marketinfo_df.head(2))

        col1=constants_dict['marketIds'][constants_dict['market_names'][0]]+"_return"
        col2=constants_dict['marketIds'][constants_dict['market_names'][1]]+"_return"
        wi=-constants_dict['trading_parameters']['look_out_window']
        ##print(trade_w_df.columns)

        data_pca=trade_w_df[[col1,col2]].iloc[-wi:]
        ##print(data_pca.columns)
        data_pca.columns=data_pca.columns.get_level_values(0)
        print(data_pca.columns)
        print("\n")
        print(data_pca.head(2))
        print("\n")
        print(data_pca.tail(2))
        print("\n")


        quant_trader_ind= QuantIndicators(constants_dict)
        pca_res=quant_trader_ind.calculate_pca(data=data_pca)
        pca_res1=quant_trader_ind.calculate_size(pca_res)
        pca_res2=quant_trader_ind.calculate_score(pca_res1)
        self.pca_df=pca_res2

        self.pca_df['corr']=trade_w_df['corr'].iloc[-1]

        ##print(pca_df.columns)    
        ##print(pca_df.head(2))
        #print(pca_df.columns)

    


    def create_single_position_dictionaries(self,paired_positions,name1):

        open_direction={name1:paired_positions[name1]['position']['direction']}
        dealIds={name1:paired_positions[name1]['position']['dealId']}
        positions_size={name1:paired_positions[name1]['position']['dealSize']}
        open_prices={name1:paired_positions[name1]['position']['openLevel']}
        #close_direction={name1:paired_positions[name2]['position']['direction']}

        ##print(open_prices)

        current_price1=(paired_positions[name1]['market']['bid']+paired_positions[name1]['market']['offer'])/2
        #current_price2=(paired_positions[name2]['market']['bid']+paired_positions[name2]['market']['offer'])/2

        current_prices={name1:current_price1}

        ##print(str(current_price1))
        ##print(str(current_price2))

        spread1=(paired_positions[name1]['market']['offer']-paired_positions[name1]['market']['bid'])
        #spread2=(paired_positions[name2]['market']['offer']-paired_positions[name2]['market']['bid'])

        ##print(str(spread1))
        ##print(str(spread2))
        spreads={name1:spread1}
        

            

        if paired_positions[name1]['position']['direction']=="BUY":
            position1={"dealId":paired_positions[name1]['position']['dealId'],"direction":"SELL",'size':paired_positions[name1]['position']['dealSize']}
        else:
            position1={"dealId":paired_positions[name1]['position']['dealId'],"direction":"BUY",'size':paired_positions[name1]['position']['dealSize']}

        #position2={"dealId":paired_positions[name2]['position']['dealId'],"direction":paired_positions[name1]['position']['direction'],'size':paired_positions[name2]['position']['dealSize']}

        close_dict={name1:position1}

        return open_prices,open_direction,positions_size,current_prices,spreads,close_dict    



    def create_open_position_dictionaries(self,paired_positions,name1,name2):

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

    def make_paired_trades(self,open_trades_file="open_positions_history.json",close_trades_file="close_positions_history.json",units=[1,1],SL=25.0,TP=45.0):
    
        #paired_positions = json.load( open(open_trades_file) )

        #global open_positions_dict
        #global data_reader
        #global open_positions_dict
        #global pca_df
        #global marketinfo_df
        #global igconnector
        marketinfo_df=self.marketinfo_df.copy()
        print(marketinfo_df.dtypes)
        constants_dict=self.__constants_dict
    
        pca_res2=self.pca_df.copy()
        paired_positions=self.__open_positions_dict

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
        print("\t mean return 12: \t"+str(self.mean_return.iloc[-1])+"\n")


        if not bool(paired_positions):
        
            ##name1=marketIds[market_names[0]]
            name1=constants_dict['marketIds'][constants_dict['market_names'][0]]
            name2=constants_dict['marketIds'][constants_dict['market_names'][1]]

            ##print("\t"+name1+"\t"+name2)
            #print(name2)

            epic1=constants_dict['ids_epics'][name1]
            epic2=constants_dict['ids_epics'][name2]

            tradeable1=marketinfo_df[marketinfo_df.marketId==name1].marketStatus.iloc[0]
            tradeable2=marketinfo_df[marketinfo_df.marketId==name2].marketStatus.iloc[0]

    
    
    
        elif bool(paired_positions):

            ###paired_positions=open_positions_dict

            #name1=market_names[0]
            #name2=market_names[1]

            #id1=marketIds[market_names[0]]
            #id2=marketIds[market_names[1]]
            if paired_positions>1:

                isOpen=True 
                print(isOpen)

    
                #i=0

                for key in paired_positions:
                    dealId_epic[key]=paired_positions[key]['market']['epic']
                    #id0=epics_ids[epic0]
                    #paired_positions[id0] = paired_positions.pop(key)
                    dealIds.append(key)
                    #i+=1

        
                epic01=dealId_epic[dealIds[0]]
                epic02=dealId_epic[dealIds[1]]

                if constants_dict['epics_ids'][epic01]==constants_dict['marketIds'][constants_dict['market_names'][0]]:
                    name1=constants_dict['epics_ids'][epic01]
                    name2=constants_dict['epics_ids'][epic02]
                elif constants_dict['epics_ids'][epic01]==constants_dict['marketIds'][constants_dict['market_names'][1]]:
                    name2=constants_dict['epics_ids'][epic01]
                    name1=constants_dict['epics_ids'][epic02]
        

                epic1=constants_dict['ids_epics'][name1]
                epic2=constants_dict['ids_epics'][name2]
        
                ##print("\t"+name1+"\t"+name2)
                #print(name1)
                #print(name2)

                paired_positions[name1] = paired_positions.pop(dealIds[0])
                paired_positions[name2] = paired_positions.pop(dealIds[1])



                if (paired_positions[name1]['market']['marketStatus']=="TRADEABLE") and (paired_positions[name2]['market']['marketStatus']=="TRADEABLE"):   
        
                     
            

                    open_prices,open_direction,positions_size,current_prices,spreads,close_dict=self.create_open_position_dictionaries(paired_positions,name1,name2)
              
                      
        
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

        
        if paired_positions==1:

        
            isOpen=True 
            singlePosition=True
    
           

            for key in paired_positions:
                dealId_epic[key]=paired_positions[key]['market']['epic']
               
                dealIds.append(key)
                #i+=1

        
            epic01=dealId_epic[dealIds[0]]

            
            name1=epics_ids[epic01]
               
            
        

            epic1=ids_epics[name1]
            
        
       

            paired_positions[name1] = paired_positions.pop(dealIds[0])
           



            if (paired_positions[name1]['market']['marketStatus']=="TRADEABLE"):   
        
                     
            

                open_prices,open_direction,positions_size,current_prices,spreads,close_dict=self.create_single_position_dictionaries(paired_positions,name1)
              
                      
        
                if open_direction[name1]=="BUY":

                    isLong=True
                    profit.append((positions_size[name1]*(current_prices[name1]-open_prices[name1]-spreads[name1]/2))/marketinfo_df[marketinfo_df.marketId==name1].exchangeRate.iloc[0])
                    #profit.append((positions_size[name2]*(open_prices[name2]-current_prices[name2]-spreads[name2]/2))/marketinfo_df[marketinfo_df.marketId==name2].exchangeRate.iloc[0])

        
                elif open_direction[name1]=="SELL":
                
                    isShort=True
                    #profit.append((positions_size[name2]*(current_prices[name2]-open_prices[name2]-spreads[name2]/2))/marketinfo_df[marketinfo_df.marketId==name2].exchangeRate.iloc[0])
                    profit.append((positions_size[name1]*(open_prices[name1]-current_prices[name1]-spreads[name1]/2))//marketinfo_df[marketinfo_df.marketId==name1].exchangeRate.iloc[0])

                PnL=sum(profit)
                print("\t PnL :\t"+str(PnL)+"\n")

            else: 
                print("\t Some Instruments are not TRADEABLE \n")
                print("########################################################### \n")
                return None


              


        if (not isOpen) and (tradeable1=="TRADEABLE") and (tradeable2=="TRADEABLE"):

            order_size1=round(pca_res2[name1+"_size"].iloc[0]*marketinfo_df[marketinfo_df.marketId==name1].minSize.iloc[0],2)
            #stop_distance1=(350/marketinfo_df[marketinfo_df.marketId==name1].pipValue.iloc[0])*marketinfo_df[marketinfo_df.marketId==name1].exchangeRate.iloc[0]
            my_currency1=marketinfo_df[marketinfo_df.marketId==name1].currency.iloc[0]
            stop_distance1=40
            stop_increment1=30
            limit_distance1=80
     

           

            
            order_size2=round(pca_res2[name2+"_size"].iloc[0]*marketinfo_df[marketinfo_df.marketId==name2].minSize.iloc[0],2)
            #order_size2=round(pca_res2[name2+"_size"].iloc[0]*marketinfo_df[marketinfo_df.marketId==name2].minSize.iloc[0],2)

            stop_distance2=60
            stop_increment2=50
            limit_distance2=100
           

            
            
            #stop_distance2=(350/marketinfo_df[marketinfo_df.marketId==name2].pipValue.iloc[0])*marketinfo_df[marketinfo_df.marketId==name2].exchangeRate.iloc[0]
            my_currency2=marketinfo_df[marketinfo_df.marketId==name2].currency.iloc[0]


            if ( score > constants_dict['trading_parameters']['short_entry']) and (correl>constants_dict['trading_parameters']['min_correl']) :

                trade_order1={"direction":"SELL","epic":epic1,"size":order_size1,"currency":my_currency1,"stop_distance":None}
                trade_order2={"direction":"BUY","epic":epic2,"size":order_size2,"currency":my_currency2,"stop_distance":None}

                order_dict={name1:trade_order1,name2:trade_order2}
                print(order_dict)
            
                open_position1, open_position2=self.igconnector.open_paired_position(marketIds=[name1,name2],positions=order_dict,units=units,open_json=constants_dict["open_positions"])


                if bool(open_position1) and bool(open_position2):
                    if open_position1['status']=="OPEN" and open_position2['status']=="OPEN":   
                        print("\t OPEN short paired position \n")
                        print("########################################################### \n")
                
            elif ( score < constants_dict['trading_parameters']['long_entry']) and (correl>constants_dict['trading_parameters']['min_correl']):

                trade_order1={"direction":"BUY","epic":epic1,"size":order_size1,"currency":my_currency1,"stop_distance":None}
                trade_order2={"direction":"SELL","epic":epic2,"size":order_size2,"currency":my_currency2,"stop_distance":None}

                order_dict={name1:trade_order1,name2:trade_order2}
                print(order_dict)
            
                open_position1, open_position2=self.igconnector.open_paired_position(marketIds=[name1,name2],positions=order_dict,units=units,open_json=constants_dict["open_positions"])

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
                    self.current_open_positions=open_positions
                    self.write_close_positions(open_positions,constants_dict['open_positions_hist']) 
                    ###json.dump(open_positions, open(open_trades_file, 'w' ) )
                    ###print("OPEN paired position")
                
        elif  (not isOpen) and ((tradeable1!="TRADEABLE") or (tradeable2!="TRADEABLE")):
             print("\t Some Instruments are not TRADEABLE \n")
             print("########################################################### \n")
             return None

        elif isOpen and (not singlePosition):
            close_positions={}

        

            if isLong and (score > constants_dict['trading_parameters']['close_long']) and (PnL > -15*units[0]):  

                close_position1,close_position2=self.igconnector.close_paired_position(marketIds=[name1,name2],positions=close_dict)

            
            elif isShort and (score < constants_dict['trading_parameters']['close_short']) and (PnL>-15*units[0]):   

                close_position1,close_position2=self.igconnector.close_paired_position(marketIds=[name1,name2],positions=close_dict)
            
            elif PnL>(TP*units[0]):
            
                close_position1,close_position2=self.igconnector.close_paired_position(marketIds=[name1,name2],positions=close_dict)
            
            elif PnL<(-SL*units[0]):
            
                close_position1,close_position2=self.igconnector.close_paired_position(marketIds=[name1,name2],positions=close_dict)

            else:
                print("########################################################### \n")
            
                
            if bool(close_position1) and  bool(close_position2):
                if close_position1['status']=="CLOSED" and close_position2['status']=="CLOSED" :

                    close_positions={name1:close_position1,name2:close_position2}

                    self.write_close_positions(close_positions,constants_dict['close_positions_hist']) 
                    ##json.dump(close_positions, open( close_trades_file, 'w' )) 
                    with open(constants_dict['open_positions'],'w') as f:
                        json.dump({}, f,indent = 4) 
                    ##json.dump({}, open("open_positions.json", 'w' )) 
                    print("\t Close paired positions \n")
                    print("########################################################### \n")


        elif isOpen and singlePosition:
            close_positions={}

        

        
            
            close_position1=self.igconnector.close_single_position(marketIds=[name1],positions=close_dict)

                   
                
            if bool(close_position1):
                if close_position1['status']=="CLOSED":

                    close_positions={name1:close_position1}

                    self.write_close_positions(close_positions,constants_dict['close_positions_hist']) 
                    ##json.dump(close_positions, open( close_trades_file, 'w' )) 
                    with open(constants_dict['open_positions'],'w') as f:
                        json.dump({}, f,indent = 4) 
                    ##json.dump({}, open("open_positions.json", 'w' )) 
                    print("\t Close paired positions \n")
                    print("########################################################### \n")

    



    def run_trading_functions(self):

        self.create_quant_indicators()

       
        self.make_paired_trades(units=self.__constants_dict['trading_parameters']['unit_size'],SL=self.__constants_dict['trading_parameters']['stop_loss'],TP=self.__constants_dict['trading_parameters']['take_profit'])
       

    
   
    def update_price_data(self,callback0, callback1):

        ##global data_reader
        ##global igconnector
        ##global open_positions_dict
        constants_dict=self.__constants_dict

        ##sys.stdout=open("output.txt","a")
    
        #session_details=self.igconnector.create_ig_session()
        #print("\t"+str(session_details)+"\n")
        ##self.igconnector.refresh_session()
        #print("\t"+str(session_details)+"\n")

        callback0()
        if self.marketinfo_df.empty:
            self.__open_positions_dict={}
            return None
        #time.sleep()

    
        watchlist_df=self.igconnector.fetch_watchlist(constants_dict['watchlist_id'])

        if watchlist_df.shape[0]>1:
            self.data_reader.append_prices(watchlist_df=watchlist_df[["epic","offer","bid"]])
            self.data_reader.write_newprices()
        
        #drawdown=session_details['accountInfo']['profitLoss']/session_details['accountInfo']['available']
        #if drawdown>0.25:
        #    self.__open_positions_dict={}
        #    return None

        callback1()
        #del igconnector
        #del self.igconnector
        ##self.igconnector.disconnect()
        self.__open_positions_dict={}

        ##sys.stdout.close()


    def run_pairs_algo(self,hours="*/4",minutes=0,sec_offset=15):

            #data_reader=DataReader(epics,market_names,marketIds)
            #pca_df=pd.DataFrame()
            #igconnector=IGConnector(account_id,acc_password,api_key,acc_environment)    
            #open_positions_dict={}
           

        #scheduler = BlockingScheduler()
        scheduler = BackgroundScheduler()

        #self.update_price_data(self.check_open_positions,self.run_trading_functions)
    
           
        scheduler.add_job(self.update_price_data,args=[self.check_open_positions,self.run_trading_functions], trigger='cron', hour=hours,minute=minutes,second=sec_offset,jitter=2,timezone="UTC")
        scheduler.start()
