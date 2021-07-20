import os
import sys
import json
import numpy as np
from sklearn.decomposition import PCA
import pandas as pd
from statsmodels.tsa.stattools import adfuller
from pandas.tseries.offsets import DateOffset
from datetime import datetime



class QuantTrader(object):
    """description of class"""


    trade_df=None
    marketinfo_df=None

          

    def __init__(self, epics,market_names,marketIds):
        self.epic1=epics[market_names[0]]
        self.epic2=epics[market_names[1]]

        self.name1=marketIds[market_names[0]]
        self.name2=marketIds[market_names[1]]

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




    

    def make_paired_trades(data_df,pca_df,marketinfo_df,open_trades_file,close_trades_file,units=[1,1],SL=250.0,TP=450.0):
    
        paired_positions = json.load( open(open_trades_file) )

        #name1=market_names[0]
        #name2=market_names[1]

        #id1=marketIds[market_names[0]]
        #id2=marketIds[market_names[1]]

        name1=marketIds[market_names[0]]
        name2=marketIds[market_names[1]]

    
        profit=[]
        PnL=0
        isOpen=False
        isLong=False
        isShort=False
    
        score=pca_res2["score"].iloc[0]
        correl=pca_res2["corr"].iloc[0]
    
        if paired_positions:
            if paired_positions[name1]['status']=="OPEN" and paired_positions[name2]['status']=="OPEN":   
        
                isOpen=True          
                open_direction={name1:paired_positions[name1]['direction'],name2:paired_positions[name2]['direction']}
                dealIds={name1:paired_positions[name1]['dealId'],name2:paired_positions[name2]['dealId']}
                positions_size={name1:paired_positions[name1]['size'],name2:paired_positions[name2]['size']}
                open_prices={name1:paired_positions[name1]['level'],name2:paired_positions[name2]['level']}
                close_direction={name1:paired_positions[name2]['direction'],name2:paired_positions[name1]['direction']}
        

            


                position1={"dealId":paired_positions[name1]['dealId'],"direction":paired_positions[name2]['direction'],'size':paired_positions[name1]['size']}
                position2={"dealId":paired_positions[name2]['dealId'],"direction":paired_positions[name1]['direction'],'size':paired_positions[name2]['size']}

                close_dict={name1:position1,name2:position2}

            
              
                      
        
                if open_direction[name1]=="BUY":

                    isLong=True
                    profit[0]=positions_size[name1]*(x_price[0]-open_prices[0]-spread[0]/2)
                    profit[1]=positions_size[name2]*(open_prices[1]-x[1]-spread[1]/2)*units

        
                elif open_direction[name1]=="SELL":
                
                    isShort=True
                    profit[1]=positions_size[name2]*(x_price[1]-open_prices[1]-spread[1]/2)
                    profit[0]=positions_size[name1]*(open_prices[0]-x[0]-spread[0]/2)

                PnL=sum(profit)
            


        if not isOpen:

            order_size1=pca_res2[name1+"_size"].iloc[0]*marketinfo_df[marketinfo_df.marketId==name1].minSize.iloc[0]
            stop_distance1=(350/marketinfo_df[marketinfo_df.marketId==name1].pipvalue.iloc[0])*marketinfo_df[marketinfo_df.marketId==name1].exchange.iloc[0]
            my_currency1=marketinfo_df[marketinfo_df.marketId==name1].currency.iloc[0]

           

            
            order_size2=pca_res2[name2+"_size"].iloc[0]*marketinfo_df[marketinfo_df.marketId==name2].minSize.iloc[0]
            stop_distance2=(350/marketinfo_df[marketinfo_df.marketId==name2].pipvalue.iloc[0])*marketinfo_df[marketinfo_df.marketId==name2].exchange.iloc[0]
            my_currency2=marketinfo_df[marketinfo_df.marketId==name2].currency.iloc[0]


            if (score>1) and (correl>0.6) :
                trade_order1={"direction":"SELL","epic":epics[market_names[0]],"size":order_size1,"currency":my_currency1,"stop_distance":stop_distance1}
                trade_order2={"direction":"BUY","epic":epics[market_names[1]],"size":order_size2,"currency":my_currency2,"stop_distance":stop_distance2}

                order_dict={name1:trade_order1,name2:trade_order2}
            
                open_position1, open_position2=ig_service.open_paired_position(marketIds=[name1,name2],positions=order_dict,units=units)


                if open_position1 and open_position2:
                    if open_position1['status']=="OPEN" and open_position2['status']=="OPEN":   
                        isShort=True
                
            elif   (score<-1) and (correl>0.6):

                trade_order1={"direction":"BUY","epic":epics[market_names[0]],"size":order_size1,"currency":my_currency1}
                trade_order2={"direction":"SELL","epic":epics[market_names[1]],"size":order_size2,"currency":my_currency2}

                order_dict={name1:trade_order1,name2:trade_order2}
            
                open_position1, open_position2=ig_service.open_paired_position(marketIds=[name1,name2],positions=order_dict,units=units)

                if open_position1 and open_position2:
                    if open_position1['status']=="OPEN" and open_position2['status']=="OPEN":   
                        isLong=True
            
                
            if open_position1 and open_position2:
                if open_position1['status']=="OPEN" and open_position2['status']=="OPEN" :

                    open_positions={name1:open_position1,name2:open_position2}
                    json.dump(open_positions, open(open_trades_file, 'w' ) )
                    print("OPEN position")
            else:
                print("No Positions are open")
                
        if isOpen:
            close_position=None

        

            if isLong and (score>-1):  

                close_position1,close_position2=igconnector.close_paired_position(marketIds=[name1,name2],positions=close_dict)
            
            elif isShort and (score<1):   

                close_position1,close_position2=igconnector.close_paired_position(marketIds=[name1,name2],positions=close_dict)
            
            if PnL>(TP):
            
                close_position1,close_position2=igconnector.close_paired_position(marketIds=[name1,name2],positions=close_dict)
            
            elif PnL<(-SL):
            
                close_position1,close_position2=igconnector.close_paired_position(marketIds=[name1,name2],positions=close_dict)
            
                
            if close_position:
                if close_position1['status']=="CLOSED" and close_position2['status']=="CLOSED" :

                    close_positions={name1:close_position1,name2:close_position2}
                    json.dump(close_positions, open( close_trades_file, 'w' )) 
                    json.dump({}, open( open_trades_file, 'w' )) 
                    print("Close paired positions")



    
   


