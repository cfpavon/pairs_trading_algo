



#watchlist_id="14233239"

constants={}

pairs=["crude","narus"]

folder_data="dat"

constants["crude_oil"]={

    "watchlist_id":"15401213","marketIds":{"brent":"LCO","wti":"CL"},
    "epics":{"brent":"CC.D.LCO.UMA.IP","wti":"CC.D.CL.UMA.IP"},
    "epics_ids":{"CC.D.LCO.UMA.IP":"LCO","CC.D.CL.UMA.IP":"CL"},
    "ids_epics":{"LCO":"CC.D.LCO.UMA.IP","CL":"CC.D.CL.UMA.IP"},
    "market_names":["brent","wti"],"data_filename":'./crude_07_26_21-test.csv',
    "marketinfo_filename":"./MarketInfoPrices.txt",
    "file_price_monitor":"./price_monitor.txt",
    "open_positions":folder_data+"/open_positions_"+pairs[0]+".json",
    "open_positions_hist":folder_data+"/open_hist_"+pairs[0]+".json",
    "close_positions_hist":folder_data+"/close_hist_"+pairs[0]+".json",

    "trading_parameters":{"long_entry":-0.25,"short_entry":+0.25,"min_correl":0.6,
                   "close_long":0,"close_short":0,"look_out_window":120,
                   "macd_hist_value":0,"mean_ret_value":0,
                   "monitor_take":20,"monitor_stop":-100,
                   "stop_loss":75.0,"take_profit":150.0,"unit_size":[1.0,1.0]}
    }


constants["narus"]={

    "watchlist_id":"15845956","marketIds":{"nasdaq":"USTECH","russell":"R2000"},
    "epics":{"nasdaq":"IX.D.NASDAQ.IFA.IP","russell":"IX.D.RUSSELL.IFM.IP"},
    "epics_ids":{"IX.D.NASDAQ.IFA.IP":"USTECH","IX.D.RUSSELL.IFM.IP":"R2000"},
    "ids_epics":{"USTECH":"IX.D.NASDAQ.IFA.IP","R2000":"IX.D.RUSSELL.IFM.IP"},
    "market_names":["nasdaq","russell"],"data_filename":'./narus_test.csv',
    "marketinfo_filename":"./MarketInfoPrices_narus.txt",
    "file_price_monitor":"./price_monitor.txt",
    "open_positions":folder_data+"/open_positions_"+pairs[1]+".json",
    "open_positions_hist":folder_data+"/open_hist"+pairs[1]+".json",
    "close_positions_hist":folder_data+"/close_hist"+pairs[1]+".json",

    "trading_parameters":{"long_entry":0,"short_entry":0,"min_correl":0.6,
                   "close_long":-0.7,"close_short":0.7,"look_out_window":90,
                   "macd_hist_value":0,"mean_ret_value":0,
                   "monitor_take":75,"monitor_stop":-100,
                   "stop_loss":75.0,"take_profit":150.0,"unit_size":[3.0,1.0]}
    }


#items=["CHART:CC.D.LCO.UMA.IP:SECOND","CHART:CC.D.CL.UMA.IP:SECOND"]

#watchlist_id="15401213"

#marketIds={"brent":"LCO","wti":"CL"}

#epics={"brent":"CC.D.LCO.UMA.IP","wti":"CC.D.CL.UMA.IP"}


#epics_ids={"CC.D.LCO.UMA.IP":"LCO","CC.D.CL.UMA.IP":"CL"}

#ids_epics={"LCO":"CC.D.LCO.UMA.IP","CL":"CC.D.CL.UMA.IP"}

#market_names=["brent","wti"]



###data_filename='./crude_06_23_21-test.csv'
#data_filename='./crude_07_26_21-test.csv'
###data_filename="./crude1.csv"
#marketinfo_filename="./MarketInfoPrices.txt"
#open_positions="./open_positions.json"

#trading_parameters={"long_entry":-1,"short_entry":1,"min_correl":0.6,
#                   "close_long":-0.7,"close_short":0.7,"look_out_window":120,
#                   "stop_loss":75.0,"take_profit":150.0,"unit_size":[2,2]}
