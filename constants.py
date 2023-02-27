



#watchlist_id="14233239"

constants={}

pairs=["crude","narus","btceth","spdow"]

folder_data="dat"

constants["crude_oil"]={

    "watchlist_id":"15401213","marketIds":{"brent":"LCO","wti":"CL"},
    "epics":{"brent":"CC.D.LCO.UMA.IP","wti":"CC.D.CL.UMA.IP"},
    "epics_ids":{"CC.D.LCO.UMA.IP":"LCO","CC.D.CL.UMA.IP":"CL"},
    "ids_epics":{"LCO":"CC.D.LCO.UMA.IP","CL":"CC.D.CL.UMA.IP"},
    "market_names":["brent","wti"],"data_filename":'./crude_2022_10_31.csv',
    "marketinfo_filename":"./MarketInfoPrices.txt",
    "file_price_monitor":"./price_monitor.txt",
    "open_positions":folder_data+"/open_positions_"+pairs[0]+".json",
    "open_positions_hist":folder_data+"/open_hist_"+pairs[0]+".json",
    "close_positions_hist":folder_data+"/close_hist_"+pairs[0]+".json",
    "decimals":[2,2],
    "trading_parameters":{"long_entry":-1.1,"short_entry":1.1,"long_entry_lag":-1.5,"short_entry_lag":1.5,"min_correl":0.6,
                   "close_long":5,"close_short":-5,"look_out_window":120,
                   "macd_hist_value":0.2,"mean_ret_value":0,
                   "monitor_take":25,"monitor_stop":-400,
                   "monitor_take_single":250,"monitor_stop_single":-400,
                   "stop_pct1":0.02,"take_pct1":0.05,
                   "stop_pct2":0.02,"take_pct2":0.05,
                   "stop_loss":-400.0,"take_profit":25.0,"unit_size":[1.0,1.0]}
    }


constants["narus"]={

    "watchlist_id":"15845956","marketIds":{"nasdaq":"USTECH","russell":"R2000"},
    "epics":{"nasdaq":"IX.D.NASDAQ.IFA.IP","russell":"IX.D.RUSSELL.IFM.IP"},
    "epics_ids":{"IX.D.NASDAQ.IFA.IP":"USTECH","IX.D.RUSSELL.IFM.IP":"R2000"},
    "ids_epics":{"USTECH":"IX.D.NASDAQ.IFA.IP","R2000":"IX.D.RUSSELL.IFM.IP"},
    "market_names":["nasdaq","russell"],"data_filename":'./nasrus_2022_10_31.csv',
    "marketinfo_filename":"./MarketInfoPrices_narus.txt",
    "file_price_monitor":"./price_monitor.txt",
    "open_positions":folder_data+"/open_positions_"+pairs[1]+".json",
    "open_positions_hist":folder_data+"/open_hist"+pairs[1]+".json",
    "close_positions_hist":folder_data+"/close_hist_"+pairs[1]+".json",
    "decimals":[2,2],
    "trading_parameters":{"long_entry":-1.5,"short_entry":1.5,"min_correl":0.6,
                   "close_long":5,"close_short":-5,"look_out_window":90,
                   "macd_hist_value":0,"mean_ret_value":1000,
                   "monitor_take":400,"monitor_stop":-200,
                   "monitor_take_single":400,"monitor_stop_single":-200,
                   "stop_pct1":0.025,"take_pct1":0.05,
                   "stop_pct2":0.025,"take_pct2":0.05,
                   "stop_loss":-200.0,"take_profit":400.0,"unit_size":[1.0,1.0]}
    }




constants["btceth"]={

    "watchlist_id":"17546970","marketIds":{"bitcoin":"BITCOIN","ether":"ETHUSD"},
    "epics":{"bitcoin":"CS.D.BITCOIN.CFD.IP","ether":"CS.D.ETHUSD.CFD.IP"},
    "epics_ids":{"CS.D.BITCOIN.CFD.IP":"BITCOIN","CS.D.ETHUSD.CFD.IP":"ETHUSD"},
    "ids_epics":{"BITCOIN":"CS.D.BITCOIN.CFD.IP","ETHUSD":"CS.D.ETHUSD.CFD.IP"},
    "market_names":["bitcoin","ether"],"data_filename":'./btceth_test.csv',
    "marketinfo_filename":"./MarketInfoPrices_btceth.txt",
    "file_price_monitor":"./price_monitor.txt",
    "open_positions":folder_data+"/open_positions_"+pairs[2]+".json",
    "open_positions_hist":folder_data+"/open_hist"+pairs[2]+".json",
    "close_positions_hist":folder_data+"/close_hist_"+pairs[2]+".json",
    "decimals":[3,4],
    "trading_parameters":{"long_entry":0,"short_entry":0,"min_correl":0.6,
                   "close_long":-0.7,"close_short":0.7,"look_out_window":90,
                   "macd_hist_value":0,"mean_ret_value":0,
                   "monitor_take":75,"monitor_stop":-100,
                   "monitor_take_single":75,"monitor_stop_single":-75,
                   "stop_pct1":0.025,"take_pct1":0.05,
                   "stop_pct2":0.025,"take_pct2":0.05,
                   "stop_loss":-75.0,"take_profit":150.0,"unit_size":[10.0,1200.0]}
    }


constants["spdow"]={

    "watchlist_id":"17546987","marketIds":{"sp500":"US500","dow":"WALL"},
    "epics":{"sp500":"IX.D.SPTRD.IFA.IP","dow":"IX.D.DOW.IFA.IP"},
    "epics_ids":{"IX.D.SPTRD.IFA.IP":"US500","IX.D.DOW.IFA.IP":"WALL"},
    "ids_epics":{"US500":"IX.D.SPTRD.IFA.IP","WALL":"IX.D.DOW.IFA.IP"},
    "market_names":["sp500","dow"],"data_filename":'./spdow_2022_10_31.csv',
    "marketinfo_filename":"./MarketInfoPrices_spdow.txt",
    "file_price_monitor":"./price_monitor_1.txt",
    "open_positions":folder_data+"/open_positions_"+pairs[3]+".json",
    "open_positions_hist":folder_data+"/open_hist"+pairs[3]+".json",
    "close_positions_hist":folder_data+"/close_hist_"+pairs[3]+".json",
    "decimals":[2,2],
    "trading_parameters":{"long_entry":-1.0,"short_entry":1.0,"min_correl":0.6,
                   "close_long":5,"close_short":-5,"look_out_window":120,
                   "macd_hist_value":2,"mean_ret_value":1.5,
                   "monitor_take":100,"monitor_stop":-450,
                   "monitor_take_single":100,"monitor_stop_single":-450,
                   "stop_pct1":0.02,"take_pct1":0.04,
                   "stop_pct2":0.02,"take_pct2":0.04,
                   "stop_loss":-450,"take_profit":100.0,"unit_size":[1.0,1.0]}
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
