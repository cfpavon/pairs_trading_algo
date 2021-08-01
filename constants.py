



#watchlist_id="14233239"


account_id=""
acc_password=""
api_key=""
acc_environment=""

watchlist_id="15401213"

marketIds={"brent":"LCO","wti":"CL"}

epics={"brent":"CC.D.LCO.UMA.IP","wti":"CC.D.CL.UMA.IP"}


epics_ids={"CC.D.LCO.UMA.IP":"LCO","CC.D.CL.UMA.IP":"CL"}

ids_epics={"LCO":"CC.D.LCO.UMA.IP","CL":"CC.D.CL.UMA.IP"}

market_names=["brent","wti"]



##data_filename='./crude_06_23_21-test.csv'
data_filename='./crude_07_26_21-test.csv'
##data_filename="./crude1.csv"
marketinfo_filename="./MarketInfoPrices.txt"
open_positions="./open_positions.json"

trading_parameters={"long_entry":-1,"short_entry":1,"min_correl":0.6,
                   "close_long":-0.7,"close_short":0.7,"look_out_window":120,
                   "stop_loss":75.0,"take_profit":150.0,"unit_size":[2,2]}
