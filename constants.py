



#account_id="cfpavon"
#acc_password="Cafer#4664!"
#api_key="2a47939b5f469133dc59ca17143533c1adefa3d5"
#acc_environment="LIVE"

#watchlist_id="14233239"


account_id="cpavon31"
acc_password="Cafer4777"
api_key="d30a3f30272ae879465252eab6fa6fdb1d23ceb2"
acc_environment="DEMO"

watchlist_id="15401213"

marketIds={"brent":"LCO","wti":"CL"}

epics={"brent":"CC.D.LCO.UMA.IP","wti":"CC.D.CL.UMA.IP"}


epics_ids={"CC.D.LCO.UMA.IP":"LCO","CC.D.CL.UMA.IP":"CL"}

ids_epics={"LCO":"CC.D.LCO.UMA.IP","CL":"CC.D.CL.UMA.IP"}

market_names=["brent","wti"]



##data_filename='./crude_06_23_21-test.csv'
data_filename='./crude_2022_06_08.csv'
##data_filename="./crude1.csv"
marketinfo_filename="./MarketInfoPrices.txt"
open_positions="./open_positions.json"

trading_parameters={"long_entry":-1.0,"short_entry":1.0,"min_correl":0.6,
                   "close_long":-0.5,"close_short":0.5,"look_out_window":120,
                   "stop_loss":75.0,"take_profit":150.0,"unit_size":[4,1]}

