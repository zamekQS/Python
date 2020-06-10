# Veronika Klevarova
# 2020-06-01
# This script reads and processes equity prices data (+ shares out data), it uploads the calculated summary tables to the SQL database

import numpy as np
import os
import pymysql
import glob
import pandas as pd
from datetime import datetime, date
from datetime import timedelta

def fun_summary_table(delete_full_table_summ,delete_table,datadir_equity,datadir_tmp,minimum_date,datadir_save_csv,save_csv,save_feather):
    print("Processing of the summary tables was initiated.")
    # reading .csv from the equity folder
    extension = 'csv'
    os.chdir(datadir_equity)
    result = glob.glob('*.{}'.format(extension))
    result = sorted(result)  
    
    today = date.today()
    datadir_data = datadir_save_csv + '{}\\'.format(str(today)[:10])  # creates new directory named by the day of the program run 
    if not os.path.exists(datadir_data): 
        os.makedirs(datadir_data)     
    
    # load the full master table from sql
    sql = "SELECT ticker, comp_name, exchange, zacks_x_sector_desc from stock_pairs.master_tables"
    conn = pymysql.connect(host='192.168.204.208', port=3306, user='root', passwd='root', db='mysql', local_infile = 1)
    cur = conn.cursor()
    cur.execute(sql)
    records = cur.fetchall()
    column_names = ['ticker','comp_name','exchange','zacks_x_sector_desc']
    full_master_table = pd.DataFrame(records,columns=column_names)
    conn.close()    
    
    full_master_table = full_master_table.set_index(full_master_table['ticker'])
    
    if save_feather == 1:
        if minimum_date == '1999-11-01':
            dates = pd.date_range(start='2000-01-01',end=today, freq = 'B') 
        else: 
            dates = pd.date_range(start=minimum_date,end=today, freq = 'B') 
        table_close_stock = pd.DataFrame(index=dates)
    
    # Deleting the FULLL summary prices table, if desired
    if delete_full_table_summ == 1:
        sql = "DELETE FROM `stock_pairs`.`summary_prices`;"
        conn = pymysql.connect(host='192.168.204.208', port=3306, user='root', passwd='root', db='mysql', local_infile = 1)
        cur = conn.cursor()
        cur.execute(sql)
        conn.close()     
       
    # Processing the stocks one by one
    j = 0
    for i in range(0,len(result)): 
        print("Processing stock: "+result[i])
        
        data_equity = pd.read_csv(datadir_equity+result[i],sep=",",na_filter=False, dtype={'ticker':str})
        data_equity = data_equity[(data_equity['date']>minimum_date)]
        data_equity = data_equity.replace(r'^\s*$', np.nan, regex = True)
        
        if len(data_equity) == 0:
            print("No equity prices data or the data were fully filtered based on the minimum date, skipping the "+result[i])
            continue
        
        master_table = full_master_table[full_master_table.index.isin(result[i][:-4].split(','))]
          
        # load the supplement table from sql
        sql = "SELECT ticker, per_end_date, shares_out from stock_pairs.shares_out_tables WHERE ticker='"+str(data_equity['ticker'].iloc[0])+"'"
        conn = pymysql.connect(host='192.168.204.208', port=3306, user='root', passwd='root', db='mysql', local_infile = 1)
        cur = conn.cursor()
        cur.execute(sql)
        records = cur.fetchall()
        column_names = ['ticker','per_end_date','shares_out']
        data_supp = pd.DataFrame(records,columns=column_names)
        conn.close() 
            
        if delete_table == 1:
            sql = "DELETE FROM `stock_pairs`.`summary_prices` WHERE ticker='"+str(data_equity.loc[0,'ticker'])+"' AND Date >= '"+minimum_date+"'"
            conn = pymysql.connect(host='192.168.204.208', port=3306, user='root', passwd='root', db='mysql', local_infile = 1)
            cur = conn.cursor()
            cur.execute(sql)
            conn.close()  
          
        # additional data in the dataframe  
        
        data_equity['date'] = pd.to_datetime(data_equity['date'])        
        cols = ['open','high','low','close','volume']
        data_equity[cols] = data_equity[cols].apply(pd.to_numeric, errors='coerce', axis=1)
        
        data_supp['per_end_date'] = pd.to_datetime(data_supp['per_end_date'])
        data_supp['per_end_date_plus_one'] = data_supp['per_end_date'] + timedelta(days = 1)
        data_supp['per_end_date_plus_one_plus_month'] = data_supp['per_end_date_plus_one'] + pd.DateOffset(months=1)
        data_supp['per_end_date_plus_one_plus_2months'] = data_supp['per_end_date_plus_one'] + pd.DateOffset(months=2)
        
        data_equity["delta_close"] = data_equity["close"].diff()
        data_equity["close_shift"] = data_equity["close"].shift(+1)
        data_equity["abs_close_shift_perc"] = abs(data_equity["delta_close"]/data_equity["close_shift"])*100 
        data_equity["abs_close_shift_perc"] = data_equity["abs_close_shift_perc"].fillna(0)
        data_equity["abs_close_shift_perc"] = data_equity["abs_close_shift_perc"].replace(np.inf,999999)
        
        data_equity["maximum_price"] = data_equity[["close","high"]].max(axis=1)
        data_equity["minimum_price"] = data_equity[["close","low"]].min(axis=1)
        
        data_equity["volume_no_nan"] = data_equity["volume"].fillna(0)
        data_equity["close_no_nan"] = data_equity["close"].fillna(0)
        data_equity["volume_no_nan_close"] = data_equity["volume_no_nan"]*data_equity["close_no_nan"]
        data_equity["vol_close_avg_50"] = data_equity["volume_no_nan_close"].rolling(window=50).mean()
        
        # extension of the supplement table by three months, saving shares_out values
        tmp_date_supp1 = data_supp[["per_end_date_plus_one","shares_out"]].rename({'per_end_date_plus_one':'Date','shares_out':'shares_out'},axis=1)
        tmp_date_supp2 = data_supp[["per_end_date_plus_one_plus_month","shares_out"]].rename({'per_end_date_plus_one_plus_month':'Date','shares_out':'shares_out'},axis=1)
        tmp_date_supp3 = data_supp[["per_end_date_plus_one_plus_2months","shares_out"]].rename({'per_end_date_plus_one_plus_2months':'Date','shares_out':'shares_out'},axis=1)     
        
        tmp_date_supp = tmp_date_supp1.append([tmp_date_supp2,tmp_date_supp3])
        
        # processing equity prices data, grouping
        data_equity["Year"] =(data_equity["date"]).apply(lambda x: x.year)
        data_equity["Month"] =(data_equity["date"]).apply(lambda x: x.month)
        
        g = data_equity.groupby(["Year", "Month"])
        df_tmp = g.agg( avg_price=pd.NamedAgg( column="close", aggfunc=np.mean ), min_price=pd.NamedAgg( column="minimum_price", aggfunc=min), 
                       max_price=pd.NamedAgg( column="maximum_price", aggfunc=max ), num_opens=pd.NamedAgg( column="open", aggfunc='count'),
                       num_closes=pd.NamedAgg( column="close", aggfunc='count'), num_highs=pd.NamedAgg( column="high", aggfunc='count'),
                       num_lows=pd.NamedAgg( column="low", aggfunc='count'), num_volumes=pd.NamedAgg( column="volume", aggfunc='count'),
                       max_price_change_perc=pd.NamedAgg( column="abs_close_shift_perc", aggfunc=max ), avg_doll_vol_month=pd.NamedAgg( column="volume_no_nan_close", aggfunc=np.mean ),
                       avg_doll_vol_50_bars=pd.NamedAgg( column="vol_close_avg_50", aggfunc='last' )
                       ).reset_index()
            
        df_new = data_equity.groupby(["Year", "Month"]).size().to_frame(name='bars_count').reset_index()
        df_new["Day"] = pd.Series([1 for x in range(len(df_new.index))])
        df_new["Date"] = pd.to_datetime(df_new[['Year','Month','Day']])
        
        try:
            df_new["ticker"] = pd.Series([master_table['ticker'].iloc[0] for x in range(len(df_new.index))])
            df_new["comp_name"] = pd.Series([master_table['comp_name'].iloc[0] for x in range(len(df_new.index))])
            df_new["exchange"] = pd.Series([master_table['exchange'].iloc[0] for x in range(len(df_new.index))])
        except:    
            print("Master table does not exist, 'comp_name' and 'exchange' will be NULL")
            df_new["ticker"] = pd.Series([data_equity['ticker'].iloc[0] for x in range(len(df_new.index))])
            df_new["comp_name"] = pd.Series(['NULL' for x in range(len(df_new.index))])
            df_new["exchange"] = pd.Series(['NULL' for x in range(len(df_new.index))])
          
        df_new["avg_price"] = df_tmp["avg_price"]        
        tmp = pd.merge(tmp_date_supp,df_new,on="Date")
        tmp['capitalization'] = tmp['shares_out']*tmp['avg_price']
        tmp = tmp[['Date','capitalization']]
        df_new = df_new.merge(tmp,how='left',left_on='Date',right_on='Date')
        try:
        	if master_table['zacks_x_sector_desc'].iloc[0] != None:
            		df_new["fundamental_sector"] = pd.Series([str(master_table['zacks_x_sector_desc'].iloc[0]) for x in range(len(df_new.index))])
        	else:
            		df_new["fundamental_sector"] = pd.Series(['NULL' for x in range(len(df_new.index))])
        except:
            print("Master table does not exist, 'fundamental sector' will be NULL")
            df_new["fundamental_sector"] = pd.Series(['NULL' for x in range(len(df_new.index))])
        
        df_new["avg_doll_vol_month"] = df_tmp["avg_doll_vol_month"]
        df_new["avg_doll_vol_50_bars"] = df_tmp["avg_doll_vol_50_bars"]
        df_new["min_price"] = df_tmp["min_price"]
        df_new["max_price"] = df_tmp["max_price"]
        df_new["missing_opens"] = df_new["bars_count"] - df_tmp["num_opens"]
        df_new["missing_closes"] = df_new["bars_count"] - df_tmp["num_closes"]
        df_new["missing_highs"] = df_new["bars_count"] - df_tmp["num_highs"]
        df_new["missing_lows"] = df_new["bars_count"] - df_tmp["num_lows"]
        df_new["missing_volumes"] = df_new["bars_count"] - df_tmp["num_volumes"]
        df_new["max_price_change_perc"] = df_tmp["max_price_change_perc"]
        
        df_new["Date"] = df_new["Date"].dt.strftime('%Y-%m-%d')
        df_new = df_new.drop(['Year','Month','Day'],axis=1)
        df_new.fillna(value=pd.np.nan, inplace=True)
        df_new = df_new.replace(np.nan, 'NULL')
        
        df_new = df_new[['Date', 'ticker', 'comp_name', 'avg_price', 'capitalization', 'fundamental_sector', 'avg_doll_vol_month', 'avg_doll_vol_50_bars', 'min_price', 'max_price', 'missing_opens', 'missing_closes', 'missing_highs', 'missing_lows', 'missing_volumes', 'max_price_change_perc','bars_count','exchange']]
        if minimum_date == '1999-11-01':
            df_new = df_new[(df_new['Date']>='2000-01-01')]
        else: 
            df_new = df_new[(df_new['Date']>minimum_date)]
        
        df_new.to_csv(datadir_tmp+"tmp.csv",sep=",",index=False)
        
        # creating and saving equity prices data to the datadir_tmp directory
        if save_csv == 1:
            data_equity_save = data_equity[['date','open','high','low','close','volume']]
            data_equity_save.columns = ['Date','Open','High','Low','Close','Volume']
            data_equity_save["Date"] = data_equity_save["Date"].dt.strftime('%Y-%m-%d')
            try:
                data_equity_save.to_csv(datadir_data+str(data_equity['ticker'].iloc[0])+"_"+str(master_table['exchange'].iloc[0])+".csv",sep=";",index=False)
            except:
                data_equity_save.to_csv(datadir_data+str(data_equity['ticker'].iloc[0])+"_None.csv",sep=";",index=False)
        
        # import the temporary .csv to database
        path = "N:\\\\2020_02-MarketdriversZV\\\\data\\\\tmp\\\\tmp.csv"
        tmp = str("'\\r\\n'")
        tmp1 = "`stock_pairs`.`summary_prices`"
        columns_sql = "`Date`, `ticker`, `comp_name`, `avg_price`, `capitalization`, `fundamental_sector`, `avg_doll_vol_month`, `avg_doll_vol_50_bars`, `min_price`, `max_price`, `missing_opens`, `missing_closes`, `missing_highs`, `missing_lows`, `missing_volumes`, `max_price_change_perc`, `bars_count`, `exchange`"
        
        sql = "LOAD DATA LOW_PRIORITY LOCAL INFILE '"+path+"' REPLACE INTO TABLE "+tmp1+" FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY "+tmp+" IGNORE 1 LINES ("+columns_sql+");"
        conn = pymysql.connect(host='192.168.204.208', port=3306, user='root', passwd='root', db='mysql', local_infile = 1)
        cur = conn.cursor()
        cur.execute(sql)
        conn.close() 
        
        if save_feather == 1:
            data_equity = data_equity.set_index(data_equity["date"])
            table_close_stock = pd.merge(table_close_stock,data_equity[["close"]],how='left',left_index=True,right_index=True) 
            table_close_stock = table_close_stock.rename({'close':str(data_equity['ticker'].iloc[0])},axis=1)
        
        j = j+1
        
    if save_feather == 1:
        table_close_stock = table_close_stock.dropna(how='all')
        table_close_stock = table_close_stock.reset_index()
        table_close_stock = table_close_stock.rename({'index':'Date'},axis=1)
        table_close_stock["Date"] = table_close_stock["Date"].dt.strftime('%Y-%m-%d')
        if len(str(today.month)) == 1:
            table_close_stock.to_feather(datadir_tmp+"close_stock_0"+str(today.month)+"_"+str(today.year)+".ftr")
        else:
            table_close_stock.to_feather(datadir_tmp+"close_stock_"+str(today.month)+"_"+str(today.year)+".ftr")
            
    return(j)