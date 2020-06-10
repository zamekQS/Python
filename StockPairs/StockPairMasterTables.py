# Reads and saves content of master tables in N:\DataTrhu-ZACKS\MasterTable
import numpy as np
import os
import pymysql
import glob
import pandas as pd

def fun_master_table(delete_table,datadir_master,datadir_tmp):
    print("Processing of the master tables was initiated.")
    extension = 'csv'
    os.chdir(datadir_master)
    result = glob.glob('*.{}'.format(extension))
    result = sorted(result)
    
    column_names = ['m_ticker','ticker','comp_name','comp_name_2','exchange','currency_code','ticker_type','active_ticker_flag','comp_url','sic_4_code','sic_4_desc','zacks_x_ind_code','zacks_x_ind_desc','zacks_x_sector_code','zacks_x_sector_desc']
    column_names = column_names+['zacks_m_ind_code','zacks_m_ind_desc','per_end_month_nbr','mr_split_date','mr_split_factor','comp_cik','country_code','country_name','comp_type','optionable_flag','sp500_member_flag','asset_type']
    df = pd.DataFrame(columns = column_names)
    for i in range(0,len(result)): 
        print(result[i])
        data_tmp = pd.read_csv(datadir_master+result[i],sep=",", encoding='latin1',na_filter=False, dtype={'ticker':str})
        try:
            del data_tmp['Unnamed: 0']
        except:
            del data_tmp['None']
        df = df.append(data_tmp)
    
    df = df.replace(r'^\s*$', 'NULL', regex = True)
    df.to_csv(datadir_tmp+"all_stocks_master.csv",sep=",",index=False, encoding='latin1')
       
    if delete_table == 1:
        sql = "DELETE FROM `stock_pairs`.`master_tables`;"
        conn = pymysql.connect(host='192.168.204.208', port=3306, user='root', passwd='root', db='mysql', local_infile = 1)
        cur = conn.cursor()
        cur.execute(sql)
        conn.close()      
        
    path = "N:\\\\2020_02-MarketdriversZV\\\\data\\\\tmp\\\\all_stocks_master.csv"
    tmp = str("'\\r\\n'")
    tmp1 = "`stock_pairs`.`master_tables`"
    columns_sql = "`m_ticker`, `ticker`, `comp_name`, `comp_name_2`, `exchange`, `currency_code`, `ticker_type`, `active_ticker_flag`, `comp_url`, `sic_4_code`, `sic_4_desc`, `zacks_x_ind_code`, `zacks_x_ind_desc`, `zacks_x_sector_code`, `zacks_x_sector_desc`, `zacks_m_ind_code`, `zacks_m_ind_desc`, `per_end_month_nbr`, `mr_split_date`, `mr_split_factor`, `comp_cik`, `country_code`, `country_name`, `comp_type`, `optionable_flag`, `sp500_member_flag`, `asset_type`"
    
    sql = "LOAD DATA LOW_PRIORITY LOCAL INFILE '"+path+"' INTO TABLE "+tmp1+" FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY "+tmp+" IGNORE 1 LINES ("+columns_sql+");"
    conn = pymysql.connect(host='192.168.204.208', port=3306, user='root', passwd='root', db='mysql', local_infile = 1)
    cur = conn.cursor()
    cur.execute(sql)
    conn.close()  
    
    return(len(df))
