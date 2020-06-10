# Reads and saves content of shares supplement tables in N:\DataTrhu-ZACKS\SharesOutSupplement
import numpy as np
import os
import pymysql
import glob
import pandas as pd

def fun_supplement_table(delete_table,datadir_supp,datadir_tmp):
    print("Processing of the supplement tables was initiated.")
    extension = 'csv'
    os.chdir(datadir_supp)
    result = glob.glob('*.{}'.format(extension))
    result = sorted(result)
    
    column_names = ['ticker','m_ticker','comp_name','fye','per_type','per_end_date','active_ticker_flag','shares_out','avg_d_shares']
    df = pd.DataFrame(columns = column_names)
    j = 0
    for i in range(0,len(result)): 
        if result[i] == '000000000.csv':
            continue
        if result[i] == 'Sektors Robert ZACKS 5813symbolu.csv':
            continue
        if result[i] == 'sektorizace.csv':
            continue
        print(result[i])
        try:
            data_tmp = pd.read_csv(datadir_supp+result[i],sep=",",na_filter=False, dtype={'ticker':str})
        except:
            print("Supplement table doesn't exist, skipping the: "+result[i])
            continue
        try:
            del data_tmp['Unnamed: 0']
        except:
            del data_tmp['None']
        df = df.append(data_tmp)
        j = j+1
    
    df = df.replace(r'^\s*$', 'NULL', regex = True)
    df['per_end_date'] = pd.to_datetime(df['per_end_date'])
    df["per_end_date"] = df["per_end_date"].dt.strftime('%Y-%m-%d')
    df.to_csv(datadir_tmp+"all_stocks_supp.csv",sep=",",index=False, encoding='latin1')
       
    if delete_table == 1:
        sql = "DELETE FROM `stock_pairs`.`shares_out_tables`;"
        conn = pymysql.connect(host='192.168.204.208', port=3306, user='root', passwd='root', db='mysql', local_infile = 1)
        cur = conn.cursor()
        cur.execute(sql)
        conn.close()      
        
    path = "N:\\\\2020_02-MarketdriversZV\\\\data\\\\tmp\\\\all_stocks_supp.csv"
    tmp = str("'\\r\\n'")
    tmp1 = "`stock_pairs`.`shares_out_tables`"
    columns_sql = "`ticker`, `m_ticker`, `comp_name`, `fye`, `per_type`, `per_end_date`, `active_ticker_flag`, `shares_out`, `avg_d_shares`"
    
    sql = "LOAD DATA LOW_PRIORITY LOCAL INFILE '"+path+"' INTO TABLE "+tmp1+" FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY "+tmp+" IGNORE 1 LINES ("+columns_sql+");"
    conn = pymysql.connect(host='192.168.204.208', port=3306, user='root', passwd='root', db='mysql', local_infile = 1)
    cur = conn.cursor()
    cur.execute(sql)
    conn.close()  
    
    return(j)
    
#datadir = "n:\\DataTrhu-ZACKS\\SharesOutSupplement\\" # source of supplement tables
#datadir_tmp = "n:\\2020_02-MarketdriversZV\\data\\tmp\\" # here, the full csv is saved
#delete_table = 1   # 1 = the current sql table will be deleted
 
#fun_supplement_table(delete_table,datadir,datadir_tmp)