# Veronika Klevarova
# 2020-06-01
# This script performs a download of the following ZACKS data: master tables, supplement tables, equity prices tables

import quandl 
import pandas as pd
import glob
import os
import subprocess
from datetime import date
from pathlib import Path
import requests
from bs4 import BeautifulSoup as bs

def fun_quandl_download(url_zacks,datadir_master,datadir_supp,datadir_equity,zip_save,datadir_archive,zip_dir,datadir_zacks_table,datadir_zachs):
    
    print("Download of the Quandl data was initiated.")    
    quandl.ApiConfig.api_key = "QizZXpe8sN9qm-nM-zbW"
    
    # download the ZACKS ticker table from the url
    page = requests.get(url_zacks)
    content = page.content
    soup = bs(content, "html.parser")
    s=str(soup)
    file = open(datadir_archive+"ZACKS_ticker.txt","w") 
    file.write(s)
    tickers = pd.read_csv(datadir_archive+"ZACKS_ticker.txt",sep=",",na_filter=False)
    
    today = date.today()
    
    ticker_list = tickers['ticker'].to_list()
    
    #comparison with the last table
    tickers_old = pd.read_csv(datadir_zacks_table,sep=",",na_filter=False)
    ticker_list_old = tickers_old['ticker'].to_list()
    diff_tickers = [x for x in ticker_list if (x not in ticker_list_old)]
    
    missing_master = []; missing_supp = []; missing_equity = []
    extension = 'csv'
    
    # 7zip compression of old directories    
    if zip_save == 1:
        if len(str(today.month)) == 1:
            archive_master = datadir_archive+"archive_master_table_0"+str(today.month)+"_"+str(today.year)+".7z"
            archive_supp = datadir_archive+"archive_supp_table_0"+str(today.month)+"_"+str(today.year)+".7z"
            archive_equity = datadir_archive+"archive_equity_prices_0"+str(today.month)+"_"+str(today.year)+".7z"
            archive_zacks_table = datadir_archive+"archive_zacks_table_0"+str(today.month)+"_"+str(today.year)+".7z"
        else:
            archive_master = datadir_archive+"archive_master_table_"+str(today.month)+"_"+str(today.year)+".7z"            
            archive_supp = datadir_archive+"archive_supp_table_"+str(today.month)+"_"+str(today.year)+".7z"          
            archive_equity = datadir_archive+"archive_equity_prices_"+str(today.month)+"_"+str(today.year)+".7z"           
            archive_zacks_table = datadir_archive+"archive_zacks_table_"+str(today.month)+"_"+str(today.year)+".7z"
        
        print("Creating ZACKS ticker table archive..")
        subprocess.call(zip_dir + " a \"" + archive_zacks_table + "\" \"" + datadir_zacks_table + "\" -mx=7")
        print("Creating master table archive..")
        subprocess.call(zip_dir + " a \"" + archive_master + "\" \"" + datadir_master + "\" -mx=7")
        print("Creating supplement table archive..")
        subprocess.call(zip_dir + " a \"" + archive_supp + "\" \"" + datadir_supp + "\" -mx=7")
        print("Creating equity prices table archive..")
        subprocess.call(zip_dir + " a \"" + archive_equity + "\" \"" + datadir_equity + "\" -mx=7")
        
        # in case that the files were compressed and moved, delete all .csv in the respective folders and rewrite the ZACKS table
        [f.unlink() for f in Path(datadir_master).glob('*.{}'.format(extension)) if f.is_file()] 
        [f.unlink() for f in Path(datadir_supp).glob('*.{}'.format(extension)) if f.is_file()] 
        [f.unlink() for f in Path(datadir_equity).glob('*.{}'.format(extension)) if f.is_file()]  
        tickers.to_csv(datadir_zachs+"ZACKS_last.csv",sep=",",index=False)
        
    os.chdir(datadir_master)
    result_master = glob.glob('*.{}'.format(extension))
    
    os.chdir(datadir_supp)
    result_supp = glob.glob('*.{}'.format(extension))   
    
    os.chdir(datadir_equity)
    result_equity = glob.glob('*.{}'.format(extension))
    
    tmp = list(set(result_master) & set(result_supp) & set(result_equity))
    tmp = [x[:-4] for x in tmp]
    if len(tmp) ==0 or zip_save == 1:
        to_do_ticker = ticker_list
    else:        
        to_do_ticker = [x for x in ticker_list if (x not in tmp)]
    
    j = 0
    for i in range(0,len(to_do_ticker)):
        # download master table
        print("Downloading data for: "+str(to_do_ticker[i]))
        try:
            master_table = quandl.get_table('ZACKS/MT',ticker = str(to_do_ticker[i]))
            master_table.to_csv(datadir_master+str(to_do_ticker[i])+".csv",sep=",")
        except:
            missing_master.append(to_do_ticker[i])
        # download supplement table
        try:
            supp_table = quandl.get_table('ZACKS/SHRS',ticker = str(to_do_ticker[i]))
            supp_table.to_csv(datadir_supp+str(to_do_ticker[i])+".csv",sep=",")
        except:
            missing_supp.append(to_do_ticker[i])
        # download equity prices
        try:
            equity_table = quandl.get_table('ZACKS/P',ticker = str(to_do_ticker[i]))
            equity_table.to_csv(datadir_equity+str(to_do_ticker[i])+".csv",sep=",")
        except:
            missing_equity.append(to_do_ticker[i])
        j = j+1
    return(j,len(tickers),missing_master,missing_supp,missing_equity,diff_tickers)
    