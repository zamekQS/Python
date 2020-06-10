# Veronika Klevarova
# 2020-06-01
# Script initiates download and processing of data provided by ZACKS

from StockPairZacksDownload import fun_quandl_download
from StockPairMasterTables import fun_master_table
from StockPairSummaryTables import fun_summary_table
from StockPairSharesOutTables import fun_supplement_table
from datetime import datetime
import time 

t0 = time.time()

url_zacks = "https://s3.amazonaws.com/quandl-production-static/coverage/ZACKS_P.csv"   # ZACKS table with tickers is downloaded from here
datadir_archive = "n:\\2020_02-MarketdriversZV\\data\\tmp\\" # here, archived master, supplement and equity files are saved
datadir_master = "n:\\DataTrhu-ZACKS\\MasterTable\\" # source/storage of master tables
datadir_supp = "n:\\DataTrhu-ZACKS\\SharesOutSupplement\\" # source/storage of supplement tables
datadir_equity = "n:\\DataTrhu-ZACKS\\EquityPrices\\" # source/storage of equity prices
datadir_tmp = "n:\\2020_02-MarketdriversZV\\data\\tmp\\" # here, the full csv is saved
datadir_save_csv = "n:\\2020_02-MarketdriversZV\\data\\tmp\\" # here, temporary files are saved
datadir_zip = "n:\\bin\\7z.exe" # path to the 7zip.exe 
datadir_zacks_table = "n:\\DataTrhu-ZACKS\\ZACKS_last.csv"  # most recent version of the ZACKS table
datadir_zacks = "n:\\DataTrhu-ZACKS\\"  # the newest ZACKS table will be saved here

# User of the script should set the four variables below:
run_fun_download = 0 # switch to 0 if you don't want to download master, supplement and equity prices tables
run_fun_master = 0   # switch to 0 if you don't want to process master tables
run_fun_supp = 0     # switch to 0 if you don't want to process supplement tables
run_fun_summary = 1  # switch to 0 if you don't want to create summary tables

now = datetime.now()

def write_text(text):
    with open(datadir_tmp+"QUANDL_data_download_report.txt", 'r+') as fp:
        lines = fp.readlines()     
        lines.insert(0, text)  
        fp.seek(0)                
        fp.writelines(lines) 
        fp.close()

print("QUANDL ZACKS data download and/or processing was initiated, time of run: "+now.strftime("%Y-%m-%d-%H:%M:%S"))   
     
if run_fun_download == 1:
    zip_save = 0 # 1 -> SET TO 1 ONLY FOR THE VERY FIRST RUN OF THE MONTH!! If you rerun this script, set it to 0; old master, supplement and equity prices tables will be archived (7zip), saved in datadir_archive and the content of the directories will be deleted
    (proc_ticker,num_ticker,missing_master,missing_supp,missing_equity,diff_zacks_table) = fun_quandl_download(url_zacks,datadir_master,datadir_supp,datadir_equity,zip_save,datadir_archive,datadir_zip,datadir_zacks_table,datadir_zacks)
    text1 = "\nReport for QUANDL ZACKS data download&processing, time of run: "+now.strftime("%Y-%m-%d-%H:%M:%S")+", "+str(num_ticker)+" tickers were recognized in ZACKS ticker .csv, of which "+str(proc_ticker)+" were downloaded.\n" 
    text2 = "Number of not downloaded master tables: "+str(len(missing_master))+", the following were not downloaded: "+str(missing_master).strip('[]')+"\n" 
    text3 = "Number of not downloaded supplement tables: "+str(len(missing_supp))+", the following were not downloaded: "+str(missing_supp).strip('[]')+"\n" 
    text4 = "Number of not downloaded equity prices tables: "+str(len(missing_equity))+", the following were not downloaded: "+str(missing_equity).strip('[]')+"\n"        
    text5 = "The following symbols differ in the old and new ZACKS tables: "+str(diff_zacks_table).strip('[]')+"\n" 
    write_text(text1+text2+text3+text4+text5)
else:
    num_ticker = 'NA'; missing_master = 'NA'; missing_supp = 'NA'; missing_equity = 'NA'; proc_ticker = 'NA'; diff_zacks_table = 'NA'
    text1 = "\nReport for QUANDL ZACKS data download&processing, time of run: "+now.strftime("%Y-%m-%d-%H:%M:%S")+", "+str(num_ticker)+" tickers were recognized in ZACKS ticker .csv, of which "+str(proc_ticker)+" were downloaded.\n" 
    text2 = "No master tables were downloaded\n" 
    text3 = "No supplement tables were downloaded\n"
    text4 = "No equity prices tables were downloaded\n"
    text5 = "The following symbols differ in the old and new ZACKS tables: "+str(diff_zacks_table).strip('[]')+"\n" 
    write_text(text1+text2+text3+text4+text5)
    
if run_fun_master == 1:
    delete_master_table = 1  # 1 -> the FULL current sql master table will be deleted
    (num_master) = fun_master_table(delete_master_table,datadir_master,datadir_tmp)
    text6 = "Number of master tables uploaded to SQL: "+str(num_master)+"\n"      
    write_text(text6)
else:
    num_master = 'NA'
    text6 = "Number of master tables uploaded to SQL: "+str(num_master)+"\n"      
    write_text(text6)

if run_fun_supp == 1:
    delete_supp_table = 1   # 1 -> the FULL current sql supplement table will be deleted
    (num_supp) = fun_supplement_table(delete_supp_table,datadir_supp,datadir_tmp)
    text7 = "Number of supplement tables uploaded to SQL: "+str(num_supp)+"\n"   
    write_text(text7)
else:
    num_supp = 'NA'
    text7 = "Number of supplement tables uploaded to SQL: "+str(num_supp)+"\n"   
    write_text(text7)
    
if run_fun_summary == 1:
    delete_full_table_summ = 0   # = 1 -> the FULL current sql summary_prices table will be deleted
    delete_table_summ = 0   # = 1 -> the current sql table associated with a single stock will be deleted
    save_csv = 1            # = 1 -> the stock equity prices (shorter format) will be stored in the datadir_save_csv folder
    save_feather = 1        # = 1 -> the stock equity prices vs. date (shorter format) will be stored in the feather format in datadir_tmp
    minimum_date = '1999-11-01' # only equity prices from this date will be considered in the output, set to '1999-11-01' if you want the feather file from 2000-01-01
    (num_equity) = fun_summary_table(delete_full_table_summ,delete_table_summ,datadir_equity,datadir_tmp,minimum_date,datadir_save_csv,save_csv,save_feather)
    text8 = "Number of summary tables (calculated with the equity prices & shares out tables) uploaded to SQL: "+str(num_equity)+"\n" 
    write_text(text8) 
else:
    num_equity = 'NA'
    text8 = "Number of summary tables (calculated with the equity prices & shares out tables) uploaded to SQL: "+str(num_equity)+"\n" 
    write_text(text8)      
    
# to load the .ftr : 
# import pyarrow
# readFrame = pd.read_feather(path_to_ftr)    

time_run = time.time() - t0
print("Quandl data were processed in: "+str(time_run)+" s.")
          