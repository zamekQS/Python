# Zdeněk Valečko
# 2020-06-09
# Skript pro vytváření sektorizací a inputů pro Stock Pair Trader / Backtester

# využívá tabulku summary_prices na SQL serveru
# je potřeba mít na MySQL serveru nastavenu globální proměnnou group_concat_max_len=1000000
# jinak bude docházet k ořezávání funkce GROUP_CONCAT()

from qMySQL import LoadQuery
from qMySQL import GetConnection

import os
from datetime import timedelta
import glob
import pandas as pd
import dtw
from tslearn.clustering import TimeSeriesKMeans
from tslearn.datasets import CachedDatasets
from tslearn.preprocessing import TimeSeriesScalerMeanVariance,     TimeSeriesResampler
import numpy
import matplotlib.pyplot as plt


c_StartDate = "2020-1-1"
c_EndDate = "2020-4-30"

c_CapitalizationCond = "" # podmínka kapitalizace za poslední měsíc 
c_MinPriceCond = "" # ">10"  # podmínka minimální ceny za poslední měsíc 
c_ExchangeCond = "" #in ('')"  # podmínka burzy obsažená v seznamu
c_GroupByFundamentalSector = True  # True = využívá fundamentální sektorizaci

inputs = CreateInputs(c_StartDate, c_EndDate, c_GroupByFundamentalSector, c_CapitalizationCond, c_MinPriceCond, c_ExchangeCond)
ExportInputsToXml("c:\\xml.xml", inputs, c_StartDate, c_EndDate)
    
def CreateDataFrame(date, group_by_fundamental_sector, capitalization_cond, min_price_cond,exchange_cond):
    sql="select date, "
    if group_by_fundamental_sector:
        sql+="coalesce(fundamental_sector, 'Unclassified')"
    else:
        sql+="'All'"
    sql+=" as fundamental_sector2, count(*) as count, group_concat(ticker order by ticker) as tickers from stock_pairs.summary_prices "

    sql+=" where date = '"+str(date)+"' "
    if capitalization_cond!="":
        sql+=" and capitalization "+capitalization_cond
    if min_price_cond!="":
        sql+=" and min_price "+min_price_cond
    if exchange_cond!="":
        sql+=" and exchange " +exchange_cond
    sql+=" group by fundamental_sector2  order by fundamental_sector2"
    #if group_by_fundamental_sector:
     #   sql+=""
    #else:
        #sql+="date"
    
    print(sql)
    df=LoadQuery(sql)
    return df


def CreateDates(start_date_str, end_date_str):
    start_date = pd.to_datetime(start_date_str)
    end_date = pd.to_datetime(end_date_str)
    month_start_dates = pd.date_range(start=start_date, end=end_date, freq='MS')  # datumy pro začátky jednotlivých inputů
    month_end_dates = month_start_dates+pd.DateOffset(months=1)+pd.DateOffset(days=-1) # datumy pro konce jednotlivých inputů
    last_month_start_dates = month_start_dates+pd.DateOffset(months=-1)  # datumy pro načítání údajů o akciích z databáze
    return (month_start_dates, month_end_dates, last_month_start_dates)


def CreateInputs(start_date, end_date, group_by_fundamental_sector, capitalization_cond, min_price_cond,exchange_cond):
    (month_start_dates, month_end_dates, last_month_start_dates) = CreateDates(start_date, end_date)
    inputs=[]
    for i in range(len(last_month_start_dates)):
        last_month_start_date = last_month_start_dates[i]
        print(last_month_start_date)
        df = CreateDataFrame(last_month_start_date, group_by_fundamental_sector, capitalization_cond, min_price_cond,exchange_cond)
    
        month_start_date = month_start_dates[i]
        month_end_date = month_end_dates[i]
        sectors_for_date={}  # mapa názvu sektoru na seznam akcií v sektoru
        for index, row in df.iterrows():
            fundamental_sector=row['fundamental_sector2']
            print(fundamental_sector)
            
            tickers_str = row['tickers']
            ticker_list = str.split( tickers_str, sep= "," )
            count = row['count']
            print(count)
            print(len(ticker_list))
            if len(ticker_list)!=count:
                raise NameError('Nesouhlasí počet prvků s parsovaným seznamem. ')
            print(ticker_list)
            sectors_for_date[fundamental_sector]=tickers_str
            
        input_for_date = (month_start_date, month_end_date, sectors_for_date)
        inputs.append(input_for_date)
    return inputs
    
#for record in df:
 #   print(record)
    
    


datadir="d:\\!\\R\\_S&P100\\EquityPrices\\" 
ext=".csv"
os.chdir(datadir)
csv_list = glob.glob('*'+ext)
csv_list = sorted(csv_list)

m=pd.DataFrame()
i=0
for symbol in csv_list:
    symbol = str.replace(symbol,ext,"")
    print(symbol)
    stock_data = pd.read_csv(datadir+symbol+ext)
    stock_data['date'] = pd.to_datetime(stock_data['date'])

    if i==0:
        m['date'] = stock_data['date']
        
    m=m.merge(stock_data[['date','close']], left_on='date', right_on='date')
    m.columns.values[i+1]=symbol
    i+=1
    
# nastaveni pocatecni hodnoty na 1 pro vsechny akcie
m.set_index("date", inplace=True)
begin_values=m.iloc[0]
for i in range(0,len(m.columns)):
    m.iloc[:,i] = m.iloc[:,i] / begin_values[i]
    
x=m.to_numpy().transpose()   

clusters=10
dba_km = TimeSeriesKMeans(n_clusters=clusters,
                          n_init=2,
                          n_jobs=24,
                          metric="dtw",
                          verbose=True,
                          max_iter_barycenter=10,
                          random_state=seed)

y = dba_km.fit_predict(x)
yy=pd.DataFrame(y.reshape(-1,1))
yy['name'] = m.columns
yy.set_index('name', inplace=True)
yy.columns.values[0]='sector'

for cluster in range(clusters):
    plt.subplot(5, 2, cluster+1)
    for xx in x[y == cluster]:
        plt.plot(xx.ravel(), "k-", alpha=.2)
    #plt.plot(sdtw_km.cluster_centers_[yi].ravel(), "r-")
    #plt.xlim(0, sz)
    #plt.ylim(-4, 4)
    #plt.text(0.55, 0.85,'Cluster %d' % (yi + 1),          transform=plt.gca().transAxes)
    #if yi == 1:
    #    plt.title("Soft-DTW $k$-means")
        
plt.figure()
for xx in X_train:
    plt.plot(xx.ravel(), "k-", alpha=.2)


        
#stocks=pd.read_feather("d:\\!\\R\\close_stock_06_2020.ftr")
    

