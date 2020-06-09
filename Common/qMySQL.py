# Zdeněk Valečko
# 2020-06-09
# Skript pro běžné operace přístupu k serveru MySQL

import pymysql
import os
import pandas as pd

# vrací objekt připojení k serveru MySQL, součástí jsou pšístupové údaje včetně IP adresy
# conn - objekt připojení k serveru
def GetConnection():
    conn = pymysql.connect(host='192.168.204.208', port=3306, user='root', passwd='root', db='mysql', local_infile = 1)
    return conn

# spustí dotaz, který nevrací žádné hodnoty, vhodné jen pro exekuční operace (např. mazání, spuštění procedury atd.)
# sql = prováděný SQL příkaz
def ExecQuery(sql):
    conn=GetConnection()
    cur = conn.cursor()
    #print (sql)
    cur.execute(sql)
    conn.close()

# spustí dotaz, který vrací hodnoty, vhodné pro výběrové operace (typicky SELECT)
# sql = prováděný SQL příkaz
# result = tabulka Pandas vybraných záznamů včetně názvů sloupcú podle definice SQL
def LoadQuery(sql):
    conn = GetConnection()
    cursor = conn.cursor()
    cursor.execute(sql)
    records = cursor.fetchall()
    field_names = [i[0] for i in cursor.description] # získá seznam názvů sloupců z dotazu SQL
    result = pd.DataFrame(records, columns=field_names)
    conn.close() 
    return result

# importuje data z csv souboru do databáze
# file_path = cesta ke vstupnímu CSV souboru
# db_name = název cílové databáze na serveru
# table_name = název cílové tabulky
# field_list = seznam polí, které se budou importovat
# delimiter = oddělovat polí v CSV souboru
# lines_terminator = oddělovat řádek v CSV souboru
# ignore_lines = počet řádek CSV souboru, které se budou ignorovat
def ImportFileToDb( file_path, db_name, table_name, field_list, delimiter, lines_terminator, ignore_lines ):
    if not os.path.exists( file_path ):
        print("File "+file_path+" does not exist!")
        return
    sql_file_path=str.replace(file_path,"\\","\\\\")
    
    sql="LOAD DATA LOW_PRIORITY LOCAL INFILE '"+sql_file_path+"' REPLACE INTO TABLE `"+db_name+"`.`"+table_name +  """` CHARACTER SET latin2 FIELDS TERMINATED BY '"""+delimiter+"""' 
    OPTIONALLY ENCLOSED BY '"' 
    LINES TERMINATED BY '"""+lines_terminator+"""' 
    IGNORE """+str(ignore_lines)+" LINES """+field_list
    
    print (sql)
    ExecQuery(sql)
