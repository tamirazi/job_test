import pika, sys, os, json
import pandas as pd
import time
from handlers.rabbitHandler import rabbitHandler as rabbit
from handlers.dbHandler import dbHandler as db
from configs.appConfig import DB, FILES_Q, LOADING_Q

loading_q = rabbit(LOADING_Q)
files_q = rabbit(FILES_Q)
sqlite = db(DB)
mcounter = 0

def main():
    
    try:
        files_q.connect()
        loading_q.connect()
        print('Sqlite consumer waiting for messages. To exit press CTRL+C')
        files_q.consume(onMsgRecived)
    except KeyboardInterrupt:
        print('Interrupted')
        loading_q.disconnect()
        files_q.disconnect()

def onMsgRecived(ch, method, properties, body):
    """Callback when FILES_Q recive message.

    Args:
        deafult args from pika lib

    """
    file_path = json.loads(body)['path']
    file_type = json.loads(body)['type']
    table_name = json.loads(body)['table_name']
    year = file_path.split('_')[1]
    df = getDataframe(file_path , file_type)
    insertToDb(df, year,table_name)
    loading_q.sendMsg({})
    time.sleep(1)

def getDataframe(f_path, f_type):
    """Create pandas.dataframe from file.

    Args:
        f_path (file path): file path.
        f_type (file type): csv/json

    Returns:
        DataFrame / Empty DataFrame if file type is not csv/json
    """
    filename = '{0}.{1}'.format(f_path, f_type)
    if(f_type == 'csv'):
        return pd.read_csv(filename) 
    if(f_type == 'json'):
        return pd.read_json(filename)
    return pd.DataFrame()   

def deleteYearFromDB(year):
    """Remove from invoices table all elements related to 'year'.

    Args:
        year: the year we want to delete from the database.
    """
    if(tableExists()):
        sqlite.select(
            '''
            DELETE from invoices WHERE strftime('%Y',InvoiceDate)='{}'
            '''.format(year)
            )

def insertToDb(df,year, tablename):
    """Simple insert dataframe to sqlite table.
    Args:
        df (dataframe): dataframe we want to insert.
        year: the year related to this dataframe
        tablename: the tabke name we want to insert the data into
    """
    conn = sqlite.connect()
    deleteYearFromDB(year) #Ensure there are no duplicates in the table when we run the script multiple times
    df.to_sql(tablename, con=conn, if_exists='append', index=False)
    sqlite.disconnect()

def tableExists(): 
    """Check if invoices table exists.

    Returns:
        True/False
    """
    return sqlite.select("SELECT count(name) FROM sqlite_master WHERE type='table' AND name='invoices'")[0][0] == 1

if __name__ == '__main__':
    main()