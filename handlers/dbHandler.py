import sqlite3
import os

class dbHandler:
    def __init__(self,dbPath):
        self.path=dbPath
        self.conn = None

    def getPath(self):
        return self.path

    def connect(self):
        try:
            conn = sqlite3.connect(self.path)
            self.conn = conn
            return self.conn
        except :
            print ('connection error')
            return -1

    def disconnect(self):
        if self.conn:
            self.conn.close()

    def select(self, query):
        if self.conn:
            cur = self.conn.cursor()
            cur.execute(query)
            rs = cur.fetchall()
            return rs

    def totalSalesPerMonth(self):
        self.connect()
        res = self.select(
        '''
        SELECT strftime('%Y-%m',InvoiceDate) as y_m_date,
        SUM(total)
        FROM invoices
        GROUP BY y_m_date
        ORDER BY y_m_date
        '''
        )
        self.disconnect()
        return [[i[0] for i in res], [i[1] for i in res]]

    def activeCustomersPerMonth(self):
        self.connect()
        res = self.select(
        '''
        SELECT strftime('%Y-%m',InvoiceDate) as y_m_date,
        count(CustomerId)
        FROM invoices
        GROUP BY y_m_date
        ORDER BY y_m_date
        '''
        )
        self.disconnect()
        return [[i[0] for i in res], [i[1] for i in res]]





