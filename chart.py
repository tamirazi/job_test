import random
from itertools import count
import pandas as pd
import datetime
import os

import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import matplotlib.dates as mdates

from handlers.dbHandler import dbHandler as db
from configs.appConfig import DB

def initAxes(ax,num,data):
    """Axes initialization.

    Args:
        ax (axes): matplot axes.
        num: 0/1 for the first chart or the second
        data: actual data
    """
    x = [datetime.datetime.strptime(d,"%Y-%m").date() for d in data[0]]
    y = data[1]
    ax[num].plot(x, y, '.-')

def initChart():
    """Chart initialization.

    Args:
        fig (figure): matplot figure.
        ax (axes): matplot axes.
    """
    print('Chart waiting for msg in Loading q. To exit press CTRL+C')
    fig, ax = plt.subplots(nrows=2)
    styleChart(fig,ax)
    return (fig, ax)

def styleChart(fig,ax):
    """Configurations for styling matplotlib charts.

    Args:
        fig (figure): matplot figure.
        ax (axes): matplot axes.
    """
    formatter = mdates.DateFormatter("%Y")
    years = mdates.YearLocator() 
    months = mdates.MonthLocator()
    ax[0].set_title('Total sales per month')
    ax[1].set_title('Active customer per month')
    fig.tight_layout(pad=3)
    for x in ax:
        x.xaxis.set_major_formatter(formatter)
        x.xaxis.set_major_locator(years)
        x.xaxis.set_minor_locator(months)
        x.format_xdata = mdates.DateFormatter('%Y-%m-%d')
        x.grid(True)

def animate(i):
    sqlite = db(DB)
    ax[0].clear()
    ax[1].clear()
    styleChart(fig,ax)
    initAxes(ax,0,sqlite.totalSalesPerMonth())
    initAxes(ax,1,sqlite.activeCustomersPerMonth())

fig, ax = initChart()
ani = FuncAnimation(plt.gcf(), animate, interval=1000)

plt.show()
os.remove(DB)
