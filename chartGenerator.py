import subprocess
from handlers.rabbitHandler import rabbitHandler as rabbit
from configs.appConfig import LOADING_Q

loading_q = rabbit(LOADING_Q)
displayChart = False

def main():
    try:
        loading_q.connect()
        loading_q.consume(onMsgRecived)
    except KeyboardInterrupt:
        print('Interrupted')
        loading_q.disconnect()

def onMsgRecived(ch, method, properties, body):
    global displayChart

    if not displayChart:
        displayChart = True
        subprocess.run("python chart.py", shell=True, check=True)

if __name__ == '__main__':
    main()
