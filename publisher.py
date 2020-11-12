import os ,sys ,json
from handlers.rabbitHandler import rabbitHandler as rabbit
from configs.appConfig import DATA_PATH, FILES_Q

def main():
    files_q = rabbit(FILES_Q)
    files_q.connect()
    for filename in os.listdir(DATA_PATH):
        file = '{0}/{1}'.format(DATA_PATH, filename)
        msg = createMsg(file)
        files_q.sendMsg(msg)
    files_q.disconnect()

def createMsg(file_path):
    """Create message to rabbit FILES_Q from file path.

    Args:
        file_path: Path to csv/json file.

    Returns:
        Dict: 
        {
            path: path to the given file (without the file extension)
            type: file extension csv/json
            table_name: invoices (const)
        }
    """
    filename, file_extension = os.path.splitext(file_path)
    return {
        'path': filename,
        'type': file_extension[1:],
        'table_name': 'invoices'
    }

if __name__ == '__main__':
    main()
