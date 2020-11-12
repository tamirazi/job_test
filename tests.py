import unittest
import publisher
import consumer
import pandas
from configs.appConfig import DATA_PATH


class TestPublisher(unittest.TestCase):

    def test_create_msg(self):
        json_file_path = DATA_PATH + '/invoices_2009.json'
        csv_file_path = DATA_PATH + '/invoices_2013.csv'
        jsonfile = publisher.createMsg(json_file_path)
        csvfile = publisher.createMsg(csv_file_path)
        self.assertEqual(jsonfile['path'], DATA_PATH + '/invoices_2009')
        self.assertEqual(jsonfile['type'], 'json')
        self.assertEqual(csvfile['path'], DATA_PATH + '/invoices_2013')
        self.assertEqual(csvfile['type'], 'csv')

class TestConsumer(unittest.TestCase):

    def test_getDataframe(self):
        self.assertIsInstance(consumer.getDataframe(DATA_PATH + '/invoices_2009', 'json'), pandas.core.frame.DataFrame)
        self.assertIsInstance(consumer.getDataframe(DATA_PATH + '/invoices_2013', 'csv'), pandas.core.frame.DataFrame)
        self.assertEqual(consumer.getDataframe(DATA_PATH + '/invoices_2013', 'txt').empty, True)


if __name__ == '__main__':
    unittest.main()