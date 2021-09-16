import os
import unittest
import unittest.mock
import src.pyspark_tools


class Test(unittest.TestCase):
    def test_python_location(self):

        path = src.pyspark_tools.python_location()

        self.assertIsInstance(path,str)

    def test_initialise_spark(self):

        spark = src.pyspark_tools.initialise_spark()

        self.assertTrue(spark.sql("SELECT 1"))


class TestDataStore(unittest.TestCase):
    def setUp(self) -> None:
        self.datastore = src.pyspark_tools.DataStore()
        self.test_filepath = './test_me.csv'

        with open(self.test_filepath, "wb") as f:
            f.write(b"test\n1")

        self.test_sdf = self.datastore.get_table(self.test_filepath)

    def tearDown(self) -> None:
        os.remove(self.test_filepath)

    def test_get_table(self):

        self.assertEqual(self.test_sdf.count(), 1)

    def test_save_table(self):
        # setup
        test_save_table_file = 'test2.csv'
        self.datastore.save_table(self.test_sdf, test_save_table_file)

        # test
        self.assertTrue(os.path.isfile(test_save_table_file))

        # teardown
        os.remove(test_save_table_file)

if __name__ == '__main__':
    unittest.main()