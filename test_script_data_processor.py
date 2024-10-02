## Define the Test Class

import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from sqlalchemy import create_engine

class TestSQLProcessing(unittest.TestCase):

    @patch('pyodbc.connect')
    @patch('sqlalchemy.create_engine')
    def test_create_sql_connection(self, mock_create_engine, mock_pyodbc_connect):
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_pyodbc_connect.return_value = mock_conn

        server = 'LAPTOP-U3795DN8'
        database = 'mydatabase'

        conn, engine = create_sql_connection(server, database)
        
        mock_pyodbc_connect.assert_called_once()
        mock_create_engine.assert_called_once()
        self.assertEqual(conn, mock_conn)
        self.assertEqual(engine, mock_engine)

    @patch('pandas.read_sql')
    def test_fetch_data_from_sql(self, mock_read_sql):
        mock_df = pd.DataFrame({
            'User_ID': [1002903, 1000732],
            'Cust_name': ['Sanskriti', 'Kartik'],
            'Product_ID': ['P00125942', 'P00110942'],
            'Gender': ['F', 'F'],
            'Age Group': ['26-35', '26-35'],
            'Age': [28, 35],
            'Marital_Status': [0, 1],
            'State': ['Maharashtra', 'Andhra Pradesh'],
            'Zone': ['Western', 'Southern'],
            'Occupation': ['Healthcare', 'Govt'],
            'Product_Category': ['Auto', 'Auto'],
            'Orders': [1, 3],
            'Amount': [23952.00, 23934.00],
            'Status': [None, None],
            'unnamed1': [None, None]
        })
        mock_read_sql.return_value = mock_df
        
        conn = MagicMock()
        table_name = 'dbo.dbo.Diwali'

        df_sql = fetch_data_from_sql(conn, table_name)
        
        mock_read_sql.assert_called_once_with(f"SELECT * FROM {table_name}", conn)
        self.assertIsInstance(df_sql, pd.DataFrame)
        self.assertEqual(df_sql.shape, (2, 15))  # Adjust the number of columns based on your data

    def test_clean_data(self):
        data = {
            'Unnamed': [None, None],
            'Status': ['Delivered', 'Pending'],
            'Age Group': ['18-25', '26-35'],
            'User_ID': [1, 2],
            'Cust_name': ['Alice', 'Bob']
        }
        df_sql = pd.DataFrame(data)

        cleaned_df = clean_data(df_sql)
        
        self.assertNotIn('Unnamed', cleaned_df.columns)
        self.assertNotIn('Status', cleaned_df.columns)
        self.assertIn('Age Group', cleaned_df.columns)
        self.assertEqual(len(cleaned_df), 2)
        self.assertEqual(cleaned_df['Age Group'].iloc[0], '18-25')
        self.assertEqual(cleaned_df['Age Group'].iloc[1], '26-35')

    @patch('pandas.DataFrame.to_sql')
    def test_save_data_to_sql(self, mock_to_sql):
        cleaned_df = pd.DataFrame({
            'User_ID': [1002903, 1000732],
            'Cust_name': ['Sanskriti', 'Kartik'],
            'Product_ID': ['P00125942', 'P00110942'],
            'Gender': ['F', 'F'],
            'Age Group': ['26-35', '26-35'],
            'Age': [28, 35],
            'Marital_Status': [0, 1],
            'State': ['Maharashtra', 'Andhra Pradesh'],
            'Zone': ['Western', 'Southern'],
            'Occupation': ['Healthcare', 'Govt'],
            'Product_Category': ['Auto', 'Auto'],
            'Orders': [1, 3],
            'Amount': [23952.00, 23934.00],
            'Status': [None, None],
            'unnamed1': [None, None]
        })
        
        engine = MagicMock()
        table_name = 'dbo.dbo.Diwali'

        save_data_to_sql(cleaned_df, engine, table_name)
        
        mock_to_sql.assert_called_once_with(table_name, engine, if_exists='replace', index=False)

    def test_log_error(self):
        with self.assertLogs('data_processing', level='ERROR') as cm:
            log_error("Test error message")
        
        self.assertIn("ERROR:data_processing:Error: Test error message", cm.output)

        ## run the test
        
        unittest.TextTestRunner().run(unittest.TestLoader().loadTestsFromTestCase(TestSQLProcessing))

