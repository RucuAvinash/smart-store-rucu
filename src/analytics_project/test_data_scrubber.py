print("Running DataScrubber test..")
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

import unittest
import pandas as pd
from src.analytics_project.data_scrubber import DataScrubber

class TestDataScrubber(unittest.TestCase):
    def setUp(self):
       self.df = pd.DataFrame({
           'Name' : ['Alice', 'Bob', 'Peter'],
           'Age'  : [25,None,25],
           'Income':[50000, 60000, None]
       })
       self.scrubber = DataScrubber(self.df)
       
    def test_remove_duplicates(self):
           self.scrubber.df = pd.concat([self.scrubber.df,self.scrubber.df.iloc[[0]]],ignore_index=True)
           self.scrubber.remove_duplicate_records()
           print(self.scrubber.df)
           self.assertEqual(len(self.scrubber.df),3)
        
        
    def handle_missing_data_fill(self):
        self.scrubber.handle_missing_data(fill_value = self.scrubber.df['Age'].mean())
        print(self.scrubber.df)
        self.assertFalse(self.scrubber.df['Age'] .isnull() .any())
    
    def test_drop__columns(self):
        self.scrubber.drop_columns(['Income'])
        print(self.scrubber.df)
        self.assertNotIn('Income',self.scrubber.df.columns)
        
        
         
if __name__ ==  '__main__' :
     unittest.main()