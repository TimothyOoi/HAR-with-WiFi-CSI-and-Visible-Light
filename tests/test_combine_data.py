import unittest
import shutil
import numpy as np
import os

import sys
sys.path.append('..')
from tools import combine_data

class TestGetActivityFilecount(unittest.TestCase):
    # create mock folder with 5 files starting with 'empty'
    def setUp(self) -> None:
        self.num_mock_files = 5
        self.mock_dirname_abs = os.path.join(os.getcwd(), 'mock').replace('\\', '/')
        self.example_data_fname = 'sit_100.csv'
        self.mock_csi_dir = 'mock_csi'
        self.mock_light_dir = 'mock_light'

        if not os.path.exists('mock'):
            os.mkdir('mock')
            for i in range(self.num_mock_files):
                with open(f'mock/empty_{i}.csv', 'w') as f:
                    pass
        
        # get combined data
        self.combined_data = combine_data.combine_data(self.example_data_fname, self.mock_csi_dir, self.mock_light_dir, True)
        
    
    def test_filecount(self):
        self.assertEqual(combine_data.get_activity_filecount(self.mock_dirname_abs, 'empty'), self.num_mock_files)
    
    def test_combined_data_shape_cols(self):
        csi_lgt_colnames = [f'_{idx}' for idx in range(0, 64)] + ['sensor_' + str (i) for i in range(1,10)]
        output_colnames = self.combined_data.columns.values

        self.assertEqual(len(set(output_colnames).intersection(csi_lgt_colnames)), 64+9)
    
    def test_batch_rows(self):
        # test if batch_rows returns correct number of batches
        batched_res = combine_data.batch_rows(self.combined_data, 100)
        self.assertEqual(len(batched_res), self.combined_data.shape[0]//100)
        
    def tearDown(self):
        # delete mock folder
        shutil.rmtree('mock')

if __name__ == '__main__':
    unittest.main(argv=[''], verbosity=2, exit=False)