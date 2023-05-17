import unittest
import numpy as np

import sys
sys.path.append('..')
from tools import predict


class TestGetCsiPreconditioned(unittest.TestCase):

    def test_shape(self):
        arr = np.arange(1, 65)
        arr = arr.reshape(1, 64)
        res = predict.get_csi_preconditioned(arr)
        self.assertEqual(res.shape, (1, 52))
    
    def test_columns(self):
        removed_cols = [0, 1, 2, 3, 11, 25, 32, 39, 53, 61, 62, 63]
        kept_cols = np.array([x for x in range(64) if x not in removed_cols])

        arr = np.arange(0, 64)
        arr = arr.reshape(1, 64)
        res = predict.get_csi_preconditioned(arr)[0] # get first row
        for i, j in zip(kept_cols, res):
            self.assertEqual(i, j)
    
    def test_random_matrix_shape(self):
        import random
        random.seed(0)
        arr = np.random.rand(100, 64)
        res = predict.get_csi_preconditioned(arr)
        self.assertEqual(res.shape, (100, 52))

class TestPreprocessInput(unittest.TestCase):
    def test_dimensions(self):
        arr = np.random.rand(100, 64)
        res = predict.preprocess_input(arr)
        self.assertEqual(res[0].shape[-1], 52) # csi has 52 columns
        self.assertEqual(res[1].shape[-1], 9) # light has 9 columns
    
    def test_light_unchanged(self):
        arr = np.random.rand(100, 64+9)
        arr_copy = arr.copy()
        res = predict.preprocess_input(arr_copy)
        self.assertTrue(np.array_equal(arr[:, 64:], res[1][0]))

class TestPredict(unittest.TestCase):
    def setUp(self) -> None:
        arr = np.random.rand(100, 64)
        self.probs_arr = predict.get_probs(arr)
    
    def test_returns_probs(self):
        self.assertAlmostEqual(1 - np.sum(self.probs_arr), 0) # sum of probabilities should be nearly 1, accounting precision error
    
    def test_returns_activity_name(self):
        pred_class = predict.get_predicted_class(self.probs_arr)
        self.assertIn(pred_class, ['empty', 'sit', 'stand', 'walk'])


if __name__ == '__main__':
    unittest.main(argv=[''], verbosity=2, exit=False)