"""
Script to run Human Activity Recognition model on a single combined input file
"""
import tensorflow as tf
import numpy as np
import pandas as pd
import pywt
import argparse
import os

def get_csi_preconditioned(data):
    remove_subcarrier_cols = [x+32 for x in [-21, -7, 21,  7, -32, -31, -30, -29, 31,  30,  29,  0]]
    remove_subcarrier_cols.sort()
    return np.delete(data, remove_subcarrier_cols, axis=1)

def median_absolute_deviation(x):
    """
    Returns the median absolute deviation from the window's median

    Args:
        x: Values in the window
    Returns:
        The median absolute deviatino (MAD) of x
    """
    return np.median(np.abs(x - np.median(x)))

def hampel(ts, window_size=3, t0=3):
    """
    Args:
        ts
        window_size
        t0: threshold
    Ref:
        1. https://github.com/MichaelisTrofficus/hampel_filter
    """
    # Make copy so original not edited
    vals = pd.Series(ts.copy())

    # constant scale factor for Hampel Filter
    L = 1.4826
    
    # calculate median absolute deviation
    rolling_ts = vals.rolling(window_size, center=True)
    rolling_median = rolling_ts.median().fillna(method='bfill').fillna(method='ffill')
    rolling_sigma = L*(rolling_ts.apply(median_absolute_deviation).fillna(method='bfill').fillna(method='ffill'))

    outliers_idx_lst = list(
        np.array(np.where(np.abs(vals - rolling_median) >= (t0 * rolling_sigma))).flatten())

    # replace outliers with rolling median
    vals[outliers_idx_lst] = rolling_median[outliers_idx_lst]

    # print("vals: ", vals.shape)
    return vals.to_numpy()

def dwt_denoise(vals, scheme):
    data = vals.copy()
    w = pywt.Wavelet(scheme)
    max_level = pywt.dwt_max_level(len(data), w.dec_len) # level=3
    coeffs = pywt.wavedec(data, scheme, max_level)
    rec_vals = pywt.waverec(coeffs, scheme)
    return rec_vals

def preprocess_input(x):
    csi_data = x[:, :64]
    lgt_data = x[:, -9:]
    csi_clean = get_csi_preconditioned(csi_data)
    csi_clean = np.apply_along_axis(hampel, 1, csi_clean)
    csi_clean  = dwt_denoise(csi_clean, scheme='sym5')
    return [np.array([csi_clean]), np.array([lgt_data])]
    
# Define a function to make predictions on the input data
def predict(arr):
    classes = ['empty', 'sit', 'stand','walk']
    input_mat = preprocess_input(arr)

    # Load the saved Keras model
    model = tf.keras.models.load_model('../model/v3_model')
    
    # Make a prediction using the loaded model
    prediction = model.predict(input_mat)
    
    # Return the predicted class
    for idx in range(len(classes)):
        print(f"{classes[idx]}: {prediction[0][idx]:.2%}")
    return classes[prediction.argmax(axis=-1)[0]]

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Predict acitivity from input file')
    parser.add_argument('-f', '--input_filename', type=str, help='Input file path')
    args = parser.parse_args()
    
    # check if input file exists
    if not os.path.exists(args.input_filename):
        raise Exception(f"Input file {args.input_filename} does not exist")
    
    # read input file as dataframe
    try:
        input_arr = df = pd.read_csv(args.input_filename, sep=",", header=None).values
    except Exception as e:
        raise Exception(f"Error reading input file {args.input_filename}: {e}")
    
    prediction = predict(input_arr)
    print(f"Input: {args.input_filename}, Prediction: {prediction}")