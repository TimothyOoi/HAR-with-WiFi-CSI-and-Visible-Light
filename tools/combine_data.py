import pandas as pd
import numpy as np
import glob
import os
import argparse

def get_activity_filecount(dir, activity):
    """ Return number of files in a directory which start with the activity's name
    Args:
        dir (str): directory name to search
        activity (str): activity name
    Returns:
        int: number of files in directory which start with activity's name
    """
    return len(glob.glob(f"{dir}/{activity}*.csv"))

def combine_data(fname, csi_dir, lgt_dir, lgt_has_header):
    """ Combine CSI and light data into a single pandas dataframe
    Args:
        fname (str): filename of data to be combined
        csi_dir (str): directory name for csi data file
        lgt_dir (str): directory name for light data file
        lgt_has_header (bool): True if first row of light data file is column headers, False otherwise
    Returns:
        pandas.DataFrame with 64 csi subcarriers and 9 light sensors columns
    """
    csi_df = pd.read_csv(f"{csi_dir}/{fname}").dropna(axis=1) # read in csi data and remove columns with all nulls
    if lgt_has_header:
        lgt_df = pd.read_csv(f"{lgt_dir}/{fname}")
    else:
        lgt_df = pd.read_csv(f"{lgt_dir}/{fname}", header=None, names=['msg_received_datetime', 'msg_published_datetime', 'sensor_id', 'sensor_val'])

    # parse to pandas datetime object and floor to closest 100 millisecond
    csi_df['time'] = pd.to_datetime(csi_df['time'], unit='s', utc=True).dt.tz_convert('Asia/Kuala_Lumpur').dt.floor('100ms')
    lgt_df['msg_received_datetime'] = pd.to_datetime(lgt_df['msg_received_datetime'], format='%Y-%m-%d %H:%M:%S').dt.tz_localize('Asia/Kuala_Lumpur').dt.floor('100ms')
    lgt_df['msg_published_datetime'] = pd.to_datetime(lgt_df['msg_published_datetime'], format='%Y-%m-%d %H:%M:%S.%f').dt.tz_localize('Asia/Kuala_Lumpur').dt.floor('100ms')

    # match csi start time to light start time due to issue in Raspberry Pi
    csi_min_time = csi_df['time'].min()
    lgt_min_time = lgt_df['msg_published_datetime'].min()
    csi_df['time'] = csi_df['time']  + (lgt_min_time - csi_min_time)

    # convert complex numbers to absolute values
    for idx in range(0, 64):
        csi_col = f'_{idx}'
        csi_df[csi_col] = csi_df[csi_col].apply(lambda x: np.abs(np.complex128(x)))

    # convert light data from long to wide format
    LIGHT_COLNAMES = ['sensor_' + str (i) for i in range(1,10)]
    lgt_wide_df = lgt_df.pivot_table(index="msg_published_datetime", columns="sensor_id", values="sensor_val", aggfunc="mean").add_prefix("sensor_").reset_index()
    # fill null values with previous data, then subsequent data if needed
    lgt_wide_df.loc[:, LIGHT_COLNAMES] =  lgt_wide_df.loc[:, LIGHT_COLNAMES].ffill().bfill()

    # merge csi and light data on timestamp
    merged_df = csi_df.merge(lgt_wide_df, how='inner', left_on='time', right_on='msg_published_datetime')
    return merged_df

def batch_rows(df, rows_per_group):
    """
    Args:
        df (pandas.DataFrame): dataframe to be batched
        rows_per_group (int): number of rows per batch
    Returns:
        list: list of numpy arrays, where each numpy array is a batch with rows_per_group rows
    """
    CSI_COLNAMES = [f'_{idx}' for idx in range(0, 64)]
    LIGHT_COLNAMES = ['sensor_' + str (i) for i in range(1,10)]

    res = []
    total_groups = df.shape[0]//rows_per_group
    count = 0
    while count < total_groups:
        temp = df.iloc[count*rows_per_group:(count+1)*rows_per_group, :]
        temp = temp[CSI_COLNAMES+LIGHT_COLNAMES].values
        if not np.isnan(np.min(temp)):
            res.append(temp)
        count += 1
    return res

def combine_csi_light_in_dir(csi_dir, light_dir, output_dir, lgt_has_header=True):
    """ Combine CSI and light data in separate directories and output to specified directory
    Args:
        csi_dir (str): directory name for csi data files
        light_dir (str): directory name for light data files
        output_dir (str): directory name for output files
    """
    activities = ['empty', 'sit', 'stand', 'walk']

    # check if output directory exists, if not throw error
    if not os.path.exists(csi_dir):
        raise Exception(f"CSI directory {csi_dir} does not exist")
    elif not os.path.exists(light_dir):
        raise Exception(f"Light directory {light_dir} does not exist")
    
    # create output directory if it does not exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for activity in activities:
        counter = get_activity_filecount(output_dir, activity)
        total_files = get_activity_filecount(csi_dir, activity)
        print(f"Processing {activity} files. Total files: {total_files}")
        for i in range(1, total_files+1):
            fname = f'{activity}_{i}.csv'
            try:
                res = combine_data(fname, csi_dir, light_dir, lgt_has_header)
            except Exception as e:
                print(e)
                continue
            batched_res = batch_rows(res, 100)
            for batch in batched_res:
                np.savetxt(f"{output_dir}/{activity}_{counter}.csv", batch, delimiter=",")
                counter += 1
            print(f"dirs: {(csi_dir, light_dir)}, file name: {fname}, file shape: {res.shape}, total output files: {len(batched_res)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Combine CSI and light data into a single file')
    parser.add_argument('-c', '--csi_dir', type=str, help='Directory name for CSI data')
    parser.add_argument('-l', '--light_dir', type=str, help='Directory name for light data')
    parser.add_argument('-o', '--output_dir', type=str, help='Directory name for output data')
    parser.add_argument('-n', '--no_header', action='store_false', help='Flag to indicate if light data has header')
    args = parser.parse_args()
    combine_csi_light_in_dir(args.csi_dir, args.light_dir, args.output_dir, args.no_header)