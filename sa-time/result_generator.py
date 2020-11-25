import time
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import csv
import datetime


def result_generator(dir):
    df_host = pd.read_csv(dir + 'df_host.csv')
    df_instance = pd.read_csv(dir + 'df_instance.csv')
    df_finsh = pd.read_csv(dir + 'sa_best2020-11-03-15_23_44.csv')

    df_instance = df_instance[df_instance['moved'] == 1]
    for index, row in df_instance.iterrows():
        host_id_dst = np.nonzero(df_finsh.loc[index].tolist())
        host_id_dst = df_host.loc[host_id_dst[0], 'id'].tolist()
        df_instance.loc[index, 'host_id'] = host_id_dst[0]

    df_result = pd.DataFrame()
    df_result['SOURCE_HOST_ID'] = df_instance['host_src_id']
    df_result['TARGET_HOST_ID'] = df_instance['host_id']
    df_result['est_duration'] = df_instance['est_duration']
    df_result['ID'] = df_instance['id']
    df_result['START_AT'] = 0
    df_result['END_AT'] = 0
    time_cnt = 0

    for index, row in df_result.iterrows():
        df_result.loc[index, 'START_AT'] = (
                datetime.datetime.strptime('00:00', "%H:%M") + datetime.timedelta(minutes=time_cnt)).strftime(
            "%H:%M")
        df_result.loc[index, 'END_AT'] = (datetime.datetime.strptime('00:00', "%H:%M") + datetime.timedelta(
            minutes=(time_cnt + row['est_duration'] - 1))).strftime("%H:%M")
        time_cnt += row['est_duration']
    now = time.strftime("%Y-%m-%d-%H_%M_%S", time.localtime(time.time()))
    df_result = df_result[['ID', 'SOURCE_HOST_ID', 'TARGET_HOST_ID', 'START_AT', 'END_AT', 'est_duration']]
    df_result.to_csv(dir + "result_2_" + now + ".csv", index=False)


if __name__ == '__main__':
    result_generator('../data_set_1/')
