import numpy as np
import pandas as pd


def save_df(df_host, df_instance, dir):
    coordinates = np.zeros((len(df_instance), len(df_host)), dtype=np.int)
    for index, row in df_instance.iterrows():
        host_id = df_host[df_host['id'] == row['host_id']].index.tolist()
        coordinates[index, host_id[0]] = row['memory_real']

    df = pd.DataFrame(coordinates)
    df.to_csv(dir + "sa_init.csv", index=False)
    df_host.to_csv(dir + "df_host.csv", index=False)
    df_instance.to_csv(dir + "df_instance.csv", index=False)


def data_init(file_host, file_instance, dir):
    try:
        df_host = pd.read_csv(file_host)
        df_host.drop_duplicates(inplace=True)

        df_host['cpu_vcore_used'] = 0
        df_host['memory_v_used'] = 0
        df_host['memory_used'] = 0
        df_host['index'] = df_host.index
        df_host.set_index('id', drop=False, inplace=True)

        df_instance = pd.read_csv(file_instance)
        df_instance.drop_duplicates(inplace=True)

        df_instance['host_src_id'] = df_instance['host_id']
        df_instance['moved'] = 0
        df_instance_summy = df_instance.groupby('host_id').sum()
        for index, row in df_instance_summy.iterrows():
            tmp = df_host.loc[index, 'cpu_vcore_used']
            tmp = df_host.loc[index, 'memory_v_used']
            tmp = df_host.loc[index, 'memory_used']
            df_host.loc[index, 'cpu_vcore_used'] = row['vcpus']
            df_host.loc[index, 'memory_v_used'] = row['memory']
            df_host.loc[index, 'memory_used'] = row['memory_real']

        df_host_src = df_host[0:20].copy()
        df_host_dst = df_host[20:].copy()

        df_instance_src = df_instance[df_instance['host_id'].isin(df_host_src['id'].tolist())]
        tmp_index = -1
        for index_instance, row_instance in df_instance_src.iterrows():
            print(index_instance)
            is_ok = False
            for i in range(3):
                if i > 1:
                    print(i)
                for index_host, row_host in df_host_dst.iterrows():
                    if row_host['index'] > tmp_index:
                        if int(row_host['cpu_vcore']) >= (
                                int(row_host['cpu_vcore_used']) + int(row_instance['vcpus'])) and \
                                int(row_host['memory_v']) >= (
                                int(row_host['memory_v_used']) + int(row_instance['memory'])) and \
                                (int(row_host['memory']) + 64000) > (
                                int(row_host['memory_used']) + int(row_instance['memory_real'])):
                            df_host_dst.loc[index_host, 'cpu_vcore_used'] += row_instance['vcpus']
                            df_host_dst.loc[index_host, 'memory_v_used'] += int(row_instance['memory'])
                            df_host_dst.loc[index_host, 'memory_used'] += row_instance['memory_real']

                            df_host_src.loc[row_instance['host_id'], 'cpu_vcore_used'] -= row_instance['vcpus']
                            df_host_src.loc[row_instance['host_id'], 'memory_v_used'] -= row_instance['memory']
                            df_host_src.loc[row_instance['host_id'], 'memory_used'] -= row_instance['memory_real']

                            df_instance.loc[index_instance, 'host_id'] = row_host['id']
                            df_instance.loc[index_instance, 'moved'] = 1
                            is_ok = True

                            tmp_index = row_host['index']
                            break
                if is_ok:
                    break
                tmp_index = -1

        df_instance = df_instance.sort_values(by='moved', ascending=False).reset_index(drop=True)

        save_df(df_host_dst.reset_index(drop=True), df_instance, dir)

    except Exception as e:
        print(e)


if __name__ == '__main__':
    data_init('../data_set_1/host_data_1_1.csv',
              '../data_set_1/instance_data_1_1.csv',
              '../data_set_1/')
    # data_init('data_set_2/host_data_2.csv',
    #           'data_set_2/instance_data_2.csv',
    #           'data_set_2/')
