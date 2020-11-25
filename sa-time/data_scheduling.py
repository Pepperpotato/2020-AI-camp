import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import time


def updata_df_host(df_host, df_instance, host_id_src, host_id_dst, rand_row):
    df_host.loc[host_id_dst, 'cpu_vcore_used'] += df_instance.loc[rand_row, 'vcpus']
    df_host.loc[host_id_dst, 'memory_v_used'] += df_instance.loc[rand_row, 'memory']
    df_host.loc[host_id_dst, 'memory_used'] += df_instance.loc[rand_row, 'memory_real']

    df_host.loc[host_id_src, 'cpu_vcore_used'] -= df_instance.loc[rand_row, 'vcpus']
    df_host.loc[host_id_src, 'memory_v_used'] -= df_instance.loc[rand_row, 'memory']
    df_host.loc[host_id_src, 'memory_used'] -= df_instance.loc[rand_row, 'memory_real']


def data_scheduling(dir):
    now = time.strftime("%Y-%m-%d-%H_%M_%S", time.localtime(time.time()))
    df_host = pd.read_csv(dir + 'df_host.csv')
    df_instance = pd.read_csv(dir + 'df_instance.csv')
    df_init = pd.read_csv(dir + 'sa_init.csv')
    solution_init = np.array(df_init)

    row_num = solution_init.shape[0]
    # col_num = solution_init.shape[1]
    col_num = len(df_instance[df_instance['moved'] == 1])

    solution_new = solution_init.copy()
    solution_current = solution_init.copy()
    solution_best = solution_init.copy()

    # value_current = np.std(solution_current.sum(axis=0))
    value_current = np.std(np.array(df_host['memory']) - solution_current.sum(axis=0))
    value_best = value_current

    # 冷却表参数
    alpha = 0.5
    markovlen = 1
    t = 1
    result = []  # 记录迭代过程中的最优解
    print('开始执行')

    # 模拟退火
    while t > 0.01:
        print(t)
        for i in np.arange(markovlen):
            rand_row = np.int(np.ceil(np.random.rand() * (row_num - 1)))
            rand_col = np.int(np.ceil(np.random.rand() * (col_num - 1)))

            host_id_src = np.nonzero(solution_new[rand_row])
            host_id_src = host_id_src[0]

            solution_new[rand_row] = np.roll(solution_new[rand_row], rand_col)

            host_id_dst = np.nonzero(solution_new[rand_row])
            host_id_dst = host_id_dst[0]

            if int(df_host.loc[host_id_dst, 'cpu_vcore']) >= int(df_host.loc[host_id_dst, 'cpu_vcore_used']) + int(
                    df_instance.loc[rand_row, 'vcpus']) and \
                    int(df_host.loc[host_id_dst, 'memory_v']) >= int(df_host.loc[host_id_dst, 'memory_v_used']) + int(
                df_instance.loc[rand_row, 'memory']) and \
                    int(df_host.loc[host_id_dst, 'memory']) + 64 > int(df_host.loc[host_id_dst, 'memory_used']) + int(
                df_instance.loc[rand_row, 'memory_real']):
                value_new = np.std(np.array(df_host['memory']) - solution_new.sum(axis=0))
                # value_new = np.std(solution_new.sum(axis=0))
                if value_new < value_current:  # 接受该解
                    value_current = value_new
                    solution_current = solution_new.copy()
                    updata_df_host(df_host, df_instance, host_id_src, host_id_dst, rand_row)

                    if value_new < value_best:
                        value_best = value_new
                        solution_best = solution_new.copy()
                else:  # 按一定的概率接受该解
                    if np.random.rand() < np.exp(-(value_new - value_current) / t):
                        value_current = value_new
                        solution_current = solution_new.copy()
                        updata_df_host(df_host, df_instance, host_id_src, host_id_dst, rand_row)
                    else:
                        solution_new = solution_current.copy()
            else:
                solution_new = solution_current.copy()

        t = alpha * t
        result.append(value_best)

    df = pd.DataFrame(solution_best)
    df.to_csv(dir + "sa_best" + now + ".csv", index=False)
    print('执行完成并保存结果到 sa_best.csv 文件中')
    print('score std: ' + str(value_best))

    # 用来显示结果
    plt.plot(np.array(result))
    plt.ylabel("best value")
    plt.xlabel("t")
    plt.show()

    # 优化前
    solution_init = solution_init.sum(axis=0)
    plt.plot(solution_init)
    plt.xlabel("before")
    plt.show()

    # 优化后
    solution_best = solution_best.sum(axis=0)
    plt.plot(solution_best)
    plt.xlabel("after")
    plt.show()


if __name__ == '__main__':
    data_scheduling('../data_set_1/')
    # data_scheduling('data_set_2/')
