import pandas as pd
import numpy as np
import datetime

schedule_rule = pd.read_csv('./.csv')
time_cnt = 0
time_step = 0
location = 0

col1 = ['ID', 'SOURCE_HOST_ID', 'TARGET_HOST_ID', 'est_duration']
col2 = [str(i) for i in range(0, 10000)]
col = col1 + col2

init_data = np.zeros(shape=(len(schedule_rule), len(col)))

init_df = pd.DataFrame(data=init_data, columns=col)

try:
    for index, row in schedule_rule.iterrows():
        print('当前行数{}, 总共{}'.format(index, len(schedule_rule)))
        if index == 0:
            duration = row['est_duration']
            target_host = row['TARGET_HOST_ID']
            ID = row['ID']
            init_df.loc[index, 'ID'] = row['ID']
            init_df.loc[index, 'SOURCE_HOST_ID'] = row['SOURCE_HOST_ID']
            init_df.loc[index, 'TARGET_HOST_ID'] = row['TARGET_HOST_ID']
            init_df.loc[index, 'est_duration'] = row['est_duration']
            for i in range(int(duration)):
                init_df.loc[index, str(i)] = 1

        else:
            duration = row['est_duration']
            init_df.loc[index, 'ID'] = row['ID']
            init_df.loc[index, 'SOURCE_HOST_ID'] = row['SOURCE_HOST_ID']
            init_df.loc[index, 'TARGET_HOST_ID'] = row['TARGET_HOST_ID']
            init_df.loc[index, 'est_duration'] = row['est_duration']
            while True:
                for j in range(int(duration)):

                    a = len(init_df[(init_df['SOURCE_HOST_ID'] == row['SOURCE_HOST_ID']) & (init_df[str(time_cnt + time_step)] == 1)]) + \
                        len(init_df[(init_df['TARGET_HOST_ID'] == row['SOURCE_HOST_ID']) & (init_df[str(time_cnt + time_step)] == 1)])

                    b = len(init_df[(init_df['SOURCE_HOST_ID'] == row['TARGET_HOST_ID']) & (init_df[str(time_cnt + time_step)] == 1)]) + \
                        len(init_df[(init_df['TARGET_HOST_ID'] == row['TARGET_HOST_ID']) & (init_df[str(time_cnt + time_step)] == 1)])
                    if a >= 2 or b >= 2:
                        if time_cnt > 0:
                            time_cnt = 0
                        time_step += 1
                        break
                    else:
                        time_cnt += 1
                        continue
                if duration == time_cnt:
                    location = time_step
                    time_step = 0
                if time_step > 0:
                    continue
                else:
                    break

            time_cnt = 0
            for k in range(int(duration)):
                init_df.loc[index, str(k + location)] = 1

except Exception as e:
    print(e)

init_df = init_df.ix[:, ~(init_df == 0).all()]
init_df = init_df.ix[~(init_df == 0).all(axis=1), :]
init_df['START_AT'] = 0
init_df['END_AT'] = 0
result_columns = init_df.shape[1] - 6
for index, row in init_df.iterrows():
    temp_list = []
    for time_unit in range(result_columns):
        if row[str(time_unit)] == 0:
            continue
        else:
            temp_list.append(time_unit)
    init_df.loc[index, 'START_AT'] = (datetime.datetime.strptime('00:00', "%M:%S") +
                                      datetime.timedelta(seconds=min(temp_list))).strftime("%M:%S")
    init_df.loc[index, 'END_AT'] = (datetime.datetime.strptime('00:00', "%M:%S") +
                                    datetime.timedelta(seconds=max(temp_list) + 1)).strftime("%M:%S")

result_df = init_df[['ID', 'SOURCE_HOST_ID', 'TARGET_HOST_ID', 'est_duration', 'START_AT', 'END_AT']]

result_df.to_csv('time_schedu_0.csv', index=False)

print('---' * 20)
