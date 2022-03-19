import pandas as pd
import numpy as np
from datetime import datetime
import time
import os
from datetime import date
from datetime import timedelta
from tqdm import tqdm
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
'''
делаем расчеты параметров  роста и падения акций (длительности, величины..).
осталось прикрутить:
    просчет всех тикеров,
    запись в общую базу sql,
    запись в эксель файл,
    может в джанго зальем??
     
    
'''



def extremum_invest_finder (df_index, max_ind, min_ind):

    # теперь делаем длительность роста и падений
    index_loop = 0 # индекс в датафрейме
    last_extrem = df_index.iloc[0]['high'] # начальное значение максимума-минимума
    extrem_index = 0 # начальное значение индекса экстемума
    direct_index = 1 # начальное значение для определения роста или падения (1) -рост (-1) - падение, начинаем типа с роста
    col_list = ['date', 'f_date', 'date_delta', 'delta', 'extrem']
    df_delta = pd.DataFrame(columns=col_list)
    # mean_df_index = (df_index[['high','low']].mean()).mean()
    # print ('средняя стоимость от всего срока расчета',round (mean_df_index, 1))
    df_delta.loc[0] = [df_index.iloc[0]['date'].date(), last_extrem, 0 ,0, 0 ] # первая строчка
    df_index['marker'] = 0 # задаем значение столбцу с интервалами - роста и падений - для отображения потом на графике
    # print('длительность наблюдений дней',len(df_index['date']))
    df_index.loc[  df_index['date'] > df_index.iloc[1]['date']  ,['marker']] = 1
    st_id = df_index.iloc[0] ['st_id']
    maximum_loop =0
    while index_loop < len(df_index['date'])-1:
        index_loop += 1
        maximum_loop +=1
        if maximum_loop >  3*len(df_index['high']):
            print(st_id,' max _loop')
            break
        # обрабатываем максимумы при росте
        if direct_index == 1 and df_index.iloc[index_loop]['high'] > last_extrem:
            extrem_index = index_loop
            last_extrem = df_index.iloc[index_loop]['high']
        if direct_index == 1 and df_index.iloc[index_loop]['high'] < last_extrem * max_ind:
            direct_index = -direct_index
            index_loop = extrem_index
            df_index.loc[df_index['date'] > df_index.iloc[extrem_index]['date'], ['marker']] = 1
            my_frame = [df_index.iloc[extrem_index]['date'].date(),
                df_index.iloc[extrem_index]['high'],
                ((df_index.iloc[extrem_index]['date']).date() - (df_delta.iloc[-1]['date'])).days,
                round( 100 * (df_index.iloc[extrem_index]['high'] - df_delta.iloc[-1]['f_date'])/ df_index.iloc[extrem_index]['high'],1)
                ,extrem_index
                       ]
            # print( f" {df_index.iloc[extrem_index]['date']} date {df_index.iloc[extrem_index]['high']} - {df_delta.iloc[-1]['f_date']}  drob  {df_index.iloc[extrem_index]['high']}")

            temp_df = pd.DataFrame( [my_frame], columns= col_list )
            df_delta = df_delta.append(temp_df)
            # print('delta_max',df_delta)
            # print('temp_max',temp_df)
        # обрабатываем минимумы при падении
        if direct_index == -1 and df_index.iloc[index_loop]['low'] < last_extrem:
            extrem_index = index_loop
            last_extrem = df_index.iloc[index_loop]['low']
        if direct_index == -1 and df_index.iloc[index_loop]['low'] > last_extrem * min_ind:
            direct_index = -direct_index
            index_loop = extrem_index
            df_index.loc[df_index['date'] > df_index.iloc[extrem_index]['date'], ['marker']] = -1

            my_frame = [df_index.iloc[extrem_index]['date'].date(),
                df_index.iloc[extrem_index]['low'],
                ((df_index.iloc[extrem_index]['date']).date() - (df_delta.iloc[-1]['date'])).days,
                        round(100*(df_index.iloc[extrem_index]['low'] - df_delta.iloc[-1]['f_date'])/df_index.iloc[extrem_index]['low'],1)
            ,extrem_index  ]
            # print(f"{df_index.iloc[extrem_index]['date']} date   {df_index.iloc[extrem_index]['low']} - {df_delta.iloc[-1]['f_date']}  drob  {df_index.iloc[extrem_index]['low']}")
            temp_df = pd.DataFrame([my_frame], columns= col_list )
            df_delta = df_delta.append(temp_df)
            # print('delta_min',df_delta)
            # print('temp_min',temp_df)
    if direct_index == 1:
        df_index.loc[df_index['date'] > df_index.iloc[extrem_index]['date'], ['marker']] = 1
        extrem_index = index_loop
        my_frame = [df_index.iloc[extrem_index]['date'].date(),
                    df_index.iloc[extrem_index]['high'],
                    ((df_index.iloc[extrem_index]['date']).date() - (df_delta.iloc[-1]['date'])).days,
                    round(100*(df_index.iloc[extrem_index]['high'] - df_delta.iloc[-1]['f_date'])/df_index.iloc[extrem_index]['high'],1)
                    ,extrem_index ]
        temp_df = pd.DataFrame([my_frame], columns=col_list)
    if direct_index == -1:
        df_index.loc[df_index['date'] > df_index.iloc[extrem_index]['date'], ['marker']] = -1
        extrem_index = index_loop
        my_frame = [df_index.iloc[extrem_index]['date'].date(),
                    df_index.iloc[extrem_index]['low'],
                    ((df_index.iloc[extrem_index]['date']).date() - (df_delta.iloc[-1]['date'])).days,
                    round(100 * (df_index.iloc[extrem_index]['low'] - df_delta.iloc[-1]['f_date']) / df_index.iloc[extrem_index]['low'], 1)
                    ,extrem_index ]
        temp_df = pd.DataFrame([my_frame], columns=col_list)
        df_delta = df_delta.append(temp_df)
    df_delta["st_id"] = st_id
    # print('длина массива',len(df_delta))
    # print (df_delta[df_delta['date_delta']>5][['date','f_date','date_delta','delta']] )
    # print (df_delta[df_delta['delta']<0][['date','f_date','delta']] )
    # print ('рост',df_delta[df_delta['delta']>0][['date','f_date','delta']] )
    # print(f'сумма всех ростов и падений для {st_id}', round(df_delta['delta'].sum(), 1))
    # print (f"дней роста {df_delta[df_delta['delta']>0]['date_delta'].sum()}, число фаз роста [{len(df_delta[df_delta['delta']>0]['date_delta']) }], средний процент роста {df_delta[df_delta['delta']>0]['delta'].mean().round(1)}, медиана {df_delta[df_delta['delta']>0]['delta'].median().round(1)}, максимум {df_delta[df_delta['delta']>0]['delta'].max()}, среднее дней роста {df_delta[df_delta['delta']>0]['date_delta'].mean().round(0)} ")
    # print (f"дней падения {df_delta[df_delta['delta']<0]['date_delta'].sum()}, число фаз падения [{len(df_delta[df_delta['delta']>0]['date_delta'])}], cредний процент падения {df_delta[df_delta['delta']<0]['delta'].mean().round(1)}, медиана {df_delta[df_delta['delta']<0]['delta'].median().round(1)}, максимум {df_delta[df_delta['delta']<0]['delta'].min()}, среднее дней падения {df_delta[df_delta['delta']<0]['date_delta'].mean().round()}")
    # print(df_index[['date', ''pp[marker']])
    list_df_extrem = ['st-id','дней роста', 'число фаз роста','средний процент роста', 'медиана процент роста' , 'максимум процент роста', 'среднее дней роста' , 'дней падения', 'число фаз падения','средний процент падения', 'медиана процент падения' , 'максимум процент падения', 'среднее дней падения']

    df_extrem_calc = [ st_id,df_delta[df_delta['delta']>0]['date_delta'].sum(),
                       len(df_delta[df_delta['delta']>0]['date_delta'])  ,
                       round( df_delta[df_delta['delta']>0]['delta'].mean(),1 ),
                       round(df_delta[df_delta['delta']>0]['delta'].median(), 1),
                        df_delta[df_delta['delta']>0]['delta'].max(),
                       round(df_delta[df_delta['delta']>0]['date_delta'].mean(),0),
                        df_delta[df_delta['delta'] < 0]['date_delta'].sum(),
                        len(df_delta[df_delta['delta'] < 0]['date_delta']),
                        round(df_delta[df_delta['delta'] < 0]['delta'].mean(), 1),
                        round(df_delta[df_delta['delta'] < 0]['delta'].median(), 1),
                        df_delta[df_delta['delta'] < 0]['delta'].max(),
                        round(df_delta[df_delta['delta'] < 0]['date_delta'].mean(), 0)]
    my_extrem_series = pd.DataFrame( [df_extrem_calc],  columns=list(list_df_extrem))

    return  my_extrem_series #, df_delta



if os.name == 'nt':
    linux_path = ''
    history_path = 'D:\\YandexDisk\\корень\\отчеты\\'
else:
    linux_path = '/opt/1/My_Python/st_US/'
    history_path = '/opt/1/My_Python/st_US/SAVE'
# end constant list

list_df_extrem = ['st-id','name','дней роста', 'число фаз роста','средний процент роста', 'медиана процент роста' , 'максимум процент роста', 'среднее дней роста' , 'дней падения', 'число фаз падения','средний процент падения', 'медиана процент падения' , 'максимум процент падения', 'среднее дней падения', 'date_start', 'date_close']
big_df_extrem = pd.DataFrame(columns=list_df_extrem)
my_branch_name_df = pd.read_excel(linux_path + 'tiker-branch.xlsx', index_col=0)
db_connection_str = 'mysql+pymysql://python:python@192.168.0.118/hist_data'
st_id, date_start, market = 'PBF', "2021-05-01", "SPB"
db_connection = create_engine(db_connection_str)  # connect to database
df_index_spb = pd.read_sql(f'Select date, st_id, high, low from hist_data where  date > "{date_start}";',con=db_connection)
print('sql load ok')
# market = "{market}" and
print(my_branch_name_df[my_branch_name_df['curency'] == 'USD'].head())
# print(len(my_branch_name_df))
# print(df_index_spb.head())
# print(df_index_spb.sort_values(by="st_id"))
# print(f'test for {st_id} ',extremum_invest_finder(df_index_spb[df_index_spb['st_id']== st_id].copy()))
max_ind, min_ind = 0.94, 1.06
for index_st_id in tqdm(my_branch_name_df['tiker']):
    try:
        my_df= extremum_invest_finder(df_index_spb[df_index_spb['st_id'] == index_st_id].copy(), max_ind=max_ind, min_ind=min_ind)
        my_df['name'] = my_branch_name_df[my_branch_name_df['tiker'] == index_st_id].iloc[0]['name']
        my_df[['date_start', "date_close"]] = df_index_spb[df_index_spb['st_id'] == index_st_id].iloc[0]['date'], df_index_spb[df_index_spb['st_id'] == index_st_id].iloc[-1]['date']
        big_df_extrem = big_df_extrem.append(my_df)
    except:
        print(f"error  {index_st_id}")
        continue
    # if index_st_id == 'ABT':
    #     print(f" name {index_st_id} exit")
    #     print('размер df',len(big_df_extrem))
    # print(big_df_extrem)



name_for_save = str(linux_path) + ' extrem_list ' + str(datetime.today().date()) + f"[{min_ind}]-[{max_ind}].xlsx"
with pd.ExcelWriter(name_for_save) as writer:
    big_df_extrem.to_excel(writer, sheet_name='SPB')
print(f' excel file {name_for_save}  __ saved')
        # exit()
#     pass

