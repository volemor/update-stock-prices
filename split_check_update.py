from datetime import datetime
from datetime import timedelta
import os
from tqdm import tqdm
from sqlalchemy import create_engine
import pandas as pd
import investpy
import time
from pandas_datareader import data as pdr
from update_sql import pd_df_to_sql, save_exeption_log
from pr_config import *

""" запускается проектамма в 18-10 по понедельникам - должна работу закончить до 23 часов"""

if os.name == 'nt':  # проверяем из под чего загрузка.
    linux_path = path_win
    print("start from WINDOWS")
else:
    linux_path = path_linux
    print("start from LINUX")

db_connection = create_engine(sql_login)
mysleep = 0.0001

'''
общий план:
программа проверки проведения сплитов - считываем список акций из базы hist_data , 
за 2 месяца назад ( рабочий день) из базы данных SQL и 
потом проходимся по списку акций из базы биржевых стоимостей за этот день.
если стоимость различается - то сносим в SQL базе все данные по тикеру и загружаем заново в SQL базу.

План поконкретнее:
1. расчитываем рабочий день 360 дней назад - наверное хватит времени, чтоб обновить в базе данных инфу о сплите
1.1 запуск выполняем раз в неделю - наверное в понедельник..
		(1.2 получаем из базы sql -- max_date и  min_date для каждого тикера st_id.) -- зачем??
2. из базы данных hist_data - считываем за этот день - DataFrame_sql_df - high, low, open, close, volume , date, st_id , currency, market.
3. проходим по тикерам st_id из DataFrame_sql_df - и считываем из investpy данные ( high, low, open, close, volume) по этой дате.
3.1   если hist.close / inv.close != 1 and hist.open / inv.open !=1 , 
	3.1.1 ИЛИ заряжаем запрос в investpy с датой начала min_date  и датой окончания max_date.  
	3.1.1.1 перезаписываем (стираем) данные в hist_data в столбцах high, low, open, close, volume.
	3.1.2 создаем список спитных тикеров, а потом проходимся по ним и стираем  позже начальной даты, в начальной дате ставим нули во все значения 
	ИЛИ стираем просто данные базе и потом при обновлении скрипт просто допишет "пропавшие" данные  
3.2 пишем логи о стирании - и записываем в файл split.log - True - если необходимо в ru-spb-update_sql.py увеличить диапазон проверки дельты дат
вроде все.. 



3.2  подключаемся к базе данных base_status - считываем ((max_date, min_date, branch))
проходим по DataFrame - обращаемся в investpy - загружаем данные и сравниваем с DataFrame
если различаются - делаем загрузку from max_date to min_date.  
и записываем в sql базу данных - следующие столбцы - low, high, open, Close, Volume .

вопросы:
1. необходимо найти рабочий день 2 месяца назад и потом уже из него произвеси выборку данных из базы.
2.

'''


def history_date_base_update():
    """ считаваем максимальные значения дат для каждого тикера из базы данных ,и потом записываем в отдельную таблицу для быстрого доступа"""
    global db_connection
    df_last_update = pd.read_sql(
        'Select st_id, max(date) as date_max, Currency, min(date) as date_min , market from hist_data group by st_id',
        con=db_connection)  # загрузили список тикеров из базы с последней датой
    df_last_update.to_sql(name='base_status', con=db_connection, if_exists='replace')
    print('history_date_base_update complite')


def check_for_time():
    ''' делаем проевку на время - в 23 часа надо прекратить и отключить обновление,
    так как запустили в 18 часов, в 2 ночи следующий запуск обновления исторических значений'''
    if datetime.today().time().hour == 23:
        save_log(f"Time to go to sleep ))), now exit", linux_path)
        exit()


def sql_base_clear_for_split_list(list_for_replase_data):
    global db_connection
    save_log(f"now try to delete [{list_for_replase_data}]", linux_path)
    try:
        if len(list_for_replase_data) > 0:
            base_status_df = pd.read_sql('Select * from base_status ;', con=db_connection)
            list_for_replase_df = base_status_df.loc[base_status_df['st_id'].isin(list_for_replase_data)][
                ['st_id', 'date_min']]
            list_for_replase_df.reset_index(inplace=True)
            save_log(f"now try to delete [{list_for_replase_data}]", linux_path)
            print(list_for_replase_df[['st_id', 'date_min']])
            remove_list, list_sero_set = [], []
            for index in range(len(list_for_replase_df)):  ### Delete sql list split data
                try:
                    print((db_connection.execute(
                        f"delete from hist_data where st_id ='{list_for_replase_df.iat[index, 1]}' and date > '{(list_for_replase_df.iat[index, 2]).strftime('%Y-%m-%d')}' ")).fetchall())
                    remove_list.append(list_for_replase_df.iat[index, 1])
                except Exception as _ex:
                    save_exeption_log(linux_path, 'split_check: del_row', _ex)
                    save_log(f"delete error [{list_for_replase_df.iat[index, 1]}]", linux_path)
                    print(list_for_replase_df.iat[index, 1], 'delete error')
                    continue
            save_log(f"remove DATA from SQL base [{remove_list}]", linux_path)
            for index in range(len(list_for_replase_df)):  ### set zero to first row sql data
                try:
                    print((db_connection.execute(
                        ## добавили дату конкретную.. посмотрим как будет работать
                        f"update hist_data set date = '2019-08-01', Open = '0', High = '0.0001', Low ='0.00001', Close = '0', volume ='0' where st_id ='{list_for_replase_df.iat[index, 1]}' ")).fetchall())
                    list_sero_set.append(list_for_replase_df.iat[index, 1])
                except Exception as _ex:
                    save_exeption_log(linux_path, 'split_check: set_zero', _ex)
                    save_log(f"set ZERO to first row error[{list_for_replase_df.iat[index, 1]}]", linux_path)
                    print(list_for_replase_df.iat[index, 1], 'set ZERO to first row error')
                    continue
            save_log(f"SET ZERO DATA to first row of SQL base [{list_sero_set}]", linux_path)
        else:
            save_log(f"SPLIT list is empty{list_for_replase_data}", linux_path)
    except Exception as _ex:
        save_log(f"some error[{_ex}]", linux_path)


# insert into hist_data  (index,Date , Open, High,  Low, Close, Volume, st_id , Currency, market) values (0, '2019-08-01', 0,0,0,0,0, 'REDFY', 'USD', 'US';
# update hist_data set date = '2019-08-01' Open = '0', High = '0.0001', Low ='0.00001', Close = '0', volume ='0' where st_id = 'CRMBQ'
# set Date = '2019-08-01' Open = '0', High = '0.0001', Low ='0.00001', Close = '0', volume ='0' where st_id = 'REDFY';

# TODO: наверное надо включать в лог запись - что за модуль сделал запись - а то update or split check??
def save_log(message, linux_path=''):  # сохраняет в лог файл сообщение..
    with open(linux_path + 'update.log', mode='a') as f:
        lines = '[' + str(datetime.today()) + '] ' + str(message)
        f.writelines(lines + '\n')


def split_check():
    global db_connection
    # поиск рабочей даты
    save_log('split check start--------------------', linux_path)
    market_name = ['United States', 'United States', 'russia']
    us_stock = investpy.get_stocks(country=market_name[0])['symbol']
    find_name = 'AAPL'
    today = datetime.today().date()
    date_shift = (today - timedelta(days=360)).strftime("%Y-%m-%d")  # задаем дату сканирования
    my_sql_control_date = pd.read_sql(
        f"Select date from hist_data where st_id = '{find_name}' and date > '{date_shift}' ",
        con=db_connection)
    date_control = my_sql_control_date.loc[5]['date'].date()
    print(today, '\n', f'date_control--[{date_control}]')
    my_sql_df_control_date = pd.read_sql(
        f"Select st_id, date, open, high, low,  close, volume, currency, market from hist_data where date = '{date_control}' ",
        con=db_connection)
    date_for_investpy_from, date_for_investpy_to = date_control.strftime("%d/%m/%Y"), (
            timedelta(days=1) + date_control).strftime("%d/%m/%Y")
    date_for_yahho_from, date_for_yahho_to = date_control.strftime('%Y-%m-%d'), (
            timedelta(days=1) + date_control).strftime('%Y-%m-%d')
    save_log(f"Start SPLIT control for date [{date_control}]", linux_path)
    split_list = []
    for index in range(len(my_sql_df_control_date)):
        check_for_time()
        if my_sql_df_control_date.loc[index]['market'] == "SPB":
            if us_stock.isin([my_sql_df_control_date.loc[index]['st_id']]).any():
                try:
                    df_invest_py = investpy.get_stock_historical_data(stock=my_sql_df_control_date.loc[index]['st_id'], \
                                                                      country=market_name[0],
                                                                      from_date=date_for_investpy_from,
                                                                      to_date=date_for_investpy_to)
                    time.sleep(1)
                except Exception as _ex:
                    save_exeption_log(linux_path=linux_path, modul='split_check: find_slit: investpy', message=_ex)
                    print(f"US invest_py error [{my_sql_df_control_date.loc[index]['st_id']}]")
                    continue
                if (df_invest_py[df_invest_py.index == pd.to_datetime(date_control)].iat[0, 0] /
                    my_sql_df_control_date.loc[index]['open']).round(1) != 1:
                    message = f"Split found for {my_sql_df_control_date.loc[index]['st_id']},US investpy {df_invest_py[df_invest_py.index == pd.to_datetime(date_control)].iat[0, 0].round(1)} != {my_sql_df_control_date.loc[index]['open'].round(1)} my_sql "
                    print(message)
                    split_list.append(my_sql_df_control_date.loc[index]['st_id'])
                    save_log(message=message, linux_path=linux_path)
                else:
                    print(
                        f"invest[{df_invest_py[df_invest_py.index == pd.to_datetime(date_control)].iat[0, 0]}],SQL[{my_sql_df_control_date.loc[index]['open'].round(2)}] [{my_sql_df_control_date.loc[index]['st_id']}]")
            else:
                try:
                    df_yahho = ((
                        pdr.get_data_yahoo(my_sql_df_control_date.loc[index]['st_id'], start=date_for_yahho_from,
                                           end=date_for_yahho_to))[
                        ['Open', 'High', 'Low', 'Close', 'Volume']]).round(2)
                    time.sleep(1)
                except Exception as _ex:
                    save_exeption_log(linux_path=linux_path, modul='split_check: find_slit: Yahho', message=_ex)
                    print("US yahho_ error")
                    continue
                if (df_yahho[df_yahho.index == pd.to_datetime(date_control)].at[pd.to_datetime(date_control), 'Open'] /
                    my_sql_df_control_date.loc[index]['open']).round(1) != 1:
                    message = f"Split found for {my_sql_df_control_date.loc[index]['st_id']},US yahho {df_yahho[df_yahho.index == pd.to_datetime(date_control)].at[pd.to_datetime(date_control), 'Open'].round(1)} != {my_sql_df_control_date.loc[index]['open'].round(1)} my_sql "
                    print(message)
                    split_list.append(my_sql_df_control_date.loc[index]['st_id'])
                    save_log(message=message, linux_path=linux_path)
                else:
                    print(
                        f"YAHHO [{df_yahho[df_yahho.index == pd.to_datetime(date_control)].at[pd.to_datetime(date_control), 'Open']}],SQL[{my_sql_df_control_date.loc[index]['open'].round(2)}] [{my_sql_df_control_date.loc[index]['st_id']}]")
        # elif my_sql_df_control_date.loc[index]['market'] == "RU":
    #     try:
    #         df_invest_py = investpy.get_stock_historical_data(stock=my_sql_df_control_date.loc[index]['st_id'],
    #                                                             country=market_name[2],
    #                                                             from_date=date_for_investpy ,
    #                                                         to_date=date_for_investpy )
    #         time.sleep(1)
    #         if df_invest_py.loc[0]['open'].round(2) != my_sql_df_control_date.loc[index]['open'].round(2) :
    #             message = f"Split found for {my_sql_df_control_date.loc[index]['st_id']}, RU investpy {df_invest_py.loc[0]['open'].round(2)} != {my_sql_df_control_date.loc[index]['open'].round(2)} my_sql "
    #             print (message)
    #         # save_log (message)
    #     except:
    #         print("RU invest_py error")
    #         continue

    save_log(f"split list - {split_list}", linux_path)
    save_log(f"Complite SPLIT control for date [{date_control}]", linux_path)
    # if len(split_list) > 0:
    #     f = open(linux_path + 'split.log', mode='x')
    #     lines = str(True)
    #     f.writelines(lines + '\n')
    #     f.close()
    return split_list


def insert_history_date_into_sql():
    """
    загружаем новые тикеры в sql базу на основе investpy.get_stocks.
    вставляем под market US

    ?? а какую дату загрузки делать??? - начальную можно сделать 1/1/2019, а конец - дату сегодняшнюю???
    :return:
    """
    time_count = []
    global mysleep
    save_log(message=f'--try add from investpy new stock to history_date--',
             linux_path=linux_path)
    def sleep_timer_regulator():
        '''пробуем регулировать паузу между обращениями за данными налету'''
        global mysleep
        if len(time_count) > 2:
            delta_timer_local = time_count[-1] - time_count[-2]
            if delta_timer_local < 1:
                mysleep = 1
            if delta_timer_local > 2:
                mysleep = 0.001

    market_name = ['United States', 'United States', 'russia']
    stocks_us_investpy = investpy.get_stocks(country=market_name[0])['symbol']
    df_last_update = pd.read_sql('Select * from base_status ;', con=db_connection)
    
    us_df_last = df_last_update[df_last_update['market'] == 'US']['st_id']
    spb_df_last = df_last_update[df_last_update['market'] == 'SPB']['st_id']
    set_spb, set_us, set_real_us = set(), set(), set()
    
    set_spb.update(spb_df_last)
    set_us.update(us_df_last)
    set_real_us.update(stocks_us_investpy)
    
    list_stock_from_real_us_to_us =[*set_real_us.difference(set_us.union(set_spb))] 
    print(f'is in real US and need to US:', list_stock_from_real_us_to_us)
    

    # save_log(message='insert new tiker from investpy.get_stocks to SQL history_date', linux_path=linux_path)
    # df = df_last_update.to_numpy().tolist()
    # my_2_list = stocks_us_investpy.to_numpy().tolist()
    # my_only_US_df_list = []
    # for index_us in tqdm(my_2_list):
    #     # print(index_us)
    #     if index_us in df:
    #         # print(f'is in -={index_us}')
    #         continue
    #     else:
    #         my_only_US_df_list.append(index_us)
    #         # print(f"{index_us} - add" )

    # print(f"my list len = {len(my_only_US_df_list)}")
    # print(f'all US list len = {len(my_2_list)}')
    # my_only_US_df_list.sort()
    
    save_log(message=f'find {len(list_stock_from_real_us_to_us)}, try add {list_stock_from_real_us_to_us}', linux_path=linux_path)
    from_date_m, to_date_m = '1/08/2018', datetime.today().strftime("%d/%m/%Y")
    successfully_list = []
    for only_us_index in tqdm(list_stock_from_real_us_to_us):
        check_for_time()
        try:
            time.sleep(mysleep)
            df_update = investpy.get_stock_historical_data(stock=only_us_index,
                                                           country=market_name[0],
                                                           from_date=from_date_m,
                                                           to_date=to_date_m)
            time_count.append(time.time())
            df_update[['st_id', 'Currency', 'market']] = only_us_index, "USD", 'US'
            successfully_list.append(only_us_index)
        except Exception as _ex:
            print(f'Error [{only_us_index}] loading')
            save_exeption_log(linux_path=linux_path, modul='insert US: to SQL: investpy', message=_ex)
            continue
        # TODO: обязательно допилить перенос этой функции с переменными
        pd_df_to_sql(df_update)
        sleep_timer_regulator()
    save_log(
        message=f'LEn of successfully list is [{len(successfully_list)}], apply [{round(100 * len(successfully_list) / len(list_stock_from_real_us_to_us), 0)}]%,  added list-- {successfully_list}',
        linux_path=linux_path)


save_log('------[]--------split check start---------[]-----', linux_path= linux_path)
split_list_return = split_check()
# split_list_return = ['ADS', 'T']

sql_base_clear_for_split_list(split_list_return)

"""  требуется ручное тестирование"""
insert_history_date_into_sql()

save_log( '-------][--------split check complite---------][---', linux_path=linux_path)
history_date_base_update()  # по итогам проверки обновляем статус базы
