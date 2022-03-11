from datetime import datetime
from datetime import timedelta
import os
from tqdm import tqdm
from sqlalchemy import create_engine
import pandas as pd
import investpy
import time
from pandas_datareader import data as pdr

if os.name == 'nt':  # проверяем из под чего загрузка.
    linux_path = ''
    history_path = 'D:\\YandexDisk\\корень\\отчеты\\'
    print("start from WINDOWS")
else:
    linux_path = '/mnt/1T/opt/gig/My_Python/st_US/'
    history_path = '/mnt/1T/opt/gig/My_Python/st_US/SAVE'
    print("start from LINUX")

db_connection_str = 'mysql+pymysql://python:python@192.168.0.118/hist_data'
db_connection = create_engine(db_connection_str)

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
def check_for_time()
    if datetime.today().time().hour = 23:
        save_log(f"Time to goto sleep ))), now exit", linux_path)
        exit()

def sql_base_clear_for_split_list(list_for_replase_data):
    if len(list_for_replase_data)>0:
        base_status_df = pd.read_sql('Select * from base_status ;', con=db_connection)
        list_for_replase_df = base_status_df.loc[base_status_df['st_id'].isin(list_for_replase_data)][['st_id', 'date_min']]
        list_for_replase_df.reset_index(inplace=True)
        print(list_for_replase_df[['st_id', 'date_min']])
        remove_list, list_sero_set = [], []
        for index in range(len(list_for_replase_df)):  ### Delete sql list split data
            try:
                print((db_connection.execute(
                    f"delete from hist_data where st_id ='{list_for_replase_df.iat[index, 1]}' and date > '{(list_for_replase_df.iat[index, 2]).strftime('%Y-%m-%d')}' ")).fetchall())
                remove_list.append(list_for_replase_df.iat[index, 1])
            except:
                print(list_for_replase_df.iat[index, 1], 'delete error')
                continue
        save_log(f"remove DATA from SQL base [{remove_list}]", linux_path)
        for index in range(len(list_for_replase_df)):  ### set zero to first row sql data
            try:
                print((db_connection.execute(
                    f"update hist_data set Open = '0', High = '0', Low ='0', Close = '0', volume ='0' where st_id ='{list_for_replase_df.iat[index, 1]}' ")).fetchall())
                list_sero_set.append(list_for_replase_df.iat[index, 1])
            except:
                print(list_for_replase_df.iat[index, 1], 'set ZERO to first row error')
                continue
        save_log(f"SET ZERO DATA to first row of SQL base [{list_sero_set}]", linux_path)
    else:
        save_log(f"SPLIT list is empty", linux_path)


def save_log(message, linux_path=''):  # сохраняет в лог файл сообщение..
    f = open(linux_path + 'update.log', mode='a')
    lines = '[' + str(datetime.today()) + '] ' + str(message)
    f.writelines(lines + '\n')
    f.close()


def split_check():
    # поиск рабочей даты
    save_log('split check start',linux_path)
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
                except:
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
                except:
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
    if len(split_list) >0:
        f = open(linux_path + 'split.log', mode='x')
        lines = str(True)
        f.writelines(lines + '\n')
        f.close()
    return split_list

split_list_return = split_check()

sql_base_clear_for_split_list(split_list_return)
