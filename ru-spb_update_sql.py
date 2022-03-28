import investpy
import numpy as np
from datetime import datetime
import time
from datetime import timedelta
import pandas as pd
from tqdm import tqdm
from sqlalchemy import create_engine
import os
from pandas_datareader import data as pdr

''' 
надо бы сделать органичение по времени работы - типа в час ночи - сказать байбай.. иначе два запуска могут наложиться
и investpy заблокирует на 3 дня нас...
типа datetime.today().time().hour = 1 - то запись в лог и  exit() ---  
добавлена обработка ошибок - и выходим если нет смысла проверять все. 
добавлена отдельный лог для ошибок при вызове функций (update_extention.log).

 
'''

def teh_an(t_name, country_teh):  # модуль сбора данных теханализа
    my_date = ['daily', 'weekly', 'monthly']
    col_list_teh = ['date', 'st_id', 'teh_daily_sel', 'teh_daily_buy', 'teh_weekly_sel', 'teh_weekly_buy',
                    'teh_monthly_sell', 'teh_monthly_buy',
                    'daily_sma_signal_200', 'daily_ema_signal_200', 'weekly_sma_signal_200', 'weekly_ema_signal_200',
                    'monthly_sma_signal_200',
                    'monthly_ema_signal_200', 'EPS', 'P_E']
    teh_df = pd.DataFrame(columns=list(col_list_teh))
    st_inf = investpy.stocks.get_stock_information(stock=t_name, country=country_teh)
    teh_df.at[0, 'date'] = datetime.today().date()
    teh_df.at[0, 'st_id'] = t_name
    teh_df.at[0, 'EPS'] = st_inf["EPS"][0]
    teh_df.at[0, 'P_E'] = st_inf["P/E Ratio"][0]
    time.sleep(1.0)
    for f in my_date:
        sig_by = 0
        sig_sel = 0
        s_data = investpy.moving_averages(name=t_name, country=country_teh, product_type='stock', interval=f)
        time.sleep(1.0)
        df1 = investpy.technical.technical_indicators(name=t_name, country=country_teh, product_type='stock',
                                                      interval=f)
        time.sleep(0.7)
        for ind in df1['signal']:
            if ind == 'buy':
                sig_by += 1
            if ind == 'sell':
                sig_sel += 1
        if f == 'daily':
            teh_df.at[0, 'teh_daily_sel'] = sig_sel
            teh_df.at[0, 'teh_daily_buy'] = sig_by
            teh_df.at[0, 'daily_sma_signal_200'] = s_data.iloc[5]['sma_signal']
            teh_df.at[0, 'daily_ema_signal_200'] = s_data.iloc[5]['ema_signal']
        if f == 'weekly':
            teh_df.at[0, 'teh_weekly_sel'] = sig_sel
            teh_df.at[0, 'teh_weekly_buy'] = sig_by
            teh_df.at[0, 'weekly_sma_signal_200'] = s_data.iloc[5]['sma_signal']
            teh_df.at[0, 'weekly_ema_signal_200'] = s_data.iloc[5]['ema_signal']
        if f == 'monthly':
            teh_df.at[0, 'teh_monthly_sell'] = sig_sel
            teh_df.at[0, 'teh_monthly_buy'] = sig_by
            teh_df.at[0, 'monthly_sma_signal_200'] = s_data.iloc[5]['sma_signal']
            teh_df.at[0, 'monthly_ema_signal_200'] = s_data.iloc[5]['ema_signal']
    # print(teh_df)
    return teh_df


def save_log(linux_path, message):
    '''сохраняет в лог файл сообщение о работе программы.. '''
    f = open(linux_path + 'update.log', mode='a')
    lines = '[' + str(datetime.today()) + '] ' + str(message)
    f.writelines(lines + '\n')
    f.close()

def save_exeption_log (linux_path, modul, message):
    '''записываем в файл логи ошибок с указанием модуля из которого был вызов '''
    global stock_not_found_teh_an
    if 'not found' in message and modul == 'teh_an':
        stock_not_found_teh_an.append( message.split()[2].upper())
    if message !='0' or 'Date' not in message or 'signal' not in message:
        f = open(linux_path + 'update_extention.log', mode='a')
        lines = '[' + str(datetime.today()) + f']-[{modul}] ' + str(message)
        f.writelines(lines + '\n')
        f.close()
    else:
        pass



def stock_name_table(linux_path):
    ru_stos1 = investpy.stocks.get_stocks(country='russia')
    ru_stos = ru_stos1[['name', 'symbol']]
    mmm = np.zeros((len(ru_stos), 1)) + 3
    my_id = pd.DataFrame(data=mmm, columns=['markets_id'])
    ru_stos = ru_stos.join(my_id)
    # print(ru_stos)
    big_df = pd.DataFrame(columns=['name', 'symbol'])
    big_df = big_df.append(other=ru_stos)
    df_spb = pd.read_csv('ListingSecurityList.csv', delimiter=';',
                         encoding='cp1251')  # читаем список всех тикеров на СПБ
    stock_spb = df_spb[df_spb['s_currency_kotir'] == 'USD']  # вычисляем список эмитентов СПБ с курсом в USD
    us_st_spb = stock_spb[['name', 'symbol']]  # срезаем лишние столбцы из списка -- надо менять название столбца!!!!!
    mmm = np.zeros((len(df_spb), 1)) + 2
    my_id = pd.DataFrame(data=mmm, columns=['markets_id'])
    us_st_spb = us_st_spb.join(my_id)
    # print(us_st_spb)
    us_stos_usa1 = investpy.stocks.get_stocks(country='United States')
    us_stos_usa = us_stos_usa1[['name', 'symbol']]

    mmm = np.zeros((len(us_stos_usa), 1)) + 1
    my_id = pd.DataFrame(data=mmm, columns=['markets_id'])
    us_stos_usa = us_stos_usa.join(my_id)
    # print(us_stos_usa)
    big_df = big_df.append(other=us_st_spb, ignore_index=True)
    big_df = big_df.append(other=us_stos_usa, ignore_index=True)
    # print('all', big_df)

    with pd.ExcelWriter(linux_path + 'my_all_st.xlsx') as writer:
        big_df.to_excel(writer, sheet_name='all')  ### работает!!!
    # big_df.to_csv('my_test_all_st.csv', sep=';', encoding='cp1251', line_terminator='/n', index=True)


def my_start():  # исходные данные - константы
    col_list = ['tiker', 'name', 'today_close',
                'mean_min_dek1', 'mean_min_dek2', 'mean_min_dek3', 'mean_min_dek4',
                'mean_min_dek5', 'mean_min_dek6', 'mean_min_dek7', 'mean_min_dek8',
                'mean_min_pr_dek1',
                'mean_min_pr_dek2', 'mean_min_pr_dek3', 'mean_min_pr_dek4', 'mean_min_pr_dek5', 'mean_min_pr_dek6',
                'mean_min_pr_dek7', 'mean_min_pr_dek8',
                'min_pr_delta_1_2', 'min_pr_delta_2_3', 'min_pr_delta_3_4', 'min_pr_delta_4_5', 'min_pr_delta_5_6',
                'min_pr_delta_6_7', 'min_pr_delta_7_8',
                'max_dek1', 'max_dek2', 'max_dek3', 'max_dek4', 'max_dek5',
                'max_dek6', 'max_dek7', 'max_dek8',
                'max_pr_dek1', 'max_pr_dek2', 'max_pr_dek3', 'max_pr_dek4',
                'max_pr_dek5', 'max_pr_dek6', 'max_pr_dek7', 'max_pr_dek8',
                'max_pr_delta_1_2', 'max_pr_delta_2_3', 'max_pr_delta_3_4', 'max_pr_delta_4_5', 'max_pr_delta_5_6',
                'max_pr_delta_6_7', 'max_pr_delta_7_8',
                'min_y', 'max_y', 'today_y_pr_min',
                'today_y_pr_max', 'day_start', 'day_close']

    #                'teh_daily_sel', 'teh_daily_buy', 'teh_weekly_sel', 'teh_weekly_buy', 'teh_monthly_sell',
    #                'teh_monthly_buy',
    #                'daily_sma_signal 200', 'daily_ema_signal 200', 'weekly_sma_signal 200', 'weekly_ema_signal 200',
    #                'monthly_sma_signal 200', 'monthly_ema_signal 200'
    #                ]
    return col_list


def history_data(linux_path):  # сохраняем все  -- вроде работает
    from_date, to_date = '1/08/2019', '01/08/2021'
    stock_list_data = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'st_id', 'market']
    all_stock = pd.read_excel(linux_path + 'my_test_all_st.xlsx', sheet_name='all')  # , index_col=0)
    # test_stock = pd.DataFrame.reset_index(all_stock[all_stock['markets_id']==2], drop=True) # сбросили индексы
    test_stock = all_stock[all_stock['markets_id'] == 2]  ##загружаем SPB
    big_df_table = pd.DataFrame(columns=stock_list_data)
    # print(test_stock.head(4))
    print(big_df_table)
    set_1 = 3
    my_test_set = 20
    print(my_test_set)
    # print(test_stock.iloc[[set_1],[0]]) # индекс второй строки set_1=2
    for set_1 in tqdm(range(my_test_set)):
        try:
            df_1 = investpy.get_stock_historical_data(test_stock.symbol.iloc[set_1], country='United States',
                                                      from_date=from_date, to_date=to_date)
            print('name', test_stock.symbol.iloc[set_1])
            len_mmm = test_stock.values[[set_1], [0]]
            mmm = np.ones((len(df_1), 1))  # len_mmm
            st_id = pd.DataFrame(data=mmm, columns=['st_id'])
            st_id['st_id'] = test_stock.symbol.iloc[set_1]
            # print(st_id)
            # print('df1',df_1)
            df_1.reset_index(level=['Date'], inplace=True)  # замена индекса
            df_1.set_index(pd.Index(range(len(df_1))), inplace=True)
            # df_1 = df_1.join(pd.DataFrame(st_id, columns=['st_id']))
            # print('join', df_1)
            big_df_table = big_df_table.append(df_1, list(['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'st_id']))

        except:
            print(f'name {test_stock.symbol.iloc[set_1]} Error')
            continue
    print('Big', big_df_table)
    big_df_table.to_excel(linux_path + 'history_data.xlsx')

    # stock_hist_data -- id, date ,open ,high ,low , close, st_id ### состав таблицы
    # df_test= df_1[['Date','High', 'Low', 'Volume']]#,)
    # print('test', df_test)

    # 1)Записали все тикеры в один файл. с разбивкой на рынки - USA-1, SPB-2, RU-3.-- теперь это таблица №1 далее..
    # 2)загружаем наименование рынков - пусть будет таблица №2 3)запускаем цикл по таблице №2 и пробегаемся по всем
    # значениям таблицы №1 - и в итоге собираем исторические данные в один датафрейм - df_hist_all - - таблица 3 -
    # записываем в файл в нем формат столбцов - (data), open, (high), (low), close, (symbol), (markets_id) ---  в
    # скобочках - обязательные значения. 4) добавление данных, которых нет в таблице 3 - загружаем таблицу 3 -
    # смотрим на даты - выбираем самую большую (для каждого тикера) -  находим разницу с сегодняшней - если больше 7
    # дней - запускаем подгрузку с сайта и дописываем в конец. по итогам сортируем датафрейм по symbol - записываем в
    # файл. ps. -- необходиом в пункте 1 добавить возможность проверять есть ли такой тикер уже.. и если есть -- не
    # дописывать, а пропускать.. -- короче надо проверять актуальность а теперь пробуем все это на паре тикеров
    '''
    CREATE TABLE ten_an
(
    date datetime NOT NULL ,
    st_id VARCHAR (6) NOT NULL,
    teh_daily_sel int ,
    teh_daily_buy int,
    teh_weekly_sel int,
    teh_weekly_buy int,
    teh_monthly_sell int,
    teh_monthly_buy int,
    daily_sma_signal_200 VARCHAR (4), 
    daily_ema_signal_200 VARCHAR (4),
    weekly_sma_signal_200 VARCHAR (4),
    weekly_ema_signal_200 VARCHAR (4),
    monthly_sma_signal_200 VARCHAR (4),
    monthly_ema_signal_200 VARCHAR (4),
    EPS float,
    P_E float
    
) ;  
    '''



def history_updater(linux_path, db_connection_str):  # делаем обновление базы mysql
    cur_date = datetime.today()
    time_count = []
    global mysleep, stock_not_found_teh_an
    def sleep_timer_regulator():
        '''пробуем регулировать паузу между обращениями за данными налету'''
        global mysleep
        if len(time_count)>2:
            delta_timer_local = time_count[-1] - time_count[-2]
            if delta_timer_local < 1:
                mysleep = 1
            if delta_timer_local > 2:
                mysleep = 0.001

    teh_an_list = ['date', 'st_id', 'teh_daily_sel', 'teh_daily_buy', 'teh_weekly_sel', 'teh_weekly_buy',
                   'teh_monthly_sell', 'teh_monthly_buy',
                   'daily_sma_signal_200', 'daily_ema_signal_200', 'weekly_sma_signal_200', 'weekly_ema_signal_200',
                   'monthly_sma_signal_200',
                   'monthly_ema_signal_200', 'EPS', 'P_E']

    line_1 = [cur_date.strftime("%Y-%m-%d"), 'NO NAme', 0, 0, 0, 0, 0, 0, 'No DAta', 'No DAta', 'No DAta',
              'No DAta', 'No DAta', 'No DAta', 0, 0]
    teh_an_df_nodata = pd.DataFrame(columns=teh_an_list)
    teh_an_df_nodata.loc[0] = line_1
    # print('teh no data', teh_an_df_nodata)
    print('сегодня...', cur_date.strftime("%Y-%m-%d"))
    save_log(linux_path, '---------------start update--------------')
    market_name = ['United States', 'United States', 'russia']
    db_connection = create_engine(db_connection_str)  # connect to database

    df_last_update = pd.read_sql('Select st_id, max(date) as date_max, Currency, market from hist_data group by st_id',
                                 con=db_connection)  # загрузили список тикеров из базы с последней датой

    print("load from Mysql...OK")
    df_last_teh = pd.read_sql(
        'Select st_id, max(date) as date_max from teh_an group by st_id',
        con=db_connection)  # загружаем последние данные по теханализу
    max_date_df = pd.DataFrame.max(df_last_update.iloc[:, 1])
    print('Maximum date is--', max_date_df)
    print(f'длина массива {len(df_last_update)}')

    save_log(linux_path, 'DataFrame leght ' + str(len(df_last_update)))
    us_stock = investpy.get_stocks(country=market_name[0])['symbol']  # список тикеров в США

    for ind in tqdm(range(len(df_last_update))):
        deltadays = (cur_date - df_last_update.iloc[ind, 1]).days
        if deltadays <= 70 + delta_data_koeff*100 and deltadays > 1 and datetime.today().time().hour > 1:
            from_date_m, to_date_m = (timedelta(days=1) + df_last_update.iloc[ind, 1]).strftime(
                "%d/%m/%Y"), cur_date.strftime("%d/%m/%Y")  # граничные даты обновления..
            # print(f'\n:{df_last_update.iloc[ind, 0]}:last date-{(df_last_update.iloc[ind, 1]).strftime("%Y-%m-%d")},'
            #       f' дней без обновления-{deltadays}')
            # print('from_date:', from_date_m, ',to_Date:',to_date_m)
            # print('from_date:', datetime.strptime(from_date_m, '%d/%m/%Y').strftime('%Y-%m-%d'), ',to_Date:',
            #       datetime.strptime(to_date_m, '%d/%m/%Y').strftime('%Y-%m-%d'))
            if str(df_last_update.iloc[ind, 3]) == 'SPB':
                if pd.DataFrame.any(us_stock) == df_last_update.iloc[
                    ind, 0]:  # если в списке тикеров на investpy - то ищем, иначе лезем в YAHHO
                    print(df_last_update.iloc[ind, 0], ' Is in InvestPy')
                    try:
                        df_update = investpy.get_stock_historical_data(stock=df_last_update.iloc[ind, 0],
                                                                       country=market_name[0],
                                                                       from_date=from_date_m,
                                                                       to_date=to_date_m)
                        time.sleep(mysleep)
                        time_count.append(time.time())
                        df_update['market'] = "SPB"
                        # print(f'SPB load ok {df_last_update.iloc[ind, 0]}')
                        df_update.reset_index(level=['Date'], inplace=True)  # замена индекса
                        df_update['st_id'] = df_last_update.iloc[ind, 0]
                        df_update.drop_duplicates(subset='Date', inplace=True)
                        pd_df_to_sql(df_update)
                    except Exception as _ex:
                        print("USA investpy load error", df_last_update.iloc[ind, 0])
                        save_exeption_log(linux_path,modul='history' , message=str(_ex))
                        if 'Max retries exceeded with' in str(_ex):
                            save_log(linux_path, 'Too litle timedelta, need 2 pause')
                            exit()
                        continue
                    print(df_update)
                else:  # иначе лезем в YAHHO
                    print(df_last_update.iloc[ind, 0], ' Is in YAhho')
                    try:
                        name = df_last_update.iloc[ind, 0]
                        df_update = (
                            (pdr.get_data_yahoo(name,
                                                start=datetime.strptime(from_date_m, '%d/%m/%Y').strftime('%Y-%m-%d')
                                                , end=datetime.strptime(to_date_m, '%d/%m/%Y').strftime('%Y-%m-%d')
                                                ))[
                                ['Open', 'High', 'Low', 'Close', 'Volume']]).round(4)
                        time.sleep(mysleep)
                        time_count.append(time.time())
                        df_update[['Currency', 'market']] = "USD", 'SPB'
                        df_update.reset_index(level=['Date'], inplace=True)  # замена индекса
                        df_update.drop_duplicates(subset='Date', inplace=True)
                        df_update['st_id'] = df_last_update.iloc[ind, 0]
                        df_update = df_update[
                            df_update['Date'] >= datetime.strptime(from_date_m, '%d/%m/%Y').strftime('%Y-%m-%d')]
                        pd_df_to_sql(df_update)
                    except Exception as _ex:
                        save_exeption_log(linux_path,modul='history' , message=str(_ex))
                        print("USA YAHHO load error", df_last_update.iloc[ind, 0])
                        continue
                    print('yahho SPB \n', df_update)
                    # pdr.get_data_yahoo(df_last_update.iloc[ind,0], start=from_date_m.strftime("%Y-%m-%d"), end=from_date_m.strftime("%Y-%m-%d")))
                    # continue
            elif df_last_update.iloc[ind, 3] == 'US' and datetime.weekday(datetime.today()) > 4:
                if pd.DataFrame.any(us_stock) == df_last_update.iloc[
                    ind, 0]:  # если в списке тикеров на investpy - то ищем, иначе лезем в YAHHO
                    try:
                        df_update = investpy.get_stock_historical_data(stock=df_last_update.iloc[ind, 0],
                                                                       country=market_name[0],
                                                                       from_date=from_date_m,
                                                                       to_date=to_date_m)
                        time.sleep(mysleep)
                        time_count.append(time.time())
                        # print(df_last_update.iloc[ind, 0], df_update.tail(3))
                        df_update['market'] = "US"
                        df_update.reset_index(level=['Date'], inplace=True)  # замена индекса
                        df_update['st_id'] = df_last_update.iloc[ind, 0]
                        pd_df_to_sql(df_update)
                    except Exception as _ex:
                        save_exeption_log(linux_path, modul='history', message=str(_ex))
                        # print("USA investpy load error", df_last_update.iloc[ind, 0])
                        continue
                else:  # иначе лезем в YAHHO
                    try:
                        name = df_last_update.iloc[ind, 0]
                        df_update = (
                            (pdr.get_data_yahoo(name,
                                                start=datetime.strptime(from_date_m, '%d/%m/%Y').strftime('%Y-%m-%d')
                                                , end=datetime.strptime(to_date_m, '%d/%m/%Y').strftime('%Y-%m-%d')
                                                ))[
                                ['Open', 'High', 'Low', 'Close', 'Volume']]).round(4)
                        time.sleep(mysleep)
                        time_count.append(time.time())
                        df_update[['st_id', 'Currency', 'market']] = name, "USD", 'US'
                        # print('data fro YAHHO', df_update.tail(3))
                        df_update.reset_index(level=['Date'], inplace=True)  # замена индекса
                        df_update.drop_duplicates(subset='Date', inplace=True)
                        df_update = df_update[
                            df_update['Date'] >= datetime.strptime(from_date_m, '%d/%m/%Y').strftime('%Y-%m-%d')]
                        pd_df_to_sql(df_update)
                    except Exception as _ex:
                        save_exeption_log(linux_path,modul='history' ,message= str(_ex))
                        # print("USA YAHHO load error", df_last_update.iloc[ind, 0])
                        continue
            elif df_last_update.iloc[ind, 3] == 'RU':
                try:
                    df_update = investpy.get_stock_historical_data(stock=df_last_update.iloc[ind, 0],
                                                                   country=market_name[2],
                                                                   from_date=from_date_m,
                                                                   to_date=to_date_m)
                    # print(df_last_update.iloc[ind, 0],df_update)
                    time.sleep(mysleep)
                    time_count.append(time.time())
                    df_update['market'] = "RU"
                    df_update.reset_index(level=['Date'], inplace=True)  # замена индекса
                    df_update.drop_duplicates(subset='Date', inplace=True)
                    df_update['st_id'] = df_last_update.iloc[ind, 0]
                    pd_df_to_sql(df_update)
                except Exception as _ex:
                    save_exeption_log(linux_path, modul='history' ,message= str(_ex))
                    print("RU load error", df_last_update.iloc[ind, 0])
                    continue
            else:
                print(f"may be US < {max_wait_days} day==[{df_last_update.loc[ind]['st_id']}]")
                # exit()
                continue
            # print(df_update)
        sleep_timer_regulator()
        if len(time_count) == 300:
            ''' try to find and save timedalta between operation '''
            delta_time = []
            for ind in range(300-1):
                delta = round(time_count[ind + 1] - time_count[ind], 2)
                delta_time.append(delta)
            delta_f = pd.Series(delta_time)
            save_log(linux_path, f'mean of timer_count is [{delta_f.mean().round(2)}], min is [{delta_f.min()}], max is [{delta_f.max()}]')
            f = open(linux_path + 'timer.log', mode='a')
            f.writelines(f'today [{datetime.today()}] -'+ str(delta_f) + '\n')
            f.close()
            save_log(linux_path, 'first 300 delta time saved to timer.log')
    save_log(linux_path, 'update complite')
    # запускаем обновление актуальности данных в history_data
    history_date_base_update(db_connection_str)
    save_log(linux_path, 'teh indicator update start')
    ''' try to find and save timedalta between operation '''
    update_teh = 0
    for indexx in tqdm(df_last_update.st_id):
        if pd.DataFrame.any(
                df_last_teh.st_id == indexx) == False:  # если нет в списке - расчитываем и добавляем, иначе просто расчитываем
            if df_last_update[df_last_update.st_id == indexx].iloc[0]["Currency"] == 'USD':
                try:
                    teh_analis_local = teh_an(indexx, country_teh=market_name[0])
                    print(f"insert USD DATA {indexx}")
                except Exception as _ex:
                    save_exeption_log(linux_path, modul='teh_an' , message=str(_ex))
                    teh_an_df_nodata.loc[0]['date'] = cur_date.strftime("%Y-%m-%d")
                    teh_an_df_nodata.loc[0]['st_id'] = str(indexx)
                    teh_analis_local = teh_an_df_nodata
                    print(f"insert NO DATA {indexx}")
                teh_an_to_sql(teh_analis_local)
                # print(teh_analis_local)
            if df_last_update[df_last_update.st_id == indexx].iloc[0]["Currency"] == 'RUB':
                try:
                    teh_analis_local = teh_an(indexx, country_teh=market_name[2])
                    print(f"insert RU DATA {indexx}")
                except Exception as _ex:
                    save_exeption_log(linux_path,modul='teh_an' , message=str(_ex))
                    teh_an_df_nodata.loc[0]['date'] = cur_date.strftime("%Y-%m-%d")
                    teh_an_df_nodata.loc[0]['st_id'] = str(indexx)
                    teh_analis_local = teh_an_df_nodata
                    print(f"insert NO DATA {indexx}")
                teh_an_to_sql(teh_analis_local)
                # print(teh_analis_local)
        else:
            deltadays = (cur_date.date() - df_last_teh[df_last_teh.st_id == indexx].iloc[0]['date_max']).days
            if deltadays <= 700 and deltadays > 15 and datetime.today().time().hour < 14:
                if df_last_update[df_last_update.st_id == indexx].iloc[0]["Currency"] == 'USD':
                    try:
                        teh_analis_local = teh_an(indexx, country_teh=market_name[0])
                        print(f"update USD DATA {indexx}")
                        update_teh +=1
                    except Exception as _ex:
                        save_exeption_log(linux_path,modul='teh_an', message=str(_ex))
                        teh_an_df_nodata.loc[0]['date'] = cur_date.strftime("%Y-%m-%d")
                        teh_an_df_nodata.loc[0]['st_id'] = str(indexx)
                        teh_analis_local = teh_an_df_nodata
                    teh_an_to_sql(teh_analis_local)
                    # print(teh_analis_local)
                if df_last_update[df_last_update.st_id == indexx].iloc[0]["Currency"] == 'RUB':
                    try:
                        teh_analis_local = teh_an(indexx, country_teh=market_name[2])
                        print(f"update RU DATA {indexx}")
                        update_teh += 1
                    except Exception as _ex:
                        save_exeption_log(linux_path, modul='teh_an',message=str(_ex))
                        teh_an_df_nodata.loc[0]['date'] = cur_date.strftime("%Y-%m-%d")
                        teh_an_df_nodata.loc[0]['st_id'] = str(indexx)
                        teh_analis_local = teh_an_df_nodata
                        print(f"update NO DATA {teh_analis_local}")
                    teh_an_to_sql(teh_analis_local)
                    # print(teh_analis_local)
    # save_log(linux_path, 'teh indicator is up to date [ ' + str(update_teh) + ' ]')
    # save_log(linux_path, 'teh indicator smooler ' + str(deltadays))
    save_log(linux_path, f'teh indicator update complite, make {update_teh} records')
    save_log(linux_path, f'teh indicator not found [{len(stock_not_found_teh_an)}] next stock [{stock_not_found_teh_an}] , ')
    # df_last_update = pd.read_sql(
    #     'Select st_id, max(date) as date_max, Currency, min(date) as date_min , market from hist_data group by st_id',
    #     con=db_connection)  # загрузили список тикеров из базы с последней датой
    # df_last_update.to_sql(name='base_status', con=db_connection, if_exists='replace')  # append , replace
    save_log(linux_path, 'base_status update complite')
    return df_last_update



def history_date_base_update(db_connection_str):
    """ считаваем максимальные значения дат для каждого тикера из базы данных ,и потом записываем в отдельную таблицу для быстрого доступа"""
    db_connection = create_engine(db_connection_str)
    df_last_update = pd.read_sql(
        'Select st_id, max(date) as date_max, Currency, min(date) as date_min , market from hist_data group by st_id',
        con=db_connection)  # загрузили список тикеров из базы с последней датой
    df_last_update.to_sql(name='base_status', con=db_connection, if_exists='replace')
    print('history_date_base_update complite')


def first_read_sort(linux_path):
    stocks_us = investpy.stocks.get_stocks(country='United States')  # читаем список тикеров на биржах США
    df_spb = pd.read_csv('ListingSecurityList.csv', delimiter=';',
                         encoding='cp1251')  # читаем список всех тикеров на СПБ
    stock_spb = df_spb[df_spb['s_currency_kotir'] == 'USD']  # вычисляем список эмитентов СПБ с курсом в USD
    us_st_spb = stock_spb[['e_full_name', 'symbol', 's_currency_kotir']]  # срезаем лишние столбцы из списка
    us_st_spb.to_csv('us_stocks_SPB.csv', sep=';', encoding='cp1251')  # сохраняем список тикеров СПБ с курсом в USD
    stocks_us.to_csv('us_stocks.csv', sep=';', encoding='cp1251')  # сохраняем список тикеров США в .CSV
    us_st_read = pd.read_csv('us_stocks.csv', delimiter=';', encoding='cp1251')  # Читаем список тикеров США
    us_st_spb.sort_values(by='symbol')  # сортируем
    us_st_read = us_st_read.sort_values(by='symbol')  # сортируем
    spb_mod = us_st_spb[['e_full_name', 'symbol']]  # выделяем столбцы
    us_mod = us_st_read[['full_name', 'symbol']]  # выделяем столбцы
    intersection = spb_mod[['symbol']].merge(us_mod[['symbol']]).drop_duplicates()  # делаем пересекающийся список
    print(intersection)
    df = pd.concat([spb_mod.merge(intersection), us_mod.merge(intersection)])
    # df = pd.concat([us_mod, spb_mod], axis= 1)
    # print(df)
    intersection.to_csv('intersection.csv', sep=';', encoding='cp1251', header=None)  # сохраняем пересекающийся список
    # придумал как делать без всего этого - на лету проверяем на соответствие и вуаля - все работает!!!


def teh_an_to_sql(teh_an_df):
    """записываем значения теханализа в sql базу """
    engine = create_engine('mysql+pymysql://python:python@192.168.0.118/hist_data')
    try:
        teh_an_df.to_sql(name='teh_an', con=engine, if_exists='append')  # append , replace
        print(f"teh_an save_to MYSQL [{teh_an_df.loc[0]['st_id']}]...... OK")
    except Exception as _ex:
        save_exeption_log(linux_path, modul='teh_an_sql',message=str(_ex))
        print(f"Error MYSQL _ teh_an [{teh_an_df.loc[0]['st_id']}]")


def pd_df_to_sql(df):
    """записываем исторические значения в sql базу """
    engine = create_engine('mysql+pymysql://python:python@192.168.0.118/hist_data')
    try:
        df.to_sql(name='hist_data', con=engine, if_exists='append')  # append , replace
    except Exception as _ex:
        save_exeption_log(linux_path, modul='hist_sql', message=str(_ex))
        print('SQL save errorrr \n ', df.shape, df, '\n')
    print(f"save_to MYSQL [{df.loc[0][['st_id', 'market']]}]...... OK")


def statistic_data_base(df_last_update):
    """ модуль для подсчета статистики по базе данных - считаем 2 поздние дату и сколько значений в ними, и записываем в лог"""
    global linux_path
    listing_ll = pd.Series({c: df_last_update[c].unique() for c in df_last_update})
    listing_ll['date_max'].sort()
    save_log(linux_path, "Start statistic calculation")
    for market_s in df_last_update['market'].unique():
        listing_ll = pd.Series(
            {c: df_last_update[df_last_update['market'] == market_s][c].unique() for c in df_last_update})
        listing_ll['date_max'].sort()
        for num_1 in range(len(listing_ll['date_max']) - 2, len(listing_ll['date_max'])):
            line = f"data for [{market_s}]- [{pd.to_datetime(listing_ll['date_max'][num_1]).date()}] is [{len(df_last_update[(df_last_update['market'] == market_s) & (df_last_update['date_max'] == listing_ll['date_max'][num_1])]['date_max'])}] "
            save_log(linux_path, line)
    save_log(linux_path, "statistic calculation complete")


linux_path = ''
db_connection_str = 'mysql+pymysql://python:python@192.168.0.118/hist_data'
delta_data_koeff = 20
mysleep, max_wait_days = 0.001, 45
stock_not_found_teh_an = []

def main():
    global linux_path, mysleep
    print('программа обновления базы данных торговой истории')
    print('START')
    # My constant list
    col_list = my_start()
    global db_connection_str  # = 'mysql+pymysql://python:python@192.168.0.118/hist_data'
    linux_path = '/opt/1/My_Python/st_US/'
    linux_path = ''
    if os.name == 'nt':  # проверяем из под чего загрузка.
        linux_path = ''
        history_path = 'D:\\YandexDisk\\корень\\отчеты\\'
        print("start from WINDOWS")
        mysleep = 1
    else:
        linux_path = '/mnt/1T/opt/gig/My_Python/st_US/'
        history_path = '/mnt/1T/opt/gig/My_Python/st_US/SAVE'
        print("start from LINUX")


    # end constant list

    # pand_to_csv(linux_path) ##конвертация базы в формат csv для загрузки вручную

    # pd_df_to_sql(big_df_table)
    # my mysql request
    ## ALTER IGNORE TABLE hist_data ADD UNIQUE ( Date, st_id(6));  ## удаляем дубликаты в mysql
    # SELECT * FROM hist_data WHERE st_id = 'A';  ## выделяем строки в столбце, где st_id = A
    ## SELECT * FROM hist_data WHERE st_id = 'A' AND open > 70;
    ## SELECT * FROM hist_data WHERE st_id = 'A' AND open > 70 AND date > '2021-08-09';
    # SELECT Date, Low, High, st_id FROM hist_data WHERE st_id = 'APA' AND date > '2021-08-09';
    ##  SELECT Date, Low, High FROM hist_data WHERE st_id = 'APA' AND date BETWEEN '2021-08-09' AND '2021-09-10';
    # пробуем получить список тикеров с максимальными датами
    # df = pd.read_sql('SELECT'+ '*' + 'FROM hist_data', con=db_connection)
    #     ##Select st_id, max(date) as date_max from hist_data group by st_id;
    #
    #     select *  from hist_data where market is null  ;

    # my mysql request end###

    # load_from_mysql(db_connection_str)

    df_last_update = history_updater(linux_path, db_connection_str)  # тестируем загрузку и обновление sql базы
    # update statistic
    df_last_update = pd.read_sql('Select * from base_status ;', con=db_connection_str)
    statistic_data_base(df_last_update)

    exit()

    print("UPDATE complite.. start remove dublikate")
    engine = create_engine('mysql+pymysql://python:python@192.168.0.118/hist_data')
    db_connection = create_engine(db_connection_str)  # connect to database
    engine.execute("ALTER IGNORE TABLE hist_data ADD UNIQUE ( Date, st_id(6))").fetchall()  # удаляем дубликаты в mysql
    print("MYSQL dublikate delete...OK")


if __name__ == "__main__":
    main()
