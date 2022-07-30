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
import threading
from pr_config import *
from pr_config import Config_Up
from moduls import save_log, save_exception_log

# TODO: вообще надо бы сделать типа fastapi --- программа засылает запрос, который перехватывает собственный модуль
''' 
!!!!!вообще надо бы сделать типа rest api --- программа засылает запрос, который перехватывает собственный модуль,
!!!!!КОТОРЫЙ будет заодно делать проверку на частоту обращения к интернет сервисам!!!!
1) продумать возможность выстроения очереди и регулирования её длины.  
 или может сделать распаралеливание обращений к Yahho  и Investpy  
2) надо подумать как китайские акции сюда подгружать!!!

программа обновления исторических данных курсов акций американских и российских: 
hist_data: date, high, low, open, close, volume
и данных теханализа путем загрузки данных c сервисов Investpy и Yahho
с сохранением в базу mysql. 


работа проги расчитана для запуска с 2.40 ночи (примерно в это время уже все записи обновлены) и до 16 ( в 18-10 запускается модуль контроля сплитов)



добавлена обработка ошибок - и выходим если нет смысла проверять все. 
добавлен отдельный лог для ошибок при вызове функций (update_extention.log).

 
'''


def teh_an(t_name, country_teh):
    '''модуль сбора данных теханализа'''
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


# def save_log(linux_path, message):
#     '''сохраняет в лог файл сообщение о работе программы.. '''
#     f = open(linux_path + 'update.log', mode='a')
#     lines = '[' + str(datetime.today()) + '] ' + str(message)
#     f.writelines(lines + '\n')
#     f.close()


# def save_exeption_log(modul, message: str):
#     '''записываем в файл логи ошибок с указанием модуля из которого был вызов
#     при этом делаем некую фильтрацию
#     :type linux_path: str
#     :type modul: str'''
# 
#     global stock_not_found_teh_an, no_data_fetched_hist_yahho
#     try:
#         if len(message) != 0:
#             if 'not found' in message and modul == 'teh_an':
#                 stock_not_found_teh_an.append(message.split()[2].upper())
#                 return
#             if 'No data fetched' in message:
#                 return no_data_fetched_hist_yahho.append(message.split()[5])
#             if "'Date'" in message:
#                 return
#             if 'signal' in message:
#                 return print('signal')
#             # TODO: не фильтруется и все же пишется в лог No data fetched
#             # TODO: надо бы придумать такой фокус, кок сравнение с предудущей записью - если одно и тоже, то просто считаем и
#             # TODO: записываем число повторений, если запись поменялась.
# 
#             if message != '0':
#                 f = open(linux_path + 'update_extention.log', mode='a')
#                 lines = '[' + str(datetime.today()) + f']-[{modul}] ' + str(message)
#                 f.writelines(lines + '\n')
#                 f.close()
#             else:
#                 print('exit pass')
#                 pass
#     except:
#         pass

def exception_filter(modul: str, message: str):
    '''записываем в файл логи ошибок с указанием модуля из которого был вызов
    при этом делаем некую фильтрацию
    '''

    global stock_not_found_teh_an, no_data_fetched_hist_yahho
    try:
        if len(message) != 0:
            if 'not found' in message and modul == 'teh_an':
                stock_not_found_teh_an.append(message.split()[2].upper())
                return ''
            if 'No data fetched' in message:
                return no_data_fetched_hist_yahho.append(message.split()[5])
            if "'Date'" in message:
                return
            if 'signal' in message:
                return
            # TODO: не фильтруется и все же пишется в лог No data fetched
            # TODO: надо бы придумать такой фокус, кок сравнение с предудущей записью - если одно и тоже, то просто считаем и
            # TODO: записываем число повторений, если запись поменялась.
            if message != '0':
                save_exception_log(modul, message)
            else:
                print('exit pass')
                return
    except:
        return


def history_updater():
    """основной модуль обновления исторических данных
    hist_data: date, high, low, open, close, volume
    и данных теханализа путем загрузки данных c сервисов Investpy и Yahho
    """

    # делаем обновление базы mysql
    cur_date = datetime.today()
    time_count = []
    global mysleep, stock_not_found_teh_an, no_data_fetched_hist_yahho

    def sleep_timer_regulator():
        '''пробуем регулировать паузу между обращениями за данными налету'''
        global mysleep
        if len(time_count) > 2:
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
    print('сегодня...', cur_date.strftime("%Y-%m-%d"))
    save_log('---------------start update--------------')
    market_name = ['United States', 'United States', 'russia']
    db_connection = create_engine(sql_login)  # connect to database

    df_last_update = pd.read_sql('Select st_id, max(date) as date_max, Currency, market from hist_data group by st_id',
                                 con=db_connection)  # загрузили список тикеров из базы с последней датой

    print("load from Mysql...OK")
    df_last_teh = pd.read_sql(
        'Select st_id, max(date) as date_max from teh_an group by st_id',
        con=db_connection)  # загружаем последние данные по теханализу
    max_date_df = pd.DataFrame.max(df_last_update.iloc[:, 1])
    print('Maximum date is--', max_date_df)
    print(f'длина массива {len(df_last_update)}')

    save_log('DataFrame leght ' + str(len(df_last_update)))
    us_stock = investpy.get_stocks(country=market_name[0])['symbol']  # список тикеров в США

    for ind in tqdm(range(len(df_last_update))):
        deltadays = (cur_date - df_last_update.iloc[ind, 1]).days
        if deltadays <= 70 + delta_data_koeff * 100 and deltadays > 1 and datetime.today().time().hour < 16 and datetime.today().time().hour > 1:
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
                        time.sleep(mysleep)
                        df_update = investpy.get_stock_historical_data(stock=df_last_update.iloc[ind, 0],
                                                                       country=market_name[0],
                                                                       from_date=from_date_m,
                                                                       to_date=to_date_m)
                        time_count.append(time.time())
                        df_update['market'] = "SPB"
                        # print(f'SPB load ok {df_last_update.iloc[ind, 0]}')
                        df_update.reset_index(level=['Date'], inplace=True)  # замена индекса
                        df_update['st_id'] = df_last_update.iloc[ind, 0]
                        df_update.drop_duplicates(subset='Date', inplace=True)
                        pd_df_to_sql(df_update)
                    except Exception as _ex:
                        print("USA investpy load error", df_last_update.iloc[ind, 0])
                        exception_filter(modul='history [Ipy]',
                                         message=str(_ex) + str(df_last_update.iloc[ind, 0]))
                        if 'Max retries exceeded with' in str(_ex):
                            save_log('Too litle timedelta, need 2 pause')
                            exit()
                        continue
                    print(df_update)
                else:  # иначе лезем в YAHHO
                    print(df_last_update.iloc[ind, 0], ' Is in YAhho')
                    try:
                        name = df_last_update.iloc[ind, 0]
                        time.sleep(mysleep)
                        df_update = (
                            (pdr.get_data_yahoo(name,
                                                start=datetime.strptime(from_date_m, '%d/%m/%Y').strftime('%Y-%m-%d')
                                                , end=datetime.strptime(to_date_m, '%d/%m/%Y').strftime('%Y-%m-%d')
                                                ))[
                                ['Open', 'High', 'Low', 'Close', 'Volume']]).round(4)

                        time_count.append(time.time())
                        df_update[['Currency', 'market']] = "USD", 'SPB'
                        df_update.reset_index(level=['Date'], inplace=True)  # замена индекса
                        df_update.drop_duplicates(subset='Date', inplace=True)
                        df_update['st_id'] = df_last_update.iloc[ind, 0]
                        df_update = df_update[
                            df_update['Date'] >= datetime.strptime(from_date_m, '%d/%m/%Y').strftime('%Y-%m-%d')]
                        pd_df_to_sql(df_update)
                    except Exception as _ex:
                        exception_filter(modul='history [Yah]',
                                         message=str(_ex) + str(df_last_update.iloc[ind, 0]))
                        print("USA YAHHO load error", df_last_update.iloc[ind, 0])
                        continue
                    print('yahho SPB \n', df_update)
                    # pdr.get_data_yahoo(df_last_update.iloc[ind,0], start=from_date_m.strftime("%Y-%m-%d"), end=from_date_m.strftime("%Y-%m-%d")))
                    # continue
            elif df_last_update.iloc[ind, 3] == 'USA' and datetime.weekday(datetime.today()) > 4:
                if pd.DataFrame.any(us_stock) == df_last_update.iloc[
                    ind, 0]:  # если в списке тикеров на investpy - то ищем, иначе лезем в YAHHO
                    try:
                        time.sleep(mysleep)
                        df_update = investpy.get_stock_historical_data(stock=df_last_update.iloc[ind, 0],
                                                                       country=market_name[0],
                                                                       from_date=from_date_m,
                                                                       to_date=to_date_m)
                        time_count.append(time.time())
                        # print(df_last_update.iloc[ind, 0], df_update.tail(3))
                        df_update['market'] = "USA"
                        df_update.reset_index(level=['Date'], inplace=True)  # замена индекса
                        df_update['st_id'] = df_last_update.iloc[ind, 0]
                        pd_df_to_sql(df_update)
                    except Exception as _ex:
                        exception_filter(modul='history [Ipy]',
                                         message=str(_ex) + str(df_last_update.iloc[ind, 0]))
                        # print("USA investpy load error", df_last_update.iloc[ind, 0])
                        continue
                else:  # иначе лезем в YAHHO
                    try:
                        name = df_last_update.iloc[ind, 0]
                        time.sleep(mysleep)
                        df_update = (
                            (pdr.get_data_yahoo(name,
                                                start=datetime.strptime(from_date_m, '%d/%m/%Y').strftime('%Y-%m-%d')
                                                , end=datetime.strptime(to_date_m, '%d/%m/%Y').strftime('%Y-%m-%d')
                                                ))[
                                ['Open', 'High', 'Low', 'Close', 'Volume']]).round(4)
                        time_count.append(time.time())
                        df_update[['st_id', 'Currency', 'market']] = name, "USD", 'USA'
                        # print('data fro YAHHO', df_update.tail(3))
                        df_update.reset_index(level=['Date'], inplace=True)  # замена индекса
                        df_update.drop_duplicates(subset='Date', inplace=True)
                        df_update = df_update[
                            df_update['Date'] >= datetime.strptime(from_date_m, '%d/%m/%Y').strftime('%Y-%m-%d')]
                        pd_df_to_sql(df_update)
                    except Exception as _ex:
                        exception_filter(modul='history [Yah]',
                                         message=str(_ex) + str(df_last_update.iloc[ind, 0]))
                        # print("USA YAHHO load error", df_last_update.iloc[ind, 0])
                        continue
            elif df_last_update.iloc[ind, 3] == 'RU':
                try:
                    time.sleep(mysleep)
                    df_update = investpy.get_stock_historical_data(stock=df_last_update.iloc[ind, 0],
                                                                   country=market_name[2],
                                                                   from_date=from_date_m,
                                                                   to_date=to_date_m)
                    # print(df_last_update.iloc[ind, 0],df_update)
                    time_count.append(time.time())
                    df_update['market'] = "RU"
                    df_update.reset_index(level=['Date'], inplace=True)  # замена индекса
                    df_update.drop_duplicates(subset='Date', inplace=True)
                    df_update['st_id'] = df_last_update.iloc[ind, 0]
                    pd_df_to_sql(df_update)
                except Exception as _ex:
                    exception_filter(modul='history', message=str(_ex))
                    print("RU load error", df_last_update.iloc[ind, 0])
                    continue
            else:
                print(f"may be USA < {max_wait_days} day==[{df_last_update.loc[ind]['st_id']}]")
                # exit()
                continue
            # print(df_update)
        sleep_timer_regulator()
        if len(time_count) == 2800:
            ''' try to find and save timedalta between operation '''
            delta_time = []
            for ind in range(len(time_count) - 1):
                delta = round(time_count[ind + 1] - time_count[ind], 2)
                delta_time.append(delta)
            delta_f = pd.Series(delta_time)
            save_log(
                f'mean of timer_count[{len(time_count)}] is [{delta_f.mean().round(2)}], min is [{delta_f.min()}], max is [{delta_f.max()}]')
            f = open(Config_Up.Path_linux + 'timer.log', mode='a')
            f.writelines(f'today [{datetime.today()}]  {[delta_time]} ' + '\n')
            f.close()
            # save_log( 'first 300 delta time saved to timer.log')
    save_log('update complite')
    # save_log(
    #          f'in YahhoDReader no data fetched [{len(no_data_fetched_hist_yahho)}] next stocks [{no_data_fetched_hist_yahho}] ')

    # history_date_base_update(sql_login)
    ''' пишем в лог результат обновления hist_data '''
    
    def update_log_statistic():  #
        # save_log( 'Tread start_____')
        # запускаем обновление актуальности данных в history_data
        history_date_base_update(sql_login)
        df_last_update = pd.read_sql('Select * from base_status ;', con=db_connection)
        statistic_data_base(df_last_update)
        # save_log('Tread complite_____')

    update_log_tread = threading.Thread(target=update_log_statistic)  ## just play with some keys
    update_log_tread.start()

    save_log('teh indicator update start')
    ''' try to find and save timedalta between operation '''
    update_teh = 0
    for indexx in tqdm(df_last_update.st_id):
        if not (
                df_last_teh.st_id == indexx).any():  # если нет в списке - расчитываем и добавляем, иначе просто расчитываем
            if df_last_update[df_last_update.st_id == indexx].iloc[0]["Currency"] == 'USD':
                try:
                    teh_analis_local = teh_an(indexx, country_teh=market_name[0])
                    print(f"insert USD DATA {indexx}")
                except Exception as _ex:
                    exception_filter(modul='teh_an', message=str(_ex))
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
                    exception_filter(modul='teh_an', message=str(_ex))
                    teh_an_df_nodata.loc[0]['date'] = cur_date.strftime("%Y-%m-%d")
                    teh_an_df_nodata.loc[0]['st_id'] = str(indexx)
                    teh_analis_local = teh_an_df_nodata
                    print(f"insert NO DATA {indexx}")
                teh_an_to_sql(teh_analis_local)
                # print(teh_analis_local)
        else:
            deltadays = (cur_date.date() - df_last_teh[df_last_teh.st_id == indexx].iloc[0]['date_max']).days
            if deltadays <= 700 and deltadays > 8 and datetime.today().time().hour < 14:
                if df_last_update[df_last_update.st_id == indexx].iloc[0]["Currency"] == 'USD':
                    try:
                        teh_analis_local = teh_an(indexx, country_teh=market_name[0])
                        print(f"update USD DATA {indexx}")
                        update_teh += 1
                    except Exception as _ex:
                        exception_filter(modul='teh_an', message=str(_ex))
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
                        exception_filter(modul='teh_an', message=str(_ex))
                        teh_an_df_nodata.loc[0]['date'] = cur_date.strftime("%Y-%m-%d")
                        teh_an_df_nodata.loc[0]['st_id'] = str(indexx)
                        teh_analis_local = teh_an_df_nodata
                        print(f"update NO DATA {teh_analis_local}")
                    teh_an_to_sql(teh_analis_local)
                    # print(teh_analis_local)
    save_log(f'teh indicator update complite, make {update_teh} records')
    if len(stock_not_found_teh_an) != 0:
        save_log(
            f'teh indicator not found [{len(stock_not_found_teh_an)}] next stock [{stock_not_found_teh_an}] , ')
    update_log_tread.join()
    save_log('base_status update complite')
    return df_last_update


def history_date_base_update(sql_login: str):
    """ считаваем максимальные значения дат для каждого тикера из базы данных ,и потом записываем в отдельную таблицу для быстрого доступа
    необходимо для правильного обращения к данным при расчетах"""
    db_connection = create_engine(sql_login)
    df_last_update = pd.read_sql(
        'Select st_id, max(date) as date_max, Currency, min(date) as date_min , market from hist_data group by st_id',
        con=db_connection)
    df_last_update.to_sql(name='base_status', con=db_connection, if_exists='replace')
    print('history_date_base_update complite')


def teh_an_to_sql(teh_an_df):
    """записываем значения теханализа в sql базу """
    engine = create_engine(sql_login)
    try:
        teh_an_df.to_sql(name='teh_an', con=engine, if_exists='append')  # append , replace
        print(f"teh_an save_to MYSQL [{teh_an_df.loc[0]['st_id']}]...... OK")
    except Exception as _ex:
        exception_filter(modul='teh_an_sql', message=str(_ex) + str(teh_an_df.loc[0]['st_id']))
        print(f"Error MYSQL _ teh_an [{teh_an_df.loc[0]['st_id']}]")


def pd_df_to_sql(df):
    """записываем исторические значения в sql базу """
    engine = create_engine(sql_login)
    try:
        df.to_sql(name='hist_data', con=engine, if_exists='append')  # append , replace
    except Exception as _ex:
        exception_filter(modul='hist_sql', message=str(_ex))
        print('SQL save error: \n ', df.shape, df, '\n')
    print(f"save_to MYSQL [{df.loc[0][['st_id', 'market']]}]...... OK")


def statistic_data_base(df_last_update: pd.DataFrame):
    """ модуль для подсчета статистики по базе данных - 
    считаем 2 поздние дату и сколько значений в ними, и записываем в лог"""
    listing_ll = pd.Series({c: df_last_update[c].unique() for c in df_last_update})
    listing_ll['date_max'].sort()
    save_log("Start statistic calculation")
    for market_s in df_last_update['market'].unique():
        listing_ll = pd.Series(
            {c: df_last_update[df_last_update['market'] == market_s][c].unique() for c in df_last_update})
        listing_ll['date_max'].sort()
        for num_1 in range(len(listing_ll['date_max']) - 2, len(listing_ll['date_max'])):
            line = f"data for [{market_s}]- [{pd.to_datetime(listing_ll['date_max'][num_1]).date()}] is [{len(df_last_update[(df_last_update['market'] == market_s) & (df_last_update['date_max'] == listing_ll['date_max'][num_1])]['date_max'])}] "
            save_log(line)
    save_log("statistic calculation complete")


def start_control():
    ''' модуль контроля времени запуска - чтоб не помешать другим программам по обращению к Investpy Yahho'''

    if datetime.today().time().hour > 16 or datetime.today().time().hour < 1:
        save_log('!!!!! TRY run in BLOKED time!!!!!! ')
        print('!!!!! TRY run in BLOKED time!!!!!! ')
        exit()


delta_data_koeff = 20
mysleep, max_wait_days = 0.001, 45
stock_not_found_teh_an, no_data_fetched_hist_yahho = [], []


def main():
    global mysleep
    print('программа обновления базы данных торговой истории')
    print('START')
    # My constant list
    global sql_login  # =  sql_login
    # linux_path = '/opt/1/My_Python/st_US/'
    # linux_path = ''
    if os.name == 'nt':  # проверяем из под чего загрузка.

        history_path = 'D:\\YandexDisk\\корень\\отчеты\\'
        print("start from WINDOWS")
        mysleep = 1
    else:
        # linux_path = '/mnt/1T/opt/gig/My_Python/st_US/'
        history_path = '/mnt/1T/opt/gig/My_Python/st_US/SAVE'
        print("start from LINUX")

    start_control()
    history_updater()  # загрузка и обновление sql базы
    save_log('------------update complited --------------')

    exit()

    print("UPDATE complite.. start remove dublikate")

    db_connection = create_engine(sql_login)  # connect to database
    engine.execute("ALTER IGNORE TABLE hist_data ADD UNIQUE ( Date, st_id(6))").fetchall()  # удаляем дубликаты в mysql
    print("MYSQL dublikate delete...OK")


if __name__ == "__main__":
    main()

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
    # ALTER IGNORE     TABLE    tiker_report ADD UNIQUE(day_close, tiker(6));
    # load_from_mysql(sql_login)
