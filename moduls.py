from pr_config import *
from pr_config import Config_Up
from datetime import datetime


def save_log(message: str):  # сохраняет в лог файл сообщение..
    with open(Config_Up.Path_linux + 'update.log', mode='a') as f:
        lines = '[' + str(datetime.today()) + '] ' + str(message)
        f.writelines(lines + '\n')



def save_exception_log(modul, message: str):
    with open(Config_Up.Path_linux + 'update_extention.log', mode='a') as f:
        lines = f"[{datetime.today()}]-[{modul}]{message}\n"
        f.writelines(lines)


####  старинные модули - не используются - может не хватить импорта

def old_mod():
    def stock_name_table(linux_path):
        """загрузка тикеров из инета и файла с СПБ в эксель файл
        !! сейчас не используется"""
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

    def history_data(linux_path):  # сохраняем все  -- вроде работает
        """модуль для загрузки исторических данных по тикерам в Большой xlsx файл - использовался до внедрения sql базы
         данные каждый раз загружались с нуля. накопления не было.
         !!!! сейчас не используется
         """
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


