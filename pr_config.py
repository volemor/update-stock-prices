""" переменные для работы программы, пароли в отдельном файле """
sql_login = 'mysql+pymysql://python:python@192.168.0.118/hist_data'
path_linux = '/mnt/1T/opt/gig/My_Python/st_US/'
path_history_linux = '/mnt/1T/opt/gig/My_Python/st_US/SAVE/'
path_win = ''
path_history_win = 'D:\\YandexDisk\\корень\\отчеты\\'
prj_path = ''
history_path = ''
xlsx_sample = 'df_help_06.03.2022.xlsx'
my_start_date = '2020-02-01'
max_old_days = 25

"""
пользовательские списки текеров
"""

dmitry_list_spb = {'SEDG', 'CSCO', 'ALB', 'AMAT', 'MU', 'SPCE', 'RBLX', 'APA', 'BYND', 'SPR', 'INTC', 'SQ',
                   'TSLA', 'FSLR', 'NEE', 'DAL',
                   'AA', 'PLAY', 'BABA', 'ARNC', 'PBF', 'AMD', 'NDAQ', 'QCOM', 'DVN', 'LTHM', 'COP',
                   'NVDA', 'U', 'BIDU', 'SAVE', 'AAPL', 'MVIS', 'CCJ', 'MP', 'MTRN', 'COIN'}
zina_list_spb = {'AA', 'APA', 'BA', 'FSLR', 'INTC', 'NEE', 'PBF', 'SPR', 'LTHM', 'SPCE', 'UAL', 'NVDA', 'NDAQ', 'AAPL'}

# client_mail_vip = ['volemor@yandex.ru', 'azinaidav@mail.ru', 'krotar@mail.ru',
#                    'ton2244@yandex.ru']  ## 'fedorov.efrem@gmail.com' ]
# client_mail_not_vip = ['p.trubitsyn7@yandex.ru', 'dom_teh@mail.ru']


""" словарь с адресами почты для рассылки"""
mail_global_dict = {'vip': ['ton2244@yandex.ru', 'azinaidav@mail.ru', 'krotar@mail.ru', 'volemor@yandex.ru'],
                    'not_vip': ['p.trubitsyn7@yandex.ru', 'dom_teh@mail.ru']}
""" наименования столбцов для формирования датафрейма при расчетах таблицы tiker_report"""
col_list = ['tiker', 'name', 'branch', 'today_close',
            'min_dek1', 'min_dek2', 'min_dek3', 'min_dek4',
            'min_dek5', 'min_dek6', 'min_dek7', 'min_dek8',
            'min_pr_dek1',
            'min_pr_dek2', 'min_pr_dek3', 'min_pr_dek4', 'min_pr_dek5', 'min_pr_dek6',
            'min_pr_dek7', 'min_pr_dek8',
            'min_pr_delta_1_2', 'min_pr_delta_2_3', 'min_pr_delta_3_4', 'min_pr_delta_4_5', 'min_pr_delta_5_6',
            'min_pr_delta_6_7', 'min_pr_delta_7_8',
            'max_dek1', 'max_dek2', 'max_dek3', 'max_dek4', 'max_dek5',
            'max_dek6', 'max_dek7', 'max_dek8',
            'max_pr_dek1', 'max_pr_dek2', 'max_pr_dek3', 'max_pr_dek4',
            'max_pr_dek5', 'max_pr_dek6', 'max_pr_dek7', 'max_pr_dek8',
            'max_pr_delta_1_2', 'max_pr_delta_2_3', 'max_pr_delta_3_4', 'max_pr_delta_4_5', 'max_pr_delta_5_6',
            'max_pr_delta_6_7', 'max_pr_delta_7_8',
            'min_y', 'max_y', 'today_y_pr_min',
            'today_y_pr_max', 'day_start', 'day_close',
            'teh_daily_sel', 'teh_daily_buy', 'teh_weekly_sel', 'teh_weekly_buy', 'teh_monthly_sell',
            'teh_monthly_buy', 'daily_sma_signal_200', 'daily_ema_signal_200', 'weekly_sma_signal_200',
            'weekly_ema_signal_200', 'monthly_sma_signal_200', 'monthly_ema_signal_200', 'EPS', 'P_E',
            'm1_max', 'm1_min', 'm3_max', 'm3_min', 'm6_max', 'm6_min', 'year1_max', 'year1_min', 'pr_30_day_max',
            'pr_30_day_min',
            'pr_90_day_max', 'pr_90_day_min',
            'pr_6_m_max', 'pr_6_m_min', 'pr_1y_max', 'pr_1y_min']

""" команды для управления мультипоточной загрузкой из базы данных"""

sql_comm_key = ['tiker_branch', 'base_status', 'teh_an_status', 'hist_SPB', 'hist_RU', 'hist_US', 'teh_an_base',
                'tiker_report']
sql_command_list = ['Select * from tiker_branch ;',
                    'Select * from base_status ;',
                    'Select st_id, max(date) as date_max from teh_an group by st_id',
                    f'Select date, high, low, close, st_id, Currency from hist_data  WHERE market=\'SPB\' and date > \'{my_start_date}\';',
                    f'Select date, high, low, close, st_id, Currency from hist_data  WHERE market=\'RU\' and date > \'{my_start_date}\';',
                    f'Select date, high, low, close, st_id, Currency from hist_data  WHERE market=\'US\' and date > \'{my_start_date}\';',
                    'Select hd.* from teh_an hd join (Select hd.st_id, max(hd.date) as date_max from teh_an hd group by hd.st_id) hist_data_date_max on hist_data_date_max.st_id = hd.st_id and hist_data_date_max.date_max = hd.date;',
                    'Select tiker, max(day_close) as max_day_close from tiker_report group by tiker;'
                    ]

"""  базовый словарь sheet_name для сохранения df в excel """

excel_sheet_name = { 'SPB', 'RU', 'USA', 'Teh_analis_all', 'D_list', 'Z_list'}

"""
base_status: -> df_last_update
        index | st_id  | date_max  | Currency | date_min | market
"""
# CREATE TABLE tiker_report
# (
# market VARCHAR(3) NOT NULL,
# tiker VARCHAR(6) NOT NULL,
# name VARCHAR(6) NOT NULL,
# branch VARCHAR(6) NOT NULL,
# today_close INT(4) NOT NULL,
# min_dek1 INT(4) NOT NULL,
# min_dek2 INT(4) NOT NULL,
# min_dek3 INT(4) NOT NULL,
# min_dek4 INT(4) NOT NULL,
# min_dek5 INT(4) NOT NULL,
# min_dek6 INT(4) NOT NULL,
# min_dek7 INT(4) NOT NULL,
# min_dek8 INT(4) NOT NULL,
# min_pr_dek1 INT(2) NOT NULL,
# min_pr_dek2 INT(2) NOT NULL,
# min_pr_dek3 INT(2) NOT NULL,
# min_pr_dek4 INT(2) NOT NULL,
# min_pr_dek5 INT(2) NOT NULL,
# min_pr_dek6 INT(2) NOT NULL,
# min_pr_dek7 INT(2) NOT NULL,
# min_pr_dek8 INT(2) NOT NULL,
# min_pr_delta_1_2 INT(2) NOT NULL,
# min_pr_delta_2_3 INT(2) NOT NULL,
# min_pr_delta_3_4 INT(2) NOT NULL,
# min_pr_delta_4_5 INT(2) NOT NULL,
# min_pr_delta_5_6 INT(2) NOT NULL,
# min_pr_delta_6_7 INT(2) NOT NULL,
# min_pr_delta_7_8 INT(2) NOT NULL,
# max_dek1 INT(4) NOT NULL,
# max_dek2 INT(4) NOT NULL,
# max_dek3 INT(4) NOT NULL,
# max_dek4 INT(4) NOT NULL,
# max_dek5 INT(4) NOT NULL,
# max_dek6 INT(4) NOT NULL,
# max_dek7 INT(4) NOT NULL,
# max_dek8 INT(4) NOT NULL,
# max_pr_dek1 INT(2) NOT NULL,
# max_pr_dek2 INT(2) NOT NULL,
# max_pr_dek3 INT(2) NOT NULL,
# max_pr_dek4 INT(2) NOT NULL,
# max_pr_dek5 INT(2) NOT NULL,
# max_pr_dek6 INT(2) NOT NULL,
# max_pr_dek7 INT(2) NOT NULL,
# max_pr_dek8 INT(2) NOT NULL,
# max_pr_delta_1_2 INT(2) NOT NULL,
# max_pr_delta_2_3 INT(2) NOT NULL,
# max_pr_delta_3_4 INT(2) NOT NULL,
# max_pr_delta_4_5 INT(2) NOT NULL,
# max_pr_delta_5_6 INT(2) NOT NULL,
# max_pr_delta_6_7 INT(2) NOT NULL,
# max_pr_delta_7_8 INT(2) NOT NULL,
# min_y INT(2) NOT NULL,
# max_y INT(2) NOT NULL,
# today_y_pr_min INT(2) NOT NULL,
# today_y_pr_max INT(2) NOT NULL,
# day_start DATE NOT NULL,
# day_close DATE NOT NULL,
# teh_daily_sel INT(2) NOT NULL,
# teh_daily_buy INT(2) NOT NULL,
# teh_weekly_sel INT(2) NOT NULL,
# teh_weekly_buy INT(2) NOT NULL,
# teh_monthly_sell INT(2) NOT NULL,
# teh_monthly_buy INT(2) NOT NULL,
# daily_sma_signal_200 VARCHAR(7) NOT NULL,
# daily_ema_signal_200 VARCHAR(7) NOT NULL,
# weekly_sma_signal_200 VARCHAR(7) NOT NULL,
# weekly_ema_signal_200 VARCHAR(7) NOT NULL,
# monthly_sma_signal_200 VARCHAR(7) NOT NULL,
# monthly_ema_signal_200 VARCHAR(7) NOT NULL,
# EPS INT(4) NOT NULL,
# P_E INT(4) NOT NULL,
# m1_max INT(4) NOT NULL,
# m1_min INT(4) NOT NULL,
# m3_max INT(4) NOT NULL,
# m3_min INT(4) NOT NULL,
# m6_max INT(4) NOT NULL,
# m6_min INT(4) NOT NULL,
# year1_max INT(4) NOT NULL,
# year1_min INT(4) NOT NULL,
# pr_30_day_max INT(4) NOT NULL,
# pr_30_day_min INT(4) NOT NULL,
# pr_90_day_max INT(4) NOT NULL,
# pr_90_day_min INT(4) NOT NULL,
# pr_6_m_max INT(4) NOT NULL,
# pr_6_m_min INT(4) NOT NULL,
# pr_1y_max INT(4) NOT NULL,
# pr_1y_min INT(4) NOT NULL
# );