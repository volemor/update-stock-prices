# my telegramm bot
"""
телеграмм бот для мониторинга системы 'ban'
команды бота - описание:
start - ps axf|grep python3
up_log - update_log
netstat - netstat 22 port
info - user info
bot_modul_update - подгрузка модулей
tiker_report_status - report_status
"""

import os
import telebot
from tendo import singleton
import time
import datetime
from sqlalchemy import create_engine
from my_os_test_config import *

sql_login = 'mysql+pymysql://python:python@192.168.0.118/hist_data'
db_connection = create_engine(sql_login, connect_args={'connect_timeout': 10})
import pandas as pd

if os.name == 'nt':  # проверяем из под чего загрузка.
    proj_path = 'W:\My_Python\st_US'
    print("start from WINDOWS")
    exit()
else:
    proj_path = '/mnt/1T/opt/gig/My_Python/st_US/'
    print("start from LINUX")

me = singleton.SingleInstance()  ### проверка на работу и запуск альтернативной версии скрипта - чтоб не задвоялась

# проверка собстенными силами.. не всегда может сработать - имя поменять и все..
processoutput = os.popen("ps -axf").read()
my_list = processoutput.split('\n')
count_my_prog = 0
for index in my_list:
    if 'python3' in index:
        # my_process_py += index + '\n'
        if 'my_os_test.py' in index:
            count_my_prog += 1
        if count_my_prog == 2:
            print('pogramm allready run!!!')
            exit()




def my_monitor():
    '''
    программа для мониторинга запущенных процессов в системе.
    если нужного процесса нет - то.... пробуем запустить, или прочитать лог.. и по итогам может помощь позвать?? телеграммом.
    наверное запускается через крон -
    пока прототип
    '''
    processoutput = os.popen("ps -axf").read()
    # print(processoutput.split('\n'))
    my_list = processoutput.split('\n')
    netstat_my = os.popen('netstat -antp').read().split('\n')
    for index in netstat_my:
        pass
        print(index)
    my_tail_up_log = os.popen('tail -30 ./update-sql.log').read()
    # print(type(my_tail_up_log))


def save_user_info(message):
    log_contact = f'id:[{message.from_user.id}] first_name [{message.from_user.first_name}]'
    with open(proj_path + 'telebot.log', 'a') as file_log:
        file_log.write(log_contact + '\n')
    print(log_contact)


def save_exeption(exeption):
    with open(proj_path + 'telebot_ex.log', 'a') as file_log:
        file_log.write(f'[{datetime.date.today()}]' + str(exeption) + '\n')


# my_monitor()

bot = telebot.TeleBot(telegramm_token)
""" бот для запуска на линуксе и мониторинге """


def check_for_access(name):
    if str(name) in my_access_list:
        return True
    else:
        return False


@bot.message_handler(commands=['start'])
def start(message):
    # save_user_info(message)
    if check_for_access(message.from_user.id):
        my_process_py = ''
        processoutput = os.popen("ps -axf").read()
        my_list = processoutput.split('\n')
        for index in my_list:
            if 'python3' in index:
                my_process_py += index + '\n'
        bot.send_message(message.chat.id, my_process_py)
    else:
        bot.send_message(message.chat.id, 'запуск прошел успешно')


@bot.message_handler(commands=['up_log'])
def update_log_status(message):
    save_user_info(message)
    if check_for_access(message.from_user.id):
        bot.send_message(message.chat.id, os.popen('tail -19 /root/update-sql.log').read())
    else:
        bot.send_message(message.chat.id, 'все ок')


@bot.message_handler(commands=['netstat'])
def nenstat_status(message):
    save_user_info(message)
    if check_for_access(message.from_user.id):
        my_process_py = ''
        processoutput = os.popen("netstat -antp").read()
        my_list = processoutput.split('\n')
        for index in my_list:
            if 'ESTABLISHED' in index and ':22' in index:
                my_process_py += index + '\n'
        if len(my_process_py) == 0:
            my_process_py = 'сейчас нет соединений по 22 порту'
        bot.send_message(message.chat.id, my_process_py)
    else:
        bot.send_message(message.chat.id, 'нет соединений')


@bot.message_handler(commands=['info'])
def user_info(message):
    save_user_info(message)
    if check_for_access(message.from_user.id):
        log_contact = f'id:[{message.from_user.id}] first_name [{message.from_user.first_name}]'
        print(log_contact)
        bot.send_message(message.chat.id, message)
    else:
        bot.send_message(message.chat.id, 'нет инфы')


@bot.message_handler(commands=['bot_modul_update'])
def update_modul(message):
    if check_for_access(message.from_user.id):
        bot.send_message(message.chat.id, 'дополнительные модули подгружены')
    else:
        bot.send_message(message.chat.id, 'дополнительные модули не найдены')


@bot.message_handler(commands=['tiker_report_status'])
def user_info(message):
    # save_user_info(message)
    my_mes = 'tiker_report_status \n'
    if check_for_access(message.from_user.id):
        sql_message = 'Select tiker, max(day_close) as max_day_close, market from tiker_report group by tiker;'
        df = pd.read_sql(sql_message, con=db_connection)

        for market in df['market'].unique():
            statistik_list = pd.Series({c: df[df['market'] == market][c].unique() for c in df})
            statistik_list['max_day_close'].sort()
            # my_mes += ''.join([f'len max_day_close [{len(statistik_list[1])}]\n'])
            my_mes += ''.join([
                                  f"[{market}][{statistik_list.iat[1][-2]}][{len(df[(df['max_day_close'] == statistik_list.iat[1][-2]) & (df['market'] == market)]['tiker'])}]\n"])
            my_mes += ''.join([
                                  f"[{market}][{statistik_list.iat[1][-1]}][{len(df[(df['max_day_close'] == statistik_list.iat[1][-1]) & (df['market'] == market)]['tiker'])}]\n"])

        bot.send_message(message.chat.id, my_mes)
    else:
        bot.send_message(message.chat.id, 'нет инфы')


while True:
    try:
        bot.polling(none_stop=True)
    except Exception as _ex:
        save_exeption(_ex)
    time.sleep(10)
