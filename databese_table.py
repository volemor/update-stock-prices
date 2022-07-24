from sqlalchemy import Column, ForeignKey, Integer, String, Date, Float, create_engine
from sqlalchemy.ext.declarative import declarative_base

"""
модуль создания таблиц в базе данных.
"""
Base = declarative_base()


class BaseStatus(Base):
    __tablename__ = 'base_status'
    __tableargs__ = {
        'comment': 'статус обновления таблицы hist_date -- обобщенные значения'
    }
    index = Column(Integer)  # может удалить??
    st_id = Column(String(9), primary_key=True, comment='краткое наименование акции(тикер)')
    date_max = Column(Date, comment='Дата последнего значения')
    Currency = Column(String(3), comment='валюта обращения тикера')
    date_min = Column(Date, comment='Дата первого значения')
    market = Column(String(3), comment='рынок обращения тикера')


class Hist_data(Base):
    __tablename__ = 'hist_data'
    __tableargs__ = {
        'comment': 'historical data of stocks'
    }
    index = Column(Integer, primary_key=True)  # может удалить??
    Date = Column(Date, comment='Дата')
    Open = Column(Float, comment='Курс Open на дату')
    High = Column(Float, comment='Курс High на дату')
    Low = Column(Float, comment='Курс Low на дату')
    Close = Column(Float, comment='Курс Close на дату')
    Volume = Column(Float, comment='оборот Volume на дату')
    st_id = Column(String(9), comment='краткое наименование акции(тикер)')
    Currency = Column(String(3), comment='валюта обращения тикера')
    market = Column(String(3), comment='рынок обращения тикера')


class Teh_an(Base):
    __tablename__ = 'teh_an'
    __tableargs__ = {
        'comment': 'данные теханализа по тикерам'
    }

    index = Column(Integer)  # может удалить??
    date = Column(Date, primary_key=True, comment='Дата расчета данных теханализа')
    st_id = Column(String(9), comment='краткое наименование акции(тикер)')
    teh_daily_sel = Column(Integer, comment='число dayly индикаторов продавать')
    teh_daily_buy = Column(Integer, comment='число dayly индикаторов покупать')
    teh_weekly_sel = Column(Integer, comment='число weekly индикаторов продавать')
    teh_weekly_buy = Column(Integer, comment='число weekly индикаторов покупать')
    teh_monthly_sell = Column(Integer, comment='число monthly индикаторов продавать')
    teh_monthly_buy = Column(Integer, comment='число monthly индикаторов покупать')
    daily_sma_signal_200 = Column(String(4), comment='dayly сигнал по простым скользящим средним')
    daily_ema_signal_200 = Column(String(4), comment='dayly сигнал по экспоненциальным скользящим средним')
    weekly_sma_signal_200 = Column(String(4), comment='weekly сигнал по простым скользящим средним')
    weekly_ema_signal_200 = Column(String(4), comment='weekly сигнал по экспоненциальным скользящим средним')
    monthly_sma_signal_200 = Column(String(4), comment='monthly сигнал по простым скользящим средним')
    monthly_ema_signal_200 = Column(String(4), comment='monthly сигнал по экспоненциальным скользящим средним')
    EPS = Column(Float, comment='значение индикатора ESP')
    P_E = Column(Float, comment='значение индикатора P/E')


class Tiker_branch(Base):
    __tablename__ = 'tiker_branch'
    __tableargs__ = {
        'comment': 'распределение тикеров по отраслям, валютам, рынкам, полное наименование тикеров'
    }
    index = Column(Integer)  # может удалить??
    st_id = Column(String(9), primary_key=True, comment='краткое наименование акции(тикер)')
    Currency = Column(String(3), comment='валюта обращения тикера')
    market = Column(String(3), comment='рынок обращения тикера')
    name = Column(String(80), comment='полное наименование тикера')
    branch = Column(String(50), comment='отрасль тикера')


class Tiker_report(Base):
    __tablename__ = 'tiker_report'
    __tableargs__ = {
        'comment': 'Расчетные значения по тикерам для формирования отчетной таблицы'
    }
    market = Column(String(3), comment='рынок обращения тикера')
    tiker = Column(String(9), primary_key=True, comment='краткое наименование акции(тикер)')
    name = Column(String(80), comment='полное наименование тикера')
    branch = Column(String(50), comment='отрасль тикера')
    today_close = Column(Float, comment='цена закрытия на дату закрытия')
    min_dek1 = Column(Float, comment='минимальная цена за 1 период 0:-5 дней')
    min_dek2 = Column(Float, comment='минимальная цена за 2 период -6:-10 дней')
    min_dek3 = Column(Float, comment='минимальная цена за 3 период -11:-15 дней')
    min_dek4 = Column(Float, comment='минимальная цена за 4 период -16:-20 дней')
    min_dek5 = Column(Float, comment='минимальная цена за 5 период -21:-30 дней')
    min_dek6 = Column(Float, comment='минимальная цена за 6 период -31:-40 дней')
    min_dek7 = Column(Float, comment='минимальная цена за 7 период -41:-50 дней')
    min_dek8 = Column(Float, comment='минимальная цена за 8 период -51:-60 дней')
    min_pr_dek1 = Column(Integer, comment='процент изменения стоимости закрытия к минимальной за период 1')
    min_pr_dek2 = Column(Integer, comment='процент изменения стоимости закрытия к минимальной за период 2')
    min_pr_dek3 = Column(Integer, comment='процент изменения стоимости закрытия к минимальной за период 3')
    min_pr_dek4 = Column(Integer, comment='процент изменения стоимости закрытия к минимальной за период 4')
    min_pr_dek5 = Column(Integer, comment='процент изменения стоимости закрытия к минимальной за период 5')
    min_pr_dek6 = Column(Integer, comment='процент изменения стоимости закрытия к минимальной за период 6')
    min_pr_dek7 = Column(Integer, comment='процент изменения стоимости закрытия к минимальной за период 7')
    min_pr_dek8 = Column(Integer, comment='процент изменения стоимости закрытия к минимальной за период 8')
    min_pr_delta_1_2 = Column(Integer, comment='изменение в процентах между периодами 1 и 2')
    min_pr_delta_2_3 = Column(Integer, comment='изменение в процентах между периодами 2 и 3')
    min_pr_delta_3_4 = Column(Integer, comment='изменение в процентах между периодами 3 и 4')
    min_pr_delta_4_5 = Column(Integer, comment='изменение в процентах между периодами 4 и 5')
    min_pr_delta_5_6 = Column(Integer, comment='изменение в процентах между периодами 5 и 6')
    min_pr_delta_6_7 = Column(Integer, comment='изменение в процентах между периодами 6 и 7')
    min_pr_delta_7_8 = Column(Integer, comment='изменение в процентах между периодами 7 и 8')
    max_dek1 = Column(Float, comment=' максимальная цена за 1 период 0:-5 дней')
    max_dek2 = Column(Float, comment=' максимальная цена за 2 период -6:-10 дней')
    max_dek3 = Column(Float, comment=' максимальная цена за 3 период -11:-15 дней')
    max_dek4 = Column(Float, comment=' максимальная цена за 4 период -16:-20 дней')
    max_dek5 = Column(Float, comment=' максимальная цена за 5 период -21:-30 дней')
    max_dek6 = Column(Float, comment=' максимальная цена за 6 период -31:-40 дней')
    max_dek7 = Column(Float, comment=' максимальная цена за 7 период -41:-50 дней')
    max_dek8 = Column(Float, comment=' максимальная цена за 8 период -51:-60 дней')
    max_pr_dek1 = Column(Integer, comment='процент изменения стоимости закрытия к максимальной за период 1')
    max_pr_dek2 = Column(Integer, comment='процент изменения стоимости закрытия к максимальной за период 2')
    max_pr_dek3 = Column(Integer, comment='процент изменения стоимости закрытия к максимальной за период 3')
    max_pr_dek4 = Column(Integer, comment='процент изменения стоимости закрытия к максимальной за период 4')
    max_pr_dek5 = Column(Integer, comment='процент изменения стоимости закрытия к максимальной за период 5')
    max_pr_dek6 = Column(Integer, comment='процент изменения стоимости закрытия к максимальной за период 6')
    max_pr_dek7 = Column(Integer, comment='процент изменения стоимости закрытия к максимальной за период 7')
    max_pr_dek8 = Column(Integer, comment='процент изменения стоимости закрытия к максимальной за период 8')
    max_pr_delta_1_2 = Column(Integer, comment='изменение в процентах между периодами 1 и 2')
    max_pr_delta_2_3 = Column(Integer, comment='изменение в процентах между периодами 2 и 3')
    max_pr_delta_3_4 = Column(Integer, comment='изменение в процентах между периодами 3 и 4')
    max_pr_delta_4_5 = Column(Integer, comment='изменение в процентах между периодами 4 и 5')
    max_pr_delta_5_6 = Column(Integer, comment='изменение в процентах между периодами 5 и 6')
    max_pr_delta_6_7 = Column(Integer, comment='изменение в процентах между периодами 6 и 7')
    max_pr_delta_7_8 = Column(Integer, comment='изменение в процентах между периодами 7 и 8')
    min_y = Column(Float, comment=' минимальная цена с day_start')
    max_y = Column(Float, comment=' максимальная цена с day_start')
    today_y_pr_min = Column(Integer, comment='процент изменение минимальной (с day_start) и цены закрытия')
    today_y_pr_max = Column(Integer, comment='процент изменение максимальной (с day_start) и цены закрытия')
    day_start = Column(Date, comment='дата начальных значений')
    day_close = Column(Date, comment='дата последних значений')
    teh_daily_sel = Column(Integer, comment='число dayly индикаторов продавать')
    teh_daily_buy = Column(Integer, comment='число dayly индикаторов покупать')
    teh_weekly_sel = Column(Integer, comment='число weekly индикаторов продавать')
    teh_weekly_buy = Column(Integer, comment='число weekly индикаторов покупать')
    teh_monthly_sell = Column(Integer, comment='число monthly индикаторов продавать')
    teh_monthly_buy = Column(Integer, comment='число monthly индикаторов покупать')
    daily_sma_signal_200 = Column(String(4), comment='dayly сигнал по простым скользящим средним')
    daily_ema_signal_200 = Column(String(4), comment='dayly сигнал по экспоненциальным скользящим средним')
    weekly_sma_signal_200 = Column(String(4), comment='weekly сигнал по простым скользящим средним')
    weekly_ema_signal_200 = Column(String(4), comment='weekly сигнал по экспоненциальным скользящим средним')
    monthly_sma_signal_200 = Column(String(4), comment='monthly сигнал по простым скользящим средним')
    monthly_ema_signal_200 = Column(String(4), comment='monthly сигнал по экспоненциальным скользящим средним')
    EPS = Column(Float, comment='значение индикатора ESP')
    P_E = Column(Float, comment='значение индикатора P/E')
    m1_max = Column(Float, comment='максимальная цена за 30 дней')
    m1_min = Column(Float, comment='минимальная цена за 30 дней')
    m3_max = Column(Float, comment='максимальная цена за 90 дней')
    m3_min = Column(Float, comment='минимальная цена за 90 дней')
    m6_max = Column(Float, comment='максимальная цена за 180 дней')
    m6_min = Column(Float, comment='минимальная цена за 180 дней')
    year1_max = Column(Float, comment='максимальная цена за 360 дней')
    year1_min = Column(Float, comment='минимальная цена за 360 дней')
    pr_30_day_max = Column(Integer, comment='процент изменение максимальной за 30 дней и цены закрытия')
    pr_30_day_min = Column(Integer, comment='процент изменение минимальной за 30 дней и цены закрытия')
    pr_90_day_max = Column(Integer, comment='процент изменение максимальной за 90 дней и цены закрытия')
    pr_90_day_min = Column(Integer, comment='процент изменение минимальной за 90 дней и цены закрытия')
    pr_6_m_max = Column(Integer, comment='процент изменение максимальной за 180 дней и цены закрытия')
    pr_6_m_min = Column(Integer, comment='процент изменение минимальной за 180 дней и цены закрытия')
    pr_1y_max = Column(Integer, comment='процент изменение максимальной за 360 дней и цены закрытия')
    pr_1y_min = Column(Integer, comment='процент изменение минимальной за 360 дней и цены закрытия')


databases_name = 'hist_data_test'


# create_database_com = f'create database {databases_name};'


def create_database():
    with sqlalchemy.create_engine(
            "mysql+pymysql://root:root@192.168.0.118:3306/",
            isolation_level='AUTOCOMMIT'
    ).connect() as connection:
        connection.execute(f'CREATE DATABASE {databases_name} charset="utf8"')

        database_user_rights = f"grant all privileges on {databases_name}.* to 'python'@'%' IDENTIFIED BY 'python';"
        connection.execute(database_user_rights)


server_ip = '192.168.0.118'
user = 'python'
password = 'python'
sql_login = f'mysql+pymysql://{user}:{password}@{server_ip}/{databases_name}'

engine = create_engine(sql_login)

Base.metadata.create_all(engine)
