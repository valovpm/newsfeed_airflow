# coding=utf-8
from airflow.decorators import dag, task
from datetime import date, datetime, timedelta
import io
import matplotlib.pyplot as plt
import os
import pandahouse as ph
import pandas as pd
import seaborn as sns
import string
import sys
import telegram
from textwrap import dedent

sns.set()


# Connection with 'simulator_20221220' database
connection_simulator = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database': 'simulator_20221220',
    'user': 'student',
    'password': 'dpo_python_2020'
}

#
query_feed = \
"""
SELECT
      toStartOfFifteenMinutes(time) as t_15m
    , toDate(time) as t_date
    , formatDateTime(t_15m, '%R') as t_hm
    , uniqExact(user_id) as feed_dau
    , countIf(user_id, action='view') as feed_views
    , countIf(user_id, action='like') as feed_likes
    , round(feed_likes / feed_views, 4) as feed_ctr
    , round(feed_likes / feed_dau, 2) as feed_lpu
    , round(feed_views / feed_dau, 2) as feed_vpu
FROM simulator_20221220.feed_actions
WHERE time >= today() - 1
    AND time < toStartOfFifteenMinutes(now())
GROUP BY t_15m, t_date, t_hm
ORDER BY t_15m
"""

query_mssg = \
"""
SELECT
      toStartOfFifteenMinutes(time) as t_15m
    , toDate(time) as t_date
    , formatDateTime(t_15m, '%R') as t_hm
    , uniqExact(user_id) as mssg_dau
    , count(user_id) as mssg_messages
    , round(mssg_messages / mssg_dau, 2) as mssg_mpu
FROM simulator_20221220.message_actions
WHERE time >= today() - 1
    AND time < toStartOfFifteenMinutes(now())
GROUP BY t_15m, t_date, t_hm
ORDER BY t_15m
"""

# Default arguments for Airflow tasks
default_args = {
    'owner': 'p-valov-14',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 2, 2),
}

# Расписание для DAG'ов
# Ежедневно в 11:00
# schedule_interval = '0 11 * * *'
# Ежечасно
# schedule_interval = '0 * * * *'
# Каждые 15 минут
schedule_interval = '*/15 * * * *'

# Список анализируемых метрик
metrics = {
    'feed_dau': {
        'alias': 'feed_dau',
        'name': 'DAU (новостная лента)',
        'data': 'feed'
    },

    'feed_views': {
        'alias': 'feed_views',
        'name': 'Просмотры (новостная лента)',
        'data': 'feed'
    },

    'feed_likes': {
        'alias': 'feed_likes',
        'name': 'Лайки (новостная лента)',
        'data': 'feed'
    },

    'feed_ctr': {
        'alias': 'feed_ctr',
        'name': 'CTR (новостная лента)',
        'data': 'feed'
    },

    'feed_lpu': {
        'alias': 'feed_lpu',
        'name': 'LPU, кол-во лайков на пользователя (новостная лента)',
        'data': 'feed'
    },

    'feed_vpu': {
        'alias': 'feed_vpu',
        'name': 'VPU, кол-во просмотров на пользователя (новостная лента)',
        'data': 'feed'
    },

    'mssg_dau': {
        'alias': 'mssg_dau',
        'name': 'DAU (мессенджер)',
        'data': 'mssg'
    },

    'mssg_messages': {
        'alias': 'mssg_messages',
        'name': 'Кол-во сообщений (мессенджер)',
        'data': 'mssg'
    },

    'mssg_mpu': {
        'alias': 'mssg_mpu',
        'name': 'MPU, кол-во сообщений на пользователя (мессенджер)',
        'data': 'mssg'
    }
}


@dag(default_args=default_args,
     schedule_interval=schedule_interval,
     catchup=False)
def p_valov_14_lesson_8_task_1():

    @task()
    def extract_feed():
        """
        Извлечь данные по новостной ленте
        за предыдущие и текущие сутки с интервалом по 15 минут

        :return: dataframe с данными
        """
        df_feed = ph.read_clickhouse(
            query_feed,
            connection=connection_simulator)
        return df_feed

    @task()
    def extract_mssg():
        """
        Извлечь данные по сервису сообщений
        за предыдущие и текущие сутки с интервалом по 15 минут

        :return: dataframe с данными
        """
        df_mssg = ph.read_clickhouse(
            query_mssg,
            connection=connection_simulator)
        return df_mssg

    def check_for_anomaly(df, metric, a=5, n=5):
        """
        Проверить данные на наличие аномалии

        :param df: данные для анализа на аномалии
        :param metric: целевая метрика
        :param a: коэффициент ширины интервала
        :param n: количество временных промежутков

        :return is_anomaly: найдена ли аномалия
        :return df: исходные данные, с добавленными интервалами для визуализации
        """

        # Рассчитать квантили, верхние и нижние границы аномалии
        df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
        df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
        df['iqr'] = df['q75'] - df['q25']
        df['upper'] = df['q75'] + a * df['iqr']
        df['lower'] = df['q25'] - a * df['iqr']

        # Сгладить квантили
        df['upper'] = df['upper'].rolling(n, center=True, min_periods=1).mean()
        df['lower'] = df['lower'].rolling(n, center=True, min_periods=1).mean()

        # Значение метрики, которое будем анализировать
        value = df[metric].iloc[-1]
        lower = df['lower'].iloc[-1]
        upper = df['upper'].iloc[-1]
        is_anomaly = False

        # Если значение метрики выходит за границы, значит это аномалия
        if value < lower or value > upper:
            is_anomaly = True

        return is_anomaly, df

    @task()
    def transform_get_report(df_feed, df_mssg):
        """
        Получить отчеты по аномалиям в метриках

        :param df_feed: Данные по ленте новостей за предыдущие и текущие сутки
                        с интервалом по 15 минут
        :param df_mssg: Данные по мессенджеру за предыдущие и текущие сутки
                        с интервалом по 15 минут

        :return reports: отчеты по аномалиям
        """

        # Данные для отчетов по аномалиям
        reports = {}

        for key, metric in metrics.items():
            # Выбрать данные для метрики
            data = df_feed if metric['data'] == 'feed' else df_mssg

            # Проверить на наличие аномалии

            df = data[['t_15m', 't_date', 't_hm', metric['alias']]].copy()
            is_anomaly, df = check_for_anomaly(df, metric['alias'])

            # Отправить отчет при аномалии
            if is_anomaly:
                value_curr = df[metric['alias']].iloc[-1]
                value_prev = df[metric['alias']].iloc[-2]
                value_diff = abs(1 - value_curr / value_prev)

                text =("НАЙДЕНА АНОМАЛИЯ:\n"
                       "Метрика: {name}\n"
                       "Текущее значение: {curr}\n"
                       "Отклонение от предыдущего значения: {diff:.2%}")

                text = text.format(name=metric['name'], curr=value_curr, diff=value_diff)

                # Сформировать график
                sns.set(rc={'figure.figsize': (16, 10)})
                # plt.tight_layout()

                ax = sns.lineplot(x=df['t_15m'], y=df[metric['alias']], label='metric')
                ax = sns.lineplot(x=df['t_15m'], y=df['upper'], label='upper')
                ax = sns.lineplot(x=df['t_15m'], y=df['lower'], label='lower')

                # xlabels = df['t_15m'].dt.strftime('%Y-%m-%d %H:%M').sort_values()  # .unique()
                plt.xticks(df['t_15m'], df['t_hm'], rotation=45, ha='right', fontsize=8)

                for index, label in enumerate(ax.get_xticklabels()):
                    if index % 2 == 0:
                        label.set_visible(True)
                    else:
                        label.set_visible(False)

                ax.set(xlabel='Время')
                ax.set(ylabel=metric['name'])
                ax.set(ylim=(0, None))
                ax.set(xlim=(df['t_15m'].min(), df['t_15m'].max() + timedelta(minutes=15)))
                date_min = df['t_15m'].min().strftime('%Y-%m-%d %H:%M')
                date_max = df['t_15m'].max().strftime('%Y-%m-%d %H:%M')
                ax.set_title(f"{metric['name']} за {date_min} / {date_max}")

                plot_object = io.BytesIO()
                ax.figure.savefig(plot_object)
                plot_object.name = f"{metric['alias']}.png"
                plot_object.seek(0)
                plt.close()

                reports[metric['alias']] = {
                    'text': text,
                    'plot': plot_object
                }

        return reports

    @task()
    def load(reports):
        """
        Отправить отчеты в Телеграм

        :param reports: отчеты по аномалиям
        :return: None
        """
        
        # Использовать предоставленный chat_id
        # или сохраненный chat_id
        pv_id = 502947799  # личный chat_id
        kc_id = -850804180  # групповой chat_id
        chat_id = kc_id  # chat_id or pv_id

        # Инициализировать бота для Телеграм
        bot_token = 'TOKEN'
        bot = telegram.Bot(token=bot_token)

        # Отправить отчеты по аномалиям
        for alias, report in reports.items():
            # bot.sendPhoto(chat_id=pv_id,
            #               photo=report['plot'],
            #               caption=report['text'])

            bot.sendPhoto(chat_id=kc_id,
                          photo=report['plot'],
                          caption=report['text'])

    # Извлечь данные
    df_feed = extract_feed()
    df_mssg = extract_mssg()

    # Проанализировать данные и получить отчеты по аномалиям
    reports = transform_get_report(df_feed, df_mssg)

    # Отправить отчеты в Телеграм
    load(reports)


p_valov_14_lesson_8_task_1 = p_valov_14_lesson_8_task_1()
