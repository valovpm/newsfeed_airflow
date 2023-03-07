# coding=utf-8
import io
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
import matplotlib.pyplot as plt
import seaborn as sns
import telegram
import os

sns.set()

# Соединение с базой 'simulator_20221220',
# для получения данных
connection_simulator = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database': 'simulator_20221220',
    'user': 'student',
    'password': 'dpo_python_2020'
}

# Соединение с базой 'test',
# для отправки данных
connection_test = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database': 'test',
    'user': 'student-rw',
    'password': '656e2b0c9c'
}

# Запрос данных по новостному фиду
query_feed_yesterday = \
"""
SELECT
    toDate(time) datetime,
    count(DISTINCT user_id) DAU,
    countIf(user_id, action = 'view') views,
    countIf(user_id, action = 'like') likes,
    likes / views CTR
FROM simulator_20221220.feed_actions
WHERE datetime >= today() - 1
AND datetime <= today() - 1
GROUP BY datetime
ORDER BY datetime DESC
"""

query_feed_previous_7 = \
"""
SELECT
    toDate(time) datetime,
    count(DISTINCT user_id) DAU,
    countIf(user_id, action = 'view') views,
    countIf(user_id, action = 'like') likes,
    likes / views CTR
FROM simulator_20221220.feed_actions
WHERE datetime >= today() - 7
AND datetime <= today() - 1
GROUP BY datetime
ORDER BY datetime DESC
"""

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'p-valov-14',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 2, 2),
}

# Интервал запуска DAG
# Запускать каждый день в 11:00
schedule_interval = '0 11 * * *'
# Запускать каждый час
# schedule_interval = '0 * * * *'

@dag(default_args=default_args,
     schedule_interval=schedule_interval,
     catchup=False)
def p_valov_14_lesson_7_task_1():

    @task()
    def extract_feed_yesterday():
        """
        Извлечь данные по новостной ленте за вчерашний день

        :return: dataframe с данными
        """
        df_feed_yesterday = ph.read_clickhouse(
            query_feed_yesterday,
            connection=connection_simulator)
        return df_feed_yesterday

    @task()
    def extract_feed_previous_7():
        """
        Извлечь данные по новостной ленте за предыдущие 7 дней

        :return: dataframe с данными
        """
        df_feed_previous_7 = ph.read_clickhouse(
            query_feed_previous_7,
            connection=connection_simulator)
        return df_feed_previous_7

    @task()
    def transform_get_telegram_text(df_feed_yesterday):
        """
        Сформировать текст сообщения для Telegram

        :param df_feed_yesterday: dataframe с данными за предыдущий день
        :return: текст сообщения для Telegram
        """

        text = \
            "Отчет по ленте новостей за {0:%Y-%m-%d}:\n" \
            "DAU: {1:,}\n" \
            "Просмотры: {2:,}\n" \
            "Лайки: {3:,}\n" \
            "CTR: {4:.2%}".format(
                df_feed_yesterday.datetime[0],
                df_feed_yesterday.DAU[0],
                df_feed_yesterday.views[0],
                df_feed_yesterday.likes[0],
                df_feed_yesterday.CTR[0]
            )

        return text

    @task()
    def transform_get_telegram_graphs(df_feed_previous_7):

        fig, axes = plt.subplots(2, 2, figsize = (16, 9))
        fig.suptitle('')

        plots = {
            (0, 0): {'y': 'DAU', 'title': 'DAU (Уникальные пользователи)'},
            (0, 1): {'y': 'views', 'title': 'Просмотры'},
            (1, 0): {'y': 'likes', 'title': 'Лайки'},
            (1, 1): {'y': 'CTR', 'title': 'CTR (отношение лайков к просмотрам)'}
        }

        for i in range(2):
            for j in range(2):
                sns.lineplot(ax=axes[i, j], data=df_feed_previous_7,
                             x='datetime', y=plots[(i, j)]['y'])
                axes[i, j].set_title(plots[(i, j)]['title'])
                axes[i, j].set(xlabel=None)
                axes[i, j].set(ylabel=None)

                for index, label in enumerate(axes[i, j].get_xticklabels()):
                    if index % 3 == 0:
                        label.set_visible(True)
                    else:
                        label.set_visible(False)

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.name = 'feed_stat.png'
        plot_object.seek(0)
        plt.close()
        return plot_object

    @task()
    def load(telegram_text, telegram_graphs):

        # Инициализировать бот для Телеграм
        my_token = 'TOKEN'
        bot = telegram.Bot(token=my_token)

        pv_id = 502947799
        kc_id = -850804180
        # bot.sendMessage(chat_id=kc_id, text=telegram_text)
        bot.sendPhoto(chat_id=kc_id, photo=telegram_graphs,
                      caption=telegram_text)

    df_feed_yesterday = extract_feed_yesterday()
    df_feed_previous_7 = extract_feed_previous_7()

    telegram_text = transform_get_telegram_text(df_feed_yesterday)
    telegram_graphs = transform_get_telegram_graphs(df_feed_previous_7)

    print(telegram_text)
    load(telegram_text, telegram_graphs)


p_valov_14_lesson_7_task_1 = p_valov_14_lesson_7_task_1()
