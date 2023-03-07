# coding=utf-8
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph


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
query_feed = \
"""
SELECT DISTINCT
    toDate(time) event_date,
    user_id,
    os,
    gender,
    age,
    countIf(action, action = 'view') views,
    countIf(action, action = 'like') likes
FROM simulator_20221220.feed_actions
WHERE toDate(time) = today() - 1
GROUP BY toDate(time), user_id, os, gender, age
ORDER BY toDate(time), user_id, os, gender, age
"""

# Запрос данных по сообщениям
query_message = \
"""
WITH
-- Пользователи
users AS (
SELECT DISTINCT
    toDate(time) event_date,
    user_id,
    gender,
    age,
    os
FROM simulator_20221220.message_actions
WHERE toDate(time) = today() - 1
ORDER BY user_id),

-- Количество ПОЛУЧЕННЫХ сообщений для каждого пользователя
inbox AS (
SELECT reciever_id, COUNT(*) n
FROM simulator_20221220.message_actions
WHERE toDate(time) = today() - 1
GROUP BY reciever_id
ORDER BY reciever_id),

-- Количество ОТПРАВЛЕННЫХ сообщений для каждого пользователя
outbox AS (
SELECT user_id, COUNT(*) n
FROM simulator_20221220.message_actions
WHERE toDate(time) = today() - 1
GROUP BY user_id
ORDER BY user_id),

-- Отправители и получатели
sender_reciever AS (
SELECT user_id sender, reciever_id reciever
FROM simulator_20221220.message_actions
WHERE toDate(time) = today() - 1
GROUP BY user_id, reciever_id
ORDER BY user_id, reciever_id),

-- Количество ОТПРАВИТЕЛЕЙ для каждого получателя
sender_count AS (
SELECT reciever, COUNT(sender) n
FROM sender_reciever
GROUP BY reciever
ORDER BY reciever),

-- Количество ПОЛУЧАТЕЛЕЙ для каждого отправителя
reciever_count AS (
SELECT sender, COUNT(reciever) n
FROM sender_reciever
GROUP BY sender
ORDER BY sender)

SELECT DISTINCT
    u.event_date event_date,
    u.user_id user_id,
    u.gender gender,
    u.age age,
    u.os os,
    inbox.n messages_received,
    outbox.n messages_sent,
    sender_count.n users_received,
    reciever_count.n users_sent

FROM users u
LEFT JOIN inbox
ON u.user_id = inbox.reciever_id
LEFT JOIN outbox
ON u.user_id = outbox.user_id
LEFT JOIN sender_count
ON u.user_id = sender_count.reciever
LEFT JOIN reciever_count
ON u.user_id = reciever_count.sender

ORDER BY u.user_id
"""

query_create_table = \
'''
CREATE TABLE IF NOT EXISTS test.p_valov_14_lesson_6
(
    event_date Date NOT NULL,
    dimension String NOT NULL,
    dimension_value String NOT NULL,
    views Int32 NOT NULL,
    likes Int32 NOT NULL,
    messages_received Int32 NOT NULL,
    messages_sent Int32 NOT NULL,
    users_received Int32 NOT NULL,
    users_sent Int32 NOT NULL
)
ENGINE = MergeTree()
PRIMARY KEY (event_date, dimension, dimension_value)
ORDER BY (event_date, dimension, dimension_value)
'''

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'p-valov-14',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 1, 31),
}

# Интервал запуска DAG
# Запускать каждый день в 00:00
schedule_interval = '0 0 * * *'

@dag(default_args=default_args,
     schedule_interval=schedule_interval,
     catchup=False)
def p_valov_14_final():

    @task()
    def extract_feed():
        """
        Извлечь данные по новостной ленте

        :return: dataframe с данными
        """
        df_feed = ph.read_clickhouse(
            query_feed,
            connection=connection_simulator)
        return df_feed

    @task()
    def extract_message():
        """
        Извлечь данные по сервису сообщений

        :return: dataframe с данными
        """
        df_message = ph.read_clickhouse(
            query_message,
            connection=connection_simulator)
        return df_message

    @task()
    def transform_join_feed_message(df_feed, df_message):
        """
        Объединить таблицы с данными по новостям и сообщениям

        :param df_feed: dataframe с данными по новостям
        :param df_message: dataframe с данными по сообщениям
        :return:
        """

        # Объединить таблицы
        df_joined = df_feed.merge(
            df_message, how='outer',
            on=['event_date', 'user_id', 'gender', 'age', 'os'])
        df_joined.fillna(0, inplace=True)

        # Привести данные к целочисленному типу
        cols = ['views', 'likes', 'messages_received', 'messages_sent',
                'users_received', 'users_sent']
        for col in cols:
            df_joined[col] = df_joined[col].round().astype('int')

        return df_joined

    @task()
    def transform_get_slice(df_joined, dimension):
        """
        Получить срез данных по указанному измерению

        :param df_joined: dataframe с данными по новостям и сообщениям
        :param dimension: измерение, по которому необходимо произвести срез
        :return:
        """
        group_cols = ['event_date', dimension]
        df_slice = df_joined.groupby(group_cols, as_index=False).agg(
            views=pd.NamedAgg(column='views', aggfunc='sum'),
            likes=pd.NamedAgg(column='likes', aggfunc='sum'),
            messages_received=pd.NamedAgg(column='messages_received', aggfunc='sum'),
            messages_sent=pd.NamedAgg(column='messages_sent', aggfunc='sum'),
            users_received=pd.NamedAgg(column='users_received', aggfunc='sum'),
            users_sent=pd.NamedAgg(column='users_sent', aggfunc='sum')
        )
        df_slice.rename(columns={dimension: 'dimension_value'}, inplace=True)
        df_slice.insert(loc=1, column='dimension', value=dimension)
        return df_slice

    @task()
    def load(df_slice_gender, df_slice_age, df_slice_os):

        # Объединить различные срезы данных
        df_slices = pd.concat([df_slice_gender, df_slice_age, df_slice_os])

        # Создать таблицу в базе данных, если уже не существует
        ph.execute(query=query_create_table, connection=connection_test)

        # Выгрузить объединенные данные в созданную таблицу
        ph.to_clickhouse(df_slices, table='p_valov_14_lesson_6',
                         connection=connection_test, index=False)

    df_feed = extract_feed()
    df_message = extract_message()
    df_joined = transform_join_feed_message(df_feed, df_message)
    df_slice_gender = transform_get_slice(df_joined, 'gender')
    df_slice_age = transform_get_slice(df_joined, 'age')
    df_slice_os = transform_get_slice(df_joined, 'os')
    load(df_slice_gender, df_slice_age, df_slice_os)

p_valov_14_final = p_valov_14_final()
