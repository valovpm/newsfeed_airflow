# coding=utf-8
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import io
import matplotlib.pyplot as plt
import os
import pandahouse as ph
import pandas as pd
import seaborn as sns
import string
import telegram

sns.set()

# Connection with 'simulator_20221220' database
connection_simulator = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database': 'simulator_20221220',
    'user': 'student',
    'password': 'dpo_python_2020'
}

# Query data for BOTH newsfeed and messenger DAU
query_both_dau = \
"""
WITH data AS (
SELECT
    datetime,
    uniqExact(user_id) dau_total,
    uniqExactIf(user_id, os='iOS') dau_ios,
    uniqExactIf(user_id, os='Android') dau_drd
    
FROM (
    SELECT DISTINCT
        toDate(time) datetime,
        user_id,
        os
    FROM simulator_20221220.feed_actions
    WHERE datetime <= today() - 1
    
    UNION ALL
    
    SELECT DISTINCT
        toDate(time) datetime,
        user_id,
        os
    FROM simulator_20221220.message_actions
    WHERE datetime <= today() - 1
) as t
GROUP BY datetime
ORDER BY datetime DESC)

SELECT
    d.datetime datetime,
    d.dau_total dau_total,
    d.dau_ios dau_ios,
    d.dau_drd dau_drd,
    
    -- dau_total
    (d.dau_total - m1.dau_total) / m1.dau_total AS to_dau_total_day_ago,
    (d.dau_total - m7.dau_total) / m7.dau_total AS to_dau_total_week_ago,
    (d.dau_total - m30.dau_total) / m30.dau_total AS to_dau_total_month_ago,
    
    -- dau_ios
    (d.dau_ios - m1.dau_ios) / m1.dau_ios AS to_dau_ios_day_ago,
    (d.dau_ios - m7.dau_ios) / m7.dau_ios AS to_dau_ios_week_ago,
    (d.dau_ios - m30.dau_ios) / m30.dau_ios AS to_dau_ios_month_ago,
    
    -- dau_drd
    (d.dau_drd - m1.dau_drd) / m1.dau_drd AS to_dau_drd_day_ago,
    (d.dau_drd - m7.dau_drd) / m7.dau_drd AS to_dau_drd_week_ago,
    (d.dau_drd - m30.dau_drd) / m30.dau_drd AS to_dau_drd_month_ago

FROM data d
LEFT JOIN data m1
ON d.datetime = m1.datetime + 1
LEFT JOIN data m7
ON d.datetime = m7.datetime + 7
LEFT JOIN data m30
ON d.datetime = m30.datetime + 30
WHERE d.datetime >= today() - 31
"""

# Query data for BOTH newsfeed and messenger NEW USERS
query_both_new = \
"""
WITH data AS (
SELECT
      datetime
    , uniqExact(user_id) new_users
    , uniqExactIf(user_id, source='ads') new_users_ads
    , uniqExactIf(user_id, source='organic') new_users_org

FROM (
    SELECT
        user_id,
        source,
        min(min_datetime) datetime
    FROM (
        SELECT
            user_id,
            min(toDate(time)) min_datetime,
            source
        FROM simulator_20221220.feed_actions
        WHERE toDate(time) <= today() - 1
        GROUP BY user_id, source
        
        UNION ALL
        
        SELECT
            user_id,
            min(toDate(time)) min_datetime,
            source
        FROM simulator_20221220.message_actions
        WHERE toDate(time) <= today() - 1
        GROUP BY user_id, source
    ) t1
    GROUP BY user_id, source
) t2
GROUP BY datetime
ORDER BY datetime DESC)

SELECT
    d.datetime datetime,
    d.new_users new_users,
    d.new_users_ads new_users_ads,
    d.new_users_org new_users_org,
    
    -- new_users
    (d.new_users - m1.new_users) / m1.new_users AS to_users_total_day_ago,
    (d.new_users - m7.new_users) / m7.new_users AS to_users_total_week_ago,
    (d.new_users - m30.new_users) / m30.new_users AS to_users_total_month_ago,
        
    -- new_users_ads
    (d.new_users_ads - m1.new_users_ads) / m1.new_users_ads AS to_users_ads_day_ago,
    (d.new_users_ads - m7.new_users_ads) / m7.new_users_ads AS to_users_ads_week_ago,
    (d.new_users_ads - m30.new_users_ads) / m30.new_users_ads AS to_users_ads_month_ago,
        
    -- new_users_org
    (d.new_users_org - m1.new_users_org) / m1.new_users_org AS to_users_org_day_ago,
    (d.new_users_org - m7.new_users_org) / m7.new_users_org AS to_users_org_week_ago,
    (d.new_users_org - m30.new_users_org) / m30.new_users_org AS to_users_org_month_ago

FROM data d
LEFT JOIN data m1
ON d.datetime = m1.datetime + 1
LEFT JOIN data m7
ON d.datetime = m7.datetime + 7
LEFT JOIN data m30
ON d.datetime = m30.datetime + 30
WHERE d.datetime >= today() - 31
"""

# Query data for NEWSFEED
query_feed = \
"""
WITH data AS (
SELECT
    toDate(time) datetime,
    count(DISTINCT user_id) dau_feed,
    countIf(user_id, action = 'like') likes_feed,
    countIf(user_id, action = 'view') views_feed,
    likes_feed + views_feed events_feed,
    likes_feed / views_feed ctr_feed,
    likes_feed / dau_feed lpu_feed,
    count(DISTINCT post_id) posts_feed
FROM simulator_20221220.feed_actions
WHERE datetime <= today() - 1
-- WHERE datetime >= today() - 31
-- AND datetime <= today() - 1
GROUP BY datetime
ORDER BY datetime DESC)

SELECT
    d.datetime datetime,
    d.dau_feed dau_feed,
    d.likes_feed likes_feed,
    d.views_feed views_feed,
    d.events_feed events_feed,
    d.ctr_feed ctr_feed,
    d.lpu_feed lpu_feed,
    d.posts_feed posts_feed,
    
    -- dau_feed
    (d.dau_feed - m1.dau_feed) / m1.dau_feed AS to_dau_feed_day_ago,
    (d.dau_feed - m7.dau_feed) / m7.dau_feed AS to_dau_feed_week_ago,
    (d.dau_feed - m30.dau_feed) / m30.dau_feed AS to_dau_feed_month_ago,

    -- ctr_feed
    (d.ctr_feed - m1.ctr_feed) / m1.ctr_feed AS to_ctr_feed_day_ago,
    (d.ctr_feed - m7.ctr_feed) / m7.ctr_feed AS to_ctr_feed_week_ago,
    (d.ctr_feed - m30.ctr_feed) / m30.ctr_feed AS to_ctr_feed_month_ago,
    
    -- lpu_feed
    (d.lpu_feed - m1.lpu_feed) / m1.lpu_feed AS to_lpu_feed_day_ago,
    (d.lpu_feed - m7.lpu_feed) / m7.lpu_feed AS to_lpu_feed_week_ago,
    (d.lpu_feed - m30.lpu_feed) / m30.lpu_feed AS to_lpu_feed_month_ago,
    
    -- likes_feed
    (d.likes_feed - m1.likes_feed) / m1.likes_feed AS to_likes_feed_day_ago,
    (d.likes_feed - m7.likes_feed) / m7.likes_feed AS to_likes_feed_week_ago,
    (d.likes_feed - m30.likes_feed) / m30.likes_feed AS to_likes_feed_month_ago,
    
    -- views_feed
    (d.views_feed - m1.views_feed) / m1.views_feed AS to_views_feed_day_ago,
    (d.views_feed - m7.views_feed) / m7.views_feed AS to_views_feed_week_ago,
    (d.views_feed - m30.views_feed) / m30.views_feed AS to_views_feed_month_ago,
    
    -- events_feed
    (d.events_feed - m1.events_feed) / m1.events_feed AS to_events_feed_day_ago,
    (d.events_feed - m7.events_feed) / m7.events_feed AS to_events_feed_week_ago,
    (d.events_feed - m30.events_feed) / m30.events_feed AS to_events_feed_month_ago,
    
    -- posts_feed
    (d.posts_feed - m1.posts_feed) / m1.posts_feed AS to_posts_feed_day_ago,
    (d.posts_feed - m7.posts_feed) / m7.posts_feed AS to_posts_feed_week_ago,
    (d.posts_feed - m30.posts_feed) / m30.posts_feed AS to_posts_feed_month_ago

FROM data d
LEFT JOIN data m1
ON d.datetime = m1.datetime + 1
LEFT JOIN data m7
ON d.datetime = m7.datetime + 7
LEFT JOIN data m30
ON d.datetime = m30.datetime + 30
WHERE d.datetime >= today() - 31
"""

# Query data for MESSENGER
query_mssg = \
"""
WITH data AS (
SELECT
    toDate(time) datetime,
    count(DISTINCT user_id) dau_mssg,
    count(user_id) num_mssg,
    num_mssg / dau_mssg mpu_mssg 
FROM simulator_20221220.message_actions
WHERE datetime <= today() - 1
GROUP BY datetime
ORDER BY datetime DESC)

SELECT
    d.datetime datetime,
    d.dau_mssg dau_mssg,
    d.num_mssg num_mssg,
    d.mpu_mssg mpu_mssg,
    
    -- dau_mssg
    (d.dau_mssg - m1.dau_mssg) / m1.dau_mssg AS to_dau_mssg_day_ago,
    (d.dau_mssg - m7.dau_mssg) / m7.dau_mssg AS to_dau_mssg_week_ago,
    (d.dau_mssg - m30.dau_mssg) / m30.dau_mssg AS to_dau_mssg_month_ago,
    
    -- num_mssg
    (d.num_mssg - m1.num_mssg) / m1.num_mssg AS to_num_mssg_day_ago,
    (d.num_mssg - m7.num_mssg) / m7.num_mssg AS to_num_mssg_week_ago,
    (d.num_mssg - m30.num_mssg) / m30.num_mssg AS to_num_mssg_month_ago,
    
    -- mpu_mssg
    (d.mpu_mssg - m1.mpu_mssg) / m1.mpu_mssg AS to_mpu_mssg_day_ago,
    (d.mpu_mssg - m7.mpu_mssg) / m7.mpu_mssg AS to_mpu_mssg_week_ago,
    (d.mpu_mssg - m30.mpu_mssg) / m30.mpu_mssg AS to_mpu_mssg_month_ago

FROM data d
LEFT JOIN data m1
ON d.datetime = m1.datetime + 1
LEFT JOIN data m7
ON d.datetime = m7.datetime + 7
LEFT JOIN data m30
ON d.datetime = m30.datetime + 30
WHERE d.datetime >= today() - 31
"""


# Default arguments for Airflow tasks
default_args = {
    'owner': 'p-valov-14',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 2, 2),
}

# Schedule for Airflow DAG start
# Start daily at 11:00
schedule_interval = '0 11 * * *'
# Start hourly
# schedule_interval = '0 * * * *'

@dag(default_args=default_args,
     schedule_interval=schedule_interval,
     catchup=False)
def p_valov_14_lesson_7_task_2():

    @task()
    def extract_both_dau():
        """
        Извлечь данные по DAU всего приложения за предыдущие 30 дней

        :return: dataframe с данными за предыдущий месяц
        """
        df_both_dau = ph.read_clickhouse(
            query_both_dau,
            connection=connection_simulator)
        return df_both_dau

    @task()
    def extract_both_new():
        """
        Извлечь данные по новым пользователям всего приложения за предыдущие 30 дней

        :return: dataframe с данными за предыдущий месяц
        """
        df_both_new = ph.read_clickhouse(
            query_both_new,
            connection=connection_simulator)
        return df_both_new

    @task()
    def extract_feed():
        """
        Извлечь данные по новостной ленте за предыдущие 30 дней

        :return: dataframe с данными за предыдущий месяц
        """
        df_feed = ph.read_clickhouse(
            query_feed,
            connection=connection_simulator)
        return df_feed

    @task()
    def extract_mssg():
        """
        Извлечь данные по сервису сообщений за предыдущие 30 дней

        :return: dataframe с данными за предыдущий месяц
        """
        df_mssg = ph.read_clickhouse(
            query_mssg,
            connection=connection_simulator)
        return df_mssg

    @task()
    def transform_get_text(df_both_dau, df_both_new, df_feed, df_mssg):
        """
        Сформировать текст сообщения для Telegram-отчета

        :param df_both_dau: данные по DAU для всего приложения
        :param df_both_new: данные по новым пользователям для всего приложения
        :param df_feed: данные по новостному фиду
        :param df_mssg: данные по сервису сообщений
        :return: текст для Telegram-отчета
        """

        text = """ОТЧЕТ ПО РАБОТЕ ПРИЛОЖЕНИЯ (за {datetime:%Y-%m-%d})
ПРИЛОЖЕНИЕ (Новостная Лента + Мессенджер):
DAU (общее): {dau_total:,} ({to_dau_total_day_ago:+.2%} to day ago, {to_dau_total_week_ago:+.2%} to week ago, {to_dau_total_month_ago:+.2%} to month ago)
DAU (по ОС пользователя):
    iOS:     {dau_ios:,} ({to_dau_ios_day_ago:+.2%} to day ago, {to_dau_ios_week_ago:+.2%} to week ago, {to_dau_ios_month_ago:+.2%} to month ago)
    Android: {dau_drd:,} ({to_dau_drd_day_ago:+.2%} to day ago, {to_dau_drd_week_ago:+.2%} to week ago, {to_dau_drd_month_ago:+.2%} to month ago)
Новые пользователи (всего): {users_total:,} ({to_users_total_day_ago:+.2%} to day ago, {to_users_total_week_ago:+.2%} to week ago, {to_users_total_month_ago:+.2%} to month ago)
Новые пользователи (по источнику привлечения):
    Ads:     {users_ads:,} ({to_users_ads_day_ago:+.2%} to day ago, {to_users_ads_week_ago:+.2%} to week ago, {to_users_ads_month_ago:+.2%} to month ago)
    Organic: {users_org:,} ({to_users_org_day_ago:+.2%} to day ago, {to_users_org_week_ago:+.2%} to week ago, {to_users_org_month_ago:+.2%} to month ago)

НОВОСТНАЯ ЛЕНТА:
DAU: {dau_feed:,} ({to_dau_feed_day_ago:+.2%} to day ago, {to_dau_feed_week_ago:+.2%} to week ago, {to_dau_feed_month_ago:+.2%} to month ago)
CTR: {ctr_feed:,.2%} ({to_ctr_feed_day_ago:+.2%} to day ago, {to_ctr_feed_week_ago:+.2%} to week ago, {to_ctr_feed_month_ago:+.2%} to month ago)
LPU (лайки на пользователя): {lpu_feed:.2f} ({to_lpu_feed_day_ago:+.2%} to day ago, {to_lpu_feed_week_ago:+.2%} to week ago, {to_lpu_feed_month_ago:+.2%} to month ago)
Лайки: {likes_feed:,} ({to_likes_feed_day_ago:+.2%} to day ago, {to_likes_feed_week_ago:+.2%} to week ago, {to_likes_feed_month_ago:+.2%} to month ago)
Просмотры: {views_feed:,} ({to_views_feed_day_ago:+.2%} to day ago, {to_views_feed_week_ago:+.2%} to week ago, {to_views_feed_month_ago:+.2%} to month ago)
События: {events_feed:,} ({to_events_feed_day_ago:+.2%} to day ago, {to_events_feed_week_ago:+.2%} to week ago, {to_events_feed_month_ago:+.2%} to month ago)
Посты: {posts_feed:,} ({to_posts_feed_day_ago:+.2%} to day ago, {to_posts_feed_week_ago:+.2%} to week ago, {to_posts_feed_month_ago:+.2%} to month ago)

МЕССЕНДЖЕР:
DAU: {dau_mssg:,} ({to_dau_mssg_day_ago:+.2%} to day ago, {to_dau_mssg_week_ago:+.2%} to week ago, {to_dau_mssg_month_ago:+.2%} to month ago)
Сообщения: {num_mssg:,} ({to_num_mssg_day_ago:+.2%} to day ago, {to_num_mssg_week_ago:+.2%} to week ago, {to_num_mssg_month_ago:+.2%} to month ago)
MPU (сообщения на пользователя): {mpu_mssg:.2} ({to_mpu_mssg_day_ago:+.2%} to day ago, {to_mpu_mssg_week_ago:+.2%} to week ago, {to_mpu_mssg_month_ago:+.2%} to month ago)
"""

        datetime = df_feed['datetime'].max()
        row_both_dau = df_both_dau[df_both_dau['datetime'] == datetime]
        row_both_new = df_both_new[df_both_new['datetime'] == datetime]
        row_feed = df_feed[df_feed['datetime'] == datetime]
        row_mssg = df_mssg[df_mssg['datetime'] == datetime]

        text = text.format(
            datetime=datetime,
            dau_total=row_both_dau.iloc[0]['dau_total'],
            to_dau_total_day_ago=row_both_dau.iloc[0]['to_dau_total_day_ago'],
            to_dau_total_week_ago=row_both_dau.iloc[0]['to_dau_total_week_ago'],
            to_dau_total_month_ago=row_both_dau.iloc[0]['to_dau_total_month_ago'],
            dau_ios=row_both_dau.iloc[0]['dau_ios'],
            to_dau_ios_day_ago=row_both_dau.iloc[0]['to_dau_ios_day_ago'],
            to_dau_ios_week_ago=row_both_dau.iloc[0]['to_dau_ios_week_ago'],
            to_dau_ios_month_ago=row_both_dau.iloc[0]['to_dau_ios_month_ago'],
            dau_drd=row_both_dau.iloc[0]['dau_drd'],
            to_dau_drd_day_ago=row_both_dau.iloc[0]['to_dau_drd_day_ago'],
            to_dau_drd_week_ago=row_both_dau.iloc[0]['to_dau_drd_week_ago'],
            to_dau_drd_month_ago=row_both_dau.iloc[0]['to_dau_drd_month_ago'],
            users_total=row_both_new.iloc[0]['new_users'],
            to_users_total_day_ago=row_both_new.iloc[0]['to_users_total_day_ago'],
            to_users_total_week_ago=row_both_new.iloc[0]['to_users_total_week_ago'],
            to_users_total_month_ago=row_both_new.iloc[0]['to_users_total_month_ago'],
            users_ads=row_both_new.iloc[0]['new_users_ads'],
            to_users_ads_day_ago=row_both_new.iloc[0]['to_users_ads_day_ago'],
            to_users_ads_week_ago=row_both_new.iloc[0]['to_users_ads_week_ago'],
            to_users_ads_month_ago=row_both_new.iloc[0]['to_users_ads_month_ago'],
            users_org=row_both_new.iloc[0]['new_users_org'],
            to_users_org_day_ago=row_both_new.iloc[0]['to_users_org_day_ago'],
            to_users_org_week_ago=row_both_new.iloc[0]['to_users_org_week_ago'],
            to_users_org_month_ago=row_both_new.iloc[0]['to_users_org_month_ago'],

            dau_feed=row_feed.iloc[0]['dau_feed'],
            to_dau_feed_day_ago=row_feed.iloc[0]['to_dau_feed_day_ago'],
            to_dau_feed_week_ago=row_feed.iloc[0]['to_dau_feed_week_ago'],
            to_dau_feed_month_ago=row_feed.iloc[0]['to_dau_feed_month_ago'],
            ctr_feed=row_feed.iloc[0]['ctr_feed'],
            to_ctr_feed_day_ago=row_feed.iloc[0]['to_ctr_feed_day_ago'],
            to_ctr_feed_week_ago=row_feed.iloc[0]['to_ctr_feed_week_ago'],
            to_ctr_feed_month_ago=row_feed.iloc[0]['to_ctr_feed_month_ago'],
            lpu_feed=row_feed.iloc[0]['lpu_feed'],
            to_lpu_feed_day_ago=row_feed.iloc[0]['to_lpu_feed_day_ago'],
            to_lpu_feed_week_ago=row_feed.iloc[0]['to_lpu_feed_week_ago'],
            to_lpu_feed_month_ago=row_feed.iloc[0]['to_lpu_feed_month_ago'],
            likes_feed=row_feed.iloc[0]['likes_feed'],
            to_likes_feed_day_ago=row_feed.iloc[0]['to_likes_feed_day_ago'],
            to_likes_feed_week_ago=row_feed.iloc[0]['to_likes_feed_week_ago'],
            to_likes_feed_month_ago=row_feed.iloc[0]['to_likes_feed_month_ago'],
            views_feed=row_feed.iloc[0]['views_feed'],
            to_views_feed_day_ago=row_feed.iloc[0]['to_views_feed_day_ago'],
            to_views_feed_week_ago=row_feed.iloc[0]['to_views_feed_week_ago'],
            to_views_feed_month_ago=row_feed.iloc[0]['to_views_feed_month_ago'],
            events_feed=row_feed.iloc[0]['events_feed'],
            to_events_feed_day_ago=row_feed.iloc[0]['to_events_feed_day_ago'],
            to_events_feed_week_ago=row_feed.iloc[0]['to_events_feed_week_ago'],
            to_events_feed_month_ago=row_feed.iloc[0]['to_events_feed_month_ago'],
            posts_feed=row_feed.iloc[0]['posts_feed'],
            to_posts_feed_day_ago=row_feed.iloc[0]['to_posts_feed_day_ago'],
            to_posts_feed_week_ago=row_feed.iloc[0]['to_posts_feed_week_ago'],
            to_posts_feed_month_ago=row_feed.iloc[0]['to_posts_feed_month_ago'],

            dau_mssg = row_mssg.iloc[0]['dau_mssg'],
            to_dau_mssg_day_ago = row_mssg.iloc[0]['to_dau_mssg_day_ago'],
            to_dau_mssg_week_ago = row_mssg.iloc[0]['to_dau_mssg_week_ago'],
            to_dau_mssg_month_ago = row_mssg.iloc[0]['to_dau_mssg_month_ago'],
            num_mssg = row_mssg.iloc[0]['num_mssg'],
            to_num_mssg_day_ago = row_mssg.iloc[0]['to_num_mssg_day_ago'],
            to_num_mssg_week_ago = row_mssg.iloc[0]['to_num_mssg_week_ago'],
            to_num_mssg_month_ago = row_mssg.iloc[0]['to_num_mssg_month_ago'],
            mpu_mssg = row_mssg.iloc[0]['mpu_mssg'],
            to_mpu_mssg_day_ago = row_mssg.iloc[0]['to_mpu_mssg_day_ago'],
            to_mpu_mssg_week_ago = row_mssg.iloc[0]['to_mpu_mssg_week_ago'],
            to_mpu_mssg_month_ago = row_mssg.iloc[0]['to_mpu_mssg_month_ago']
        )

        return text

    @task()
    def transform_get_graphs(df_both_dau, df_both_new, df_feed, df_mssg):
        """
        Сформировать графики для Telegram-отчета

        :param df_both_dau: данные по DAU для всего приложения
        :param df_both_new: данные по новым пользователям для всего приложения
        :param df_feed: данные по новостному фиду
        :param df_mssg: данные по сервису сообщений
        :return: графики для Telegram-отчета
        """

        # Список графиков
        plot_objects = []

        # Объединить все данные для удобства
        data = pd.merge(df_feed, df_mssg, on='datetime')
        data = pd.merge(data, df_both_new, on='datetime')
        data = pd.merge(data, df_both_dau, on='datetime')
        data['events_app'] = data['events_feed'] + data['num_mssg']

        # Отобрать данные за предыдущую неделю
        datetime_today = df_feed['datetime'].max()
        datetime_m7 = datetime_today - timedelta(days=7)
        data = data[data['datetime'] >= datetime_m7]


        # СТАТИСТИКА ПО ВСЕМУ ПРИЛОЖЕНИЮ
        fig, axes = plt.subplots(3, figsize = (10, 14))
        fig.suptitle('Статистика по Приложению (новости и мессенджер) за 7 дней')

        app_dict = {
            0: {'y': ['events_app'], 'title': 'События',
                'l': ['События']},
            1: {'y': ['dau_total', 'dau_ios', 'dau_drd'], 'title': 'DAU',
                'l': ['DAU общее', 'DAU iOS', 'DAU Android']},
            2: {'y': ['new_users', 'new_users_ads', 'new_users_org'], 'title': 'Новые пользователи',
                'l': ['Новые пользователи', 'Новые пользователи (Ads)', 'Новые пользователи (Organic)']}
        }

        for i in range(3):
            for y in app_dict[i]['y']:
                sns.lineplot(ax=axes[i], data=data, x='datetime', y=y)
                axes[i].set_title(app_dict[i]['title'])
                axes[i].set(xlabel=None)
                axes[i].set(ylabel=None)
                axes[i].legend(app_dict[i]['l'])
                for index, label in enumerate(axes[i].get_xticklabels()):
                    if index % 3 == 0:
                        label.set_visible(True)
                    else:
                        label.set_visible(False)

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.name = 'app_stat.png'
        plot_object.seek(0)
        plt.close()
        plot_objects.append(plot_object)


        # СТАТИСТИКА ПО ЛЕНТЕ НОВОСТЕЙ
        fig, axes = plt.subplots(2, 3, figsize = (16, 9))
        fig.suptitle('Статистика по Ленте Новостей за 7 дней')

        feed_dict = {
            (0, 0): {'y': ['dau_feed'], 'title': 'DAU'},
            (0, 1): {'y': ['ctr_feed'], 'title': 'CTR'},
            (0, 2): {'y': ['lpu_feed'], 'title': 'LPU'},

            (1, 0): {'y': ['likes_feed', 'views_feed'], 'title': 'Лайки и Просмотры'},
            (1, 1): {'y': ['events_feed'], 'title': 'События'},
            (1, 2): {'y': ['posts_feed'], 'title': 'Посты'},
        }

        for i in range(2):
            for j in range(3):
                for y in feed_dict[i, j]['y']:
                    sns.lineplot(ax=axes[i, j], data=data, x='datetime', y=y)  # y=feed_dict[(i, j)]['y'])
                    axes[i, j].set_title(feed_dict[(i, j)]['title'])
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
        plot_objects.append(plot_object)


        # СТАТИСТИКА ПО МЕССЕНДЖЕРУ
        fig, axes = plt.subplots(3, figsize = (10, 14))
        fig.suptitle('Статистика по Мессенджеру за 7 дней')

        msg_dict = {
            0: {'y': ['dau_mssg'], 'title': 'DAU'},
            1: {'y': ['num_mssg'], 'title': 'Сообщения'},
            2: {'y': ['mpu_mssg'], 'title': 'MPU (сообщения на пользователя)'},
        }

        for i in range(3):
            for y in msg_dict[i]['y']:
                sns.lineplot(ax=axes[i], data=data, x='datetime', y=y)
                axes[i].set_title(msg_dict[i]['title'])
                axes[i].set(xlabel=None)
                axes[i].set(ylabel=None)
                for index, label in enumerate(axes[i].get_xticklabels()):
                    if index % 3 == 0:
                        label.set_visible(True)
                    else:
                        label.set_visible(False)

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.name = 'msg_stat.png'
        plot_object.seek(0)
        plt.close()
        plot_objects.append(plot_object)


        return plot_objects

    @task()
    def load(text, plots):

        # Инициализировать бот для Телеграм
        my_token = 'TOKEN'
        bot = telegram.Bot(token=my_token)

        pv_id = 502947799
        kc_id = -850804180
        final_id = kc_id
        bot.sendMessage(chat_id=final_id, text=text)
        for plot in plots:
            bot.sendPhoto(chat_id=final_id, photo=plot)

    # Extract data
    df_both_dau = extract_both_dau()
    df_both_new = extract_both_new()
    df_feed = extract_feed()
    df_mssg = extract_mssg()

    # Transform data to get report text and graphs
    text = transform_get_text(df_both_dau, df_both_new, df_feed, df_mssg)
    plots = transform_get_graphs(df_both_dau, df_both_new, df_feed, df_mssg)

    load(text, plots)


p_valov_14_lesson_7_task_2 = p_valov_14_lesson_7_task_2()
