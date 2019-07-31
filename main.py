#!/usr/bin/env python
# coding: utf-8

# TODO: synchronous logging (in Docker logs are written in batched and you cannot view them in real time)

from datetime import timedelta
import datetime as dt
import logging

import pandas as pd

from bogoslovskiy.model import ConfigWorker
from bogoslovskiy.model.db.Implementation import InHouseDbWorker, BigQueryWorker
from src.PortgesDataLoader import PostgresDataLoader

# file handler
filepath: str = "/logging/etl_retention.log"
fh = logging.FileHandler(filepath)
fh.setLevel(logging.DEBUG)

# stream handler
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)-8s%(asctime)s        %(module)-25.25s %(message)s \n",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=(fh, ch,)
)

db_cw = ConfigWorker("/configs/db.ini")

# --------------------------------------------------------------------------------------------------------------------
#                                                   DATABASE
# --------------------------------------------------------------------------------------------------------------------
logging.debug("Database objects initialization")

bq = BigQueryWorker(db_cw, "bigquery")
monolith = InHouseDbWorker(db_cw, "monolith")
postgres = InHouseDbWorker(db_cw, "postgres")
data_loader = PostgresDataLoader(postgres)

# --------------------------------------------------------------------------------------------------------------------
#                                                   CALCULATION
# --------------------------------------------------------------------------------------------------------------------
ld = postgres.get_iterable("select max(date_state) from core_state_series").fetchone()[0]
ld = ld if ld else dt.date.today() - timedelta(days=9)

ld_matrix = postgres.get_iterable("select max(date_state) from core_migration_matrix").fetchone()[0]
ld_matrix = ld_matrix if ld_matrix else dt.date.today() - timedelta(days=9)

# kostyl time!!!
# ld = '2019-07-01'
# ld_matrix = '2019-07-01'
# end kostyl 

last_date_matrix = pd.to_datetime(ld_matrix).date()
last_date = pd.to_datetime(ld).date()

period = last_date_matrix - timedelta(days=30)
year_period = last_date_matrix - timedelta(days=365)

logging.debug("Payments query")
df_payments_full = monolith.get_dataframe(
    """
    select distinct 
        id_user, 
        date(po.date_created) as po_date, 
        case 
            when u.id_mirror in (1, 11, 14, 17, 20, 29,  30, 31, 32, 35, 37, 38, 40, 42, 43, 45) then 'cis' 
            when u.id_mirror in (23, 26, 39, 44, 46, 47, 48, 49) then 'asia' 
            when u.id_mirror = 41 then 'latam' 
        end as region
    from 
        x27_payment_orders po
    join 
        x27_users u on u.id = po.id_user
    where 1=1
        and po.date_created >= '{}'
        and po.code_package not in ('code', 'learn', 'test')
        and po.id_status in (3, 18, 21)
        and u.id_partner not in ('-1', '1', '2', '3', '4', '5', 'mikula', 'tech_vb_test')
    order by id_user, po_date asc
    """.format(str(year_period))
)

df_payments_full['num'] = df_payments_full.groupby('id_user').cumcount() + 1
df_payments_full['po_date'] = pd.to_datetime(df_payments_full['po_date']).dt.date

logging.debug("Logins query")
df = bq.get_dataframe(
    """
        WITH users AS (
          SELECT 
            id, 
            date_created, 
            id_mirror 
          FROM 
            product.db_users 
          WHERE 1=1
              AND id_partner not in ('-1', '1', '2', '3', '4', '5', 'mikula', 'tech_vb_test', 'test')
              AND gender = 'male'
              -- AND (id_blocked is NULL or id_blocked = 0) 
        ),

        logins AS (
          SELECT 
            id_user, 
            date_created 
          FROM 
            product.users_logins 
          WHERE 1=1
            AND date_created >= TIMESTAMP('{}')
        )


        select distinct 
            u.id as id_user, 
            date(u.date_created) as u_date_created,
            date(e.date_created) as date, 
            case 
                when u.id_mirror in (1, 11, 14, 17, 20, 29,  30, 31, 32, 35, 37, 38, 40, 42, 43, 45) then 'cis' 
                when u.id_mirror in (23, 26, 39, 44, 46, 47, 48, 49) then 'asia' 
                when u.id_mirror = 41 then 'latam' 
            end as region
        from 
           logins e
        inner join 
            users u on u.id = e.id_user
    """.format(str(period))
)

logging.debug("Transformation")
df['date'] = pd.to_datetime(df['date']).dt.date
new_df = pd.merge(df, df_payments_full, how='outer', left_on=['id_user', 'date', 'region'], right_on=['id_user', 'po_date', 'region'])
new_df['po_date'] = pd.to_datetime(new_df['po_date']).dt.date
new_df['date'] = pd.to_datetime(new_df['date']).dt.date
new_df['u_date_created'] = pd.to_datetime(new_df['u_date_created']).dt.date
new_df = new_df.drop_duplicates()


# --------------------------------------------------------------------------------------------------------------------
#                                                   MATRIX
# --------------------------------------------------------------------------------------------------------------------
def new_ns_validator(now, region):
    matrix = pd.DataFrame()

    now = now.date()
    _7days_ago = (now - timedelta(days=6))
    _7days_after = (now + timedelta(days=7))

    new_df_region = new_df[new_df['region'] == region]

    had_payments = set(
        new_df_region[
            (new_df_region.po_date <= now) &
            (new_df_region.num >= 1)
            ]["id_user"]
    )

    date_created = set(
        new_df_region[
            (new_df_region['u_date_created'] >= _7days_ago) &
            (new_df_region['u_date_created'] <= now)
            ]["id_user"]
    )

    res = date_created.difference(had_payments)

    df_temp = new_df_region[
        (new_df_region['date'] > now) &
        (new_df_region['date'] <= _7days_after) &
        (new_df_region["id_user"].isin(res))
        ]

    more_than_1_pymnt = set(
        df_temp[df_temp.num > 1]['id_user']
    )

    only_1_pymnt = (set(df_temp[df_temp.num == 1]['id_user'])).difference(more_than_1_pymnt)

    matrix.loc['new_ns', 'active_ns'] = len(
        set(df_temp['id_user']).difference(set(df_temp[df_temp.num >= 1]['id_user']))) / len(res) * 100

    matrix.loc['new_ns', 'churn_ns'] = (len(res) - len(set(df_temp['id_user']))) / len(res) * 100

    matrix.loc['new_ns', 'new_spenders'] = len(only_1_pymnt) / len(res) * 100

    matrix.loc['new_ns', 'active_spenders'] = len(set(df_temp[df_temp.num > 1]['id_user'])) / len(res) * 100

    return matrix


def active_ns_validator(now, region):
    matrix = pd.DataFrame()
    now = now.date()
    new_df_region = new_df[new_df['region'] == region]
    _7days_ago = (now - timedelta(days=6))
    had_payments = set(new_df_region[(new_df_region.po_date <= now) & (new_df_region.num >= 1)].id_user)
    login_and_date_created = set(new_df_region[
                                     (new_df_region['date'] >= _7days_ago) & (new_df_region['date'] <= now) & (
                                                 new_df_region['u_date_created'] < _7days_ago)].id_user)
    res = login_and_date_created.difference(had_payments)
    _7days_after = (now + timedelta(days=7))
    df_temp = pd.DataFrame()
    df_temp = new_df_region[
        (new_df_region['date'] > now) & (new_df_region['date'] <= _7days_after) & (new_df_region.id_user.isin(res))]
    more_than_1_pymnt = set(df_temp[df_temp.num > 1]['id_user'])
    only_1_pymnt = (set(df_temp[df_temp.num == 1]['id_user'])).difference(more_than_1_pymnt)
    matrix.loc['active_ns', 'active_ns'] = len(
        set(df_temp['id_user']).difference(set(df_temp[df_temp.num >= 1]['id_user']))) / len(res) * 100
    matrix.loc['active_ns', 'churn_ns'] = (len(res) - len(set(df_temp['id_user']))) / len(res) * 100
    matrix.loc['active_ns', 'new_spenders'] = len(only_1_pymnt) / len(res) * 100
    matrix.loc['active_ns', 'active_spenders'] = len(set(df_temp[df_temp.num > 1]['id_user'])) / len(res) * 100
    return matrix


def churn_ns_validator(now, region):
    matrix = pd.DataFrame()
    now = now.date()
    new_df_region = new_df[new_df['region'] == region]
    _7days_ago = (now - timedelta(days=6))
    _30days_ago = (now - timedelta(days=29))
    had_payments = set(new_df_region[(new_df_region.po_date <= now) & (new_df_region.num >= 1)].id_user)
    login_last_month = set(
        new_df_region[(new_df_region['date'] >= _30days_ago) & (new_df_region['date'] < _7days_ago)].id_user)
    login_last_week = set(new_df_region[(new_df_region['date'] >= _7days_ago) & (new_df_region['date'] <= now)].id_user)
    date_created_no_payments = login_last_month.difference(had_payments)
    res = date_created_no_payments.difference(login_last_week)
    _7days_after = (now + timedelta(days=7))
    df_temp = pd.DataFrame()
    df_temp = new_df_region[
        (new_df_region['date'] > now) & (new_df_region['date'] <= _7days_after) & (new_df_region.id_user.isin(res))]
    more_than_1_pymnt = set(df_temp[df_temp.num > 1]['id_user'])
    only_1_pymnt = (set(df_temp[df_temp.num == 1]['id_user'])).difference(more_than_1_pymnt)
    matrix.loc['churn_ns', 'active_ns'] = len(
        set(df_temp['id_user']).difference(set(df_temp[df_temp.num >= 1]['id_user']))) / len(res) * 100
    matrix.loc['churn_ns', 'churn_ns'] = (len(res) - len(set(df_temp['id_user']))) / len(res) * 100
    matrix.loc['churn_ns', 'new_spenders'] = len(only_1_pymnt) / len(res) * 100
    matrix.loc['churn_ns', 'active_spenders'] = len(set(df_temp[df_temp.num > 1]['id_user'])) / len(res) * 100
    return matrix


def new_spenders_validator(now, region):
    matrix = pd.DataFrame()
    now = now.date()
    new_df_region = new_df[new_df['region'] == region]
    _7days_ago = (now - timedelta(days=6))
    count_1 = set(new_df_region[(new_df_region.po_date >= _7days_ago) & (new_df_region.po_date <= now) & (
                new_df_region.num == 1)].id_user)
    count_more = set(new_df_region[(new_df_region.po_date >= _7days_ago) & (new_df_region.po_date <= now) & (
                new_df_region.num > 1)].id_user)
    res = count_1.difference(count_more)
    _7days_after = (now + timedelta(days=7))
    df_temp = pd.DataFrame()
    df_temp = new_df_region[
        (new_df_region['date'] > now) & (new_df_region['date'] <= _7days_after) & (new_df_region.id_user.isin(res))]
    more_payments = set(df_temp[(df_temp.num > 1)].id_user)
    login = set(df_temp['id_user'])
    matrix.loc['new_spenders', 'churn_spenders'] = (len(res) - len(set(df_temp['id_user']))) / len(res) * 100
    matrix.loc['new_spenders', 'active_users'] = len(login.difference(more_payments)) / len(res) * 100
    matrix.loc['new_spenders', 'active_spenders'] = len(set(df_temp[df_temp.num > 1]['id_user'])) / len(res) * 100
    return matrix


def active_spenders_validator(now, region):
    matrix = pd.DataFrame()
    now = now.date()
    new_df_region = new_df[new_df['region'] == region]
    _7days_ago = (now - timedelta(days=6))
    res = set(new_df_region[(new_df_region.po_date >= _7days_ago) & (new_df_region.po_date <= now) & (
                new_df_region.num > 1)].id_user)
    _7days_after = (now + timedelta(days=7))
    df_temp = pd.DataFrame()
    df_temp = new_df_region[
        (new_df_region['date'] > now) & (new_df_region['date'] <= _7days_after) & (new_df_region.id_user.isin(res))]
    more_payments = set(df_temp[(df_temp.num > 1)].id_user)
    login = set(df_temp['id_user'])
    matrix.loc['active_spenders', 'churn_spenders'] = (len(res) - len(set(df_temp['id_user']))) / len(res) * 100
    matrix.loc['active_spenders', 'active_users'] = len(login.difference(more_payments)) / len(res) * 100
    matrix.loc['active_spenders', 'active_spenders'] = len(set(df_temp[df_temp.num > 1]['id_user'])) / len(res) * 100
    return matrix


def active_users_validator(now, region):
    matrix = pd.DataFrame()
    now = now.date()
    new_df_region = new_df[new_df['region'] == region]
    _7days_ago = (now - timedelta(days=6))
    had_payments = set(new_df_region[(new_df_region.po_date <= now) & (new_df_region.num >= 1)].id_user)
    new_login = set(new_df_region[(new_df_region['date'] >= _7days_ago) & (new_df_region['date'] <= now)].id_user)
    payment_and_login = new_login.intersection(had_payments)
    payment_last_week = set(
        new_df_region[(new_df_region.po_date >= _7days_ago) & (new_df_region.po_date <= now)].id_user)
    res = payment_and_login.difference(payment_last_week)
    _7days_after = (now + timedelta(days=7))
    df_temp = pd.DataFrame()
    df_temp = new_df_region[
        (new_df_region['date'] > now) & (new_df_region['date'] <= _7days_after) & (new_df_region.id_user.isin(res))]
    more_payments = set(df_temp[(df_temp.num > 1)].id_user)
    login = set(df_temp['id_user'])
    matrix.loc['active_users', 'churn_spenders'] = (len(res) - len(set(df_temp['id_user']))) / len(res) * 100
    matrix.loc['active_users', 'active_users'] = len(login.difference(more_payments)) / len(res) * 100
    matrix.loc['active_users', 'active_spenders'] = len(set(df_temp[df_temp.num > 1]['id_user'])) / len(res) * 100
    return matrix


def churn_spenders_validator(now, region):
    matrix = pd.DataFrame()
    now = now.date()
    new_df_region = new_df[new_df['region'] == region]
    _7days_ago = (now - timedelta(days=6))
    _30days_ago = (now - timedelta(days=29))
    early = set(
        new_df_region[(new_df_region['po_date'] >= _30days_ago) & (new_df_region['po_date'] < _7days_ago)].id_user)
    new = set(new_df_region[(new_df_region['date'] >= _7days_ago) & (new_df_region['date'] <= now)].id_user)
    res = early.difference(new)
    _7days_after = (now + timedelta(days=7))
    df_temp = pd.DataFrame()
    df_temp = new_df_region[
        (new_df_region['date'] > now) & (new_df_region['date'] <= _7days_after) & (new_df_region.id_user.isin(res))]
    more_payments = set(df_temp[(df_temp.num > 1)].id_user)
    login = set(df_temp['id_user'])
    matrix.loc['churn_spenders', 'churn_spenders'] = (len(res) - len(set(df_temp['id_user']))) / len(res) * 100
    matrix.loc['churn_spenders', 'active_users'] = len(login.difference(more_payments)) / len(res) * 100
    matrix.loc['churn_spenders', 'active_spenders'] = len(set(df_temp[df_temp.num > 1]['id_user'])) / len(res) * 100
    return matrix


def create_matrix(now, region):
    l = [
        new_ns_validator(now, region),
        active_ns_validator(now, region),
        churn_ns_validator(now, region),
        new_spenders_validator(now, region),
        active_spenders_validator(now, region),
        active_users_validator(now, region),
        churn_spenders_validator(now, region),
    ]

    return pd.concat(l)


logging.debug("Matrix iterations")
for region in ('cis', 'asia', 'latam'):
    for one_day in pd.date_range(last_date_matrix, dt.date.today() - timedelta(days=8)).to_pydatetime():
        matrix = create_matrix(one_day, region)
        matrix = matrix.reset_index()
        matrix.columns = ["source_state" if x == 'index' else x for x in matrix.columns]
        matrix["region"] = region
        matrix["date_state"] = str(one_day)[0:10]
        data_loader.upload_data("core_migration_matrix", matrix)

# --------------------------------------------------------------------------------------------------------------------
#                                                   SERIES
# --------------------------------------------------------------------------------------------------------------------


def active_users_series(now, region):
    now = now.date()
    new_df_region = new_df[new_df['region'] == region]
    _7days_ago = (now - timedelta(days=6))
    had_payments = set(new_df_region[(new_df_region.po_date <= now) & (new_df_region.num >= 1)].id_user)
    new_login = set(new_df_region[(new_df_region['date'] >= _7days_ago) & (new_df_region['date'] <= now)].id_user)
    payment_and_login = new_login.intersection(had_payments)
    payment_last_week = set(
        new_df_region[(new_df_region.po_date >= _7days_ago) & (new_df_region.po_date <= now)].id_user)
    res = payment_and_login.difference(payment_last_week)

    df = pd.DataFrame(
        {
            "region": region,
            "date_state": str(now)[0:10],
            "state": "active_users",
            "users_count": [len(res)],
        }
    )

    return df


def new_spenders_series(now, region):
    now = now.date()
    new_df_region = new_df[new_df['region'] == region]
    _7days_ago = (now - timedelta(days=6))
    count_1 = set(new_df_region[(new_df_region.po_date >= _7days_ago) & (new_df_region.po_date <= now) & (
                new_df_region.num == 1)].id_user)
    count_more = set(new_df_region[(new_df_region.po_date >= _7days_ago) & (new_df_region.po_date <= now) & (
                new_df_region.num > 1)].id_user)
    res = count_1.difference(count_more)

    df = pd.DataFrame(
        {
            "region": region,
            "date_state": str(now)[0:10],
            "state": "new_spenders",
            "users_count": [len(res)],
        }
    )

    return df


def active_spenders_series(now, region):
    now = now.date()
    new_df_region = new_df[new_df['region'] == region]
    _7days_ago = (now - timedelta(days=6))
    res = set(new_df_region[(new_df_region.po_date >= _7days_ago) & (new_df_region.po_date <= now) & (
                new_df_region.num > 1)].id_user)
    df = pd.DataFrame(
        {
            "region": region,
            "date_state": str(now)[0:10],
            "state": "active_spenders",
            "users_count": [len(res)],
        }
    )

    return df


def churn_spenders_series(now, region):
    now = now.date()
    new_df_region = new_df[new_df['region'] == region]

    _7days_ago = (now - timedelta(days=6))
    _30days_ago = (now - timedelta(days=29))

    early = set(new_df_region[(new_df_region['po_date']>=_30days_ago)&(new_df_region['po_date']<_7days_ago)].id_user)
    new = set(new_df_region[(new_df_region['date'] >= _7days_ago) & (new_df_region['date'] <= now)].id_user)
    res = early.difference(new)
    df = pd.DataFrame(
        {
            "region": region,
            "date_state": str(now)[0:10],
            "state": "churn_spenders",
            "users_count": [len(res)],
        }
    )

    return df


def new_ns_series(now, region):
    now = now.date()
    new_df_region = new_df[new_df['region'] == region]

    _7days_ago = (now - timedelta(days=6))
    had_payments = set(new_df_region[(new_df_region.po_date <= now) & (new_df_region.num >= 1)].id_user)
    date_created = set(new_df_region[(new_df_region['u_date_created'] >= _7days_ago) & (
                new_df_region['u_date_created'] <= now)].id_user)
    res = date_created.difference(had_payments)
    df = pd.DataFrame(
        {
            "region": region,
            "date_state": str(now)[0:10],
            "state": "new_ns",
            "users_count": [len(res)],
        }
    )

    return df


def active_ns_series(now, region):
    now = now.date()
    new_df_region = new_df[new_df['region'] == region]
    _7days_ago = (now - timedelta(days=6))
    had_payments = set(new_df_region[(new_df_region.po_date <= now) & (new_df_region.num >= 1)].id_user)
    login_and_date_created = set(new_df_region[
                                     (new_df_region['date'] >= _7days_ago) & (new_df_region['date'] <= now) & (
                                                 new_df_region['u_date_created'] < _7days_ago)].id_user)
    res = login_and_date_created.difference(had_payments)
    df = pd.DataFrame(
        {
            "region": region,
            "date_state": str(now)[0:10],
            "state": "active_ns",
            "users_count": [len(res)],
        }
    )

    return df


logging.debug("Series iteration")
for region in ('cis', 'asia', 'latam'):
    for now in pd.date_range(last_date, dt.date.today() - timedelta(days=1)).to_pydatetime():
        temp = pd.concat([
            new_ns_series(now, region),
            active_ns_series(now, region),
            new_spenders_series(now, region),
            active_spenders_series(now, region),
            churn_spenders_series(now, region),
            active_users_series(now, region),
        ])

        data_loader.upload_data("core_state_series", temp)


