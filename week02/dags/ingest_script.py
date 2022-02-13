from sqlalchemy import create_engine
from time import time
import pandas as pd


def ingest_callable(
        user, password, host, port, db, table_name, csv_file, execution_date,
        datetime_cols
):
    print(table_name, csv_file, execution_date)

    engine = _create_engine(user, password, host, port, db)
    df_iter = pd.read_csv(csv_file, iterator=True, chunksize=100000)

    more = _insert_chunk(df_iter, table_name, engine, 'replace', datetime_cols)

    while more:
        more = _insert_chunk(df_iter, table_name, engine, 'append', datetime_cols)


def _create_engine(user, password, host, port, db):
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    engine.connect()
    print('connection established successfully')

    return engine


def _insert_chunk(df_iter, table_name, engine, if_exists, datetime_cols):
    try:
        t_start = time()
        df = next(df_iter)

        for col in datetime_cols:
            df[col] = pd.to_datetime(df[col])

        df.to_sql(name=table_name, con=engine, if_exists=if_exists)

        t_end = time()

        print('inserted another chunk, took %.3f second' % (t_end - t_start))

        return True
    except StopIteration:
        print('completed')
        return False
