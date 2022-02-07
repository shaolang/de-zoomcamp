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

    more = _insert_chunk(df_iter, table_name, engine, True, datetime_cols)

    while more:
        more = _insert_chunk(df_iter, table_name, engine, False, datetime_cols)


def _create_engine(user, password, host, port, db):
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    engine.connect()
    print('connection established successfully')

    return engine


def _insert_chunk(df_iter, table_name, engine, first_chunk, datetime_cols):
    try:
        t_start = time()
        df = next(df_iter)

        for col in datetime_cols:
            df[col] = pd.to_datetime(df[col])

        if first_chunk:
            df.to_sql(name=table_name, con=engine, if_exists='replace')
        else:
            df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()

        print('inserted another chunk, took %.3f second' % (t_end - t_start))

        return False
    except StopIteration:
        print('completed')
        return True
