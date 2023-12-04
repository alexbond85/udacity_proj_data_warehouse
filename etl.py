import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Load data from S3 into staging tables on Redshift.

    The copy queries are defined in sql_queries.py

    Parameters
    ----------
    cur : psycopg2.extensions.cursor
        Cursor to database
    conn : psycopg2.extensions.connection
        Connection to database
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Insert data from staging tables into analytics tables on Redshift.

    The insert queries are defined in sql_queries.py

    Parameters
    ----------
    cur : psycopg2.extensions.cursor
        Cursor to database
    conn : psycopg2.extensions.connection
        Connection to database
    """
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main() -> None:
    """Connect to Redshift cluster and load data from S3 into staging tables
    and insert data from staging tables into analytics tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            *config['CLUSTER'].values()))
    cur = conn.cursor()

    # load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()