import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn) -> None:
    """
    Drop tables in Redshift cluster. The drop queries are defined in
    sql_queries.py

    Parameters
    ----------
    cur : psycopg2.extensions.cursor
        Cursor to database
    conn : psycopg2.extensions.connection
        Connection to database
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn) -> None:
    """
    Create tables in Redshift cluster. The create queries are defined in
    sql_queries.py

    Parameters
    ----------
    cur : psycopg2.extensions.cursor
        Cursor to database
    conn : psycopg2.extensions.connection
        Connection to database
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main() -> None:
    """Connect to Redshift cluster and create tables."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    connection_string = (
        "host={} dbname={} user={} password={} port={}"
        .format(*config['CLUSTER'].values())
    )
    print(connection_string)
    conn = psycopg2.connect(connection_string)
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
