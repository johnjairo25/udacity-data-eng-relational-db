import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    Creates a connection to the database and re-creates the database sparkifyDB
    :return: (cur, conn) - The cursor and connection objects for the sparkifyDB database
    """
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """
    Drops all the tables defined in the `drop_table_queries` list
    :param cur:
        The cursor to execute statements against the sparkifyDB database
    :param conn:
        The connection to the sparkifyDB database
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates all the tables defined in the `create_table_queries` list
    :param cur:
        The cursor to execute statements against the sparkifyDB database
    :param conn:
        The connection to the sparkifyDB database
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Creates the sparkifyDB databe, drops all tables if they exist and creates them again.
    It cleans the database to re-run the ETL process.
    """
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()