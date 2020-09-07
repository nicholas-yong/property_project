import psycopg2


cur = None
conn = None

def connect():
    try:
        global conn
        conn = psycopg2.connect(
                host = "localhost",
                database = "domain_properties",
                user = "postgres",
                password = "namihana"
        )
        
        #Create a Cursor
        global cur
        cur = conn.cursor()
        return cur
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)

connect()