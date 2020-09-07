from database import cur, conn
import psycopg2


def VarIf( expression, string1, string2 ):
    if expression:
        return string1
    else:
        return string2


def convertJSONDate( jsonDATE ):
    convert_string = jsonDATE.replace( "T", " " )
    strip_index = convert_string.find( ".")
    convert_string = convert_string[:-(len(convert_string) - strip_index)]
    return convert_string


def getKeyValue( key ):
    try:
        query =  f"SELECT k.key_value FROM keytable k WHERE k.key_name = '{key}'"
        cur.execute( query, "")
        row = cur.fetchone()
        # After retrieving the ID, update it according it to the value of k.inc_val for that row.
        update_query = f"UPDATE keytable SET key_value = key_value + incval WHERE key_name = '{key}'"
        cur.execute( update_query, "")
        return row[0]
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)

def QueryWithSingleValue( tableName, searchColumn, searchValue, valueColumn, searchRequiresQuotes ):
    try:
        appended_search_value = VarIf(searchRequiresQuotes, "'" + searchValue + "'", searchValue )
        query =  f"SELECT {valueColumn} FROM {tableName} WHERE {searchColumn} = {appended_search_value}"
        cur.execute( query, "")
        row = cur.fetchone()
        return row[0]
    except(Exception, psycopg2.DatabaseError) as error:
        print("Offending Query: " + query)
        print(error)
