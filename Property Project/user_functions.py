from database import cur, conn
import psycopg2


def VarIf( expression, trueParameter, falseParameter ):
    """
    Function returns the firstParameter if the expression evaluates to True. Otherwise it returns the second parameter.

    Keyword arguements:
    expresion -- Expression to evaluate.
    firstParameter -- Parameter that is returned if expression evaluates to True.
    secondParameter - Parameter that is returned if expression evalutes to False.

    """
    if expression:
        return trueParameter
    else:
        return falseParameter


def convertJSONDate( jsonDATE ):
    """
    This Function converts a JSON formatted date into a date_string that can be inserted into postgres. (The Function returns the converted string).

    Keyword arguements:
    jsonDATE -- Json Formatted Date String to format.

    """
    convert_string = jsonDATE.replace( "T", " " )
    strip_index = convert_string.find( "Z" )
    convert_string = convert_string[:-(len(convert_string) - strip_index)]
    return convert_string


def getKeyValue( key ):
    """
    This Function retrieves the keyvalue of a particular keyword inside the keytable table.
    After retrieving the keyvalue, it then incremenets that keyvalue (by the keyword's incval).

    Keyword arguements:
    key -- Keyword Name.

    """
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
    """
    This Function retrieves a single value from a Sql Query built from the various passed parameters.

    The format is:
    SELECT (valueColumn) FROM (tableName) WHERE (searchColumn) = (appended_search_value)

    If there are multiple values returned from the query, only the very first one is returned.

    Keyword arguements:
    tableName -- Name of the Table.
    searchColumn -- Comparison Column(against searchValue)
    searchValue -- Value we are using in the search.
    valueColumn -- Column that holds the value we are searching for.
    searchRequiresQuotes -- If True, a set of quotes is appended to the searchValue.

    """
    try:
        if searchRequiresQuotes:
            appended_search_value = cleanForSQL( searchValue )
        appended_search_value = VarIf(searchRequiresQuotes, "'" + appended_search_value + "'", appended_search_value )
        query =  f"SELECT {valueColumn} FROM {tableName} WHERE {searchColumn} = {appended_search_value}"
        cur.execute( query, "")
        row = cur.fetchone()
        if row is None:
            return None
        else:
            return row[0]
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)

def returnNextSerialID( tableName, serialColumn ):
    """
    Function returns the next id for a serial type column inside the database. (1 is returned if there are no rows inside the table)

    Keyword arguements:
    tableName -- Name of table where the serialColumn resides.
    serialColumn -- Name of the Serial Column.

    """
    try:
        #To avoid getting an error, we need to check to see if there are any rows in the table.
        serial_search_query = f"""SELECT MAX({serialColumn}) FROM {tableName}"""
        cur.execute( serial_search_query, "" )
        row = cur.fetchone()
        if row[0] != None:
            return row[0] + 1
        else:
            return 1
    except(Exception, psycopg2.DatabaseError) as error:
        print("Offending Query: " + serial_search_query)
        print(error)

def cleanForSQL( strToClean ):
    """
    Function (currently) replaces all single quotes inside a string to double quotes. (This is to escape the quotes for inserting into postgres)

    Keyword arguements:
    strToClean -- String parameter to clean. (this is returned by the function)

    """
    return strToClean.replace( "'", "''" )

def checkRowExists( query ):
    """
    Function checks to see if a particular row exists inside the query parameter. The query parameter should be formatted in this fashion:
    SELECT 1 FROM (tableName) WHERE (columnName) = (valueToFind)

    Keyword arguements:
    query -- The query that checks to see if a particular row exists (as described above).

    """
    try:
        cur.execute( query, "" )
        return cur.fetchone() is not None
    except(Exception, psycopg2.DatabaseError ) as error:
        print (error)
        print( "Offending Query " + query )

def ifNotNone( item, ifNoneReturn ):
    if item is not None:
        return item
    else:
        return ifNoneReturn


