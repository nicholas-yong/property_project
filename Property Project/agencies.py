import psycopg2
import json
from user_functions import VarIf, convertJSONDate, getKeyValue, QueryWithSingleValue, returnNextSerialID, cleanStringForInsert, checkRowExists
from database import cur, conn
from user_constants import *
from properties import storeAddress


### Agency Storage Functions

### End Agency Storage Functions

def storeAgency( agencyObject ):
    
    #Set values for particular variables that may be null
    agencyProfile = agencyObject['profile']
    agencyDetails = agencyObject['details']
    
    try:
        #Since we need to get the address_id after it gets stored, we will need to store the address_id first.
        street_name = agencyDetails['streetAddress1'] + ' ' + agencyDetails['streetAddress2']
        full_address = street_name + ' ' + agencyDetails['suburb'] + ' ' + agencyDetails['state'] + ' ' + str(agencyDetails['postcode'])_

        #Always try and get the number from the first streetAddress first (streetAddress1)
        address_id = storeAddress( full_address, street_name, 

        agency_object_insert_statement = f""" INSERT INTO agencies( description, entered_when, num_rent, num_sale, name, principal_agent_name,  domain_agency_id, raw_agency_json )
                                              VALUES( {agencyProfile['agencyDescription']}, current_timestamp, {agencyProfile['numberForRent']}, {agencyProfile['numberForSale']},
                                              '{agencyObject['name']}', '{agencyDetails['principalName']}', {agencyObject['id']}, '{agencyObject}' ) """
        
        cur.execute( agency_object_insert_statement, "" )

        #Once we've stored the agency, we need to store a few other things...
        #First, the location of the agency. (I.E, The address)

    except: