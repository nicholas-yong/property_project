import json
import sys
import requests
import os.path
from os import path
import user_functions
import psycopg2
from properties import storePropertyInfo
from database import cur, conn

client_id = "client_b90eaedfc6a2f7db118ac0266dec1c92"
client_secret = "secret_3d6127c6a2e4bdfc5992d524b86bef8a"
scopes = ['api_properties_read']
auth_url = "https://auth.domain.com.au/v1/connect/token"
url_endpoint_suburb_info = "https://api.domain.com.au/v1/properties/_suggest"
url_endpoint_property_info = "https://api.domain.com.au/v1/properties/"

#store the access_token here
access_token_cache = 'd5a92c35594895d1eda9b177d9a066d9'

def get_properties_info( suburbName ):
    auth = {'Authorization': 'Bearer ' + access_token}
    url = url_endpoint_suburb_info + '?terms=' + str(suburbName) + '%26WA'
    print(url)
    req = requests.get( url, headers = auth )
    results = req.json()
    return results

def get_access_token():
    response = requests.post(auth_url, data = 
    {
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'client_credentials',
        'scope': scopes,
        'Content-Type': 'text/json'
    })
    json_res = response.json()
    print(json_res)
    return json_res['access_token']

def getSuburbList():
    file_object = open('suburb_list', 'r')
    return file_object

#def domain-storeIDs(suburbData):
    #check if property_id file exists
    #property_id_file = open( 'property_id', 'a' )
    #for x in suburbData:
        #property_id_file.write( x['id'] + '\n' )
        
def getPropertyInfo( domain_property_id ):
    auth = {'Authorization': 'Bearer ' + access_token }
    url = url_endpoint_property_info + str(domain_property_id)
    req = requests.get( url, headers = auth )
    results = req.json()
    return results

#comment this Function for future use.
#def updateJSON( ):
#    all_current_properties = f""" SELECT p.domain_prop_id FROM properties p"""
#    cur.execute( all_current_properties, "" )
#    results = cur.fetchall()
#    for result in results:
#        property_info = getPropertyInfo(result[0])
#        property_info = json.dumps(property_info)
#        update_json_query = f""" UPDATE properties SET raw_json = '{property_info}' WHERE domain_prop_id = '{result[0]}'"""
#        cur.execute( update_json_query )
#    conn.commit()
#    print( results );

def storeProperties():
    # this opens suburbs_list.txt and returns a list of suburbs seperated by a line from that file.
    suburb_list = getSuburbList()
    # this gets each line of the suburbs_list file.
    suburbs = suburb_list.readlines()

    # initialize variables
    count = 0
    true_count = 2

    #Iterate through each line
    try:
        for suburb in suburbs:
            if count < true_count:
                count = count + 1
                continue
            if count == 6:
                break
            else:
                properties = get_properties_info(suburb)
                for property in properties:
                    property_info = getPropertyInfo(property['id'])
                    storePropertyInfo( property_info )
                 #commit if storage succeeds
                conn.commit()   
            count = count + 1
    except(Exception, psycopg2.DatabaseError) as error:
        conn.rollback()
        print(error)

if len(access_token_cache) == 0:
   access_token = get_access_token()
else:
    access_token = access_token_cache
storeProperties() 

        

