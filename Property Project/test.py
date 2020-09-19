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
scopes_properties = ['api_properties_read']
scopes_listings = ['api_listings_read']
auth_url = "https://auth.domain.com.au/v1/connect/token"
url_endpoint_suburb_info = "https://api.domain.com.au/v1/properties/_suggest"
url_endpoint_property_info = "https://api.domain.com.au/v1/properties/"
url_endpoint_suburbListings_info = "https://api.domain.com.au/v1/listings/residential/_search"

#store the access_token here
access_token_cache = 'f18382b9901fb576aad0d91aafa3831d'

### Connectivity Functions
def get_access_token( scopes_type ):
    #Set the endpoint scope that we are using.
    scopes = scopes_type
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
### End Connectivity Functions

### GET Functions

def getSuburbInfo( suburbName ):
    auth = {'Authorization': 'Bearer ' + access_token}
    url = url_endpoint_suburb_info + '?terms=' + str(suburbName) + 'WA'
    print(url)
    req = requests.get( url, headers = auth )
    results = req.json()
    return results

def getSuburbList():
    file_object = open('suburb_list', 'r')
    return file_object
        
def getPropertyInfo( domain_property_id ):
    auth = {'Authorization': 'Bearer ' + access_token }
    url = url_endpoint_property_info + str(domain_property_id)
    req = requests.get( url, headers = auth )
    results = req.json()
    return results

def getSuburbListingsInfo( suburbName ):
    auth = {'Authorization': 'Bearer ' + access_token} 
    url = url_endpoint_suburbListings_info
    # Need to build the Suburb JSON object to the Domain API (build a Dict)
    suburbObject = {
        "listingType" : "Sale",
        "propertyTypes": "House",
        "minBedrooms": 3,
        "minBathrooms": 2,
        "locations":
        {
            "state": "WA",
            "suburb": f"{suburbName}"
        }
    }
    # Convert the suburbObject to a JSON-formatted string.
    suburbObject_JSON = json.dumps( suburbObject )
    print( suburbObject_JSON )
    req = requests.post( url, headers = auth, json= suburbObject_JSON )
    print( req )
    results = req.json()
    print( results )
    

    

### End Functions


### DB Functions

def DB_storeProperties():
    # this opens suburbs_list.txt and returns a list of suburbs seperated by a line from that file.
    suburb_list = getSuburbList()
    # this gets each line of the suburbs_list file.
    suburbs = suburb_list.readlines()

    # initialize variables
    count = 0
    true_count = 21

    #Iterate through each line
    try:
        for suburb in suburbs:
            if count < true_count:
                count = count + 1
                continue
            if count == 36:
                break
            else:
                properties = getSuburbInfo(suburb)
                for property in properties:
                    property_info = getPropertyInfo(property['id'])
                    storePropertyInfo( property_info )
                 #commit if storage succeeds
                conn.commit()   
            count = count + 1
    except(Exception, psycopg2.DatabaseError) as error:
        conn.rollback()
        print(error)

### End DB Functions.

if len(access_token_cache) == 0:
  access_token = get_access_token( scopes_listings )
else:
    access_token = access_token_cache
getSuburbListingsInfo("Armadale")

        

