import json
import sys
import requests
import os.path
from os import path
from user_functions import QueryWithSingleValue
import psycopg2
from properties import storePropertyInfo, StoreListings
from database import cur, conn

client_id = "client_b90eaedfc6a2f7db118ac0266dec1c92"
client_secret = "secret_3d6127c6a2e4bdfc5992d524b86bef8a"
scopes_properties = ['api_properties_read']
scopes_listings = ['api_listings_read']
scopes_agencies = ['api_agencies_read']
auth_url = "https://auth.domain.com.au/v1/connect/token"
url_endpoint_suburb_info = "https://api.domain.com.au/v1/properties/_suggest"
url_endpoint_property_info = "https://api.domain.com.au/v1/properties/"
url_endpoint_suburbListings_info = "https://api.domain.com.au/v1/listings/residential/_search"
url_endpoint_listings_info = "https://api.domain.com.au/v1/listings/"

#store the access_token here
access_token_cache = '1cbc7a7fb8c7cd16c0a51e98848e08ce'

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
    postCode = QueryWithSingleValue( "suburbs", "name", suburbName, "postcode", True )
    # Need to build the Suburb JSON object to the Domain API (build a Dict)
    suburbObject = {
        "listingType" : "Sale",
        "propertyTypes": 
        [
            "House"
        ],
        "pageSize": 100,
        "minBedrooms": 1,
        "minBathrooms": 1,
        "locations":
        [
            {
                "state": "WA",
                "region": "",
                "area": "",
                "suburb": f"{suburbName}",
                "postcode": f"{postCode}"
            }
        ]
    }
    # Convert the suburbObject to a JSON-formatted string.
    suburbObject_JSON = json.dumps( suburbObject )
    # Send the request to the Residential Search listings endpoint.
    req = requests.post( url, headers = auth, data= suburbObject_JSON )
    # Get the results.
    results = req.json()
    debug = open( "debug.txt", "w")
    debug.write( json.dumps( results ) )
    # Scroll through each listing in the results and store them (Use try/except to scan for errors)
    #for listing in results:
    #    listingObject = getListing( listing['id'] )
    #    StoreListing( listingObject )

def getListing( listing_id ):
    auth = {'Authorization': 'Bearer ' + access_token }
    url = url_endpoint_listings_info + str(listing_id)
    req = requests.get( url, headers = auth )
    results = req.json()
    return results
    
### End Functions


### DB Functions

#def DB_storeProperties():
    # this opens suburbs_list.txt and returns a list of suburbs seperated by a line from that file.
#    suburb_list = getSuburbList()
    # this gets each line of the suburbs_list file.
#    suburbs = suburb_list.readlines()

    #Iterate through each line
#    try:
#        for suburb in suburbs:
#            properties = getSuburbInfo(suburb)
#            firstProperty = properties[0]
#            addressComponents_PostCode = firstProperty['addressComponents']['postcode']
#            stripped_suburb = suburb.replace('\n', "")
#            storeSuburbPostCode( stripped_suburb, addressComponents_PostCode )
#    except(Exception, psycopg2.DatabaseError) as error:
#        print(error)
#    conn.commit()

### End DB Functions.

### Let's store the listing first. Then request the access token for the agencies endpoint.
if len(access_token_cache) == 0:
  access_token = get_access_token( scopes_listings )
else:
    access_token = access_token_cache
listing_id = 2016469542
listingObject = getListing(listing_id)
StoreListings(listingObject)
conn.commit()

        

