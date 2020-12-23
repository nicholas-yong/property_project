import json
import sys
import requests
import os.path
from os import path
from user_functions import QueryWithSingleValue, checkRowExists, ifNotNone
import psycopg2
from properties import Property
from listings import StoreListings
from database import cur, conn
from agencies import Agent, Agency
from files import File
from contact_details import ContactDetails

client_id = "client_b90eaedfc6a2f7db118ac0266dec1c92"
client_secret = "secret_3d6127c6a2e4bdfc5992d524b86bef8a"
scopes_properties = ['api_properties_read']
scopes_listings = ['api_listings_read']
scopes_agencies = ['api_agencies_read']
auth_url = "https://auth.domain.com.au/v1/connect/token"
url_endpoint_suburb_info = "https://api.domain.com.au/v1/properties/_suggest?terms=10+"
url_endpoint_property_info = "https://api.domain.com.au/v1/properties/"
url_endpoint_suburbListings_info = "https://api.domain.com.au/v1/listings/residential/_search"
url_endpoint_listings_info = "https://api.domain.com.au/v1/listings/"
url_endpoint_agencies_info = "https://api.domain.com.au/v1/agencies/"
url_endpoint_agent_info = "https://api.domain.com.au/v1/agents"

#Constants for Access Tokens
listings_access_token_cache = ''
agencies_access_token_cache = ''
properties_access_token_cache = ''

listings_access_token = ''
agencies_access_token = ''
properties_access_token = ''

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
    auth = {'Authorization': 'Bearer ' + properties_access_token}
    url = url_endpoint_suburb_info + '?terms=' + str(suburbName) + 'WA'
    print(url)
    req = requests.get( url, headers = auth )
    results = req.json()
    return results

def getSuburbList():
    file_object = open('suburb_list', 'r')
    return file_object
        
def getPropertyInfo( domain_property_id ):
    auth = {'Authorization': 'Bearer ' + properties_access_token }
    url = url_endpoint_property_info + str(domain_property_id)
    req = requests.get( url, headers = auth )
    results = req.json()
    return results

def getSuburbListingsInfo( suburbName ):
    auth = {'Authorization': 'Bearer ' + listings_access_token } 
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
    print( req )
    results = req.json()
    debug = open( "debug.txt", "w")
    debug.write( json.dumps( results ) )
    # Scroll through each listing in the results and store them (Use try/except to scan for errors)
    #for listing in results:
    #    listingObject = getListing( listing['id'] )
    #    StoreListing( listingObject )

def getListing( listing_id ):
    auth = {'Authorization': 'Bearer ' + listings_access_token }
    url = url_endpoint_listings_info + str(listing_id)
    req = requests.get( url, headers = auth )
    results = req.json()
    return results

def getAgency( agency_id ):
    auth = {'Authorization': 'Bearer ' + agencies_access_token }
    url = url_endpoint_agencies_info + str(agency_id)
    req = requests.get(url, headers = auth)
    results = req.json()
    return results

def getAgent( agent_id ): 
    auth = {'Authorization': 'Bearer ' + agencies_access_token }
    url = url_endpoint_agent_info + str(agent_id)
    req = requests.get(url, headers = auth)
    results = req.json()
    return results

def getPropertyFromListing( address ):
    auth = {'Authorization': 'Bearer ' + properties_access_token }
    url = url_endpoint_suburb_info + str(address)
    req = requests.get( url, headers = auth)
    results = req.json()
    return results

def getAccessTokens():
    access_token_file = open( "access_tokens.txt", "a" )
    access_token_file.truncate(0)
    global properties_access_token
    global listings_access_token
    global agencies_access_token
    #Get and set properties access token
    #Set the endpoint scope that we are using.
    if properties_access_token_cache == '':
        scopes = scopes_properties
        response = requests.post(auth_url, data = 
        {
            'client_id': client_id,
            'client_secret': client_secret,
            'grant_type': 'client_credentials',
            'scope': scopes,
            'Content-Type': 'text/json'
        })
        json_res = response.json()
        access_token_file.write( 'properties ' + json_res['access_token'] + '\n'  )
        properties_access_token = json_res['access_token']
    else:
        properties_access_token = properties_access_token_cache
    
    if listings_access_token_cache == '':
        #Get and set listings access token
        scopes = scopes_listings
        response = requests.post(auth_url, data = 
        {
            'client_id': client_id,
            'client_secret': client_secret,
            'grant_type': 'client_credentials',
            'scope': scopes,
            'Content-Type': 'text/json'
        })
        json_res = response.json()
        access_token_file.write( 'listings ' + json_res['access_token'] + '\n' )
        listings_access_token = json_res['access_token']
    else:
        listings_access_token = listings_access_token_cache

    if agencies_access_token_cache == '':
        #Get and set agencies access token
        scopes = scopes_agencies
        response = requests.post(auth_url, data = 
        {
            'client_id': client_id,
            'client_secret': client_secret,
            'grant_type': 'client_credentials',
            'scope': scopes,
            'Content-Type': 'text/json'
        })
        json_res = response.json()
        access_token_file.write( 'agencies ' + json_res['access_token'] + '\n' )
        agencies_access_token = json_res['access_token']
    else:
        agencies_access_token = agencies_access_token_cache
    
    access_token_file.close()
### End Functions

### Storage Functions (Not the actual storage functions, but these act as the function initializers)

def storeFullListing( listing_id ):
    try:
        #Get thje listingObject via the getListing Function
        listingObject = getListing( listing_id )
        #Once we've obtained the listingObject, store it.
        StoreListings( listingObject )
        #If the listing is successfully stored...
        #Check to see if it was advertised by an agency, and if it was, check to see if that agency and its agents have already been stored.
        if 'advertiserIdentifiers' in listingObject and listingObject['advertiserIdentifiers']['advertiserType'] == 'agency':
            advertiserObject = listingObject['advertiserIdentifiers']
            if not checkRowExists( f"SELECT 1 FROM agencies WHERE domain_agency_id = {advertiserObject['advertiserId']} " ):
                #Create a new Agency Object here.
                agencyObject = getAgency( advertiserObject['advertiserId'] )
                print( agencyObject['details']['streetAddress1'] )
                #Seperate parts of the agency object into several different dictionaries to make life easier.
                agencyProfile = agencyObject['profile']
                listingAgency = Agency( ifNotNone( agencyProfile['agencyBanner'], None ), ifNotNone( agencyProfile['agencyWebsite'], None ), ifNotNone( agencyProfile['agencyLogoStandard'], None ),
                                                agencyProfile['mapLongitude'], agencyProfile['mapLatitude'], agencyProfile['agencyDescription'], agencyProfile['numberForSale'],
                                                agencyProfile['numberForRent'], agencyObject['name'], agencyObject['id'], agencyObject['details']['principalName'], agencyObject,
                                                agencyObject['contactDetails'], agencyObject['details']['streetAddress1'], agencyObject['details']['streetAddress2'],
                                                agencyObject['details']['suburb'] )
                if not listingAgency.storeAgency( ):
                    raise RuntimeError( "Error in Function storeAgency for Agency " + agencyObject['name'] )
                if 'agents' in agencyObject:
                    for agent in agencyObject['agents']:
                        agentObject = Agent( agent['email'], agent['firstName'], agent['lastName'], agent.get('profile_text', 'NULL'), agent.get('mugshot_url', 'NULL'),
                                        agent['id'], agencyObject['id'], agent.get('facebook_url', 'NULL'), agent.get('twitter_url', 'NULL'), agent.get('phone', 'NULL'), agent.get('photo', 'NULL') )
                        if not agentObject.storeAgent( False ):
                            raise RuntimeError( "Error in Function storeAgent for Agent " + agent['firstName'] + " " + agent['lastName'] )
        #Once the advertiser has been saved, you want to store any linked property details inside the propery table.
        listing_address = listingObject['addressParts']['displayAddress']
        #We don't want to store the address against the listing. Instead, we want to store it to the property that is linked to the listing.
        propertyObject = getPropertyFromListing( listing_address )
        #We need to get the first property object when this returns...
        propertyStore = Property.initFromObject( getPropertyInfo( propertyObject[0]['id']) )
        propertyStore.saveProperty( False ) 
    except( Exception, psycopg2.DatabaseError ) as error:
        print (error)



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

#This always has to be called at the start of running test.py
getAccessTokens()
listing_id = 2016573445
storeFullListing(listing_id)
conn.commit()

