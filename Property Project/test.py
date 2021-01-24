import json
import sys
import requests
import os.path
import re
from os import path
from user_functions import QueryWithSingleValue, checkRowExists, ifNotNone, cleanForSQL, VarIf
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
url_endpoint_agent_info = "https://api.domain.com.au/v1/agents/"

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
                "postCode": f"{postCode}",
                "includeSurroundingSuburbs": False
            }
        ]
    }

    json_string = json.dumps( suburbObject )
    # Send the request to the Residential Search listings endpoint.
    req = requests.post( url, headers = auth, data= json_string )
    # Get the results.
    print( req )
    results = req.json()
    debug = open( "debug.txt", "w")
    debug.write( json.dumps( results ) )
    return results

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
        new_listing_id = StoreListings( listingObject )
        DB_Agent_ID = []
        #If the listing is successfully stored...
        #Check to see if it was advertised by an agency, and if it was, check to see if that agency and its agents have already been stored.
        if 'advertiserIdentifiers' in listingObject and listingObject['advertiserIdentifiers']['advertiserType'] == 'agency':
            new_agency = False
            #Get the Agents who organized the listing and place it into a seperate array.
            agent_listings_ids = listingObject['advertiserIdentifiers'].get('contactIds')
            advertiserObject = listingObject['advertiserIdentifiers']
            if not checkRowExists( f"SELECT 1 FROM agencies WHERE domain_agency_id = {advertiserObject['advertiserId']} " ):
                #Create a new Agency Object here.
                agencyObject = getAgency( advertiserObject['advertiserId'] )
                #Seperate parts of the agency object into several different dictionaries to make life easier.
                agencyProfile = agencyObject['profile']
                listingAgency = Agency(  agencyProfile.get( 'agencyBanner' ), agencyProfile.get('agencyWebsite'), agencyProfile.get('agencyLogoStandard'),
                                                agencyProfile['mapLongitude'], agencyProfile['mapLatitude'], agencyProfile['agencyDescription'], agencyProfile['numberForSale'],
                                                agencyProfile['numberForRent'], agencyObject['name'], agencyObject['id'], agencyObject['details']['principalName'], agencyObject,
                                                agencyObject['contactDetails'], agencyObject['details']['streetAddress1'], agencyObject['details']['streetAddress2'],
                                                agencyObject['details']['suburb'], None, None, None, agencyObject['details']['state'], agencyObject['details']['postcode'] )
                new_agency_id = listingAgency.storeAgency()
                if new_agency_id is None:
                    raise RuntimeError( "Error in Function storeAgency for Agency " + agencyObject['name'] )
                if 'agents' in agencyObject:
                    for agent in agencyObject['agents']:
                        #Check to see if the Agent has already been stored.
                        if not checkRowExists( f"SELECT 1 FROM agents WHERE domain_agent_id = {agent['id']}"  ):
                            agentObject = Agent( agent['email'], agent['firstName'], agent['lastName'], agent.get('profileText'), agent.get('mugshot_url'),
                                        agent['id'], new_agency_id, agent.get('facebook_url'), agent.get('twitter_url'), agent.get('phone'), agent.get('photo') )
                            new_agent_id = agentObject.storeAgent( False )
                            if new_agent_id is None:
                                raise RuntimeError( "Error in Function storeAgent for Agent " + agent['firstName'] + " " + agent['lastName'] )
                            #We only need to do this if there was Agency that created this listing.
                            if agent_listings_ids is not None and agent['id'] in agent_listings_ids:
                                DB_Agent_ID.append( new_agent_id )
                        elif agent_listings_ids is not None and agent['id'] in agent_listings_ids:
                            #Get the ID of the Agent and append it to the DB_Agent_ID Contact
                            cur.execute( """ SELECT a.agent_id 
                                             FROM agents a 
                                             WHERE a.domain_agent_id = %s
                                                AND a.cnewrev IS NULL """,
                                            ( agent['id'] ) )
                            agent_id = cur.fetchone()
                            DB_Agent_ID.append( agent_id )
                new_agency = True
            #If we are here, that means that the listing was advertised by an Agency.
            if not new_agency:
                #If we didn't save a new_agency, we need to get the agency_id/agent_ids from the database/
                cur.execute( """ SELECT a.agency_id
                                 FROM agencies a
                                 WHERE a.domain_agency_id = %s 
                                        AND a.cnewrev IS NULL """, 
                                 ( advertiserObject['advertiserId'], ) )
                new_agency_id = cur.fetchone()[0]
                if agent_listings_ids is not None:
                    for agent in agent_listings_ids:
                        #Agency acquired, now we need to copy the query from before and get the agentids
                        cur.execute( """ SELECT a.agent_id
                                        FROM agents a
                                        WHERE a.domain_agent_id = %s """,
                                    ( agent, ) )
                        holder_value = cur.fetchone()
                        if holder_value is not None:
                            agent_id = holder_value[0]
                        else:
                            #We need to create a new Agent in that case.
                            new_agent_object = getAgent( agent )
                            if new_agent_object is not None:
                                newAgentObject = Agent( new_agent_object['email'], new_agent_object['firstName'], new_agent_object['lastName'], new_agent_object.get('profileText'), new_agent_object.get('mugshot_url'),
                                        new_agent_object['agencyId'], new_agency_id, new_agent_object.get('facebook_url'), new_agent_object.get('twitter_url'), new_agent_object.get('phone'), new_agent_object.get('photo') )
                                agent_id = newAgentObject.storeAgent(False)
                            else:
                                raise Exception( "Unable to get New Agent Data for Agent " + str(agent))

                        DB_Agent_ID.append( agent_id )

            if DB_Agent_ID is None:
                cur.execute( """ INSERT INTO agent_listings( agency_id, listings_id, entered_when )
                                 VALUES( %s, %s, current_timestamp )""",
                                 ( new_agency_id, new_listing_id ) )
            else:
                for agent_id in DB_Agent_ID:
                    cur.execute( """ INSERT INTO agent_listings( agency_id, agent_id, listings_id, entered_when )
                                     VALUES( %s, %s, %s, current_timestamp ) """,
                                     ( new_agency_id, agent_id, new_listing_id ) )

        #Once the advertiser has been saved, you want to store any linked property details inside the propery table.
        listing_address = listingObject['addressParts']['displayAddress']
        #We don't want to store the address against the listing. Instead, we want to store it to the property that is linked to the listing.
        propertyObject = getPropertyFromListing( listing_address )
        if len( propertyObject ) != 0:
            #We need to get the first property object when this returns...
            propertyStore = Property.initFromObject( getPropertyInfo( propertyObject[0]['id']) )
            new_property_id = propertyStore.saveProperty( False )
            cur.execute( """ INSERT INTO properties_listings( property_id, listings_id, entered_when )
                            VALUES( %s, %s, current_timestamp ) """,
                            ( new_property_id, new_listing_id ) )
        return True
    except( Exception, psycopg2.DatabaseError ) as error:
        print( error )
        print( "Original Exception Message" + ( error.__cause__ or error.__context__ ))
        return False

def cleanPrice( ):
    #We need to establish a set of rules to clean the data
    try:
        #As Ella suggested, split each line into a string. But we first need to query the actual information.
        cur.execute( """SELECT l.display_price, l.listings_id
                        FROM listings l
                        WHERE l.price_displayed = FALSE""",    
                        () )
        # Get the Results
        results = cur.fetchall()
        result_list_exceptions = []
        #Time for the rules section.
        for line in results:
            new_price = None
            #In actuality, the very first thing that we need to check is the - symbol, as that will identify a range.
            result = ''.join( line[0] )
            #We probably need to check if the price is the only thing there too. (just in case)
            only_price = checkIsPrice( result )
            if only_price is not None:
                new_price = only_price
            else:
                #result = re.sub( r'(\(|\))', '', result )
                check_range = result.find( "-" )
                while True:
                    if check_range != -1:
                        #Not sure how to go from here...
                        #We need to get the prices from both sides...
                        result_list = str(result).split( )
                        seperator_index = result_list.index( '-' )
                        start_price = checkIsPrice( result_list[seperator_index - 1] )
                        end_price = checkIsPrice( result_list[seperator_index + 1] )
                        if start_price is not None and end_price is not None:
                            new_price = round( ( int( start_price ) + int( end_price ) ) / 2, 2 )
                            break
                    #if the string contains the word "From" and an int that can be identified as a string...
                    from_find_result = re.findall( r'from ?\$?\d+\$?', result )
                    if len( from_find_result ) != 0:
                        result_list = from_find_result[0].split()
                        #Verify that the second string is actually a price. (It could have been something like From Your Wonderful Property Developers -_-)
                        check_price = checkIsPrice( result_list[1] )
                        if check_price is not None:
                            new_price = check_price
                            break
                    # If both the From and the Range have failed, try and disect the string normally.
                    result_list = str(result).split( " " )
                    if len(result_list) == 1:
                        #If there is only one word in the entire display price, use a regular expression to test if it could be the price.
                        word_result = checkIsPrice( result_list[0] )
                        if word_result is not None:
                            new_price = word_result
                            break
                        else:
                            new_price = None
                            result_list_exceptions.append( result )
                    #Step 2: If there is more then one eleement within the string, we need to iterate through all of them and test each one.
                    else:
                        for word in result_list:
                            #Check if it's a price first.
                            word_result = checkIsPrice( word )
                            if word_result is None:
                                continue
                            else:
                                new_price = word_result
                                break
                        #If it's completely failed
                        if new_price is None:
                            result_list_exceptions.append( result )
                    break
            if new_price is not None:
                # If the new price was successfully found for that row, perform an update
                cur.execute( """ UPDATE listings
                                 SET price = %s,
                                       price_displayed = true
                                 WHERE listings_id = %s """, 
                                 ( new_price, line[1]) )
        return True
    except( Exception, psycopg2.DatabaseError ) as error:
        print( error )
        print( line[1])
        return False

def checkIsPrice( word ):
    #Remove any commas from the word
    reg_result = word.replace( ",", "" )
    reg_result = re.sub( r'(k|K)', '000', reg_result )
    reg_result = re.sub( r' ?(MILLIONS|MILLION|mils|mil|Milions|MIL\'S|millions|million)', '000000', reg_result )
    #Try and replace any ks.
    #result = re.match( r'\$?\d+(k|K)', reg_result )
    #if result is None:
        #Restore original result
        #pass
    #else:
    #    reg_result = reg_result[0:len(reg_result) - 1] + '000'

    match_result = re.findall( r'\$?\d+\$?', reg_result )
    if len(match_result) != 0:
        return re.findall( r'\d+', reg_result )[0]
    else:
        return None


def DB_storeProperties():
    #This will be the function that we use to iterate through the suburbs and store the associated listings.

    #We first need to open the suburbs list.
    suburb_list = getSuburbList()
    #This gets the suburb_list in list form.
    suburbs = suburb_list.readlines()

    #Get the line that we are currently on.
    current_line = int( QueryWithSingleValue( 'setup', 'item_name', 'current_list_position', 'item_value', True ) )
    count = 0
    suburbs_to_insert = 3

    for suburb in suburbs:
        if count < current_line:
            count = count + 1
            continue
        if count == suburbs_to_insert + current_line:
            break
        else:
            #Get the JSON Listings Object using the getSuburbListingsInfo Function
            listings = getSuburbListingsInfo( str.strip( suburb ) )
            for listing in listings:
                print( listing['listing']['id'] )
                if not storeFullListing( listing['listing']['id'] ):
                    return False
                else:
                    cur.execute( """ UPDATE setup
                                     SET item_value = %s
                                     WHERE item_name = \'current_list_position\' """ , (count + 1,) )                  
        count = count + 1
    conn.commit()

### End DB Functions.

#This always has to be called at the start of running test.py
getAccessTokens() 
#storeFullListing( 2016566833 )
DB_storeProperties()
#conn.commit()
#cleanPrice()
#print( checkIsPrice( 'From Low $4mils' ) )
#if not cleanPrice():
#    pass
conn.commit()

