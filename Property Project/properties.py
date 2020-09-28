import psycopg2
import json
from user_functions import VarIf, convertJSONDate, getKeyValue, QueryWithSingleValue, returnNextSerialID, cleanStringForInsert
from database import cur, conn

PROPERTY_MARKET_STATUS_ONMARKET = "OnMarket"
PROPERTY_MARKET_STATUS_OFFMARKET = "OffMarket"

OBJECT_Listing = 2
OBJECT_Property = 1

### Data Retrieval Functions

def DB_GetPropertyInfo( property_id ):
    #What sort of columns do we need to retrieve?
    
    """
    Need to get address, land_size, bedroom_number, bathroom_number, features, lat/long, suburb, whether its on the market or not.
    """

### End Data Retrieval Functions

### Data Storage Functions

def storePropertyInfo( propertyObject ):
    #store the propertyObject inside a text file in case we want to debug afterwards.
    debug = open( "debug.txt", "w")
    debug.write( json.dumps(propertyObject))

    #We need to split the propertyObject into several smaller dictionaries for the sub tables.
    history = {}
    files = {}
    address = {}

    #We can assume that each property will always have an address, so we do not need to test for its existence.
    address['address_id'] = getKeyValue('address_id')
    address['full_address'] =  propertyObject['address']
    address['street_name'] = propertyObject['streetName']
    address['street_number'] = propertyObject['streetNumber']
    address['street_type'] = propertyObject['streetType']
    address['suburb_id'] = QueryWithSingleValue( "suburbs", "name", propertyObject['suburb'], "suburb_id", True )
    address['postcode'] = propertyObject['postcode']
    if 'zone' in propertyObject:
        address['zone'] = propertyObject['zone']
    if 'lotNumber' in propertyObject:
        address['lot_number'] = propertyObject['lotNumber']
    if 'addressCoordinate' in propertyObject:
        address['lon'] = propertyObject['addressCoordinate']['lon']
        address['lat'] = propertyObject['addressCoordinate']['lat']
    if "history" in propertyObject:
        history['history'] = propertyObject['history']
    if "photos" in propertyObject:
        files['photos'] = propertyObject['photos']

    # setup possibly optional propertyObjects
    if 'cadastreType' in propertyObject:
        cadastreType = propertyObject['cadastreType']
    else:
        cadastreType = "NULL"
    if 'bedrooms' in propertyObject:
        bedrooms = propertyObject['bedrooms']
    else:
        bedrooms = 0
    if 'bathrooms' in propertyObject:
        bathrooms = propertyObject['bathrooms']
    else:
        bathrooms = 0
    if 'areaSize' in propertyObject:
        areaSize = propertyObject['areaSize']
    else:
        areaSize = 0

    try:
        #store the address first
        storeAddress( address )
        raw_json = json.dumps(propertyObject)
        #once done, split the information. But for now, let's just do a test.
        insert_statement = f""" INSERT INTO properties( domain_prop_id, shape_of_land, on_market, address_id, area_size, num_bedrooms, num_bathrooms, 
                                                        features, entered_when, raw_json )
                                VALUES( '{propertyObject['id']}', '{cadastreType}', {VarIf( propertyObject['status'] == PROPERTY_MARKET_STATUS_ONMARKET, True, False )}, 
                                         {address['address_id']}, {areaSize}, {bedrooms},
                                         {bathrooms}, '{' and '.join(propertyObject['features'] )}', current_timestamp, '{raw_json}' )"""
        cur.execute(insert_statement, "")
         #store the sales history of the address if there is any
        prop_id_query = f""" SELECT p.property_id FROM properties p WHERE domain_prop_id = '{propertyObject['id']}' """
        cur.execute( prop_id_query, "")

        result = cur.fetchone()
        new_prop_id = result[0]

        if len( history ) != 0:
            storeSalesHistory( history, new_prop_id )
        if len( files ) != 0:
            storeFiles( files, new_prop_id )
    except(Exception, psycopg2.DatabaseError) as error:
        print( "Property with ID " + propertyObject['id'] + " unable to be inserted" )
        print(error)

def storeAddress( addressObject ):
    try:
        #store the lotnumber and zone
        if 'zone' in addressObject:
            zone = '\'' + addressObject['zone'] + '\''
        else:
            zone = "NULL"
        
        if 'lot_number' in addressObject:
            lotNumber = '\'' + addressObject['lot_number'] + '\''
        else:
            lotNumber = "NULL"

        if 'lat' not in addressObject and 'lon' not in addressObject:
            latlong_insert = ')'
            latlong_values = ')'
        else:
            latlong_insert = ', latlong)'
            latlong_values = ',' + '\'' + f"""SRID=4326;Point({addressObject['lon']} {addressObject['lat']})""" + '\'' + ')'
    
        address_insert_statement = f""" INSERT INTO address( address_id, full_address, street_name, street_number, street_type, 
                                                             suburb_id, postcode, zone, lot_number {latlong_insert}
                                        VALUES( {addressObject['address_id']}, '{addressObject['full_address']}', '{addressObject['street_name']}', '{addressObject['street_number']}',
                                               '{addressObject['street_type']}', {addressObject['suburb_id']}, {addressObject['postcode']}, {zone}, {lotNumber} 
                                                {latlong_values}""" 
        cur.execute(address_insert_statement, "")

    except(Exception, psycopg2.DatabaseError) as error:
        print("Offending Address Query: " + address_insert_statement)
        print(error) 

def storeFiles( fileObject, propertyID ):
    try:
        photos = fileObject['photos']

        for photo in photos:
            next_image_id = getKeyValue('image_id')
            #We need to store it inside the image table. Then get the image_id from the image table to INSERT into the files table.
            images_insert_statement = f""" INSERT into images( image_id, advert_id, date_taken, images_url )
                                           VALUES( {next_image_id}, {photo['advertId']}, to_timestamp( '{convertJSONDate(photo['date'])}',  'YYYY-MM-DD HH24:MI:SS' ), '{photo['fullUrl']}' )"""
            cur.execute( images_insert_statement, "")            
            files_insert_statement = f""" INSERT INTO files( file_id, file_type, file_link_type, keyval1, entered_when )
                                          VALUES( {next_image_id}, 'images', 1, '{propertyID}', current_timestamp ) """
            cur.execute( files_insert_statement, "")
    except(Exception, psycopg2.DatabaseError) as error:
        print("Offending Images Query: " + images_insert_statement)
        print("Offending Files Query: " + files_insert_statement)
        print(error)

def storeSalesHistory( salesHistoryObject, propertyID ):
    try:
        for history in salesHistoryObject:
            sales_history_insert_statement = f""" INSERT INTO property_sales_history( property_id, agency_id, sale_date, days_on_market, documented_as_sold, sale_price 
                                                                                      reported_as_sold, details_suppressed, price_suppressed, sale_type, agency_name,
                                                                                      property_sales_type, listings_id )
                                                  VALUES( {propertyID}, {history['apmAgencyId']}, to_timestamp( '{history['date']}', 'YYYY-MM-DD'), {history['daysOnMarket']},
                                                          {history['documentedAsSold']}, {history['price']}, {history['reportedAsSold']},
                                                          {history['suppressDetails']}, {history['suppressPrice']}, '{history['type']}', '{history['agency']}', 
                                                          '{history['propertyType']}', {history['id']} ) """
            cur.execute( sales_history_insert_statement, "")
    except(Exception, psycopg2.DatabaseError) as error:
        print("Offending Property Sales History Query: " +  sales_history_insert_statement )
        print(error)


def StoreListings( listingObject ):
    debug = open( "debug.txt", "w")
    debug.write( json.dumps( listingObject ))

    #Prepare the secondary dictionaries
    listing_sales = {}
    listing_sales_tenants = {}
    listing_sales_saleDetails = {}
    lisitng_inspections = {}
    listing_prices = listingObject['priceDetails']

    try:
        #Insert the raw listing first.
        #Get the listing_status
        listing_status = QueryWithSingleValue( 'listing_status_lkp', 'description', listingObject['status'], 'listing_status_id', True )

        #Test to see if the listing has any price details.
        if 'price' in listing_prices:
            price = listing_prices['price']
        else:
            price = 0
        if 'priceFrom' in listing_prices:
            priceFrom = listing_prices['priceFrom']
        else:
            priceFrom = 0
        if 'priceTo' in listing_prices:
            priceTo = listing_prices['priceTo']
        else:
            priceTo = 0
        if 'buildingAreaSqm' in listingObject:
            buildingAreaSqm = listingObject['buildingAreaSqm']
        else:
            buildingAreaSqm = 0
        if 'landAreaSqm' in listingObject:
            landAreaSqm = listingObject['landAreaSqm']
        else:
            landAreaSqm = 0
        if 'energyEfficiencyRating' in listingObject:
            energyEfficiencyRating = listingObject['energyEfficiencyRating']
        else:
            energyEfficiencyRating = 0

        #build the JSON from listingObject
        raw_listing_JSON = json.dumps( listingObject )

        #Get the value that will be used with listing_insert_statement
        listings_id = returnNextSerialID( 'listings', 'listings_id' )

        listing_insert_statement = f""" INSERT INTO listings( domain_listings_id, headline, price_displayed, display_price, price, price_from, price_to, seo_url, listing_objective,
                                                            listing_status, land_area, building_area, energy_efficiency, is_new_development, date_updated, date_created,
                                                            entered_when, entered_by, raw_listing, inspection_appointment_only )
                                        VALUES( '{listingObject['id']}', '{listingObject['headline']}', {listing_prices['canDisplayPrice']}, '{listing_prices['displayPrice']}', 
                                                {price}, {priceFrom}, {priceTo}, '{listingObject['seoUrl']}', '{listingObject['objective']}', {listing_status}, {landAreaSqm}, {buildingAreaSqm},
                                                {energyEfficiencyRating}, {listingObject['isNewDevelopment']}, to_timestamp( '{convertJSONDate(listingObject['dateUpdated'])}', 'YYYY-MM-DD HH24:MI:SS' ), 
                                                to_timestamp( '{convertJSONDate(listingObject['dateListed'])}', 'YYYY-MM-DD HH24:MI:SS' ),  current_timestamp, 1, '{cleanStringForInsert(raw_listing_JSON)}', {listingObject['inspectionDetails']['isByAppointmentOnly']} ) """
        cur.execute( listing_insert_statement, "" )

        #Insert the Features if the listing contains any.
        #Set the object type
        link_object_type = OBJECT_Listing
        storeFeatures( listings_id, link_object_type, listingObject['features'] )
    except(Exception, psycopg2.DatabaseError) as error:
        print(listing_insert_statement)
        print(error)

def storeFeatures( id, object_type, features ):
    try:
        for feature in features:
            #Only insert if the feature doesn't exist.
            check_feature_exists = f"SELECT feature_id FROM features_lkp WHERE UPPER( description ) = '{str.upper( feature )}'"
            cur.execute( check_feature_exists, "" )
            row = cur.fetchone()
            if row ==  None:
                feature_id = returnNextSerialID( 'object_features', 'feature_id' )
                #We need to store the new feature inside the features table.
                cur.execute( f""" INSERT INTO features_lkp( feature_id, description) VALUES( {feature_id}, '{cleanStringForInsert(feature)}' )""", "" )
            else:
                feature_id = row[0]
            #Once we've acquired the feature_id...
            object_features_insert_statement = f""" INSERT INTO object_features( id, object_type, feature_id, entered_when, entered_who )
                                                    VALUES( {id}, {object_type}, {feature_id}, current_timestamp, 1 )"""
            cur.execute( object_features_insert_statement, "")
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)


#def storeSuburbPostCode( suburbName, postCode ):
#    try:
#        suburb_postcode_insert_statement = f"""UPDATE suburbs SET postcode = '{postCode}' WHERE name = '{suburbName}'"""
#        print( suburb_postcode_insert_statement )
#        cur.execute( suburb_postcode_insert_statement, "" )
#        conn.commit()
#    except(Exception, psycopg2.DatabaseError) as error:
#        print("Offending Property Sales History Query: " +  suburb_postcode_insert_statement )
#        print(error)


#######  Start of Commands                                                                                                                                         ##########################



