import psycopg2
import json
from user_functions import VarIf, convertJSONDate, getKeyValue, QueryWithSingleValue, returnNextSerialID, cleanStringForInsert, checkRowExists
from database import cur, conn
from user_constants import *

### Data Storage Functions

def storePropertyInfo( propertyObject ):
    #store the propertyObject inside a text file in case we want to debug afterwards.
    debug = open( "debug.txt", "w")
    debug.write( json.dumps(propertyObject))

    #We need to split the propertyObject into several smaller dictionaries for the sub tables.
    history = {}
    files = {}

    if 'zone' in propertyObject:
        zone = propertyObject['zone']
    else:
        zone = None

    if 'lotNumber' in propertyObject:
        lot_number  = propertyObject['lotNumber']
    else:
        lot_number = None

    if 'addressCoordinate' in propertyObject:
        longitude = propertyObject['addressCoordinate']['lon']
        lattitude= propertyObject['addressCoordinate']['lat']
    else:
        longitude = None
        lattitude = None


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
        address_id = storeAddress( propertyObject['address'], propertyObject['streetName'], propertyObject['street_number'], propertyObject['suburb'], zone, lot_number, longitude, lattitude )
        raw_json = json.dumps(propertyObject)
        #once done, split the information. But for now, let's just do a test.
        insert_statement = f""" INSERT INTO properties( domain_prop_id, shape_of_land, on_market, address_id, area_size, num_bedrooms, num_bathrooms, 
                                                        features, entered_when, raw_json )
                                VALUES( '{propertyObject['id']}', '{cadastreType}', {VarIf( propertyObject['status'] == PROPERTY_MARKET_STATUS_ONMARKET, True, False )}, 
                                         {address_id}, {areaSize}, {bedrooms},
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

#def storeAddress( addressObject, object_type ):
#    try:
#        #store the lotnumber and zone
#        if 'zone' in addressObject:
#            zone = '\'' + addressObject['zone'] + '\''
#        else:
#            zone = "NULL"
        
#        if 'lot_number' in addressObject:
#            lotNumber = '\'' + addressObject['lot_number'] + '\''
#        else:
#            lotNumber = "NULL"

#        if 'lat' not in addressObject and 'lon' not in addressObject:
#            latlong_insert = ')'
#            latlong_values = ')'
#        else:
#            latlong_insert = ', latlong)'
#            latlong_values = ',' + '\'' + f"""SRID=4326;Point({addressObject['lon']} {addressObject['lat']})""" + '\'' + ')'
    
#        address_insert_statement = f""" INSERT INTO address( full_address, street_name, street_number, 
#                                                             suburb_id, zone, lot_number {latlong_insert}
#                                        VALUES( '{addressObject['full_address']}', '{addressObject['street_name']}', '{addressObject['street_number']}',
#                                                 {addressObject['suburb_id']}, {zone}, {lotNumber} {latlong_values}""" 
#        cur.execute(address_insert_statement, "")

#    except(Exception, psycopg2.DatabaseError) as error:
#        print("Offending Address Query: " + address_insert_statement)
#        print(error) 

def storeAddress( full_address, street_name, street_number, suburb_name, zone, lot_number, longitude, lattitude ):
    #Set the variables
    if zone != None:
        insert_zone = zone
    else:
        zone = "NULL"
    
    if lot_number != None:
        insert_lot_number = lot_number
    else:
        insert_lot_number = "NULL"
    
    if longitude = None and lattitude = None:
        latlong_insert = ')'
        latlong_values = ')'
    else:
        latlong_insert = ', latlong)'
        latlong_values = ',' + '\'' + f"""SRID=4326;Point({longitude} {lattitude})""" + '\'' + ')'

    #Get the suburb id
    suburb_id = QueryWithSingleValue( "suburbs", "name", propertyObject['suburb'], "suburb_id", True )

    
    address_insert_statement = f""" INSERT INTO address( full_address, street_name, street_number, 
                                                             suburb_id, zone, lot_number {latlong_insert}
                                        VALUES( '{full_address}', '{street_name}', {street_number}, {suburb_id}, 
                                                 {zone}, {lotNumber} {latlong_values}""" 
    cur.execute(address_insert_statement, "")

    return returnNextSerialID( 'address', 'address_id' )

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
    listing_sales = listingObject['saleDetails']
    lisitng_inspections = listingObject['inspectionDetails']
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
        #Only do this if the listing already has a features object.
        if 'features' in listingObject:
            link_object_type = OBJECT_Listing
            storeFeatures( listings_id, link_object_type, listingObject['features'] )

        #Store any media attached to the listing
        for media in listingObject['media']:
            storeMedia( FILE_TYPE_Images, OBJECT_Listing, 1, str(listings_id), None, media )

        #Store Listing Sales Information.
        #First, we need to check if the listings has any sales information attached to it.
        if 'saleDetails' in listingObject:
            listing_sales = listingObject['saleDetails']
            storeListingSalesDetails( listing_sales, listings_id )

        #Store the Inspection information
        storeInspectionDetails( listings_id, lisitng_inspections )


    except(Exception, psycopg2.DatabaseError) as error:
        print(listing_insert_statement)
        print(error)

def storeListingSalesDetails( listingSalesObject, listings_id ):
    try:
        #Need to build the individual variables first unfortuantely.
        if 'saleTerms' in listingSalesObject:
            saleTerms = listingSalesObject['saleTerms']
        else:
            saleTerms = "NULL"
        if 'annualReturn' in listingSalesObject:
            annualReturn = listingSalesObject['annualReturn']
        else:
            annualReturn = "NULL"

        #Check if there are any tenant details inside the listingSalesObject
        if 'tenderDetails' in listingSalesObject:
            tenderDetails = listingSalesObject['tenderDetails']
            #Get the Tenant Details
            if 'tenderAddress' in tenderDetails:
                tenderAddress = tenderDetails['tenderAddress']
                tenderDate = tenderDetails['tenderEndDate']
                tenderName = tenderDetails['tenderRecipientName']
            else:
                tenderAddress = "NULL"
                tenderDate = "NULL"
                tenderName = "NULL"
        else:
            tenderAddress = "NULL"
            tenderDate = "NULL"
            tenderName = "NULL"
        
        #Get the Sales_ID that will be inserted for future use.
        sales_id = returnNextSerialID( 'listing_sales', 'sales_id' )

        #We probably don't need to split the listingSalesObject.
        listing_sales_insert_statement = f""" INSERT INTO listing_sales( listings_id, sale_method, sale_terms, annual_return, tender_address, tender_date, tender_name, entered_when )
                                              VALUES( {listings_id}, '{listingSalesObject['saleMethod']}', '{saleTerms}', {annualReturn},
                                                      '{tenderAddress}', {VarIf( tenderDate != "NULL", f" to_timestamp( '{convertJSONDate(tenderDate)}', 'YYYY-MM-DD HH24:MI:SS' )", "NULL")}, '{tenderName}', current_timestamp) """

        cur.execute( listing_sales_insert_statement, "" )

        #Once we successfully insert the main listing_sales object, insert the two minor listing sale objects
        if 'soldDetails' in listingSalesObject:
            storeListingSalesSoldDetails( sales_id, listingSalesObject['soldDetails'] )
        if 'tenantDetails' in listingSalesObject:
            storeListingSalesTenantDetails( sales_id, listingSalesObject['tenantDetails'])

    except(Exception, psycopg2.DatabaseError) as error:
        print(listing_sales_insert_statement)
        print(error)

def storeListingSalesSoldDetails( sales_id, soldDetailsObject ):
    try:
        #I'm going to guess that if the Sold Details Object exists, then the soldPrice and soldDate variables have to exist too. (Otherwise it doesn't really make any sense to be honest.)
        #This means that we only need to check for the remaining three variables that we are keeping track of.
        if 'canDisplayPrice' in soldDetailsObject:
            canDisplayPrice = soldDetailsObject['canDisplayPrice']
        else:
            canDisplayPrice = "NULL"
        if 'soldAction' in soldDetailsObject:
            soldAction = soldDetailsObject['soldAction']
        else:
            soldAction = "NULL"
        if 'source' in soldDetailsObject:
            source = soldDetailsObject['source']
        else:
            source = "NULL"

        listing_sales_soldDetails_insert_statement = f""" INSERT INTO listing_sales_sold_details( sales_id, sold_price, sold_date, source, sold_action, was_price_displayed )
                                                          VALUES( {sales_id}, {soldDetailsObject['soldPrice']}, to_timestamp( '{soldDetailsObject['soldDate']}', 'YYYY-MM-DD') , '{source}', '{soldAction}', {canDisplayPrice} )"""
        cur.execute( listing_sales_soldDetails_insert_statement, "" )
                                                                  
    except(Exception, psycopg2.DatabaseError ) as error:
        print( listing_sales_soldDetails_insert_statement )
        print( error )

def storeListingSalesTenantDetails( sales_id, tenantObject ):
    try:
        #Store the Tenant Details directly.
        listing_sales_tenantDetails_insert_statement = f""" INSERT INTO listing_sales_tenants( sales_id, raw_tenant_details_json )
                                                            VALUES( {sales_id}, '{tenantObject}' ) """
        
        cur.execute( listing_sales_tenantDetails_insert_statement, "" )
    except(Exception, psycopg2.DatabaseError ) as error:
        print( listing_sales_tenantDetails_insert_statement )
        print( error )

def storeInspectionDetails( listing_id, inspectionObject ):
    try:
        #Need to build the inspections and past inspections objects.
        past_inspections = inspectionObject['pastInspections']
        current_inspections = inspectionObject['inspections']  

        #do past_inspections first.
        for inspection in past_inspections:
            if 'description' in inspection:
                description = inspection['description']
            else:
                description = "NULL"
                
            listing_inspection_insert_statement = f""" INSERT INTO listing_inspections( opening_time, closing_time, description, recurring, is_past, listings_id, entered_when )
                                                       VALUES( to_timestamp( '{convertJSONDate(inspection['openingDateTime'])}', 'YYYY-MM-DD HH24:MI:SS' ), 
                                                               to_timestamp( '{convertJSONDate(inspection['closingDateTime'])}','YYYY-MM-DD HH24:MI:SS' ), '{description}', 
                                                               '{inspection['recurrence']}', {True} , {listing_id}, current_timestamp ) """
            cur.execute( listing_inspection_insert_statement, "" )
        for inspection in current_inspections:
            if 'description' in inspection:
                description = inspection['description']
            else:
                description = "NULL"
                
            listing_inspection_insert_statement = f""" INSERT INTO listing_inspections( opening_time, closing_time, description, recurring, is_past, listings_id, entered_when )
                                                       VALUES( to_timestamp( '{convertJSONDate(inspection['openingDateTime'])}', 'YYYY-MM-DD HH24:MI:SS' ), 
                                                               to_timestamp( '{convertJSONDate(inspection['closingDateTime'])}','YYYY-MM-DD HH24:MI:SS' ), '{description}', 
                                                               '{inspection['recurrence']}', {False}, {listing_id}, current_timestamp ) """
            cur.execute( listing_inspection_insert_statement, "" )

    except(Exception, psycopg2.DatabaseError ) as error:
        print(error)

def storeFeatures( id, object_type, features ):
    """
    Stores a list of tuple of features inside the object_features table. For each feature inside the features tuple, it first queries the features_lkp table to check if that feature already exists inside the table.
    If it does, it retrieves that feature's feature_id. Otherwise, it inserts that feature into the features_lkp table and returns the newly inserted feature's feature_id.
    It then uses the feature's feature_id and the id of the object to create a linking row inside the object_features table

    Keyword arguements:
    id -- ID of the Object that the features are being linked against.
    object_type -- Object Type of the Object that the features are being linked against.
    features -- Tuple of features that are going to be linked against the object.

    """

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


def storeMedia( file_type, linked_to_object_type, document_type, keyval1, keyval2, mediaObject ):
    """
    Stores a Media Object. 

    Keyword arguements:
    file_type -- The File Type of the mediaObject being stored. Either 'documents' or 'images'.
    linked_to_object_type -- The Object Type of the Object that mediaObject is being linked to. (stored inside object_type_lkp)
    document_type -- The Document Type of mediaObject. (stored inside document_type)
    keyval1 -- The First keyvalue of the object that mediaObject is being linked to. (This will depend on the object type of the object that is being linked to)
    keyval2 -- The Second keyvalue of the object that mediaObject is being linked to. (This will depend on the object type of the object that is being linked to)
    mediaObject -- The actual mediaObject being linked.

    """
    try:
        if file_type == FILE_TYPE_Documents:
            # Return False for now. We don't have support for documents yet.
            return False
        elif file_type == FILE_TYPE_Images:
            #Check if its part of an advert.
            if 'advertId' in mediaObject:
                advertID = mediaObject['advertId']
            else:
                advertID = "NULL"
            if 'date' in mediaObject:
                dateTaken = convertJSONDate(  mediaObject['date'] )
            else:
                dateTaken = None
            #Set the url depending on if we are getting it from the listings or properties field. (The Object MUST have a Url)
            if 'url' in mediaObject:
                image_url = mediaObject['url']
            elif 'fullUrl' in mediaObject:
                image_url = mediaObject['fullUrl']
            else:
                raise ValueError( 'No image url provided in mediaObject')
            #Generate the statement required to insert the image into the images table. (if it doesn't already exist.)
            #Check to see if the image already exists. (via url).
            if not checkRowExists( f" SELECT 1 FROM images WHERE url = '{image_url}'" ):
                file_id = returnNextSerialID( 'images', 'image_id' )
                image_insert_statement = f"""  INSERT INTO images( advert_id, date_taken, url ) VALUES ( {advertID}, {VarIf( dateTaken is not None, f"to_timestamp( {dateTaken}, 'YYYY-MM-DD HH24:MI:SS' )", "NULL")}, '{image_url}' ) """
                cur.execute( image_insert_statement, "" )
            else:
                #Get the image_id of the existing iamge.
                cur.execute( f" SELECT image_id FROM images WHERE url = '{image_url}' ", "" )
                result = cur.fetchone()
                file_id = result[0]
        else:
            raise ValueError('Invalid Value for fileType')
        
        #Once the file has been inserted into its respective table (documents/images), then we can insert the link into the files table.
        files_insert_statement = f""" INSERT INTO files( file_id, file_type, object_type, keyval1, keyval2, entered_when, document_type )
                                      VALUES( {file_id}, '{file_type}', {linked_to_object_type}, '{keyval1}', '{keyval2}', current_timestamp, {document_type} ) """
        cur.execute( files_insert_statement, "" )
    except ValueError as error:
        print(error)
        return False

    









#######  Start of Commands



