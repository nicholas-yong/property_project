import psycopg2
import json
from user_functions import *
from user_constants import *
from database import conn, cur
from properties import storeFeatures
from files import File

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

        #build the JSON from listingObject
        raw_listing_JSON = json.dumps( listingObject )

        #Get the value that will be used with listing_insert_statement
        listings_id = returnNextSerialID( 'listings', 'listings_id' )

        cur.execute( """ INSERT INTO listings( domain_listings_id, headline, price_displayed, display_price, price, price_from, price_to, seo_url, listing_objective,
                                                            listing_status, land_area, building_area, energy_efficiency, is_new_development, date_updated, date_created,
                                                            entered_when, entered_by, raw_listing, inspection_appointment_only )statement, "" )
                         VALUES( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, to_timestamp( %s, 'YYYY-MM-DD HH24:MI:SS' ), to_timestamp( %s, 'YYYY-MM-DD HH24:MI:SS' ), current_timestamp, 1, %s, %s ) """,
                         ( listingObject.get( 'id' ), listingObject.get( 'headline' ), listing_prices.get( 'canDisplayPrice' ), listing_prices.get( 'displayPrice' ), listing_prices.get( 'price' ), 
                           listing_prices.get( 'priceFrom' ), listing_prices.get( 'priceTo' ), listingObject.get( 'seoUrl' ), listingObject.get( 'objective' ), listing_status, listingObject.get( 'landAreaSqm'), 
                           listingObject.get( 'buildingAreaSqm' ), listingObject.get( 'energyEfficiencyRating' ), listingObject.get( 'isNewDevelopment' ), convertJSONDate(listingObject['dateUpdated']),
                           convertJSONDate(listingObject['dateListed']), cleanStringForInsert(raw_listing_JSON), lisitng_inspections.get( 'isByAppointmentOnly' ) ) )

        #Insert the Features if the listing contains any.
        #Set the object type
        #Only do this if the listing already has a features object.
        if 'features' in listingObject:
            link_object_type = OBJECT_Listing
            storeFeatures( listings_id, link_object_type, listingObject['features'] )

        #Store any media attached to the listing
        for media in listingObject['media']:
            mediaObject = File( FILE_TYPE_Images, OBJECT_Listing, str(listings_id), None, "listing_" + media['type'] )
            mediaObject.addImageDetails( None, None, media['url'] )
            mediaObject.storeFile( False )

        #Store Listing Sales Information.
        #First, we need to check if the listings has any sales information attached to it.
        if 'saleDetails' in listingObject:
            listing_sales = listingObject['saleDetails']
            storeListingSalesDetails( listing_sales, listings_id )

        #Store the Inspection information
        storeInspectionDetails( listings_id, lisitng_inspections )

        return True
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
        return False

def storeListingSalesDetails( listingSalesObject, listings_id ):
    try:
        #Check if there are any tenant details inside the listingSalesObject
        if 'tenderDetails' in listingSalesObject:
            tenderAddress = listingSalesObject['tenderDetails'].get( 'tenderAddress' )
            if listingSalesObject['tenderDetails'].get( 'tenderDate' ) is not None:
                tenderDate = f"to_timestamp( '{convertJSONDate( listingSalesObject['tenderDetails'].get( 'tenderDate' ) )}', 'YYYY-MM-DD HH24:MI:SS' )"
            else:
                tenderDate = None
            tenderName = listingSalesObject['tenderDetails'].get( 'tenderRecipientName' )
        else:
            tenderAddress = None
            tenderDate = None
            tenderName = None
        
        #Get the Sales_ID that will be inserted for future use.
        sales_id = returnNextSerialID( 'listing_sales', 'sales_id' )


        #We probably don't need to split the listingSalesObject.
        cur.execute( """INSERT INTO listing_sales( listings_id, sale_method, sale_terms, annual_return, tender_address, tender_date, tender_name, entered_when )
                        VALUES( %s, %s, %s, %s, %s, %s, %s, current_timestamp )""", 
                        ( listings_id, listingSalesObject.get( 'saleMethod' ), listingSalesObject.get( 'saleTerms '), listingSalesObject.get( 'annualReturn'), tenderAddress, 
                          tenderDate, tenderName) )

        #Once we successfully insert the main listing_sales object, insert the two minor listing sale objects
        if 'soldDetails' in listingSalesObject:
            #There's no reason for this to be a seperate function. Remove the seperate function and just action it here.
            soldDetailsObject = listingSalesObject['soldDetails']
            cur.execute( """INSERT INTO listing_sales_sold_details( sales_id, sold_price, sold_date, source, sold_action, was_price_displayed )
                            VALUES( %s, %s, to_timestamp( %s, 'YYYY-MM-DD', %s, %s, %s )""", 
                            ( sales_id, soldDetailsObject.get( 'soldPrice' ), soldDetailsObject.get( 'soldDate' ), soldDetailsObject.get( 'source'), soldDetailsObject.get( 'soldAction' ), soldDetailsObject.get( 'canDisplayPrice') ) )

        if 'tenantDetails' in listingSalesObject:
            #There's no reason for this to be a seperate function. Remove the seperate function and just action it here.
            tenantDetailsObject = listingSalesObject['tenantDetails']
            cur.execute( """INSERT INTO listing_sales_tenants( sales_id, raw_tenant_details_json )
                            VALUES( %s, %s )""", ( sales_id, tenantDetailsObject ) )

        return True
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
        return False

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
                
            cur.execute( """INSERT INTO listing_inspections( opening_time, closing_time, description, recurring, is_past, listings_id, entered_when )
                            VALUES( to_timestamp( %s, 'YYYY-MM-DD HH24:MI:SS' ), to_timestamp( %s, 'YYYY-MM-DD HH24:MI:SS' ), %s, %s, %s, %s, current_timestamp )""" 
                            ( convertJSONDate(inspection['openingDateTime']), convertJSONDate(inspection['closingDateTime']), description, inspection.get( 'recurrence' ), True, listing_id ) )
        for inspection in current_inspections:
            if 'description' in inspection:
                description = inspection['description']
            else:
                description = "NULL"

            cur.execute( """INSERT INTO listing_inspections( opening_time, closing_time, description, recurring, is_past, listings_id, entered_when )
                            VALUES( to_timestamp( %s, 'YYYY-MM-DD HH24:MI:SS' ), to_timestamp( %s, 'YYYY-MM-DD HH24:MI:SS' ), %s, %s, %s, %s, current_timestamp )""" 
                            ( convertJSONDate(inspection['openingDateTime']), convertJSONDate(inspection['closingDateTime']), description, inspection.get( 'recurrence' ), False, listing_id ) )
        return True

    except(Exception, psycopg2.DatabaseError ) as error:
        print(error)
        return False

