import psycopg2
import json
from user_functions import *
from user_constants import *
from database import conn, cur
from properties import storeFeatures
from files import File

class Listing:

    def __init__( self, *args,  **kwargs ):
        #If the listing_id has been passed as a parameter... (that means that this listing has already been stored in the DB and we need to extract it's values from there instead)
        if 'listing_id' in kwargs:
            #This means that we are populating an existing Listing from the database.
            self.
        else:
            #Check to see if listing_sales/listing_inspections/listing_prices have been passed to this function.
            self.listing_sales = kwargs.get( 'saleDetails' )
            self.lisitng_inspections = kwargs.get( 'inspectionDetails' )
            self.listing_prices = kwargs.get( 'priceDetails' )



def StoreListings( listingObject ):

    #Prepare the secondary dictionaries
    listing_sales = listingObject.get( 'saleDetails' )
    lisitng_inspections = listingObject.get( 'inspectionDetails' )
    listing_prices = listingObject.get( 'priceDetails' )

    try:
        #Insert the raw listing first.
        #Get the listing_status
        listing_status = QueryWithSingleValue( 'listing_status_lkp', 'description', listingObject['status'], 'listing_status_id', True )

        #build the JSON from listingObject
        raw_listing_JSON = json.dumps( listingObject )

        #Get the value that will be used with listing_insert_statement
        listings_id = returnNextSerialID( 'listings', 'listings_id' )

        if lisitng_inspections is not None:
            isByAppointmentOnly = lisitng_inspections.get( 'isByAppointmentOnly' )
        else:
            isByAppointmentOnly = None

        cur.execute( """ INSERT INTO listings( domain_listings_id, headline, price_displayed, display_price, price, price_from, price_to, seo_url, listing_objective,
                                                            listing_status, land_area, building_area, energy_efficiency, is_new_development, date_updated, date_created,
                                                            entered_when, entered_by, raw_listing, inspection_appointment_only )
                         VALUES( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, to_timestamp( %s, 'YYYY-MM-DD HH24:MI:SS' ), to_timestamp( %s, 'YYYY-MM-DD HH24:MI:SS' ), current_timestamp, 1, %s, %s ) """,
                         ( listingObject.get( 'id' ), listingObject.get( 'headline' ), listing_prices.get( 'canDisplayPrice' ), listing_prices.get( 'displayPrice' ), listing_prices.get( 'price' ), 
                           listing_prices.get( 'priceFrom' ), listing_prices.get( 'priceTo' ), listingObject.get( 'seoUrl' ), listingObject.get( 'objective' ), listing_status, listingObject.get( 'landAreaSqm'), 
                           listingObject.get( 'buildingAreaSqm' ), listingObject.get( 'energyEfficiencyRating' ), listingObject.get( 'isNewDevelopment' ), convertJSONDate(listingObject['dateUpdated']),
                           convertJSONDate(listingObject['dateListed']), cleanForSQL(raw_listing_JSON), isByAppointmentOnly ) )

        #Insert the Features if the listing contains any.
        #Set the object type
        #Only do this if the listing already has a features object.
        if 'features' in listingObject:
            link_object_type = OBJECT_Listing
            for feature in listingObject['features']:
                storeFeatures( listings_id, link_object_type, feature )

        if 'media' in listingObject:
            #Store any media attached to the listing
            for media in listingObject['media']:
                mediaObject = File( FILE_TYPE_Images, OBJECT_Listing, str(listings_id), None, "listing_" + media['type'] )
                mediaObject.addImageDetails( None, None, media['url'] )
                mediaObject.storeFile( False )

        #Store Listing Sales Information.
        #First, we need to check if the listings has any sales information attached to it.
        if listing_sales is not None:
            storeListingSalesDetails( listing_sales, listings_id )

        #Store the Inspection information (if the listing_inspections array is not None)
        if lisitng_inspections is not None:
            storeInspectionDetails( listings_id, lisitng_inspections )

        return listings_id
    except(Exception, psycopg2.DatabaseError) as error:
        print("Error in INSERTING New Listing with Domain Listing ID " + "\n" + "Error: " + error )
        return None

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
        if 'soldDetails' in listingSalesObject and len(listingSalesObject['soldDetails']) != 0:
            #There's no reason for this to be a seperate function. Remove the seperate function and just action it here.
            soldDetailsObject = listingSalesObject['soldDetails']
            cur.execute( """INSERT INTO listing_sales_sold_details( sales_id, sold_price, sold_date, source, sold_action, was_price_displayed )
                            VALUES( %s, %s, to_timestamp( %s, 'YYYY-MM-DD', %s, %s, %s )""", 
                            ( sales_id, soldDetailsObject.get( 'soldPrice' ), soldDetailsObject.get( 'soldDate' ), soldDetailsObject.get( 'source'), soldDetailsObject.get( 'soldAction' ), soldDetailsObject.get( 'canDisplayPrice') ) )

        if 'tenantDetails' in listingSalesObject and len(listingSalesObject['tenantDetails']) != 0:
            #There's no reason for this to be a seperate function. Remove the seperate function and just action it here.
            tenantDetailsObject_string = json.dumps( listingSalesObject.get( 'tenantDetails' ) )
            cur.execute( """INSERT INTO listing_sales_tenants( sales_id, raw_tenant_details_json )
                            VALUES( %s, %s )""", ( sales_id, tenantDetailsObject_string ) )

        return True
    except(Exception, psycopg2.DatabaseError) as error:
        print( "Error in INSERTING Sales Details for Listing with Listing ID " + "\n" + "Error: " + error )
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
                            VALUES( to_timestamp( %s, 'YYYY-MM-DD HH24:MI:SS' ), to_timestamp( %s, 'YYYY-MM-DD HH24:MI:SS' ), %s, %s, %s, %s, current_timestamp )""" ,
                            ( convertJSONDate(inspection['openingDateTime']), convertJSONDate(inspection['closingDateTime']), description, inspection.get( 'recurrence' ), True, listing_id ) )
        for inspection in current_inspections:
            if 'description' in inspection:
                description = inspection['description']
            else:
                description = "NULL"

            cur.execute( """INSERT INTO listing_inspections( opening_time, closing_time, description, recurring, is_past, listings_id, entered_when )
                            VALUES( to_timestamp( %s, 'YYYY-MM-DD HH24:MI:SS' ), to_timestamp( %s, 'YYYY-MM-DD HH24:MI:SS' ), %s, %s, %s, %s, current_timestamp )""",
                            ( convertJSONDate(inspection['openingDateTime']), convertJSONDate(inspection['closingDateTime']), description, inspection.get( 'recurrence' ), False, listing_id ) )
        return True

    except(Exception, psycopg2.DatabaseError ) as error:
        print( "Error in INSERTING Inspection Details for listing " + "\n" + "Error: " + error )
        return False

