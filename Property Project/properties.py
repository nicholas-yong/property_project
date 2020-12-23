import psycopg2
import json
from user_functions import VarIf, convertJSONDate, getKeyValue, QueryWithSingleValue, returnNextSerialID, cleanStringForInsert, checkRowExists
from database import cur, conn
from user_constants import *
from files import File

class Property:

    def __init__( self, domain_property_id, landShape, onMarket, address_object, areaSize, numBedrooms, numBathrooms, features, rawJson, history = None, files = None, propertyID = None ):
        self.domain_property_id = domain_property_id
        self.landShape = landShape
        self.onMarket = onMarket
        self.address_object = address_object
        self.areaSize = areaSize
        self.numBedrooms = VarIf( numBedrooms is None, 0, numBedrooms )
        self.numBathrooms = VarIf( numBathrooms is None, 0, numBathrooms )
        self.features = features
        self.rawJson = rawJson
        self.history = history
        self.files = files

    
    @classmethod
    def initFromObject( cls, propertyObject, propertyID = None ):
        if 'addressCoordinate' in propertyObject:
            longitude = propertyObject['addressCoordinate']['lon']
            lattitude = propertyObject['addressCoordinate']['lat']
        else:
            longitude = None
            lattitude = None

        photos = []
        #Create File Array from the Photos Object inside the propertyObject
        for photo in propertyObject['photos']:
            propertyImage = File( FILE_TYPE_Images, OBJECT_Property, propertyID, None, 'Property ' + photo['imageType'], None )
            propertyImage.addImageDetails( photo['advertId'],  photo['date'], photo['fullUrl'], None )
            photos.append( propertyImage )
        
        new_property = cls( propertyObject['id'], propertyObject.get( "cadastreType" ), VarIf( propertyObject['status'] == PROPERTY_MARKET_STATUS_ONMARKET, True, False ), 
                            Address( propertyObject['address'], propertyObject['streetName'], propertyObject['suburb'], propertyObject['streetNumber'],
                                     propertyObject.get( "zone" ), propertyObject.get( "lotNumber" ), longitude, lattitude, None ), propertyObject.get( "areaSize" ), propertyObject.get( 'numBedrooms'),
                            propertyObject.get( 'numBathrooms' ), propertyObject.get( 'features' ), json.dumps( propertyObject ), propertyObject.get( 'history' ), photos )
        return new_property

    def saveProperty( self, commit ):
        #store the propertyObject inside a text file in case we want to debug afterwards.
        debug = open( "debug.txt", "w")
        debug.write( json.dumps( self.rawJson ))

        try:
            #Handle the address first, if required.
            if self.address_object.address_id is None:
                new_address_id = self.address_object.storeAddress()
                if new_address_id is None:
                    return False
            #Get the next property id
            next_property_id = returnNextSerialID( 'properties', 'property_id' )
            if not self.saveSalesHistory( next_property_id ):
                return False
            
            #Save the Files next.
            for item in self.files:
                item.keyval1 = next_property_id
                item.storeFile( False )
            
            #Store the Features Next
            for feature in self.features:
                storeFeatures( next_property_id, OBJECT_Property, feature )

            #Now save the property.
            cur.execute( """INSERT INTO properties( domain_prop_id, shape_of_land, on_market, address_id, area_size, num_bedrooms, num_bathrooms, entered_when, raw_json )
                            VALUES( %s, %s, %s, %s, %s, %s, %s, current_timestamp, %s )""", 
                            ( self.domain_property_id, self.landShape, self.onMarket, new_address_id, self.areaSize, self.numBedrooms, self.numBathrooms, self.rawJson ) )

            if commit:
                conn.commit()
            return True
        except(Exception, psycopg2.DatabaseError ) as error:
            print( error )
            return False

    def saveSalesHistory( self, new_property_id = None ):
        #Need to test if we have a property id to save against.
        if new_property_id is None:
            raise RuntimeError( "Invalid Property ID for Property " + self.domain_property_id )
        try:
            if self.history is not None:
                for history in self.history:
                    sales_history_insert_statement = f""" INSERT INTO property_sales_history( property_id, agency_id, sale_date, days_on_market, documented_as_sold, sale_price 
                                                                                      reported_as_sold, details_suppressed, price_suppressed, sale_type, agency_name,
                                                                                      property_sales_type, listings_id )
                                                  VALUES( {new_property_id}, {self.history['apmAgencyId']}, to_timestamp( '{self.history['date']}', 'YYYY-MM-DD'), {self.history['daysOnMarket']},
                                                          {self.history['documentedAsSold']}, {self.history['price']}, {self.history['reportedAsSold']},
                                                          {self.history['suppressDetails']}, {self.history['suppressPrice']}, '{self.history['type']}', '{self.history['agency']}', 
                                                          '{self.history['propertyType']}', {self.history['id']} ) """
                    cur.execute( sales_history_insert_statement, "")
            return True
        except(Exception, psycopg2.DatabaseError) as error:
            print("Offending Property Sales History Query: " +  sales_history_insert_statement )
            print(error)
            return False
        


### Data Storage Functions
class Address:

    def __init__( self, full_address, street_name, suburb_name, street_number = None, zone = None, lot_number = None, long = None, lat = None, address_id = None ):

        self.full_address = full_address
        self.street_name = street_name
        self.suburb_name = suburb_name
        #Get the Suburb_ID here.
        self.suburb_id = QueryWithSingleValue( "suburbs", "name", self.suburb_name, "suburb_id", True )
        self.street_number = street_number
        self.zone = zone
        self.lot_number = lot_number
        self.long = long
        self.lat = lat
        self.address_id = address_id

    def returnSuburbInformation( self ):
        """
        This Function information about the suburb represented by self.suburb_id.
        This returns both the state and postcode of the suburb in a tuple.
            Index[0] = Postcode
            Index[1] = State

        Keyword arguements:
        self -- Keyword Name.

        """
        try:
            cur.execute( f"SELECT s.postcode, s.state FROM suburbs s WHERE s.suburb_id = {self.suburb_id}", "" )
            result = cur.fetchone()
            suburb_tuple = ( result[0], result[1] )
            return suburb_tuple
        except( Exception, psycopg2.DatabaseError ) as error:
            print (error)


    def storeAddress( self ):
        if self.zone is not None:
            insert_zone = self.zone
        else:
            insert_zone = "NULL"
        
        if self.lot_number is not None:
            insert_lot_number = self.lot_number
        else:
            insert_lot_number = "NULL"
        
        if self.long is None and self.lat is None:
            latlong_insert = ')'
            latlong_values = ')'
        else:
            latlong_insert = ', latlong)'
            latlong_values = ',' + '\'' + f"""SRID=4326;Point({self.long} {self.lat})""" + '\'' + ')'

        if  self.street_number is not None:
            insert_street_number = self.street_number
        else:
            insert_street_number = "NULL"
        
        try:
            address_id = returnNextSerialID( 'address', 'address_id' )
                    
            address_insert_statement = f""" INSERT INTO address( full_address, street_name, street_number, 
                                                                    suburb_id, zone, lot_number {latlong_insert}
                                                VALUES( '{self.full_address}', '{self.street_name}', {insert_street_number}, {self.suburb_id}, 
                                                        '{insert_zone}', {insert_lot_number} {latlong_values}""" 
            cur.execute(address_insert_statement, "")

            return address_id

        except(Exception, psycopg2.DatabaseError) as error:
            print("Offending Address Query: " + address_insert_statement)
            print(error) 

def storeFeatures( id, object_type, feature ):
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



