import psycopg2
import re
import json
from database import cur, conn
from user_functions import getKeyValue, convertJSONDate, returnNextSerialID, checkRowExists, ifNotNone, VarIf
from user_constants import FILE_TYPE_Images, FILE_TYPE_Documents, MODE_New, MODE_Edit, MODE_Delete

class File:
    
    #Base Init Function for File Object
    def __init__( self, file_type, object_type, keyval1, keyval2, document_type_description, file_id = None ):
        self.file_id = file_id
        self.file_type = file_type
        self.object_type = object_type
        self.keyval1 = keyval1
        self.keyval2 = keyval2 
        self.document_type_description = document_type_description
    
    def addImageDetails( self, advert_id, date_taken, images_url, image_id = None ):
        self.image_advert_id = advert_id
        self.image_date_taken = date_taken
        self.image_images_url = images_url
        self.image_id = image_id
    
    def checkAndStoreDocType( self, commit ):
        try:
            if not checkRowExists( f"SELECT 1 FROM document_type WHERE UPPER(description) = '{self.document_type_description}'" ):
                document_type = returnNextSerialID( 'document_type', 'document_type' )
                cur.execute( f""" INSERT INTO document_type(description)
                                VALUES( '{self.document_type_description}' )""", "" )
                if commit:
                    conn.commit()
                self.document_type = document_type
            else:
                cur.execute( f"SELECT document_type FROM document_type WHERE UPPER(description) = '{self.document_type_description}'", "" )
                result = cur.fetchone()
                self.document_type = result[0]
        except( Exception, psycopg2.DatabaseError ) as error:
            print( error )
            return None
    
    def storeFile( self, commit ):
        try:
            #Need to check if we are saving an image or a document.
            if self.file_type == FILE_TYPE_Images:
                #This means that we are saving an image.
                image_id = returnNextSerialID( 'images', 'image_id' )
                File.checkAndStoreDocType( self, False )
                if self.image_date_taken is not None:
                    insert_image_date_taken = f"to_timestamp( '{convertJSONDate(self.image_date_taken)}',  'YYYY-MM-DD HH24:MI:SS' )"
                else:
                    insert_image_date_taken = "NULL"
                images_insert_statement = f""" INSERT into images( advert_id, date_taken, url )
                                               VALUES( {ifNotNone(self.image_advert_id, "NULL")}, {insert_image_date_taken}, '{self.image_images_url}' )"""
                cur.execute( images_insert_statement , "" )                       
                cur.execute( f""" INSERT INTO files( file_id, file_type, object_type, keyval1, keyval2, document_type, entered_when )
                                  VALUES( {image_id}, '{FILE_TYPE_Images}', {self.object_type}, '{self.keyval1}', '{self.keyval2}', {self.document_type}, current_timestamp ) """, "" )
                
                if commit:
                    conn.commit()
                return True
            elif self.file_type == FILE_TYPE_Documents:
                pass
            else:
                raise ValueError( str( self ) + " has an invalid file type" )
        
        except( ValueError, Exception, psycopg2.DatabaseError ) as error:
            print( error )
            print( images_insert_statement )
            return False


    def __str__(self):
        return f"File {self.file_id}"

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