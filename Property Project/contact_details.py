import psycopg2
import json
import re 
from database import cur, conn
from user_functions import checkRowExists, returnNextSerialID

class ContactDetails:

    def __init__(self, contact_details_type, object_id, object_type, details, contact_details_id=None ):
        """
        This is the Init Function for the ContactDetails Object.

        :param self: Contact Details Instance.
        :param contact_details_type: String description of what the contact detail represents.
        :type contact_details_type: String
        :param object_id: Object ID that the Contact Details belongs to.
        :param object_type: Object Type of the Object ID that the Contact Details belongs to.
        :param details: The actual details of the contact detail.
        :type details: string
        :param contact_details_id: The contact_details_id of the contact_details_type. (optional)
        
        """
        self.contact_details_type = contact_details_type
        self.object_id = object_id
        self.object_type = object_type
        self.contact_details_id = contact_details_id
        self.details = details

    def storeContactDetails( self, commit ):
        """ 
        This Function stores a new set of contact details into the contact_details table.
        It first checks to see if the contact_details_id variable has been obtained for the class instance, if it has not, it's retrieved from the contact_details_type_lkp table.
        if that contact_details_type doesn't exist yet, its inserted into the contact_details_type_lkp table.
        It then stores the details into the contact_details table. 

        :param self: Contact Details Instance.
        :param commit: If True, then the insert is instantly commited. Otherwise no commit is done.
        ...
        :return: True if the insert succeeds, False otherwise.
        """
        try:
            #First, check to see if the contact_details_type has already been stored inside the contact_details_type_lkp table
            if self.contact_details_id is None:
                if not checkRowExists( f"SELECT 1 FROM contact_details_type_lkp WHERE UPPER( description ) = '{self.contact_details_type}' " ):
                    self.contact_details_id = returnNextSerialID( 'contact_details_type_lkp', 'contact_details_type' )
                    #If the contact_details_type hasn't been stored...
                    contact_details_type_insert_statement = f"""INSERT INTO contact_details_type_lkp( description )
                                                            VALUES( '{self.contact_details_type}' ) """
                    cur.execute( contact_details_type_insert_statement, "" )
                else:
                    #Get the matching contact_details_id from the contact_details_type_lkp table.
                    cur.execute( f"SELECT contact_details_type_lkp FROM contact_details_type_lkp WHERE description = {self.contact_details_type}", "" )
                    row = cur.fetchone()
                    self.contact_details_id = row[0]

            #Once we've gotten the contact_details_id....
            cur.execute( f""" INSERT INTO contact_details( contact_details_type, object_id, object_type, details, entered_when )
                          VALUES( {self.contact_details_id}, {self.object_id}, {self.object_type}, '{self.details}', current_timestamp ) """, "" )
            if commit:
                conn.commit()
            return True
    
        except(Exception, psycopg2.DatabaseError) as error:
            print(error)
            return False


    


    