import psycopg2
import json
import re 
import unicodedata
from user_functions import VarIf, convertJSONDate, getKeyValue, QueryWithSingleValue, returnNextSerialID, cleanForSQL, checkRowExists
from database import cur, conn
from properties import Address
from contact_details import ContactDetails
from user_constants import OBJECT_Agent, OBJECT_Agency, FILE_TYPE_Images
from files import File

### Agency Class Definition
class Agency:

    variables_set = False

    def __init__( self, agency_banner, agency_website, agency_logo_standard, agency_long, agency_lat, agency_description, num_sale, num_rent, name, domain_agency_id, 
                  principal_agent_name, raw_agency_json, contact_details, streetAddress1, streetAddress2, suburb, agents = None, principal_agent_id = None, agency_id = None,
                  state = None, postCode = None ):

        self.agency_banner = agency_banner
        self.agency_website = agency_website
        self.agency_logo_standard = agency_logo_standard
        cleaned_agency_description = self.cleanAgencyDescription( agency_description )
        #We need to truncate the agency description if its larger then 1000
        if len(cleaned_agency_description) > 1000:
            self.agency_description = cleaned_agency_description[0:997] + "..."
        else:
            self.agency_description = cleaned_agency_description
        self.num_sale = num_sale
        self.num_rent = num_rent
        self.domain_agency_id = domain_agency_id
        self.name = name
        self.principal_agent_name = principal_agent_name
        self.raw_agency_json = raw_agency_json
        self.agents = agents

        #Set up the address object for the Agency Class
        street_name = streetAddress1 + ' ' + streetAddress2
        full_address = street_name + ' ' + suburb + ' ' + state  + ' ' + str(postCode)
        #We need to get the street number
        street_number_object = re.search( r"\d+/?\d*", streetAddress1 )
        if street_number_object is None:
            street_number_object = re.search( r"\d+/?\d*", streetAddress2 )
        
        if street_number_object is not None:
            street_number = street_number_object.group(0)
        else:
            street_number = None

        self.address = Address( full_address, street_name, suburb, street_number, None, None, agency_long, agency_lat, state, postCode )

        self.contact_details = contact_details
        if principal_agent_id is not None:
            self.principal_agent_id = principal_agent_id
        else:
            self.principal_agent_id = None
        if agency_id is not None:
            self.agency_id = agency_id
        else:
            self.agency_id = None
        
        self.classesConverted = False

    def cleanAgencyDescription( self, cleanString ):
        #Use unicodedata.normalize to get rid of the extra Unicode Data
        agency_description_unicode = unicodedata.normalize( "NFKD", cleanString )
        remove_html_string = re.sub( r"\<\\?[^\>]+\>",
                                        "",
                                        agency_description_unicode )
        #str_length = len( agency_description_unicode )
        #final_step_string = None
        #count = 0
        #Find the position of the first span (We know that the main data will be contained inside a span)
        #first_span_position = agency_description_unicode.find( "<span" )
        #Slice the String and get the remainder.
        #first_step_string = agency_description_unicode[first_span_position:str_length]
        #Get the location of the >, signifiying the end of the first span tag.
        #end_first_span_position = first_step_string.find( ">" )
        #Slice the string again.
        #new_str_length = len(first_step_string)
        #second_step_string = first_step_string[end_first_span_position:new_str_length]
        #Now get the position for the closing span tag.
        #second_span_position = second_step_string.find("</span")
        #final_step_string = second_step_string[1:second_span_position]

        return remove_html_string



    
    def getPrincipalAgent( self ):
        try:
            #Test to see if the Agent instance has a value for principal_agent_name
            if self.principal_agent_name == None:
                raise ValueError( 'Invalid Principal Agent Name' )
            else:
                full_name = self.principal_agent_name.split()
                result = None
                if len(full_name) == 2 :
                    cur.execute( """ SELECT a.agent_id 
                                  FROM agents a 
                                  WHERE UPPER( a.first_name ) = UPPER( %s ) 
                                        AND UPPER( a.last_name ) = UPPER( %s )""", 
                             ( cleanForSQL(full_name[0]), cleanForSQL(full_name[1] ) ) )
                    result = cur.fetchone()
                if result is None:
                    self.principal_agent_id = None
                else:
                    self.principal_agent_id = result[0]
            return True
        except( Exception, psycopg2.DatabaseError, ValueError ) as error:
            print( "Error in Getting Principal Agent ID for Agency " + self.name + "\n" + error )
            return False

    def hasAgencyID( self ):
        return self.agency_id is not None
    
    #def checkForNewAgents( self, agents ):
        #Need to first check if self.agents is populated and that the Agnecy Object has its agency id populated.
    #    if self.agents is None and self.agency_id is not None:
    #        cur.execute( """SELECT a.agent_id 
    #                        FROM agencies_agent a
    #                        WHERE a.agency_id = %s
    #                            AND a.deleted_when IS NULL""", 
    #                        ( self.agency_id ) )

    
    def convertToClasses( self ):
        """
        This Function converts all instance variables of the Agency Object that are meant to be classes...(i.e agency_banner) into their respective class.
        (These includes the Contact Details and Files Classes)

        """
        #Check to see if the instance has its agency_id set or has already had its variables converted into classes.
        try:
            if self.agency_id is None or self.classesConverted == True:
                raise Exception( "Cannot convert classes due to them already being converted or Agency Instance having a Null Agency ID" )
            else:
                agency_banner = File( FILE_TYPE_Images, OBJECT_Agency, self.agency_id, None, "agency_banner" )
                agency_banner.addImageDetails( None, None, self.agency_banner )
                self.agency_banner = agency_banner

                self.agency_website = ContactDetails( 'agency_website', OBJECT_Agency, self.agency_id, self.agency_website )

                agency_logo_standard = File( FILE_TYPE_Images, OBJECT_Agency, self.agency_id, None, "agency_logo_standard" )
                agency_logo_standard.addImageDetails( None, None, self.agency_logo_standard )
                self.agency_logo_standard = agency_logo_standard
                
                self.contact_details_converted = []
                for contactType, detailsType in self.contact_details.items():
                    #For now, only store dictionaries. We can handle exceptions by logging them to a debug file.
                    if isinstance( detailsType, dict ):
                        for contact, details in detailsType.items():
                                if details != '':
                                    self.contact_details_converted.append( ContactDetails( 'agency_' + contactType + '_' + contact, OBJECT_Agency, self.agency_id, details ) )
                    #else:
                    #    self.contact_details_converted.append( ContactDetails( 'agency_' + contactType + '_' + contact, self.agency_id, OBJECT_Agency, details ) )
                self.classesConverted = True

        except( Exception ) as error:
            print( error )

    def storeAgency( self ):
        try:
            #If the Agency ID hasn't been set for this Agency, it means that hasn't been saved yet into the database...
            if self.agency_id is None:
                self.agency_id = returnNextSerialID( 'agencies', 'agency_id' )

            if not self.getPrincipalAgent( ):
                return None
            #Convert the data inside the Agency object into Classes
            self.convertToClasses()
            
            #Save all contact details first.
            for contact in self.contact_details_converted:
                contact.storeContactDetails( False )
            
            #Store the links to the agency banner, website and standard logo.
            self.agency_website.storeContactDetails( False )
            self.agency_banner.storeFile( False )
            self.agency_logo_standard.storeFile( False )

            #Store the address first so that we can link the address to the correct row inside the agencies table.
            address_id = self.address.storeAddress()

            agency_json = json.dumps( self.raw_agency_json )

            cur.execute( """INSERT INTO agencies( description, num_sale, num_rent, entered_when, raw_agency_json, name, principal_agent, domain_agency_id, address_id )
                            VALUES( %s, %s, %s, current_timestamp, %s, %s, %s, %s, %s)""", 
                            ( self.agency_description, self.num_sale, self.num_rent, cleanForSQL( agency_json ), self.name, self.principal_agent_id, self.domain_agency_id, address_id ) )
            return self.agency_id
        except( Exception, psycopg2.DatabaseError ) as error:
            print( "Error in INSERTING New Agency for Agency " + self.name + "\n" + error )
            return None


class Agent:
    def __init__( self, email, first_name, last_name, profile_text, mugshot_url, domain_agent_id, agency_id=None, facebook_url=None, twitter_url=None, phone=None, photo=None ):
        """
        This is the Init Function for the Agent Object.

        :param self: Agent Instance.
        :param email: Agent's email
        :param first_name: Agent's first name
        :param last_name: Agent's last name
        :param profile_text: Text representation of the agent's profile.
        :param mugshot_url: Url to the Agent's mugshot.
        :param domain_agent_id: ID of the Agent inside the Domain system.
        :param agency_id: The internal id of the Agent inside our database.
        :type agency_id: int (optional)
        :param facebook_url: Url to the Agent's FB page.
        :type facebook_url: str (optional)
        :param twitter_url: Url to the Agent's twitter page.
        :type twitter_url: str (optional)
        """
        self.email = email
        self.first_name = first_name
        self.phone = phone
        self.photo = photo
        self.last_name = last_name
        self.facebook_url = facebook_url
        self.twitter_url = twitter_url
        input_profile_text = self.cleanProfileText( profile_text )
        if input_profile_text is not None and len(input_profile_text) > 3000:
            self.profile_text = input_profile_text[0:2995] + "..."
        else:
            self.profile_text = input_profile_text
        self.mugshot_url = mugshot_url
        self.agency_id = agency_id
        self.domain_agent_id = domain_agent_id
    
    @classmethod
    def initFromObject( cls, agent_id ):
        #Alternate initialization method that queries the database for the specific agent_id and returns the relevant parameters to populate the Agent Instance.
        pass
    
    def cleanProfileText( self, profile_text ):
        if profile_text is None:
            return profile_text
        clean_ampersand = profile_text.replace( "&amp;", "&" )
        clean_apostrophe = clean_ampersand.replace( "&#39;", "'" )
        clean_rsquo = clean_apostrophe.replace("&rsquo;", "'")
        clean_elipses = clean_rsquo.replace("&hellip;", "...")
        clean_quote = clean_rsquo.replace("&quot;", "\"")
        clean_profile_text_remove_close_p = clean_quote.replace( "</p>", '\n' + '\n' )
        clean_profile_text = clean_profile_text_remove_close_p.replace( "<p>", "" )
        return clean_profile_text

    
    def storeAgent( self, commit ):
        try:
            self.agent_id = returnNextSerialID( 'agents', 'agent_id' )
            #Stores information contained inside the Agent Object into the Agents table.
            cur.execute( """INSERT INTO agents( domain_agent_id, entered_when, first_name, last_name, profile_text )
                            VALUES( %s, current_timestamp, %s, %s, %s )""" ,
                            ( self.domain_agent_id, cleanForSQL(self.first_name), cleanForSQL(self.last_name), self.profile_text ) )
            #Store the link between the Agent and the Agency inside the agencies_agent table.
            cur.execute( """INSERT INTO agencies_agent( agency_id, agent_id, entered_when )
                            VALUES( %s, %s, current_timestamp)""",
                            ( self.agency_id, self.agent_id ) )

            if self.email is not None:
                #Store the agent's emaia
                contactDetails_email = ContactDetails("agent_email", OBJECT_Agent, self.agent_id, self.email )
                if not contactDetails_email.storeContactDetails( False ):
                    raise Exception ( psycopg2.DatabaseError )
            
            if self.phone is not None:
                #Store the agent's phone number
                contactDetails_phone = ContactDetails("agent_phone_number", OBJECT_Agent, self.agent_id, self.phone )
                if not contactDetails_phone.storeContactDetails( False ):
                    raise Exception ( psycopg2.DatabaseError )

            if self.facebook_url is not None:

                contactDetails_facebook = ContactDetails( "agent_facebook_url", OBJECT_Agent, self.agent_id, self.facebook_url )
                #Attempt to save the facebook details
                if not contactDetails_facebook.storeContactDetails( False ):
                    raise Exception( psycopg2.DatabaseError )

            if self.twitter_url is not None:
                contactDetails_twitter = ContactDetails( "agent_twitter_url", OBJECT_Agent, self.agent_id, self.twitter_url )
                #Attempt to save the twitter details
                if not contactDetails_twitter.storeContactDetails( False ):
                    raise Exception( psycopg2.DatabaseError )

            if self.photo is not None:

                #Save the mugshot and agent photo.
                file_agentPhoto = File( FILE_TYPE_Images, OBJECT_Agent, self.agent_id, None, "agent_photo" )
                file_agentPhoto.addImageDetails( None, None, self.photo )
                if not file_agentPhoto.storeFile( False ):
                    raise Exception( psycopg2.DatabaseError )

            if self.mugshot_url is not None:

                file_agentMugShot = File( FILE_TYPE_Images, OBJECT_Agent, self.agent_id, None, "agent_mugshot" )
                file_agentMugShot.addImageDetails( None, None, self.mugshot_url )
                if not file_agentMugShot.storeFile( False ):
                    raise Exception( psycopg2.DatabaseError )
            
            if commit:
                conn.commit()

            return self.agent_id

        except(Exception, psycopg2.DatabaseError) as error:
            print("Error in INSERTING New Agent " + self.first_name + " " + self.last_name + "\n" + error )
            return None

    