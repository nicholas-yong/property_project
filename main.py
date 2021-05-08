from flask import Flask, redirect, url_for, render_template, jsonify, request, abort, Response
from flask.json import JSONEncoder
from pyngrok import ngrok, conf
from database import conn, cur
from decimal import Decimal
import psycopg2
import os
import sys 

class CustomJSONEncoder(JSONEncoder):

    def default( self, obj ):
        try:
            if isinstance( obj, Decimal ):
                if obj is not None:
                    return str(obj)
        except TypeError:
            pass
        else:
            return None
        return JSONEncoder.default( self, obj )

#Flask Initialization and Configuration Settings.
app = Flask(__name__)
app.json_encoder = CustomJSONEncoder

@app.route("/")
def home():
    verificationCode = request.args.get( "verification" )
    print(verificationCode)
    if verificationCode is None:
        return render_template("index.html" )
    elif verificationCode == "vfy_8a05920509944d6ead78219e291886e0":
        return abort (Response( "test", 204) )
    else:
        return abort (Response( "test", 404) )

@app.route("/admin")
def admin():
    return redirect(url_for("user", name = "Admin!"))

@app.route("/suburb")
def suburbs():
    #define a query and get the suburb_id and name of the suburb in a tuple array
    results = []
    try:
        cur.execute( """SELECT s.suburb_id, s.name FROM suburbs s"""
                    , "" )
        results = cur.fetchall()
        return jsonify(results ) 
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    return "Test"


@app.route("/suburbName/<suburbName>")
def suburbName( suburbName ):
    #We need to get the relevant information for properties inside the requested suburb
    try:
        cur.execute( """ SELECT l.headline, l.price, a.full_address, ag.first_name || ' ' || ag.last_name, l.seo_url, l.display_price
                         FROM listings l, properties_listings pl, properties p, address a, suburbs s, agents ag, agent_listings al
                         WHERE l.price is not null 
	                        AND pl.listings_id = l.listings_id
	                        AND p.property_id = pl.property_id
	                        AND p.address_id = a.address_id
	                        AND a.suburb_id = s.suburb_id
                            AND ag.agent_id = al.agent_id
                            AND al.listings_id = l.listings_id
	                        AND UPPER( s.name ) = UPPER( %s ) """,
                     ( suburbName, ) )
        results = cur.fetchall()
        return jsonify( results )

        #Convert the prices to string
    except(Exception, psycopg2.DatabaseError ) as error:
        print(error)
        return "Failure"

@app.route("/domain-webhook/HTTP/1.1")
def handleNotification( ):
    #Setup the handler for the webhook notification
    abort( Response( "None", 204) )
    notification_file = open( "notification.txt", "w" )
    notification_file.write( request.form )
    notification_file.close()
    


if __name__ == "__main__":
    app.run( )

print("Server Started")