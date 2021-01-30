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
app.config.from_mapping(
        BASE_URL = "http://localhost:5000",
        USE_NGROCK = os.environ.get("USE_NGROK", "FALSE") == "True" and os.environ.get("WERKZEUG_RUN_MAIN") != "true",
)
app.config['ENV'] = "development"
app.config['USE_NGROK']= True
#Ngrok configuration settings and initialiation.
ngrok.set_auth_token( "1my7NVoYz9WN8HVt0EKh737zDSC_42sBWTj6fuRvpgBAAhvKY" )
pyngrock_config = conf.PyngrokConfig(region = 'au')
conf.set_default(pyngrock_config)
if app.config.get("ENV") == "development" and app.config.get("USE_NGROK"):
      
        # Get the dev server port (defaults to 5000 for Flask, can be overridden with `--port`
        # when starting the server
        port = sys.argv[sys.argv.index("--port") + 1] if "--port" in sys.argv else 5000

        # Open a ngrok tunnel to the dev server
        public_url = ngrok.connect("5000", None, "test_tunnel").public_url
        print(" * ngrok tunnel \"{}\" -> \"http://127.0.0.1:{}\"".format(public_url, port))

        # Update any base URLs or webhooks to use the public ngrok URL
        app.config["BASE_URL"] = public_url
app.json_encoder = CustomJSONEncoder

@app.route("/")
def home():
    verificationCode = request.args.get( "verification" )
    print(verificationCode)
    if verificationCode is None:
        return render_template("index.html" )
    elif verificationCode == "vfy_6bbf259524ee4d9298a0d788b4b4f04f":
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


if __name__ == "__main__":
    app.run( )

print("Server Started")