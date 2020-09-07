from flask import Flask, redirect, url_for, render_template, jsonify
from database import conn, cur
import psycopg2

app = Flask(__name__)

@app.route("/<name>")
def home(name):
    return render_template("index.html", content=name )

@app.route("/admin")
def admin():
    return redirect(url_for("user", name = "Admin!"))

@app.route("/suburbs")
def suburbs():
    #define a query and get the suburb_id and name of the suburb in a tuple array
    results = []
    try:
        suburb_query = f"""SELECT s.suburb_id, s.name FROM suburbs s"""
        cur.execute( suburb_query, "" )
        results = cur.fetchall()
        return jsonify(results ) 
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    return "Test"

if __name__ == "__main__":
    app.run( debug = True)

print("Server Started")