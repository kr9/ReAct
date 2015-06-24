from flask import render_template
from app import app
from cassandra.cluster import Cluster
from flask import Flask, render_template, redirect, url_for, request, jsonify, json

cluster = Cluster(['127.0.0.1'])
session = cluster.connect()
session.set_keyspace("activitydb")

def get_users(one=False):
    rows = session.execute("SELECT * FROM user")
    users = []
    for row in rows:
        users.append(map_user(row))
    return users

def get_user(user_id, one=True):
    row = session.execute("SELECT * FROM user WHERE user_id = '{0}'".format(user_id))
    return map_user(row[0])

def map_user(row):
    return {"user_id" : row.user_id, "name" : row.name, "zip" : row.zip, "lat": row.lat, "lon": row.lon }

def map_user_activity(row):
    return {'user_id' : row.user_id, 'zip' : row.zip, 'activity_type' : row.activity_type, 'duration' : row.duration }

@app.route('/users')
def all_users():
    return jsonify({"users" : get_users()})

@app.route('/users/<user_id>')
def search_users_by_id(user_id):
    return jsonify({'user' : get_user(user_id)})

@app.route('/users/<zip>/<activity_type>')
def search_users_by_zip_activity_type(zip, activity_type):
    rows = session.execute("SELECT * FROM activity_by_user WHERE zip='{1}' AND activity_type='{2}'".format(zip, activity_type))
    users = []
    for row in record_list:
        users.append(map_user_activity(row))
    return jsonify({"usersStats": users})

@app.route("/map")
def map():    
    # map_1 = folium.Map(location=[45.372, -121.6972], zoom_start=12,
    #                tiles='Stamen Terrain')
    # map_1.simple_marker([45.3288, -121.6625], popup='Mt. Hood Meadows')
    # map_1.simple_marker([45.3311, -121.7113], popup='Timberline Lodge')
    # map_1.create_map(path='~/templates/map.html')
    '''
    lat = "SELECT lat FROM {} WHERE activity_id= '123456802'".format(table)
    lon = "SELECT lon FROM {} WHERE activity_id= %s".format(table)
    act='123456802'
    lat1 = session.execute(lat,act)
    lon1 = session.execute(lon,act)
    print lat1
    print lon1
    '''
    return render_template("osm.html")

@app.route('/')
@app.route('/index')
def index():
    return render_template(
	"index.html",
        title = 'ReAct',
        users = get_users(),
        activities = ['WALKING','RUNNING','CYCLING'])
