from flask import render_template
from app import app
from cassandra.cluster import Cluster
from flask import Flask, render_template, redirect, url_for, request, jsonify, json

cluster = Cluster(["127.0.0.1"])
session = cluster.connect()
session.set_keyspace("activitydb")

@app.route('/users')
def all_users():
    table = "user"
    records = "SELECT * FROM {}".format(table)
    record_list = session.execute(records)
    users = []
    for row in record_list:
        user = {}
        user['user_id'] = row.user_id
        user['name'] = row.name
        user['zip'] = row.zip
        users.append(user)
    record_list_json = jsonify({"users": users})
    return record_list_json

@app.route('/users/<user_id>')
def search_users_by_id(user_id):
    table = "user"
    records = "SELECT * FROM {0} WHERE user_id='{1}'".format(table, user_id)
    record_list = session.execute(records)
    users = []
    for row in record_list:
        user = {}
        user['user_id'] = row.user_id
        user['name'] = row.name
        user['zip'] = row.zip
        users.append(user)
    record_list_json = jsonify({"users": users})
    return record_list_json

@app.route('/users/<zip>/<activity_type>')
def search_users_by_zip_activity_type(zip, activity_type):
    table = "activity_by_user"
    records = "SELECT * FROM {0} WHERE zip='{1}' AND activity_type='{2}'".format(table, zip, activity_type)
    record_list = session.execute(records)
    users = []
    for row in record_list:
        user = {}
        user['user_id'] = row.user_id
        user['zip'] = row.zip
        user['activity_type'] = row.activity_type
        user['duration'] = row.duration
        users.append(user)
    record_list_json = jsonify({"usersStats": users})
    return record_list_json

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
    user = { 'nickname': 'Kamal' }
    return render_template(
	"index.html",
        title = 'Home',
        user = user)
