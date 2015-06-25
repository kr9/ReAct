from flask import render_template
from app import app
from cassandra.cluster import Cluster
from flask import Flask, render_template, redirect, url_for, request, jsonify, json

cluster = Cluster(['52.26.58.1'])
session = cluster.connect()
session.set_keyspace("activitydb")

def get_users(one=False):
    rows = session.execute("SELECT * FROM user")
    users = []
    for row in rows:
        users.append(map_user(row))
    return users

def get_user(user_id):
    row = session.execute("SELECT * FROM user WHERE user_id = '{0}'".format(user_id))
    return map_user(row[0])

def map_user(row):
    return {"user_id" : row.user_id, "name" : row.name, "zip" : row.zip, "lat": row.lat, "lon": row.lon }

def map_user_activity(row):
    return {'user_id' : row.user_id, 'zip' : row.zip, 'activity_type' : row.activity_type, 'duration' : row.duration, "lat": row.lat, "lon": row.lon }

@app.route('/users')
def all_users():
    return jsonify({"users" : get_users()})

@app.route('/users/<user_id>')
def search_users_by_id(user_id):
    return jsonify({'user' : get_user(user_id)})

@app.route('/users/<zip>/<activity_type>')
def search_users_by_zip_activity_type(zip, activity_type):
    rows = session.execute("SELECT * FROM activity_by_user WHERE zip='{0}' AND activity_type='{1}'".format(zip, activity_type))
    users = []
    for row in rows:
        users.append(map_user_activity(row))
    return jsonify({"usersStats": users})

@app.route('/')
@app.route('/index')
def index():
    return render_template(
	"index.html",
        title = 'ReAct',
        users = get_users(),
        activities = ['WALKING','RUNNING','CYCLING'])
