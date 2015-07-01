from flask import render_template
from app import app
from cassandra.cluster import Cluster
from flask import Flask, render_template, redirect, url_for, request, jsonify, json

cluster = Cluster(['52.26.58.1'])
session = cluster.connect()
session.set_keyspace("activitydb")

def get_users(limited=True):
    if limited == True:
        rows = session.execute("SELECT * FROM user LIMIT 2000")
    else:
        rows = session.execute("SELECT * FROM user")
    users = []
    for row in rows:
        users.append(map_user(row))
    return users

def get_user(user_id):
    row = session.execute("SELECT * FROM user WHERE user_id = '{0}'".format(user_id))
    return map_user(row[0])

def get_user_stat(user_id, zip, activity_type):
    row = session.execute("SELECT * FROM activity_avg WHERE user_id = '{0}' AND zip='{1}' AND activity_type='{2}'".format(user_id, zip, activity_type))
    return {"user_id" : row[0].user_id, "avg": row[0].avg, "sum": row[0].sum, "activity_type" : row[0].activity_type} if len(row) != 0 else None

def map_user(row):
    return {"user_id" : row.user_id, "name" : row.name, "zip" : row.zip, "lat": row.lat, "lon": row.lon }

def map_user_activity(row):
    return {'user_id' : row.user_id, 'zip' : row.zip, 'activity_type' : row.activity_type, "lat": row.lat, "lon": row.lon }

@app.route('/users')
def all_users():
    return jsonify({"users" : get_users(False)})

@app.route('/users/<user_id>')
def search_users_by_id(user_id):
    return jsonify({'user' : get_user(user_id)})

@app.route('/userstats/<user_id>/<zip>/<activity_type>')
def search_userstats_by_id(user_id, zip, activity_type):
    return jsonify({'userStats' : get_user_stat(user_id, zip, activity_type)})

@app.route('/users/<zip>/<activity_type>')
def search_users_by_zip_activity_type(zip, activity_type):
    rows = session.execute("SELECT * FROM activity_avg WHERE zip='{0}' AND activity_type='{1}'".format(zip, activity_type))
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

@app.route('/slides')
def slides():
    return render_template(
    "slides.html")

@app.route('/links')
def links():
    return render_template(
    "links.html")
