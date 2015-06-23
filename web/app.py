from flask import Flask, render_template, redirect, url_for, request, jsonify, json
import csv
import time
from datetime import datetime, timedelta
import time
from cassandra.cluster import Cluster
import folium
#from flask_cassandra import CassandraCluster

#print dir(cassandra.cluster)

app = Flask(__name__)

#app.config['CASSANDRA_NODES'] = ['ec2-52-26-58-1.us-west-2.compute.amazonaws.com'] 

cluster = Cluster(["52.26.58.1"])
session = cluster.connect()
session.set_keyspace("activitydb")
table = "activity_master"

@app.route('/schema')
def show_schema():

    schema = {
    		"activity_id": "123456792",
		    "activity_type": "Running",
    		"lat": 37.791,
    		"lon": -122.4,
    		"time": "2015-06-09-03:15:18",
    		"user_id": "uid4"
    	     }
    
    return jsonify(schema)


@app.route('/allrecords/')
def display_all():
    records = "SELECT * FROM {}".format(table)
    record_list = session.execute(records)
    print record_list
    json_result = jsonify(record_list)
    return render_template("index.html", posts=json_result)

'''
@app.route("/map")
def map():
    
    lat = "SELECT lat FROM {} WHERE activity_id= '123456802'".format(table)
    lon = "SELECT lon FROM {} WHERE activity_id= %s".format(table)
    act='123456802'
    lat1 = session.execute(lat,act)
    lon1 = session.execute(lon,act)
    print lat1
    print lon1
    return render_template("map.html",lat=lat1,lon=lon1)
'''

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





if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)

        

        