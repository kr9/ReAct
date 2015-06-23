from cassandra.cluster import Cluster
print dir(cassandra.cluster)

from flask import Flask
from flask_cassandra import CassandraCluster

app = Flask(__name__)
cassandra = CassandraCluster()

app.config['CASSANDRA_NODES'] = ['ec2-52-26-58-1.us-west-2.compute.amazonaws.com']  # can be a string or list of nodes

@app.route("/cassandra_test")
def cassandra_test():
    session = cassandra.connect()
    session.set_keyspace("activitydb")
    cql = "SELECT * FROM activity_by_user LIMIT 1"
    r = session.execute(cql)
    return str(r[0])

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)


