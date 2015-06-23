import folium
#map_osm = folium.Map(location=[45.5236, -122.6750], tiles='Mapbox',API_key='k262629.mg7bd69b', zoom_start=10, max_zoom=15)
map_osm = folium.Map(location=[45.5236, -122.6750])
map_osm.simple_marker([45.3288, -121.6625], popup='My Popup Message')
map_osm.create_map(path='templates/osm.html')
