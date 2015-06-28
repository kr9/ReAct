COPY locations 
     (zip, lat, lon, city, state, county) 
FROM 'Locations.csv' 
WITH HEADER = 'true';
