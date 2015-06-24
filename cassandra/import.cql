COPY user 
     (user_id, name, zip, lat, lon) 
FROM 'users.csv' 
WITH HEADER = 'true';
