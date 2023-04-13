# proof of implementation for project
#COMP 4380 Winter 2023 - Submitted to Adam Pazdor
# Project group 6 
#Brandon, Danyliuk
#Nikhil, Sidhu
#Ryan, Dotzlaw
#Seth, Peters
#Tung, NGUYEN


import sqlite3
import time

# Connect to the SQLite database
conn = sqlite3.connect('requests.db')

# Create a cursor object
cur = conn.cursor()

# Define a list of queries to run
queries = [
    "SELECT complaint_type, COUNT(*) AS total_complaints,ROUND((COUNT(CASE WHEN closed_date IS NOT NULL AND closed_date <> '' THEN 1 END) * 100.0 / COUNT(*)), 2) AS closed_percentage FROM requests WHERE city LIKE 'B%' AND created_date NOT LIKE '%/%/201%' GROUP BY complaint_type ORDER BY total_complaints DESC LIMIT 10;",
    "SELECT STRFTIME('%Y', date) AS year, max(request_count) AS max_requests, month FROM ( SELECT (SUBSTR(created_date, 7, 4) || '-' || SUBSTR(created_date, 4,2) || '-' || SUBSTR(created_date, 1,2)) AS date, STRFTIME('%d', (SUBSTR(created_date, 7, 4) || '-' || SUBSTR(created_date, 4,2) || '-' || SUBSTR(created_date, 1,2))) AS month, count(*) AS request_count FROM requests where agency = 'NYPD' HAVING request_count > 2000 GROUP BY date, month) AS request_counts GROUP BY year, month;",
    "SELECT incident_address, COUNT(incident_address) FROM requests GROUP BY incident_address ORDER BY COUNT(IncidentAddress) DESC LIMIT 10;",
    "SELECT agency, agency_name, COUNT(Agency) as requests_closed FROM requests WHERE closed_date LIKE '12-25-%' AND agency IS NOT 'NYPD' GROUP BY agency ORDER BY COUNT(agency) DESC LIMIT 1",
    "SELECT complaint_type, COUNT() AS count FROM requests WHERE complaint_type LIKE'%Heat%' AND ParkBorough LIKE '%BROOKLYN%'",
    "SELECT city, descriptor, COUNT(descriptor) AS description FROM requests WHERE incident_zip LIKE '10%' AND (descriptor LIKE '%Party%' OR descriptor LIKE '%party%') GROUP BY city ORDER BY description DESC",
    "SELECT complaint_type, COUNT() as count FROM requests GROUP BY complaint_type ORDER BY count DESC LIMIT 5;",
    "SELECT city, COUNT() FROM requests WHERE complaint_type LIKE ‘%Noise%’ GROUP BY city ORDER BY DESC LIMIT 1;",
    "SELECT city, COUNT(*) as count from requests where created_date BETWEEN '2020-01-01' AND '2022-01-01' GROUP BY city ORDER BY count ASC LIMIT 4;",
    "SELECT COUNT(*) as count, complaint_type as type FROM requests WHERE JULIANDAY(STRFTIME('%Y-%m-%d', SUBSTR(created_date, 1, 10))) >= JULIANDAY('2012-10-10') AND JULIANDAY(STRFTIME('%Y-%m-%d', SUBSTR(created_date, 1, 10))) <= JULIANDAY('2012-11-03') GROUP BY type ORDER BY count DESC",
    "SELECT created_date, COUNT() as count FROM requests WHERE complaint_type LIKE'%Noise%' GROUP BY create_date ORDER BY count DESC LIMIT 3;",
    "SELECT complaint_type, descriptor FROM requests WHERE created_date LIKE'%2012-12-21%';"
]


indexes = [
    "CREATE INDEX idx_b_2020 ON requests(created_date, city) WHERE city LIKE "B%" AND created_date > '2020-01-01%';", # Optimization for Query 1
    "CREATE INDEX idx_agency_nypd ON REQUESTS (agency) WHERE agency IS 'NYPD';", # Optimization for Query 2
    "CREATE INDEX ind_incident_address on REQUESTS (incident_address)", # Optimization 3
    "CREATE INDEX ind_Agency_Closed on REQUESTS (agency, closed_date) WHERE agency IS NOT ‘NYPD’ and closed_date LIKE ‘12-25-%’", # Optimization 4
    "CREATE INDEX idx_park_borough ON REQUESTS(park_borough) WHERE complaint_type LIKE'%Heat%' AND ParkBorough LIKE '%BROOKLYN%';", # Optimization for Query 5
    "CREATE INDEX idx_party_zip ON REQUESTS(incident_zip, descriptor) WHERE incident_zip LIKE '10%' AND (descriptor LIKE '%Party%' OR descriptor LIKE '%party%');", # Optimization for Query 6
    "CREATE INDEX idx_complaint_type ON REQUESTS(complaint_type);", # Optimization for Query 7
    "CREATE INDEX idx_city ON REQUESTS (city)  where complaint_type LIKE ‘%Noise%’ ;", # Optimization for Query 8
    "CREATE INDEX idx_created_date ON REQUESTS(created_date);", # Optimization for Query 9
    "CREATE INDEX idx_sandy ON REQUESTS(created_date) WHERE created_date BETWEEN "2012-10-10" AND "2012-11-03";", # Optimization for Query 10
    "CREATE TABLE request_sorted AS SELECT * FROM REQUESTS ORDER BY created_date", # Optimization for Query 11 and 12
    "CREATE INDEX idx_sandy ON REQUESTS(created_date);" # Optimization for Query 11 and 12
    
]


optimized_queries = [
    "SELECT complaint_type, COUNT(*) AS total_complaints, ROUND((COUNT(CASE WHEN closed_date IS NOT NULL AND closed_date <> '' THEN 1 END) * 100.0 / COUNT(*)), 2) AS closed_percentage FROM requests INDEXED BY idx_b_2020 WHERE city LIKE 'B%' AND created_date > '2020-01-01' GROUP BY complaint_type ORDER BY total_complaints DESC LIMIT 10", #Query 1
    "SELECT strftime('%Y', date) AS year, strftime('%m', date) AS month, max(request_count) AS max_requests FROM ( SELECT substr(created_date, 1,10) AS date, count(*) AS request_count FROM REQUESTS INDEXED BY idx_agency_nypd WHERE agency IS 'NYPD' GROUP BY date HAVING request_count > 2000) AS request_counts GROUP BY year, month;", # Query 2
    "SELECT incident_address, count(incident_address) from REQUESTS Indexed by ind_IncidentAddress group by incident_address order by count(incident_address) desc limit 10", #Query 3
    "SELECT agency, agency_name, count(agency) as 'Requests Closed' from REQUESTS indexed by ind_Agency_Closed where ClosedDate like "12-25-%" and Agency is not 'NYPD' group by Agency order by count(Agency) desc limit 1", # Query 4
    "SELECT complaint_type, count() as count FROM REQUESTS INDEXED BY idx_park_borough WHERE complaint_type LIKE '%Heat%' AND park_borough LIKE '%BROOKLYN%'", #Query 5
    "SELECT city, descriptor AS description, COUNT(*) AS count FROM REQUESTS INDEXED BY idx_party_zip WHERE incident_zip LIKE '10%' AND (descriptor LIKE '%Party%' OR descriptor LIKE '%party%') GROUP BY city, description ORDER BY count DESC;", #Query 6
    "SELECT complaint_type, count() as count FROM REQUESTS INDEXED BY idx_complaint_type GROUP BY complaint_type ORDER BY count DESC LIMIT 5;", #Query 7
    "SELECT city, count() from REQUESTS INDEXED BY idx_city where complaint_type LIKE ‘%Noise%’ GROUP BY city ORDER BY DESC LIMIT 1;", #Query 8
    "SELECT city, COUNT(*) as count from REQUESTS where created_date  BETWEEN '2020-01-01' and '2022-01-01' GROUP BY city ORDER BY count ASC LIMIT 4;", #Query 9
    "SELECT COUNT(*) as count, complaint_type as type FROM requests WHERE julianday(strftime('%Y-%m-%d', substr(created_date, 1, 10))) >= julianday('2012-10-10') AND julianday(strftime('%Y-%m-%d', substr(CreatedDate, 1, 10))) <= julianday('2012-11-03') group by type order by count desc", #Query 10
    "SELECT created_date, COUNT() as count FROM requests_sorted WHERE complaint_type LIKE '%Noise%' GROUP BY created_date ORDER BY count DESC LIMIT 3;", #Query 11
    "SELECT complaint_type, descriptor FROM requests_sorted WHERE created_date LIKE '%2012-12-21%';" #Query 12
]

# Loop through the queries and run them
for query in queries:
    start_time = time.time()
    cur.execute(query)
    result = cur.fetchall()
    end_time = time.time()
    runtime = end_time - start_time
    print(f"Query: {query}")
    print(f"Result: {result}")
    print(f"Runtime: {runtime:.6f} seconds\n")
    
    
for index in indexes
    cur.execute(index);
    result = cur.fetchall();
    
for query in optimized_queries:
    start_time = time.time()
    cur.execute(query)
    result = cur.fetchall()
    end_time = time.time()
    runtime = end_time - start_time
    print(f"Optimized Query: {query}")
    print(f"Optimized Result: {result}")
    print(f"Optimized Runtime: {runtime:.6f} seconds\n")


# Close the cursor and database connection
cur.close()
conn.close()
