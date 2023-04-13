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
    "SELECT strftime('%Y', date) AS year, max(request_count) AS max_requests, month FROM ( SELECT (substr(created_date, 7, 4) || '-' || substr(created_date, 4,2) || '-' || substr(created_date, 1,2)) AS date, strftime('%d', (substr(created_date, 7, 4) || '-' || substr(created_date, 4,2) || '-' || substr(created_date, 1,2))) AS month, count(*) AS request_count FROM requests where agency = 'NYPD' HAVING request_count > 2000 GROUP BY date, month) AS request_counts GROUP BY year, month;",
    "SELECT IncidentAddress, count(IncidentAddress) from REQUESTS group by IncidentAddress order by count(IncidentAddress) desc limit 10;",
    "SELECT Agency, AgencyName, count(Agency) as Requests_Closed from REQUESTS where ClosedDate like '12-25-%' and Agency is not 'NYPD' group by Agency order by count(Agency) desc limit 1",
    "SELECT ComplaintType, count() as count FROM REQUESTS WHERE ComplaintType LIKE'%Heat%' AND ParkBorough LIKE '%BROOKLYN%'",
    "SELECT city, descriptor, count(descriptor) as description from REQUESTS where incident_zip like '10%' and (descriptor like '%Party%' or descriptor like '%party%') group by city order by description desc",
    "SELECT complaint_type, count() as count FROM REQUESTS GROUP BY complaint_type ORDER BY count DESC LIMIT 5;",
    "SELECT city, count() from REQUESTS where complaint_type LIKE ‘%Noise%’ GROUP BY city ORDER BY DESC LIMIT 1;",
    "SELECT City, COUNT(*) as count from Complaint where CreatedDate  BETWEEN '2020-01-01' and '2022-01-01' GROUP BY City ORDER BY count ASC LIMIT 4;",
    "SELECT COUNT(*) as count, complaint_type as type FROM REQUESTS WHERE julianday(strftime('%Y-%m-%d', substr(created_date, 1, 10))) >= julianday('2012-10-10') AND julianday(strftime('%Y-%m-%d', substr(created_date, 1, 10))) <= julianday('2012-11-03') group by type order by count desc",
    "SELECT CreatedDate, count() as count FROM REQUESTS Where ComplaintType LIKE'%Noise%' GROUP BY CreatedDate ORDER BY count DESC LIMIT 3;",
    "SELECT ComplaintType, Descriptor FROM REQUESTS WHERE CreatedDate LIKE'%2012-12-21%';"
]


indexes = [
    "", # Optimization for Query 1
    "", # Optimization for Query 2
    "create index ind_IncidentAddress on REQUESTS (IncidentAddress)", # Optimization 3
    "create index ind_Agency_Closed on REQUESTS (Agency, ClosedDate) where Agency is not ‘NYPD’ and ClosedDate like ‘12-25-%’", # Optimization 4
    "", # Optimization for Query 5
    "", # Optimization for Query 6
    "", # Optimization for Query 7
    "", # Optimization for Query 8
    "", # Optimization for Query 9
    "", # Optimization for Query 10
    "", # Optimization for Query 11
    "" # Optimization for Query 12
    
]


optimized_queries = [
    "", #Query 1
    "", #Query 2
    "select IncidentAddress, count(IncidentAddress) from REQUESTS Indexed by ind_IncidentAddress group by IncidentAddress order by count(IncidentAddress) desc limit 10", #Query 3
    "select Agency, AgencyName, count(Agency) as 'Requests Closed' from REQUESTS indexed by ind_Agency_Closed where ClosedDate like "12-25-%" and Agency is not 'NYPD' group by Agency order by count(Agency) desc limit 1", # Query 4
    "", #Query 5
    "", #Query 6
    "", #Query 7
    "", #Query 8
    "", #Query 9
    "", #Query 10
    "", #Query 11
    "" #Query 12
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
