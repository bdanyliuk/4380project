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
conn = sqlite3.connect('example.db')

# Create a cursor object
cur = conn.cursor()

# Define a list of queries to run
queries = [
    "SELECT * FROM my_table",
    "SELECT COUNT(*) FROM my_table",
    "SELECT AVG(salary) FROM employees WHERE department = 'Sales'",
    "SELECT name, age FROM employees ORDER BY age DESC LIMIT 10"
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

# Close the cursor and database connection
cur.close()
conn.close()