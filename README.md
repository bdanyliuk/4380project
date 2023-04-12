# 4380project - COMP 4380 Winter 2023
README for describing how data was obtained and maniuplated

The Data was obtained as a CSV in Feburary 2023 from an open database collection at 
https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9


The data consisted of 1 table, being around 16gbs and downloaded in a CSV format.

The data was then imported into SQLite which used an import tool to convert the CSV into a DB file. The columns
were based on the CSV headers and used the unique_key column to establish a primary key with type INTEGER

The original queries were run on the vanilla table before any structural modifidcations.

An improved copy of the table was then created to reformat the "created_date" and "closed_date" column to achieve a format SQLite recognized.

The optimized queries created non-clustered B+ tree indexes within the improved table.

To implement and automate the queries, we used a python script to connect to the SQLite database send the queries to the database trough python.

The original and optimized queries are stored as a list in the implementation.py script and are interated through and sent individually to the database,
after which the time it took to send is recored and displayed, along with the results.
