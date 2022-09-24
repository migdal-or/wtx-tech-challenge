# wtx-tech-challenge

To run this code, one needs a trades.csv file you supplied me privately via email.
Please add it to the gitcloned folder before running.
The .gitignore file contains a *.csv entry to ignore this file.
Also the requirements are:
* Working PostgreSQL database on the localhost (perfectly done with Docker)
* pip install beautifulsoup4
* psycopg2

#### Step 1 - Import / Export reports

First. I'd say that from the database approach, the process requested does not look optimal.
Instead of checking CSV file with Python and counting ports and values during this process, I'd rather:

1. Consider these CSV files are Operational Data Store, make some sort of a queue, probably with directories: inbox / archive,
2. Load all new incoming data into Staging database (probably volatile?) tables with minimum data validation,
this is faster that custom-written processing of CSV in Python even with optimizations, since most databases have their own tools for fast CSV files import and data types auto-detection.
3. Perform complete validation, cleaning and aggregation inside the database.
This is the approach that I tried to pursue in my Take-Home Case.

First, I assume the machine where the assessment takes place has Docker installed and an Internet access channel.
Then, I download a PostgreSQL official image and run its container:
```
docker run --name wtx-import -p 5432:5432 -e POSTGRES_PASSWORD=mysecretpassword -d postgres
docker stop wtx-import
docker start wtx-import
```
Now the database can be accessed from command-line with
```docker exec -it wtx-import psql -U postgres```

In real use, we can connect another arbitrary database to the script.
In the script, I create (if it does not exist) a storage table in a default database which will store the new data (in real production use this should be done with 
```
COPY STG_trades_`date+%s` (fields) FROM 'trades.csv' DELIMITER ';' CSV HEADER;)
```
And on fourth stage, I'd load data from STG table to the public accessible table, ensuring SCD changes, historical versioning schema and date of ingestion. This step would check if port codes follow the reference table, quantities and values match, replace quantity with 1 if it is empty and value is <80K, and so on.
Also during this load step I'd add values to fields such as SOURCE_SYSTEM_CD, IS_ACTIVE_FLG, DELETED_FLG, EFFECTIVE_FROM_DTTM, EFFECTIVE_TO_DTTM and PROCESSED_DTTM.

We need more knowledge on how the value_fob_usd is formatted: will it always use comma as a fractional part separator?
If yes, is it linked to any locale? Now I have hardcoded to treat comma sign as decimal separator.

What are the business requirements if data is incomplete? The actual data has an "i" for items_number, but since there is no use of this parameter I decided to ignore that and store it as text.

After the data is loaded into database, we can easily analyse it by creating views or with queries such as
* Most popular shipping routes (source port and destination port):
```SELECT source_port, destination_port, COUNT(*) FROM trades GROUP BY source_port, destination_port ORDER BY count(*) desc;```

* Average import value (in USD) per country
```SELECT destination_country, AVG(std_quantity*value_fob_usd) FROM trades GROUP BY destination_country;```

On my machine, the load of the supplied 45 Kbytes file took 0m0.324seconds.
That allows me to expect that this script will perform good enough on big volumes of data.
Real use would require more resources for indexing data, checking constraints, dealing with incorrect data, but for now a good estimate of about a thousand records per second is good.


#### Step 2 - Port information
Based on loaded shipping routes data, I would make a table to store port details.
The table should probably have an SCD2 versioning scheme to mark the historical route changelog, when the route data got changed or deleted.
For each port, I'd save the country, port_code, URL, Major towns, Shipping lines, Exp/imp requirements and the complete HTML received from the web page.
The Python code has the table creation code, the SQL to fill it and web-parsing algorithm.
The fill code uses the same SQL query to initialize the table from the beginning and to make updates later.

Now we have a list of stored ports with their respective countries in the database.
Let's get it into a list_countries to traverse it.
The webpage structure looks as following:
1. At the top https://www.cogoport.com/ports we have a list of countries.
  We can enumerate all &lt; a &gt; links on this page and check each one against country names from list_countries.
  Probable collisions: 'Sudan' vs 'South Sudan', 'Oman' vs 'Romania', 'Mali' vs 'Somalia', 'Niger' vs 'Nigeria'<br>
  Should I fix these? Nope, that already works pretty good.<br>
  For each country, we have a hyperlink to a page containing all its ports.
  So we store a list of lists list_countries_links = [ [countryname1, link1], [countryname2, link2] ] to process it further.
2. Let's make a loop in list_countries_links.
  On each loop, we query ports list for this country from the database,
  we load a webpage which can contain data for some ports,
  we enumerate all &lt; a &gt; links on this page and check each one against ports we know for this country.
  For each port found, we store it in a list of lists
  list_countries_ports_links = [ [countryname1, port_code1, link1], [countryname2, port_code2, link2] ]
3. Let's make a loop in list_countries_ports_links.
  On each loop, we query the webpage for its link,
  we extract data from the webpage,
  we check if data is the same. If data is the same, we just change the PROCESSED_DTTM.
  If data has changed, we change EFFECTIVE_TO_DTTM to NOW(), change PROCESSED_DTTM to NOW(),
  we insert a new record with changed data and
  EFFECTIVE_FROM_DTTM = NOW(), EFFECTIVE_TO_DTTM = 'infinity', PROCESSED_DTTM = NOW()
If the port in our dataset does not have information on the website, then we can skip it but keep track that we have no information available.

This code can scale the data load amounts based on PROCESSED_DTTM: with minor edits the script can take, for example, 100 records which were processed, say, more that 10 hours ago, refresh these and be ready for the next run when it is possible.










# TODO
- Make sure to describe all components being used as well an overall cost estimation and how that would scale
