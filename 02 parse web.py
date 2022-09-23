import psycopg2
conn = None
base_url = 'https://www.cogoport.com'
import requests

list_countries = []
list_countries_links = []
list_countries_ports_links = []
list_ports = []

# need to install this by pip install beautifulsoup4
from bs4 import BeautifulSoup

try:
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="mysecretpassword")

    # create a cursor
    with conn.cursor() as cur:
        # create a table to store ports data if it does not exist
        cur.execute("""CREATE TABLE IF NOT EXISTS ports(
            port_id SERIAL PRIMARY KEY,
            port_country VARCHAR(50),
            port_code VARCHAR(50),
            port_url VARCHAR(50),
            major_towns TEXT,
            shipping_lines TEXT,
            import_reqs TEXT,
            export_reqs TEXT,
            port_html TEXT,
            
            EFFECTIVE_FROM_DTTM timestamp,
            EFFECTIVE_TO_DTTM timestamp,
            PROCESSED_DTTM timestamp
            );""")
        conn.commit()
        # TODO: process possible errors here and for every cur.execute
        
        # We search for new ports available in our routes table
        # which we should add to ports and query data from webpage.
        # As these ports do not exist in the database 
        # or do not have information for the moment of NOW(),
        # their details are empty
        
        cur.execute("""INSERT INTO ports ( port_country, port_code, EFFECTIVE_FROM_DTTM, EFFECTIVE_TO_DTTM, PROCESSED_DTTM)
            SELECT port_country, port_code, NOW() as EFFECTIVE_FROM_DTTM, 'infinity' as EFFECTIVE_TO_DTTM, NOW() as PROCESSED_DTTM 
            FROM (
                SELECT distinct port_country, port_code
                FROM (
                    SELECT source_country as port_country, source_port as port_code FROM trades
                    UNION
                    SELECT destination_country, destination_port FROM trades
                ) a 
                EXCEPT SELECT port_country, port_code FROM ports WHERE NOW() BETWEEN EFFECTIVE_FROM_DTTM and EFFECTIVE_TO_DTTM
            ) b;    """)
        conn.commit()
        # TODO: work out the case when there's a port data in a future:
        # say, this is year 2022 now, 
        # and there's data that the port will be open since 2025.
        # For now, the code will add a wrong record from now to infinity.
        # correct: add a subquery so that EFFECTIVE_TO_DTTM=coalesce( min(effective_from)-(1 second) where effective_from>now(), 'infinity')

        # TODO: process possible errors here and for every cur.execute
        
        # Here we have a list of stored ports with their respective countries in the database.
        # Let's get it into a list_countries to traverse it.
        # The webpages are laid in 3 levels:
        # 1. At the top https://www.cogoport.com/ports we have list of countries.
        #   We can enumerate all <a> links on this page and check each one against country names from list_countries.
        #   Probable collisions: 'Sudan' vs 'South Sudan', 'Oman' vs 'Romania', 'Mali' vs 'Somalia', 'Niger' vs 'Nigeria'
        #   Impossible because we check for complete <div class="country__name">CountryName</div>
        #   For each country, we have a hyperlink to a page containing all its ports.
        #   So we store a list of lists list_countries_links = [ [countryname1, link1], [countryname2, link2] ] to process it further.
        
        cur.execute("SELECT DISTINCT port_country FROM ports;")        
        for record in cur:
            list_countries.append(record[0])

        r = requests.get(base_url+'/ports')
        #with open('ports.html', mode='w') as fp:
        #    fp.write(r.text)
        #with open('ports.html', mode='r') as fp:
        #r = fp.read()
        page = BeautifulSoup(r.text, 'html.parser')
        #page = BeautifulSoup(r, 'html.parser')
        for link in page.findAll('a'):
            for country in list_countries:
                linkcontents=" ".join([str(a) for a in link.contents])
                if '<div class="country__name">' +country+ '</div>' in linkcontents:
                    list_countries_links.append([country, link.get('href')])
        #print(list_countries_links)


        # 2. Let's make a loop in list_countries_links.
        #   On each loop, we query ports list for this country from the database,
        #   we load a webpage which can contain data for some ports,
        #   we enumerate all <a> links on this page and check each one against ports we know for this country.
        #   For each port found, we store it in a list of lists
        #   list_countries_ports_links = [ [countryname1, port_code1, link1], [countryname2, port_code2, link2] ]
            
        for country_and_link in list_countries_links:
            list_ports = []
            cur.execute("SELECT DISTINCT port_code FROM ports WHERE port_country = %s;", [ country_and_link[0] ])
            for record in cur:
                list_ports.append(record[0])
            # print(list_ports)
            # print (base_url+country_and_link[1])
            r = requests.get(base_url+country_and_link[1]) # for ex, /countries/italy
            page = BeautifulSoup(r.text, 'html.parser')
            for link in page.findAll('a'):
                for port in list_ports:
                    linkcontents=" ".join([str(a) for a in link.contents])
                    if '(' +port+ ')' in linkcontents:
                        list_countries_ports_links.append([country_and_link[0], port, link.get('href')])
            # print (list_countries_ports_links) # example:
            # [['Belgium', 'BEANR', '/ports/antwerp-beanr'], ['Italy', 'ITAOI', '/ports/ancona-itaoi'], ['Jordan', 'JOAQJ', '/ports/aqaba-joaqj']]

        # 3. Let's make a loop in list_countries_ports_links
        #   On each loop, we query the webpage for its link,
        #   we extract data from the webpage,
        #   we check if data is the same. If data is the same, we just change the PROCESSED_DTTM.
        #   If data has changed, we change EFFECTIVE_TO_DTTM to NOW(), change PROCESSED_DTTM to NOW(),
        #   we insert a new record with changed data and
        #   EFFECTIVE_FROM_DTTM = NOW(), EFFECTIVE_TO_DTTM = 'infinity', PROCESSED_DTTM = NOW()
        # #            port_country VARCHAR(50),
            # port_code VARCHAR(50),
            # port_url VARCHAR(50),
            # major_towns TEXT,
            # shipping_lines TEXT,
            # import_reqs TEXT,
            # export_reqs TEXT,
            # port_html TEXT,
        
        for country_port_link in list_countries_ports_links:
            list_ports = []
            cur.execute("SELECT DISTINCT port_code FROM ports WHERE port_country = %s AND port_code = %s AND (%s is Not Null);", country_port_link)
            for record in cur:
                list_ports.append(record[0])
            # print(list_ports)
            # print (base_url+country_and_link[1])
            r = requests.get(base_url+country_port_link[2]) # for ex, /ports/antwerp-beanr
            page = BeautifulSoup(r.text, 'html.parser')
            for link in page.findAll('a'):
                for port in list_ports:
                    linkcontents=" ".join([str(a) for a in link.contents])
                    if '(' +port+ ')' in linkcontents:
                        list_countries_ports_links.append([country_and_link[0], port, link.get('href')])



except (Exception, psycopg2.DatabaseError) as error:
    print("exception connect db: " + str(error))
    exit(0)

finally:
    if conn is not None:
        conn.close()
