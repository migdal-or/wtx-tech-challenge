import psycopg2
conn = None
base_url = 'https://www.cogoport.com'
#import requests
#r = requests.get(base_url+'/ports')
#with open('ports.html', mode='w') as fp:
#    fp.write(r.text)

count = 0
links_parse = []
countries = []

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
        cur.execute("""select distinct country from (
                        SELECT source_country as country from trades 
                        UNION 
                        SELECT destination_country from trades
                        ) a;""")
        for record in cur:
            #print(record, type(record), record[0])
            countries.append(record[0])

        with open('ports.html', mode='r') as fp:
            r = fp.read()
            #page = BeautifulSoup(r.text, 'html.parser')
            page = BeautifulSoup(r, 'html.parser')
            for link in page.findAll('a'):
                count+=1
                #if count > 30 and count < 40:
                #print("")
                #print(link, link.get('href'))
                for country in countries:
                    #print("01 "+ country)
                    #print(link)
                    linkcontents=" ".join([str(a) for a in link.contents])
                    #print(" ".join([str(a) for a in link.contents])) #+" " + "02"
                    if '<div class="country__name">' +country+ '</div>' in linkcontents:
                        links_parse.append(link.get('href'))
                        print(country + " added")
            print(links_parse)

except (Exception, psycopg2.DatabaseError) as error:
    print("exception connect db: " + str(error))
    exit(0)

finally:
    if conn is not None:
        conn.close()
