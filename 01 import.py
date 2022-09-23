import psycopg2
from datetime import datetime
line=" "
conn = None
filename="trades.csv"
count = 0
header=['date', 'hs_code', 'shipper_name', 'std_unit', 'std_quantity', 'value_fob_usd', 'items_number', 'source_port', 'source_country', 'destination_port', 'destination_country']
lenheader=len(header)
#country codes from https://www.iban.com/country-codes
Alpha2codes=['AF', 'AL', 'DZ', 'AS', 'AD', 'AO', 'AI', 'AQ', 'AG', 'AR', 'AM', 'AW', 'AU', 'AT', 'AZ', 'BS', 'BH', 'BD', 'BB', 'BY', 'BE', 'BZ', 'BJ', 'BM', 'BT', 'BO', 'BQ', 'BA', 'BW', 'BV', 'BR', 'IO', 'BN', 'BG', 'BF', 'BI', 'CV', 'KH', 'CM', 'CA', 'KY', 'CF', 'TD', 'CL', 'CN', 'CX', 'CC', 'CO', 'KM', 'CD', 'CG', 'CK', 'CR', 'HR', 'CU', 'CW', 'CY', 'CZ', 'CI', 'DK', 'DJ', 'DM', 'DO', 'EC', 'EG', 'SV', 'GQ', 'ER', 'EE', 'SZ', 'ET', 'FK', 'FO', 'FJ', 'FI', 'FR', 'GF', 'PF', 'TF', 'GA', 'GM', 'GE', 'DE', 'GH', 'GI', 'GR', 'GL', 'GD', 'GP', 'GU', 'GT', 'GG', 'GN', 'GW', 'GY', 'HT', 'HM', 'VA', 'HN', 'HK', 'HU', 'IS', 'IN', 'ID', 'IR', 'IQ', 'IE', 'IM', 'IL', 'IT', 'JM', 'JP', 'JE', 'JO', 'KZ', 'KE', 'KI', 'KP', 'KR', 'KW', 'KG', 'LA', 'LV', 'LB', 'LS', 'LR', 'LY', 'LI', 'LT', 'LU', 'MO', 'MG', 'MW', 'MY', 'MV', 'ML', 'MT', 'MH', 'MQ', 'MR', 'MU', 'YT', 'MX', 'FM', 'MD', 'MC', 'MN', 'ME', 'MS', 'MA', 'MZ', 'MM', 'NA', 'NR', 'NP', 'NL', 'NC', 'NZ', 'NI', 'NE', 'NG', 'NU', 'NF', 'MP', 'NO', 'OM', 'PK', 'PW', 'PS', 'PA', 'PG', 'PY', 'PE', 'PH', 'PN', 'PL', 'PT', 'PR', 'QA', 'MK', 'RO', 'RU', 'RW', 'RE', 'BL', 'SH', 'KN', 'LC', 'MF', 'PM', 'VC', 'WS', 'SM', 'ST', 'SA', 'SN', 'RS', 'SC', 'SL', 'SG', 'SX', 'SK', 'SI', 'SB', 'SO', 'ZA', 'GS', 'SS', 'ES', 'LK', 'SD', 'SR', 'SJ', 'SE', 'CH', 'SY', 'TW', 'TJ', 'TZ', 'TH', 'TL', 'TG', 'TK', 'TO', 'TT', 'TN', 'TR', 'TM', 'TC', 'TV', 'UG', 'UA', 'AE', 'GB', 'UM', 'US', 'UY', 'UZ', 'VU', 'VE', 'VN', 'VG', 'VI', 'WF', 'EH', 'YE', 'ZM', 'ZW', 'AX']
insertThis=False
#print("01", end="")

try:
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="mysecretpassword")

    # create a cursor
    with conn.cursor() as cur:
        cur.execute("""CREATE TABLE IF NOT EXISTS trades(
        imp_id SERIAL PRIMARY KEY,
        imp_date DATE,
        hs_code VARCHAR(8),
        shipper_name VARCHAR(50),
        std_unit VARCHAR(50),
        std_quantity NUMERIC,
        value_fob_usd NUMERIC,
        items_number VARCHAR(50),
        source_port VARCHAR(50),
        source_country VARCHAR(50),
        destination_port VARCHAR(50),
        destination_country VARCHAR(50),
        PROCESSED_DTTM timestamp
        );""")
        conn.commit()
        #print("02", end="")

        # filename is defined at the top
        with open(filename, mode='r', encoding='utf-8-sig') as fp:
            while line:
                # readline only consumes memory for 1 line or probably 1 storage device block,
                # the arbitrary Python binary makes choice by itself,
                # but readlineS method <=========== could waste all memory to store a large CSV file
                line = fp.readline()
                count+=1
                
                #print("03", end="")

                if line:
                    stripline=line.strip()
                    if 1==count:
                        if stripline != ';'.join(header): 
                            print('error: wrong header')
                            break
                    #if (count>1 and count < 10):
                    else:
                        linelist=stripline.split(';')
                        if lenheader != len(linelist):
                            print('error: wrong string num ' + str(count) )
                        
                        imp_date            = linelist[0]
                        hs_code             = linelist[1]
                        shipper_name        = linelist[2]
                        std_unit            = linelist[3]
                        std_quantity        = linelist[4]
                        value_fob_usd       = float(linelist[5].replace(',','.'))
                        linelist[5]=value_fob_usd
                        items_number        = linelist[6]
                        source_port         = linelist[7]
                        source_country      = linelist[8]
                        destination_port    = linelist[9]
                        destination_country = linelist[10]
                        
                        linelist.append(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                        
                        insertThis=True
                        
                        try:
                            try_date = datetime.strptime(imp_date, '%d/%m/%Y').date()
                            linelist[0] = try_date.strftime('%Y-%m-%d')
                        except Exception as err:
                                print("Exception while converting date: " + str(err) + ", ignore line " + str(count))
                                insertThis=False
                        
                        if not hs_code.startswith('870423'):
                            print('error: HS Code needs to start with `870423` but is `' + hs_code + '`, ignore line ' + str(count) )
                            insertThis=False

                        if not source_port[0:2] in Alpha2codes:
                            print('error: Source Port code needs to start with the country\'s ISO Alpha-2 code but is `'+ source_port +'`, ignore line ' + str(count) )
                            insertThis=False

                        #if not linelist[9][0:2] in Alpha2codes:
                        #    print('error: Dest Port code needs to start with the country\'s ISO Alpha-2 code but is `'+linelist[9]+'`, ignore line ' + str(count) )
                        
                        if not std_quantity.isnumeric():
                            print("q!")
                            if not (''==std_quantity and value_fob_usd<80000):
                                print('error: Quantity must be a numeric, where we can assume the number is 1 if the value is less than 80,000 USD when there\'s no information but Q is `'+ std_quantity +'` and V is `'+ str(value_fob_usd) +'`, ignore line ' + str(count) )
                                insertThis=False
                            else:
                                std_quantity=1
                                linelist[4]=std_quantity
                        
                        if insertThis:
                            #print("04", end="")
                            try:
                                #print("ins " + str(linelist), end="" )
                                cur.execute("""INSERT INTO trades ( 
                                    imp_date, hs_code, shipper_name, std_unit, std_quantity, 
                                    value_fob_usd, items_number, source_port, source_country, 
                                    destination_port, destination_country, PROCESSED_DTTM) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                                    linelist)
                            except Exception as err:
                                print("Exception while insert data: " + str(err))
                            #print("ok")

            # we commit(save) the records to the table
            conn.commit()

except (Exception, psycopg2.DatabaseError) as error:
    print("exception connect db: " + str(error))
    exit(0)

finally:
    if conn is not None:
        conn.close()
