# -*- coding: utf-8 -*-
"""
Created on Wed Dec 15 20:34:38 2021

@author: jtrelles
"""

import sqlite3

conn = sqlite3.connect('Airports.db')

import pandas as pd

Airports_df = pd.read_csv("airports.csv")
Carriers_df = pd.read_csv("carriers.csv")
Planes_df = pd.read_csv("plane-data.csv")

csv_file_list = []

for year in range(2000,2006):
    csv_file_list.append(pd.read_csv((str(year) + ".csv")))

Ontime_df = pd.concat(csv_file_list)
print(Ontime_df)

Airports_df.to_sql('Airports', con = conn, index = False)
Carriers_df.to_sql('Carriers', con = conn, index = False)
Planes_df.to_sql('Planes', con = conn, index = False)
Ontime_df.to_sql('Ontime', con = conn, index = False)

c= conn.cursor()

q1 = c.execute('''
             SELECT carriers.Description AS carrier, COUNT(*) AS total
FROM carriers JOIN ontime ON ontime.UniqueCarrier = carriers.Code
WHERE ontime.Cancelled = 1 AND carriers.Description IN ('United Air Lines Inc.', 'American Airlines Inc.', 'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')
GROUP BY carriers.Description
ORDER BY total DESC  
               ''').fetchall()

pd.DataFrame(q1)     

q2 = c.execute('''
             SELECT (SUM(o.cancelled)*100 / count(*)) AS ratio, c.Description as Carrier
                 FROM Ontime AS o Join Carriers AS c ON o.UniqueCarrier = c.code
                 WHERE c.Description IN ('United Air Lines Inc.', 'American Airlines Inc.', 'Pinnacle Airlines Inc.'
                 GROUP BY c.Description
                 ORDER BY ratio DESC  
               ''').fetchall()

pd.DataFrame(q2)     

q3 = c.execute('''
             SELECT airports.city AS city, COUNT(*) AS total
FROM airports JOIN ontime ON ontime.dest = airports.iata
WHERE ontime.Cancelled = 0
GROUP BY airports.city
ORDER BY total DESC 
               ''').fetchall()

pd.DataFrame(q3)     

q4 = c.execute('''
             SELECT model AS model, AVG(ontime.DepDelay) AS avg_delay
FROM planes JOIN ontime USING(tailnum)
WHERE ontime.Cancelled = 0 AND ontime.Diverted = 0 AND ontime.DepDelay > 0
GROUP BY model
ORDER BY avg_delay 
               ''').fetchall()

pd.DataFrame(q4)     

conn.close()
