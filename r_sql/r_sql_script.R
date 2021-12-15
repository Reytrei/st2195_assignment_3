library(DBI)
library(data.table)
library(dplyr)

conn <- dbConnect(RSQLite::SQLite(), "airline.db")

Planes <- read.csv("plane-data.csv")
Carriers <- read.csv("carriers.csv")
Airports <- read.csv("airports.csv")
Ontime <- list()

dbWriteTable(conn, "Planes", Planes)
dbWriteTable(conn, "Carriers", Carriers)
dbWriteTable(conn, "Airports", Airports)


for (i in c(2000:2005)){
  Ontime[[i]] <- read.csv(paste0(i,'.csv'))
  
} 
Combined <- rbindlist(Ontime)
dbWriteTable(conn, "Ontime", Combined)


q1 <- dbGetQuery("SELECT carriers.Description AS carrier, COUNT(*) AS total
FROM carriers JOIN ontime ON ontime.UniqueCarrier = carriers.Code
WHERE ontime.Cancelled = 1 AND carriers.Description IN ('United Air Lines Inc.', 'American Airlines Inc.', 'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')
GROUP BY carriers.Description
ORDER BY total DESC")
q1


q2 <- dbGetQuery("SELECT q1.carrier AS carrier, (CAST(q1.numerator AS FLOAT)/ CAST(q2.denominator AS FLOAT)) AS ratio
FROM ( SELECT carriers.Description AS carrier, COUNT(*) AS numerator
FROM carriers JOIN ontime ON ontime.UniqueCarrier = carriers.Code
WHERE ontime.Cancelled = 1 AND carriers.Description IN ('United Air Lines Inc.', 'American Airlines Inc.', 'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')
GROUP BY carriers.Description
) AS q1 JOIN (SELECT carriers.Description AS carrier, COUNT(*) AS denominator
FROM carriers JOIN ontime ON ontime.UniqueCarrier = carriers.Code
WHERE carriers.Description IN ('United Air Lines Inc.', 'American Airlines Inc.', 'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')
GROUP BY carriers.Description
) AS q2 USING(carrier)
ORDER BY ratio DESC")
q2

q3 <- dbGetQuery("SELECT airports.city AS city, COUNT(*) AS total
FROM airports JOIN ontime ON ontime.dest = airports.iata
WHERE ontime.Cancelled = 0
GROUP BY airports.city
ORDER BY total DESC")
q3

q4 <- dbGetQuery("SELECT model AS model, AVG(ontime.DepDelay) AS avg_delay
FROM planes JOIN ontime USING(tailnum)
WHERE ontime.Cancelled = 0 AND ontime.Diverted = 0 AND ontime.DepDelay > 0
GROUP BY model
ORDER BY avg_delay")
q4

planes_db <- tbl(conn, "Planes")
airports_db <- tbl(conn,"Airports")
Carriers_db <- tbl(conn, "Carriers")
Ontime_db <- tbl(conn,"Combined")

q5 <- inner_join(Carriers_db,Ontime_db by = c("carriers.code" = "ontime.UniqueCarrier")) %>%
  filter(ontime.Cancelled == 1 & carriers.Description IN ('United Air Lines Inc.', 'American Airlines Inc.', 'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')) %>%
  select(carriers.Description, count(*) as total)
group_by(carriers.Description) %>%
  desc(arrange(total))

#simplified version of q2

q6 <- dbGetQuery("SELECT (SUM(o.cancelled)*100 / count(*)) AS ratio, c.Description as Carrier
                 FROM Ontime AS o Join Carriers AS c ON o.UniqueCarrier = c.code
                 WHERE c.Description IN ('United Air Lines Inc.', 'American Airlines Inc.', 'Pinnacle Airlines Inc.'
                 GROUP BY c.Description
                 ORDER BY ratio DESC"
                )

dbDisconnect(conn)