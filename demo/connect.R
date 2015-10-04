library(RCassandraDriver)

connectWithCredentials("94.23.20.120", "analytics", "s5dxs4c9")
result <- executeQuery("SELECT * FROM icypricescrawler.products LIMIT 500")
disconnect()