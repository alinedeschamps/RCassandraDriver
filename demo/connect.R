source("R/RCassandraDriver.R")
source("R/setColClasses.R")
dyn.load('src/x64/cassandra.dll')
dyn.load('src/x64/libuv.dll')
dyn.load('src/RCassandraDriver.dll')

connectWithCredentials("94.23.20.120", "analytics", "s5dxs4c9")
result <- executeQuery("SELECT * FROM icypricescrawler.products LIMIT 500")

disconnect()

dyn.unload('src/x64/cassandra.dll')
dyn.unload('src/x64/libuv.dll')
dyn.unload('src/RCassandraDriver.dll')