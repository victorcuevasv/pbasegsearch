

Generate the data


mvn exec:java -Dexec.mainClass="org.dataone.daks.pbasegsearch.DatasetGenerator" -Dexec.args="graphs.txt graphs computerdomains.xml sciencedomains.xml"


Create the index


mvn exec:java -Dexec.mainClass="org.dataone.daks.pbasegsearch.CreateIndexFromJSON" -Dexec.args="graphs graphs.txt computerdomains.xml sciencedomains.xml gsearchgraph"     



Test the index:


mvn exec:java -Dexec.mainClass="org.dataone.daks.pbasegsearch.SearchIndexTest" -Dexec.args="gsearchgraphindexdb"



Create the RDF database from the JSON files


mvn exec:java -Dexec.mainClass="org.dataone.daks.pbasegsearch.DigraphJSONtoRDF" -Dexec.args="wfs wfs.txt numtraces.txt"     


Test the RDF database


mvn exec:java -Dexec.mainClass="org.dataone.daks.pbaserdf.dao.LDBDAOTest" -Dexec.args="searchgraphs"






