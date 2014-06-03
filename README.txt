

Generate the data


mvn exec:java -Dexec.mainClass="org.dataone.daks.pbasegsearch.DatasetGenerator" -Dexec.args="graphs.txt graphs"


Create the index


mvn exec:java -Dexec.mainClass="org.dataone.daks.pbasegsearch.CreateIndexFromJSON" -Dexec.args="graphs graphs.txt computerdomains.xml sciencedomains.xml gsearchgraph"     



Test the index:


mvn exec:java -Dexec.mainClass="org.dataone.daks.pbasegsearch.SearchIndexTest" -Dexec.args="gsearchgraphindexdb"



Create the RDF database from the JSON files


mvn exec:java -Dexec.mainClass="org.dataone.daks.pbasegsearch.DigraphJSONtoRDF" -Dexec.args="graphs graphs.txt"     


Test the RDF database


mvn exec:java -Dexec.mainClass="org.dataone.daks.pbasegsearch.TDBDAOTest" -Dexec.args="gsearchgraph"






