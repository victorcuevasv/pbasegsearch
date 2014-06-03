package org.dataone.daks.pbasegsearch;

import org.json.JSONException;
import org.json.JSONObject;

import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.tdb.TDBFactory;


public class TDBDAOTest {
	
	
    public static void main(String args[]) {
    	
        // Direct way: Make a TDB-back Jena model in the named directory.
        //String directory = "C:\\devp\\apache-tomcat-7.0.50\\bin\\" + args[0];
        String directory = "./" + args[0];
        Dataset dataset = TDBFactory.createDataset(directory);
        TDBDAOTest test = new TDBDAOTest();
        System.out.println("Executing LDBDAOTest with directory: " + directory);
        test.triplesQuery(dataset);
    }
    
    
    private void triplesQuery(Dataset dataset) {
        String sparqlQueryString = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }";
        Query query = QueryFactory.create(sparqlQueryString);
        QueryExecution qexec = QueryExecutionFactory.create(query, dataset);
        ResultSet results = qexec.execSelect();
        ResultSetFormatter.out(results);
        qexec.close();
        dataset.close();
    }
	
    
}



