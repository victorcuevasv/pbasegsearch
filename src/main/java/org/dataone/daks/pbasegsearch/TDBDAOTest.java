package org.dataone.daks.pbasegsearch;


import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.json.*;

import com.hp.hpl.jena.query.*;


public class TDBDAOTest {
	
	
    public static void main(String args[]) {
    	
        // Direct way: Make a TDB-back Jena model in the named directory.
        //String directory = "C:\\devp\\apache-tomcat-7.0.50\\bin\\" + args[0];
        String directory = "./" + args[0];
        TDBDAO dao = TDBDAO.getInstance();
        dao.init(directory);
        TDBDAOTest test = new TDBDAOTest();
        System.out.println("Executing LDBDAOTest with directory: " + directory);
        //test.triplesQuery(dao.getDataset());
        test.getGraphTest(dao);
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
    
    
    private void getGraphTest(TDBDAO dao) {
    	JSONObject graphObj = dao.getGraph();
    	String graphJSONStr = graphObj.toString();
    	System.out.println(graphJSONStr);
    	this.JSONtoDotFile(graphObj, false, "graphJSONStr.dot");
    }
    
    
    public void JSONtoDotFile(JSONObject graphObj, boolean btRankdir, String filename) {
        StringBuilder s = new StringBuilder();
        String NEWLINE = System.getProperty("line.separator");
        s.append("digraph dag {" + NEWLINE);
        if( btRankdir )
        	s.append("rankdir = BT;" + NEWLINE);
        try {
        	JSONArray edgesArray = graphObj.getJSONArray("edges");
        	for ( int i = 0; i < edgesArray.length(); i++ ) {
        		JSONObject edgeObj = edgesArray.getJSONObject(i);
        		String v1 = edgeObj.getString("startNodeId");
        		String v2 = edgeObj.getString("endNodeId");
        		s.append("\t" + v1 + " -> " + v2 + ";" + NEWLINE);
        	}
        	s.append("}");
			BufferedWriter writer = new BufferedWriter(new FileWriter(filename));
			writer.write(s.toString());
			writer.close();
		}
        catch (JSONException e) {
			e.printStackTrace();
		}
        catch (IOException e) {
			e.printStackTrace();
		}
    }
	
    
}



