package org.dataone.daks.pbasegsearch;

import java.util.List;

import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.tdb.TDBFactory;

import org.json.*;


public class TDBDAO {
	
	
	private Dataset ds;
	
	private String directory;
	
	private Model model;
	
	private static final TDBDAO instance = new TDBDAO();
	
	
	public TDBDAO() {

	}
	
	
	public static TDBDAO getInstance() {
    	return instance;
    }
	
	
	public synchronized void init(String directory) {
		if( this.ds == null || ( this.directory != null && ! this.directory.equals(directory) ) ) {
			if( this.ds != null )
				this.ds.close();
			this.directory = directory;
			Dataset ds = TDBFactory.createDataset(this.directory);
			this.ds = ds;
			this.directory = directory;
			this.model = this.ds.getDefaultModel();
		}
	}
	
	
	public synchronized void shutdown() {
		this.model.close();
		this.ds.close();
		this.ds = null;
		this.model = null;
	}
	
	
	public void addModel(Model m) {
		this.model.add(m);
	}
	
	
	Dataset getDataset() {
		return this.ds;
	}
	
	
	public JSONObject getGraph() {
		JSONObject graphObj = new JSONObject();
		JSONArray nodesArray = new JSONArray();
		JSONArray edgesArray = new JSONArray();
		try {
			//Get the nodes
			JSONArray dataNodesArray = this.getNodes("Data");
			for( int i = 0; i < dataNodesArray.length(); i++ )
				nodesArray.put(dataNodesArray.get(i));
			JSONArray workflowNodesArray = this.getNodes("Workflow");
			for( int i = 0; i < workflowNodesArray.length(); i++ )
				nodesArray.put(workflowNodesArray.get(i));
			JSONArray authorNodesArray = this.getNodes("Author");
			for( int i = 0; i < authorNodesArray.length(); i++ )
				nodesArray.put(authorNodesArray.get(i));
			JSONArray publicationNodesArray = this.getNodes("Publication");
			for( int i = 0; i < publicationNodesArray.length(); i++ )
				nodesArray.put(publicationNodesArray.get(i));
			//Get the edges
			JSONArray wasAttToInvWDEdgesArray = this.getEdges("wasAttToInv", "Workflow", "Data");
			for( int i = 0; i < wasAttToInvWDEdgesArray.length(); i++ )
				edgesArray.put(wasAttToInvWDEdgesArray.get(i));
			JSONArray wasAttToInvAWEdgesArray = this.getEdges("wasAttToInv", "Author", "Workflow");
			for( int i = 0; i < wasAttToInvAWEdgesArray.length(); i++ )
				edgesArray.put(wasAttToInvAWEdgesArray.get(i));
			JSONArray wasAttToInvAPEdgesArray = this.getEdges("wasAttToInv", "Author", "Publication");
			for( int i = 0; i < wasAttToInvAPEdgesArray.length(); i++ )
				edgesArray.put(wasAttToInvAPEdgesArray.get(i));
			JSONArray wasDfInvEdgesArray = this.getEdges("wasDfInv", "Data", "Data");
			for( int i = 0; i < wasDfInvEdgesArray.length(); i++ )
				edgesArray.put(wasDfInvEdgesArray.get(i));
			JSONArray usedEdgesArray = this.getEdges("used", "Publication", "Data");
			for( int i = 0; i < usedEdgesArray.length(); i++ )
				edgesArray.put(usedEdgesArray.get(i));
			//Add nodes and edges to the graph object
			graphObj.put("nodes", nodesArray);
			graphObj.put("edges", edgesArray);
		}
		catch (JSONException e) {
			e.printStackTrace();
		}
		return graphObj;
	}
	
	
	private JSONArray getNodes(String className) {
		String sparqlQueryString = "PREFIX searchont: <http://purl.org/provone/searchontology#> \n" +
        		"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n" +
        		"PREFIX dc: <http://purl.org/dc/terms/> \n" +
        		"PREFIX wfms: <http://www.vistrails.org/registry.xsd#> \n" +
        		"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> \n" +
        		"SELECT ?id ?title " + 
        		"WHERE {  ?data dc:identifier ?id . " +
        		"?data dc:title ?title . " +
        		"?data rdf:type searchont:" + className + " . }";
		JSONArray jsonArray = new JSONArray();
        Query query = QueryFactory.create(sparqlQueryString);
        QueryExecution qexec = QueryExecutionFactory.create(query, this.ds);
        ResultSet results = qexec.execSelect();
        for ( ; results.hasNext() ; ) {
            QuerySolution soln = results.nextSolution();
            JSONObject jsonObj = new JSONObject();
            String id = soln.getLiteral("id").getString();
            String title = soln.getLiteral("title").getString();
            try {
            	jsonObj.put("nodeId", id);
				jsonObj.put("title", title);
				jsonArray.put(jsonObj);
			}
            catch (JSONException e) {
				e.printStackTrace();
			}
        }
        qexec.close();
		return jsonArray;
	}
		
	
	private JSONArray getEdges(String objPropName, String sourceClassName, String destClassName) {
		String sparqlQueryString = "PREFIX searchont: <http://purl.org/provone/searchontology#> \n" +
        		"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n" +
        		"PREFIX dc: <http://purl.org/dc/terms/> \n" +
        		"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> \n" +
				"SELECT ?source_id ?dest_id WHERE {  " + 
        		"?source searchont:" + objPropName + " ?dest . " +
        		"?source rdf:type searchont:" + sourceClassName + " . " +
        		"?source dc:identifier ?source_id . " +
        		"?dest rdf:type searchont:" + destClassName + " . " +
        		"?dest dc:identifier ?dest_id . " +
        		"}";
		JSONArray edgesArray = new JSONArray();
        Query query = QueryFactory.create(sparqlQueryString);
        QueryExecution qexec = QueryExecutionFactory.create(query, this.ds);
        ResultSet results = qexec.execSelect();
        for ( ; results.hasNext() ; ) {
            QuerySolution soln = results.nextSolution();
            JSONObject jsonObj = new JSONObject();
            String sourceId = soln.getLiteral("source_id").getString();
            String destId = soln.getLiteral("dest_id").getString();
            try {
            	jsonObj.put("startNodeId", sourceId);
                jsonObj.put("endNodeId", destId);
				jsonObj.put("edgeLabel", objPropName);
			}
            catch (JSONException e) {
				e.printStackTrace();
			}
            edgesArray.put(jsonObj);
        }
        qexec.close();
		return edgesArray;
	}

	
	/**
	 * Execute a SPARQL query provided as a String.
	 * 
	 * @param query
	 * @return
	 */
	public String executeQuery(String sparqlQueryString) {
		Query query = QueryFactory.create(sparqlQueryString);
        QueryExecution qexec = QueryExecutionFactory.create(query, this.ds);
        ResultSet results = qexec.execSelect();
        Model model = results.getResourceModel();
        String retVal = null;
        try {
        	List<String> columns = results.getResultVars();
        	JSONObject jsonResult = new JSONObject();
			JSONArray columnsArray = new JSONArray();
			for(String s: columns) {
				JSONObject colVal = new JSONObject();
				colVal.put(s, "string");
				columnsArray.put(colVal);
			}
			JSONArray dataArray = new JSONArray();
			boolean first = true;
			int counter = 0;
        	for ( ; results.hasNext() ; ) {
        		QuerySolution soln = results.nextSolution();
        		JSONArray row = new JSONArray();
				for(String key: columns) {
					RDFNode rdfNode = soln.get(key);
					if ( rdfNode.isResource() ) {
						row.put(this.generateNodeJSON(soln.getResource(key), model));
						if( first )
							columnsArray.getJSONObject(counter).put(columns.get(counter), "node");
					}
					else
						row.put(soln.getLiteral(key).getString());
					counter++;
				}
				dataArray.put(row);
				first = false;
        	}
			jsonResult.put("columns", columnsArray);
			jsonResult.put("data", dataArray);
			retVal = jsonResult.toString(); 
		}
		catch(JSONException e) {
			e.printStackTrace();
		}
		finally {
			qexec.close();
		}
		return retVal;
	}	
	
	
	private JSONObject generateNodeJSON(Resource resource, Model model) {
		String DCTERMS_NS = "http://purl.org/dc/terms/";
		JSONObject nodeObj = new JSONObject();
		try {
			Property property = model.createProperty(DCTERMS_NS + "identifier");
			if( resource.hasProperty(property) )
				nodeObj.put("nodeId", resource.getProperty(property).getLiteral().getString());
		}
		catch(JSONException e) {
			e.printStackTrace();
		}
		return nodeObj;
	}
	
	
}




