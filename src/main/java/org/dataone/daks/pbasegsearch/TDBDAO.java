package org.dataone.daks.pbasegsearch;

import java.util.List;
import java.util.ArrayList;

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
	
	
    public JSONArray getProcesses(String wfID) {
    	String sparqlQueryString = "PREFIX provone: <http://purl.org/provone/ontology#> \n" +
        		"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n" +
        		"PREFIX dc: <http://purl.org/dc/terms/> \n" +
        		"PREFIX wfms: <http://www.vistrails.org/registry.xsd#> \n" +
        		"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> \n" +
        		"SELECT ?i ?t ?service ?avgtime ?avgcost ?avgreliability ?wfavgtime ?wfavgcost ?wfavgreliability " + 
        		"WHERE {  ?process dc:identifier ?i . " +
        		"?process dc:title ?t . " +
        		"OPTIONAL { ?process wfms:service ?service } . " +
        		"OPTIONAL { ?process wfms:avgtime ?avgtime } . " +
        		"OPTIONAL { ?process wfms:avgcost ?avgcost } . " +
        		"OPTIONAL { ?process wfms:avgreliability ?avgreliability } . " +
        		"OPTIONAL { ?process wfms:wfavgtime ?wfavgtime } . " +
        		"OPTIONAL { ?process wfms:wfavgcost ?wfavgcost } . " +
        		"OPTIONAL { ?process wfms:wfavgreliability ?wfavgreliability } . " +
        		"?process rdf:type provone:Process . " + 
        		"?wf rdf:type provone:Workflow . " +
        		"?wf dc:identifier " + "\"" + wfID + "\"^^xsd:string . " +
        		"?wf provone:hasSubProcess ?process . }";
        Query query = QueryFactory.create(sparqlQueryString);
        QueryExecution qexec = QueryExecutionFactory.create(query, this.ds);
        ResultSet results = qexec.execSelect();
        JSONArray nodesArray = new JSONArray();
        try {
        	for ( ; results.hasNext() ; ) {
        		JSONObject nodeObj = new JSONObject();
        		QuerySolution soln = results.nextSolution();
        		String id = soln.getLiteral("i").getString();
				nodeObj.put("nodeId", id);
				String title = soln.getLiteral("t").getString();
	            nodeObj.put("title", title);
	            Literal serviceLit = soln.getLiteral("service");
	            if( serviceLit != null )
	            	nodeObj.put("service", serviceLit.getString());
	            Literal avgtimeLit = soln.getLiteral("avgtime");
	            if( avgtimeLit != null ) {
	            	double avgtime = avgtimeLit.getDouble();
	            	String avgtimeStr = String.format("%.3f", avgtime);
	            	nodeObj.put("avgtime", avgtimeStr);
	            }
	            Literal avgcostLit = soln.getLiteral("avgcost");
	            if( avgcostLit != null ) {
	            	double avgcost = avgcostLit.getDouble();
	            	String avgcostStr = String.format("%.3f", avgcost);
	            	nodeObj.put("avgcost", avgcostStr);
	            }
	            Literal avgreliabilityLit = soln.getLiteral("avgreliability");
	            if( avgreliabilityLit != null ) {
	            	double avgreliability = avgreliabilityLit.getDouble();
	            	String avgreliabilityStr = String.format("%.3f", avgreliability);
	            	nodeObj.put("avgrebty", avgreliabilityStr);
	            }
	            Literal wfavgtimeLit = soln.getLiteral("wfavgtime");
	            if( wfavgtimeLit != null ) {
	            	double wfavgtime = wfavgtimeLit.getDouble();
	            	String wfavgtimeStr = String.format("%.3f", wfavgtime);
	            	nodeObj.put("wfavgtime", wfavgtimeStr);
	            }
	            Literal wfavgcostLit = soln.getLiteral("wfavgcost");
	            if( wfavgcostLit != null ) {
	            	double wfavgcost = wfavgcostLit.getDouble();
	            	String wfavgcostStr = String.format("%.3f", wfavgcost);
	            	nodeObj.put("wfavgcost", wfavgcostStr);
	            }
	            Literal wfavgreliabilityLit = soln.getLiteral("wfavgreliability");
	            if( wfavgreliabilityLit != null ) {
	            	double wfavgreliability = wfavgreliabilityLit.getDouble();
	            	String wfavgreliabilityStr = String.format("%.3f", wfavgreliability);
	            	nodeObj.put("wfavgrebty", wfavgreliabilityStr);
	            }
	            nodesArray.put(nodeObj);
			}
        }
        catch (JSONException e) {
			e.printStackTrace();
		}
        qexec.close();
        return nodesArray;
    }
	
	
	public List<JSONObject> getWasGenByEdges(String wfID, String runID) throws JSONException {
		String sparqlQueryString = "PREFIX provone: <http://purl.org/provone/ontology#> \n" +
        		"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n" +
        		"PREFIX dc: <http://purl.org/dc/terms/> \n" +
        		"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> \n" +
        		"PREFIX prov: <http://www.w3.org/ns/prov#> \n" +
				"SELECT ?data_id ?pexec_id WHERE {  " + 
        		"?wfpexec prov:wasAssociatedWith ?wf . " +
        		"?wf rdf:type provone:Workflow . " +
        		"?wf dc:identifier " + "\"" + wfID + "\"^^xsd:string . " +
        		"?wfpexec dc:identifier " + "\"" + runID + "\"^^xsd:string . " +
        		"?pexec provone:isPartOf ?wfpexec . " +
        		"?data rdf:type provone:Data . " +
        		"?data prov:wasGeneratedBy ?pexec . " +
        		"?data dc:identifier ?data_id . " +
        		"?pexec dc:identifier ?pexec_id . " +
        		"}";
		List<JSONObject> edgesList = new ArrayList<JSONObject>();
        Query query = QueryFactory.create(sparqlQueryString);
        QueryExecution qexec = QueryExecutionFactory.create(query, this.ds);
        ResultSet results = qexec.execSelect();
        for ( ; results.hasNext() ; ) {
            QuerySolution soln = results.nextSolution();
            JSONObject jsonObj = new JSONObject();
            String dataId = soln.getLiteral("data_id").getString();
            String pexecId = soln.getLiteral("pexec_id").getString();
            String label = "wasGenBy";
            jsonObj.put("startNodeId", dataId);
            jsonObj.put("endNodeId", pexecId);
            jsonObj.put("edgeLabel", label);
            edgesList.add(jsonObj);
        }
        qexec.close();
		return edgesList;
	}
	
	
	public List<JSONObject> getUsedEdges(String wfID, String runID) throws JSONException {
		String sparqlQueryString = "PREFIX provone: <http://purl.org/provone/ontology#> \n" +
        		"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n" +
        		"PREFIX dc: <http://purl.org/dc/terms/> \n" +
        		"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> \n" +
        		"PREFIX prov: <http://www.w3.org/ns/prov#> \n" +
				"SELECT ?data_id ?pexec_id WHERE {  " + 
        		"?wfpexec prov:wasAssociatedWith ?wf . " +
        		"?wf rdf:type provone:Workflow . " +
        		"?wf dc:identifier " + "\"" + wfID + "\"^^xsd:string . " +
        		"?wfpexec dc:identifier " + "\"" + runID + "\"^^xsd:string . " +
        		"?pexec provone:isPartOf ?wfpexec . " +
        		"?data rdf:type provone:Data . " +
        		"?pexec prov:used ?data . " +
        		"?data dc:identifier ?data_id . " +
        		"?pexec dc:identifier ?pexec_id . " +
        		"}";
		List<JSONObject> edgesList = new ArrayList<JSONObject>();
        Query query = QueryFactory.create(sparqlQueryString);
        QueryExecution qexec = QueryExecutionFactory.create(query, this.ds);
        ResultSet results = qexec.execSelect();
        for ( ; results.hasNext() ; ) {
            QuerySolution soln = results.nextSolution();
            JSONObject jsonObj = new JSONObject();
            String dataId = soln.getLiteral("data_id").getString();
            String pexecId = soln.getLiteral("pexec_id").getString();
            String label = "used";
            jsonObj.put("startNodeId", pexecId);
            jsonObj.put("endNodeId", dataId);
            jsonObj.put("edgeLabel", label);
            edgesList.add(jsonObj);
        }
        qexec.close();
		return edgesList;
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
	
	
	public JSONObject generateNodeJSON(Resource resource, Model model) {
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




