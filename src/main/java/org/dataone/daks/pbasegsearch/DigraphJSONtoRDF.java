
package org.dataone.daks.pbasegsearch;


import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;

import org.json.*;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.ontology.*;
import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.util.FileManager;



public class DigraphJSONtoRDF {
	
	
	private static final String SEARCHONT_NS = "http://purl.org/provone/searchontology#";
	private static final String DCTERMS_NS = "http://purl.org/dc/terms/";
	private static final String EXAMPLE_NS = "http://example.com/";
	private static final String WFMS_NS = "http://www.vistrails.org/registry.xsd#";
	private static final String SOURCE_URL = "http://purl.org/provone/searchontology";
	private static final String SOURCE_FILE = "./searchontology.owl";
	
	private static final String DBNAME = "gsearchgraph";
	
	private OntModel model;
	private HashMap<String, Individual> idToInd;
	private Random rand;
	
	
	public DigraphJSONtoRDF() {
		this.model = createOntModel();
		this.idToInd = new HashMap<String, Individual>();
		this.rand = new Random();
	}
	
	
	public static void main(String args[]) {
		if( args.length != 2 ) {
			System.out.println("Usage: java org.dataone.daks.pbasegsearch.DigraphJSONtoRDF" + 
							   "<json files folder> <graph ids file>");
			System.exit(0);
		}
		DigraphJSONtoRDF converter = new DigraphJSONtoRDF();
		List<String> graphNamesList = converter.createWordsList(args[1]);
		String folder = args[0];
		for(String graphName: graphNamesList) {
			String graphJSONStr = converter.readFile(folder + "/" + graphName + ".json");
			converter.createRDFGraphFromJSONString(graphJSONStr);
		}
		converter.saveModelAsXMLRDF();
		converter.createTDBDatabase(DBNAME);
	}

	
	private List<String> createWordsList(String filename) {
		String fileText = readFile(filename);
		List<String> wordsList = new ArrayList<String>();
		StringTokenizer tokenizer = new StringTokenizer(fileText);
		while( tokenizer.hasMoreTokens() ) {
			String token = tokenizer.nextToken();
			wordsList.add(token);
		}
		return wordsList;
	}
	
	
	private OntModel createOntModel() {
		OntModel m = ModelFactory.createOntologyModel( OntModelSpec.OWL_MEM );
		//[Un]comment to use file or URL
		FileManager.get().getLocationMapper().addAltEntry( SOURCE_URL, SOURCE_FILE );
		Model baseOntology = FileManager.get().loadModel( SOURCE_FILE );
		m.addSubModel( baseOntology );
		m.setNsPrefix( "searchont", SOURCE_URL + "#" );
		return m;
	}
	
	
	private String saveModelAsXMLRDF() {
		String tempXMLRDFFile = "tempdataxml.xml";
		String tempTTLRDFFile = "tempdatattl.ttl";
		try {
			FileOutputStream fos = new FileOutputStream(new File(tempXMLRDFFile));
			this.model.write(fos, "RDF/XML");
			FileOutputStream out = new FileOutputStream(new File(tempTTLRDFFile));
			Model model = RDFDataMgr.loadModel(tempXMLRDFFile);
	        RDFDataMgr.write(out, model, RDFFormat.TURTLE_PRETTY);
		}
		catch (IOException e) {
			e.printStackTrace();
	    }
		return tempXMLRDFFile;
	}
	
	
	private void createTDBDatabase(String dbname) {
		TDBDAO dao = TDBDAO.getInstance();
		dao.init(dbname);
		dao.addModel(this.model);
		dao.shutdown();
		System.out.println("Database created/updated with the name : " + dbname);
	}
	
	
	public void createRDFGraphFromJSONString(String jsonStr) {
		try {
			JSONObject graphObj = new JSONObject(jsonStr);
			JSONArray nodesArray = graphObj.getJSONArray("nodes");
			JSONArray edgesArray = graphObj.getJSONArray("edges");
			for( int i = 0; i < nodesArray.length(); i++ ) {
				JSONObject nodeObj = nodesArray.getJSONObject(i);
				String nodeId = nodeObj.getString("nodeId");
				String nodeIndId = this.createNodeEntity(nodeId);
			}
			for( int i = 0; i < edgesArray.length(); i++ ) {
				JSONObject edgeObj = edgesArray.getJSONObject(i);
				String source = edgeObj.getString("startNodeId");
				String dest = edgeObj.getString("endNodeId");
				String edgeLabel = edgeObj.getString("edgeLabel");
				this.createEdgeProperty(source, dest, edgeLabel);
			}
		}
		catch(JSONException e) {
			e.printStackTrace();
		}
	}
	
	
	private String createNodeEntity(String nodeId) {
		String classStr = null;
		if ( nodeId.startsWith("d") )
			classStr = "Data";
		else if ( nodeId.startsWith("wf") )
			classStr = "Workflow";
		else if ( nodeId.startsWith("auth") )
			classStr = "Author";
		else if ( nodeId.startsWith("pub") )
			classStr = "Publication";
		OntClass nodeClass = this.model.getOntClass( SEARCHONT_NS + classStr );
		Individual nodeInd = this.model.createIndividual( EXAMPLE_NS + nodeId, nodeClass );
		Property identifierP = this.model.createProperty(DCTERMS_NS + "identifier");
		nodeInd.addProperty(identifierP, nodeId, XSDDatatype.XSDstring);
		this.idToInd.put(nodeId, nodeInd);
		Property titleP = this.model.createProperty(DCTERMS_NS + "title");
		nodeInd.addProperty(titleP, nodeId, XSDDatatype.XSDstring);
		//Add the rankFactor attribute for nodes other than Data
		if( !nodeId.startsWith("d") ) {
			double rankFactor = this.randDouble(0, 1.0);
			Property rankFactorP = this.model.createProperty(WFMS_NS + "rankFactor");
			nodeInd.addProperty(rankFactorP, rankFactor + "", XSDDatatype.XSDdouble);
		}
		return nodeId;
	}
	
	
	private void createEdgeProperty(String startNodeIndId, String endNodeIndId, String edgeLabel) {
		Property edgeOP = this.model.createProperty(SEARCHONT_NS + edgeLabel);
		this.model.add(this.idToInd.get(startNodeIndId), edgeOP, this.idToInd.get(endNodeIndId));
	}
	
	
	private String readFile(String filename) {
		StringBuilder builder = null;
		try {
			BufferedReader reader = new BufferedReader(new FileReader(filename));
			String line = null;
			builder = new StringBuilder();
			String NEWLINE = System.getProperty("line.separator");
			while( (line = reader.readLine()) != null ) {
				builder.append(line + NEWLINE);
			}
			reader.close();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		return builder.toString();
	}
	
	
    private double randDouble(double min, double max) {
	    double randomNum = min + (max - min) * this.rand.nextDouble();
	    return randomNum;
	}
	
	
}


