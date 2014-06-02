
package org.dataone.daks.pbasegsearch;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;

import org.json.*;


public class CreateIndexFromJSON {
	
	
	private static String INDEXDB_DIR = null;
	
	private DomainCatalog catalog;
	private SearchIndex searchIndex;
	private Random rand;
	
	
	public CreateIndexFromJSON(String genericFile, String specificFile, String rdfDBDirectory) {
		this.rand = new Random();
		this.catalog = new DomainCatalog(genericFile, specificFile);
		this.searchIndex = SearchIndex.getInstance();
		INDEXDB_DIR = rdfDBDirectory + "indexdb";
		this.searchIndex.init(INDEXDB_DIR);
	}
	
	
	public static void main(String args[]) {
		if( args.length != 5 ) {
			System.out.println("Usage: java org.dataone.daks.pbasegsearch.CreateIndexFromJSON " +
					"<json files folder> <graph names file> <generic apis file> <specific apis file> " +
					"<rdf db directory>");      
			System.exit(0);
		}
		CreateIndexFromJSON indexer = new CreateIndexFromJSON(args[2], args[3], args[4]);
		List<String> graphNamesList = indexer.readWordListFromFile(args[1]);
		String folder = args[0];
		for( String graphName: graphNamesList ) {
			String graphJSONStr = indexer.readFile(folder + "/" + graphName + ".json");
			indexer.processGraphJSONString(graphJSONStr);
		}
		indexer.closeSearchIndex();
	}

	
	private List<String> readWordListFromFile(String filename) {
		String wordsText = readFile(filename);
		List<String> wordsList = new ArrayList<String>();
		StringTokenizer tokenizer = new StringTokenizer(wordsText);
		while( tokenizer.hasMoreTokens() ) {
			String token = tokenizer.nextToken();
			wordsList.add(token);
		}
		return wordsList;
	}
	
	
	public void processGraphJSONString(String jsonStr) {
		try {
			JSONObject graphObj = new JSONObject(jsonStr);
			JSONArray nodesArray = graphObj.getJSONArray("nodes");
			for( int i = 0; i < nodesArray.length(); i++ ) {
				JSONObject nodeObj = nodesArray.getJSONObject(i);
				String nodeId = nodeObj.getString("nodeId");
				int minSpecificTerms = 3;
				int maxSpecificTerms = 5;
				int minGenericTerms = 2;
				int maxGenericTerms = 4;
				//Generate the terms for data items and workflows
				Domain specificDomain = this.catalog.getRandomSpecificDomain();
				if( nodeId.startsWith("d") || nodeId.startsWith("wf") ) {
					int nSpecificTerms = this.randInt(minSpecificTerms, maxSpecificTerms);
					int nGenericTerms = this.randInt(minGenericTerms, maxGenericTerms);
					for( int j = 1; j <= nSpecificTerms; j++ ) {
						String term = specificDomain.getRandomTerm();
						this.createIndexEntry(nodeId, term);
					}
					for( int j = 1; j <= nGenericTerms; j++ ) {
						String term = this.catalog.getRandomGenericTerm();
						this.createIndexEntry(nodeId, term);
					}
				}
				//Generate the terms for publications
				else if( nodeId.startsWith("pub") ) {
					int nSpecificTerms = this.randInt(minSpecificTerms, maxSpecificTerms);
					int additionalTerms = 2;
					for( int j = 1; j <= nSpecificTerms + additionalTerms; j++ ) {
						String term = specificDomain.getRandomTerm();
						this.createIndexEntry(nodeId, term);
					}
				}
			}
		}
		catch(JSONException e) {
			e.printStackTrace();
		}
	}
	
	
	private void createIndexEntry(String nodeId, String term) {
		String oldVal = this.searchIndex.get(term);
		String newVal = null;
		if( oldVal == null ) {
			newVal = nodeId + " ";
			this.searchIndex.put(term, newVal);
		}
		else {
			newVal = oldVal + nodeId + " ";
			this.searchIndex.replace(term, newVal);
		}
	}
	
	
	private void closeSearchIndex() {
		this.searchIndex.shutdown();
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
	
	
	private int randInt(int min, int max) {
	    int randomNum = this.rand.nextInt((max - min) + 1) + min;
	    return randomNum;
	}
	
	
}


