package org.dataone.daks.pbasegsearch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;

import org.dataone.daks.pbase.treecover.Digraph;
import org.json.*;


public class DatasetGenerator {

	
	private Random rand;
	private DomainCatalog catalog;
		
		
	public DatasetGenerator(String genericFile, String specificFile) {
		this.rand = new Random();
		this.catalog = new DomainCatalog(genericFile, specificFile);
	}  
		
		
	public static void main(String args[]) {
		if( args.length != 4 ) {
			System.out.println("Usage: java org.dataone.daks.pbasegsearch.DatasetGenerator " + 
										"<graph names file> <graphs folder prefix> " +
										"<generic domains file> <specific domains file>");     
			System.exit(0);
		}
		DatasetGenerator generator = new DatasetGenerator(args[2], args[3]);
		generator.generateDataset(args[0], args[1]);
	}
		
		
	public void generateDataset(String graphNamesFile, String graphsFolderPrefix) {
		//Create a list with the graph names
		List<String> graphNames = readFileAsList(graphNamesFile);
		//Generate the expanded graphs
		for( int i = 0; i < graphNames.size(); i++ ) {
			String graphName = graphNames.get(i);
			this.generateExpandedGraph(graphName, graphsFolderPrefix, i+1);
		}
	}
		
		
	public void generateExpandedGraph(String graphName, String graphsFolderPrefix, int nGraph) {
		Digraph digraph = createDigraphFromDotFile(graphsFolderPrefix + "/" + graphName + ".dot", nGraph);
		JSONObject graphObj = this.generateGraphJSON(digraph);
		this.saveJSONObjAsFile(graphObj, graphsFolderPrefix + "/" + graphName + ".json");
			
	}
		
		
	private JSONObject generateGraphJSON(Digraph graph) {
		List<String> nodes = graph.getVertices();
		JSONArray nodesArray = new JSONArray();
		JSONArray edgesArray = new JSONArray();
		JSONObject graphObj = new JSONObject();
		try {
			for(String node1Id: nodes) {
				JSONObject nodeObj = new JSONObject();
				nodeObj.put("nodeId", node1Id);
				nodesArray.put(nodeObj);
				List<String> adjList = graph.getAdjList(node1Id);
				for( String node2Id: adjList ) {
					JSONObject edgeObj = new JSONObject();
					edgeObj.put("startNodeId", node1Id);
					edgeObj.put("edgeLabel", "");
					edgeObj.put("endNodeId", node2Id);
					edgesArray.put(edgeObj);
				}
			}
			graphObj.put("nodes", nodesArray);
			graphObj.put("edges", edgesArray);
		}
		catch (JSONException e) {
			e.printStackTrace();
		}
		return graphObj;
	}
		
		
	private List<String> readFileAsList(String filename) {
		BufferedReader reader = null;
		List<String> list = new ArrayList<String>();
		try {
			String line = null;
			reader = new BufferedReader(new FileReader(filename));
			while( (line = reader.readLine()) != null ) {
				if( line.trim().length() > 0 )
					list.add(line);
			}
			reader.close();
		}
		catch(IOException e) {
			e.printStackTrace();
		}
		return list;
	}
	
	
	private Digraph createDigraphFromDotFile(String filename, int nGraph) {
		BufferedReader reader = null;
		Digraph digraph = new Digraph();
		try {
			String line = null;
			reader = new BufferedReader(new FileReader(filename));
			//Skip line 'digraph dag {'
			line = reader.readLine();
			while( (line = reader.readLine()) != null ) {
				if( line.trim().length() <= 1 )
					continue;
				//Remove the final ';' in lines such as '0 -> 1;'
				line = line.substring(0, line.length()-1);
				StringTokenizer tokenizer = new StringTokenizer(line);
				String node1 = tokenizer.nextToken();
				String arrow = tokenizer.nextToken();
				String node2 = tokenizer.nextToken();
				//The graph is inverted to get the desired structure
				digraph.addEdge("d" + nGraph + "_" + node2, "d" + nGraph + "_" + node1);
			}
			reader.close();
		}
		catch(IOException e) {
			e.printStackTrace();
		}
		return digraph;
	}
		
		
	public void saveJSONObjAsFile(JSONObject jsonObj, String filename) {
		try {
			PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(filename)));
			String jsonStr = jsonObj.toString();
			writer.print(jsonStr);
			writer.close();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
		
		
	private int randInt(int min, int max) {
	    int randomNum = this.rand.nextInt((max - min) + 1) + min;
	    return randomNum;
	}
	
	
}




