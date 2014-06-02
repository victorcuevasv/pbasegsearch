package org.dataone.daks.pbasegsearch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;

import org.dataone.daks.pbase.treecover.Digraph;
import org.json.*;


public class DatasetGenerator {

	
	private Random rand;
	private int nAuth;
	private int nWf;
	private int nPub;
		
		
	public DatasetGenerator() {
		this.rand = new Random();
		this.nAuth = 1;
		this.nWf = 1;
		this.nPub = 1;
	}  
		
		
	public static void main(String args[]) {
		if( args.length != 2 ) {
			System.out.println("Usage: java org.dataone.daks.pbasegsearch.DatasetGenerator " + 
										"<graph names file> <graphs folder prefix>");   
			System.exit(0);
		}
		DatasetGenerator generator = new DatasetGenerator();
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
		Hashtable<String, JSONObject> nodesHT = new Hashtable<String, JSONObject>();
		JSONObject graphObj = this.generateGraphJSON(digraph, nodesHT);
		try {
			this.addBranches(digraph, graphObj, nodesHT, nGraph, true);
			this.addBranches(digraph, graphObj, nodesHT, nGraph, false);
		}
		catch (JSONException e) {
			e.printStackTrace();
		}
		this.saveJSONObjAsFile(graphObj, graphsFolderPrefix + "/" + graphName + ".json");
		digraph.toDotFile(graphsFolderPrefix + "/" + graphName + "Ext.dot", false);
	}
		
		
	private JSONObject generateGraphJSON(Digraph graph, Hashtable<String, JSONObject> nodesHT) {
		List<String> nodes = graph.getVertices();
		JSONArray nodesArray = new JSONArray();
		JSONArray edgesArray = new JSONArray();
		JSONObject graphObj = new JSONObject();
		try {
			for(String node1Id: nodes) {
				JSONObject nodeObj = new JSONObject();
				nodeObj.put("nodeId", node1Id);
				nodesArray.put(nodeObj);
				nodesHT.put(node1Id, nodeObj);
				List<String> adjList = graph.getAdjList(node1Id);
				for( String node2Id: adjList ) {
					JSONObject edgeObj = new JSONObject();
					edgeObj.put("startNodeId", node1Id);
					edgeObj.put("edgeLabel", "wasDfInv");
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
	
	
	private void addBranches(Digraph digraph, JSONObject graphObj, 
			Hashtable<String, JSONObject> nodesHT, int nGraph, boolean addWf) throws JSONException {
		double hasWfPubProb = 0.5;
		int minAuthors = 1;
		int maxAuthors = 2;
		if( !addWf ) {
			hasWfPubProb = 0.3;
			minAuthors = 1;
			maxAuthors = 4;
		}
		Set<String> nodes = nodesHT.keySet();
		JSONArray nodesArray = graphObj.getJSONArray("nodes");
		JSONArray edgesArray = graphObj.getJSONArray("edges");
		for( String nodeId : nodes ) {
			double dice = this.randDouble(0, 1.0);
			if( dice <= hasWfPubProb ) {
				String wfPubNodeId = null;
				String edgeLabel = null;
				if( addWf ) {
					wfPubNodeId = "wf" + nGraph + "_" + this.nWf;
					this.nWf++;
					edgeLabel = "wasAttToInv";
				}
				else {
					wfPubNodeId = "pub" + nGraph + "_" + this.nPub;
					this.nPub++;
					edgeLabel = "used";
				}
				JSONObject nodeObj = new JSONObject();
				nodeObj.put("nodeId", wfPubNodeId);
				nodesArray.put(nodeObj);
				JSONObject edgeObj = new JSONObject();
				edgeObj.put("startNodeId", wfPubNodeId);
				edgeObj.put("edgeLabel", edgeLabel);
				edgeObj.put("endNodeId", nodeId);
				edgesArray.put(edgeObj);
				digraph.addEdge(wfPubNodeId, nodeId);
				int diceInt = this.randInt(minAuthors, maxAuthors);
				for( int i = 1; i <= diceInt; i++ ) {
					JSONObject authObj = new JSONObject();
					String authNodeId = "auth" + nGraph + "_" + this.nAuth;
					this.nAuth++;
					authObj.put("nodeId", authNodeId);
					nodesArray.put(authObj);
					JSONObject authEdgeObj = new JSONObject();
					authEdgeObj.put("startNodeId", authNodeId);
					authEdgeObj.put("edgeLabel", "wasAttToInv");
					authEdgeObj.put("endNodeId", wfPubNodeId);
					edgesArray.put(authEdgeObj);
					digraph.addEdge(authNodeId, wfPubNodeId);
				}
			}
		}
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
				digraph.addEdge("d" + nGraph + "_" + node1, "d" + nGraph + "_" + node2);
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
	
	
    private double randDouble(double min, double max) {
	    double randomNum = min + (max - min) * this.rand.nextDouble();
	    return randomNum;
	}
	
	
}




