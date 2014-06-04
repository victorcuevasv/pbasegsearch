package org.dataone.daks.pbasegsearch;

import java.util.Hashtable;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.StringTokenizer;

import org.dataone.daks.pbase.treecover.Digraph;
import org.json.*;

import com.gs.collections.api.block.function.Function;
import com.gs.collections.api.list.*;
import com.gs.collections.api.tuple.Pair;
import com.gs.collections.impl.list.mutable.FastList;


public class EvaluateObjectRank {
	
	
	private String DBNAME = null;
	private String INDEXDBNAME = null;

	
	SearchIndex searchIndex;
	TDBDAO dao;
	
	
	public EvaluateObjectRank(String rdfDBDirectory) {
		DBNAME = rdfDBDirectory;
		INDEXDBNAME = rdfDBDirectory + "indexdb";
		this.dao = TDBDAO.getInstance();
		this.dao.init(DBNAME);
		this.searchIndex = SearchIndex.getInstance();
		this.searchIndex.init(INDEXDBNAME);
	}
	
	
	public static void main(String[] args) {
		EvaluateObjectRank evaluator = new EvaluateObjectRank(args[0]);
		List<String> queryList = new ArrayList<String>();
		for( int i = 1; i < args.length; i++ )
			queryList.add(args[i]);
		String outStr = evaluator.processKeywordQuery(queryList, true, true, "d", false);
		//String outStr = evaluator.processObjectRankQuery(true, "d");
		System.out.println(outStr);
	}
	

	public String processObjectRankQuery(boolean onlyTable, String filterStr) {
		Digraph digraph = new Digraph();
		JSONObject jsonObj = new JSONObject();
		this.fillDigraphAndTopSortedJSONObj(digraph, jsonObj);
		Hashtable<String, Double> globalObjectRankHT = this.evaluateGlobalObjectRank(digraph, jsonObj);
		String retVal = null;
		if( !onlyTable ) {
			JSONObject graphObj = this.dao.getGraph();
			retVal = this.addObjectRankValues(graphObj, globalObjectRankHT);
		}
		else {
			MutableList<Pair<String, Double>> sortedPairs = this.createSortedPairs(globalObjectRankHT);
			retVal = this.createObjectRankTable(sortedPairs, filterStr);
		}
		return retVal;
	}
	
	
	public String processKeywordQuery(List<String> queryList, boolean andSemantics,
			boolean onlyTable, String filterStr, boolean useGlobalOR) {
		Digraph digraph = new Digraph();
		JSONObject jsonObj = new JSONObject();
		this.fillDigraphAndTopSortedJSONObj(digraph, jsonObj);
		Hashtable<String, Double> globalObjectRankHT = null;
		if( useGlobalOR )
			globalObjectRankHT = this.evaluateGlobalObjectRank(digraph, jsonObj);
		Hashtable<String, Double> objectRankHT = null;
		Hashtable<String, Double> tempObjectRankHT = null;
		for( int i = 0; i < queryList.size(); i++ ) {
			String word = queryList.get(i);
			if( i == 0 ) {
				objectRankHT = this.evaluateObjectRank(digraph, jsonObj, word);     
				//System.out.println("First objectRank values:");
				//this.printObjectRankHT(objectRankHT);
			}
			if( i > 0 ) {
				tempObjectRankHT = this.evaluateObjectRank(digraph, jsonObj, word);
				//System.out.println("Next objectRank values:");
				//this.printObjectRankHT(tempObjectRankHT);
				Set<String> htKeys = tempObjectRankHT.keySet();
				for( String key : htKeys ) {
					double val = objectRankHT.get(key);
					double tempVal = tempObjectRankHT.get(key);
					if( andSemantics )
						objectRankHT.put(key, val * tempVal);
					else
						objectRankHT.put(key, val + tempVal);
				}
				//System.out.println("Combined objectRank values:");
				//this.printObjectRankHT(objectRankHT);
			}
		}
		if( globalObjectRankHT != null ) {
			Set<String> htKeys = globalObjectRankHT.keySet();
			for( String key : htKeys ) {
				double orVal = objectRankHT.get(key);
				double gorVal = globalObjectRankHT.get(key);
				if( andSemantics )
					objectRankHT.put(key, orVal * gorVal);
				else
					objectRankHT.put(key, orVal + gorVal);
			}
		}
		String retVal = null;
		if( !onlyTable ) {
			JSONObject graphObj = this.dao.getGraph();
			retVal = this.addObjectRankValues(graphObj, objectRankHT);
		}
		else {
			MutableList<Pair<String, Double>> sortedPairs = this.createSortedPairs(objectRankHT);
			retVal = this.createObjectRankTable(sortedPairs, filterStr);
		}
		return retVal;
	}
	
	
	public String addObjectRankValues(JSONObject graphObj, Hashtable<String, Double> objectRankHT) {
		JSONObject wfObj = null;
		try {
			JSONArray nodesArray = graphObj.getJSONArray("nodes");
			for( int i = 0; i < nodesArray.length(); i++ ) {
				JSONObject nodeObj = nodesArray.getJSONObject(i);
				String nodeId = nodeObj.getString("nodeId");
				double objectRankVal = objectRankHT.get(nodeId);
				String objectRankStr = String.format("%.8f", objectRankVal);
				nodeObj.put("objectrank", objectRankStr);
			}
		}
		catch (JSONException e) {
			e.printStackTrace();
		}
		return wfObj.toString();
	}
	
	
	private MutableList<Pair<String, Double>> createSortedPairs(Hashtable<String, Double> objectRankHT) {
		MutableList<String> idsList = FastList.newList();
		MutableList<Double> scoresList = FastList.newList();
		Set<String> htKeys = objectRankHT.keySet();
		for( String key : htKeys ) {
			double score = objectRankHT.get(key);
			idsList.add(key);
			scoresList.add(score);
		}
		MutableList<Pair<String, Double>> pairs = idsList.zip(scoresList); 
		MutableList<Pair<String, Double>> sortedPairs = pairs.toSortedListBy(
				new Function<Pair<String, Double>, Double>() { 
					public Double valueOf(Pair<String, Double> pair) { 
						return pair.getTwo();
					} 
				} );
		return sortedPairs.reverseThis();
	}
	
	
	private String createObjectRankTable(MutableList<Pair<String, Double>> sortedPairs, String filterStr) {
		JSONObject tableObj = new JSONObject();
		JSONArray columnsArray = new JSONArray();
		JSONArray dataArray = new JSONArray();
		try {
			JSONObject idCol = new JSONObject();
			JSONObject scoreCol = new JSONObject();
			idCol.put("id", "string");
			scoreCol.put("score", "string");
			columnsArray.put(idCol);
			columnsArray.put(scoreCol);
			JSONArray rowArray = null;
			for( int i = 0; i < sortedPairs.size(); i++ ) {
				String idStr = sortedPairs.get(i).getOne();
				if( filterStr != null && !idStr.contains(filterStr) )
					continue;
				rowArray = new JSONArray();
				rowArray.put(idStr);
				double score = sortedPairs.get(i).getTwo();
				String scoreStr = String.format("%.8f", score);
				rowArray.put(scoreStr);
				dataArray.put(rowArray);
			}
			tableObj.put("columns", columnsArray);
			tableObj.put("data", dataArray);
		}
		catch (JSONException e) {
			e.printStackTrace();
		}
		return tableObj.toString();
	}
	
	
	//IMPORTANT: digraph and jsonObj are expected as just created objects and empty
	private void fillDigraphAndTopSortedJSONObj(Digraph digraph, JSONObject jsonObj) {
		String graphJSONStr = this.dao.getGraph().toString();
		JSONObject graphObj = null;
		try {
			graphObj = new JSONObject(graphJSONStr);
			JSONArray edgesArray = graphObj.getJSONArray("edges");
			JSONArray nodesArray = graphObj.getJSONArray("nodes");
			Hashtable<String, JSONObject> nodesHT = new Hashtable<String, JSONObject>();
			for( int i = 0; i < nodesArray.length(); i++ ) {
				JSONObject nodeObj = nodesArray.getJSONObject(i);
				nodesHT.put(nodeObj.getString("nodeId"), nodeObj);
			}
			Digraph topSortDigraph = new Digraph();
        	for( int i = 0; i < edgesArray.length(); i++ ) {
        		JSONObject edgeObj = edgesArray.getJSONObject(i);
        		digraph.addEdge(edgeObj.getString("startNodeId"), edgeObj.getString("endNodeId"));
        		topSortDigraph.addEdge(edgeObj.getString("startNodeId"), edgeObj.getString("endNodeId"));
        	}
        	JSONArray topSortedNodesArray = new JSONArray();
        	for( String nodeId : topSortDigraph.topSort() ) {
        		JSONObject nodeObj = nodesHT.get(nodeId);
        		topSortedNodesArray.put(nodeObj);
        	}
    		jsonObj.put("nodes", topSortedNodesArray);
    		jsonObj.put("edges", edgesArray);
		}
		catch (JSONException e) {
			e.printStackTrace();
		}
	}
	
	
	private Hashtable<String, Double> evaluateObjectRank(Digraph digraph, JSONObject jsonObj,
			String word) {
		double d = 0.2;
		double transferRate = 0.2;
		Hashtable<String, Double> objectRankHT = new Hashtable<String, Double>();
		String matchNodesStr = this.searchIndex.get(word);
		List<String> matchNodesList = new ArrayList<String>();
		if( matchNodesStr != null ) {
			StringTokenizer tokenizer = new StringTokenizer(matchNodesStr);
			while( tokenizer.hasMoreTokens() ) {
				String token = tokenizer.nextToken();
				matchNodesList.add(token);
				//System.out.println("Match node: " + token);
			}
		}
		JSONArray topSortedNodesArray = null;
		double base = 1.0/ digraph.nVertices();
		double val = 0.0;
		if( matchNodesList.size() > 0 )
			val = (1.0-d) / matchNodesList.size();
		//System.out.println("val: " + val);
		//System.out.println("base: " + base);
		try {
			topSortedNodesArray = jsonObj.getJSONArray("nodes");
			for( int i = 0; i < topSortedNodesArray.length(); i++ ) {
				JSONObject nodeObj = topSortedNodesArray.getJSONObject(i);
				String nodeId = nodeObj.getString("nodeId");
				if( matchNodesList.contains(nodeId) )
					objectRankHT.put(nodeId, d * base + val);
				else
					objectRankHT.put(nodeId, d * base);
			}
			for( int i = 0; i < topSortedNodesArray.length(); i++ ) {
				JSONObject nodeObj = topSortedNodesArray.getJSONObject(i);
				String nodeId = nodeObj.getString("nodeId");
				//System.out.println(nodeId);
				double nodeOR = objectRankHT.get(nodeId);
				List<String> adjList = digraph.getAdjList(nodeId);
				double alpha = 0.0;
				if( adjList.size() > 0 )
					alpha = transferRate / adjList.size();
				for( String adjNode : adjList ) {
					double adjNodeOR = objectRankHT.get(adjNode);
					objectRankHT.put(adjNode, adjNodeOR + nodeOR * alpha );
				}
			}
		}
		catch (JSONException e) {
			e.printStackTrace();
		}
		return objectRankHT;
	}
	
	
	private Hashtable<String, Double> evaluateGlobalObjectRank(Digraph digraph, JSONObject jsonObj) {
		double d = 0.2;
		double transferRate = 0.2;
		Hashtable<String, Double> objectRankHT = new Hashtable<String, Double>();
		JSONArray topSortedNodesArray = null;
		int nVertices = digraph.nVertices();
		double base = 1.0/ nVertices;
		double val = 0.0;
		if( nVertices > 0 )
			val = (1.0-d) / nVertices;
		try {
			topSortedNodesArray = jsonObj.getJSONArray("nodes");
			for( int i = 0; i < topSortedNodesArray.length(); i++ ) {
				JSONObject nodeObj = topSortedNodesArray.getJSONObject(i);
				String nodeId = nodeObj.getString("nodeId");
				if( nodeObj.has("rankFactor") ) {
					double rankFactor = nodeObj.getDouble("rankFactor");
					objectRankHT.put(nodeId, (d * base + val) * (1.0 + rankFactor) );
				}
				else
					objectRankHT.put(nodeId, d * base + val);
			}
			for( int i = 0; i < topSortedNodesArray.length(); i++ ) {
				JSONObject nodeObj = topSortedNodesArray.getJSONObject(i);
				String nodeId = nodeObj.getString("nodeId");
				//System.out.println(nodeId);
				double nodeOR = objectRankHT.get(nodeId);
				List<String> adjList = digraph.getAdjList(nodeId);
				double alpha = 0.0;
				if( adjList.size() > 0 )
					alpha = transferRate / adjList.size();
				for( String adjNode : adjList ) {
					double adjNodeOR = objectRankHT.get(adjNode);
					objectRankHT.put(adjNode, adjNodeOR + nodeOR * alpha );
				}
			}
		}
		catch (JSONException e) {
			e.printStackTrace();
		}
		return objectRankHT;
	}
	
	
	private void printObjectRankHT(Hashtable<String, Double> objectRankHT) {
		Set<String> keys = objectRankHT.keySet();
		for( String key : keys ) {
			double objectRankVal = objectRankHT.get(key);
			String objectRankStr = String.format("%.3f", objectRankVal);
			System.out.println("Node " + key + ":" + objectRankStr);
		}
	}
	
	
}






