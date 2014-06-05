package org.dataone.daks.pbasegsearch.services;


import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import org.dataone.daks.pbasegsearch.TDBDAO;
import org.dataone.daks.pbasegsearch.EvaluateObjectRank;

/** Example resource class hosted at the URI path "/orevalresource"
 */
@Path("/orevalresource")
public class OrEvalResource {
    
    /** Method processing HTTP GET requests, producing "text/plain" MIME media
     * type.
     * @return String that will be send back as a response of type "text/plain".
     */
    @GET 
    @Produces("text/plain")
    public String getIt(@QueryParam("dbname") String dbname, @QueryParam("keywords") String keywords, 
    		@QueryParam("andsemantics") String andsemantics, @QueryParam("onlytable") String onlytable, 
    		@QueryParam("filter") String filter, @QueryParam("useglobal") String useglobal) {
    	String retVal = null;
    	TDBDAO dao = TDBDAO.getInstance();
    	dao.init(dbname);
    	try {
    		EvaluateObjectRank evaluator = new EvaluateObjectRank(dbname);
    		List<String> keywordList = null;
    		if( keywords != null && keywords.trim().length() > 1 ) {
    			StringTokenizer tokenizer = new StringTokenizer(keywords);
    			keywordList = new ArrayList<String>();
    			while( tokenizer.hasMoreTokens() )
    				keywordList.add( tokenizer.nextToken() );
    		}
    		boolean andSemantics = false;
    		boolean onlyTable = true;
    		boolean useGlobal = false;
    		if( andsemantics != null && andsemantics.equalsIgnoreCase("true") )
    			andSemantics = true;
    		if( onlytable != null && onlytable.equalsIgnoreCase("false") )
    			onlyTable = false;
    		if( useglobal != null && useglobal.equalsIgnoreCase("true") )
    			useGlobal = true;
    		if( keywordList != null )
    			retVal = evaluator.processKeywordQuery(keywordList, andSemantics, onlyTable, filter, useGlobal);
    		else
    			retVal = evaluator.processObjectRankQuery(onlyTable, filter);
    	}
    	catch(Exception e) {
    		e.printStackTrace();
    	}
    	return retVal;
    }
    
    
}



