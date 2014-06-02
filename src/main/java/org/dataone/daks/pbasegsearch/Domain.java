package org.dataone.daks.pbasegsearch;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;

import org.dom4j.Element;


public class Domain {
	
	
	private List<String> termsList;
	private String name;
	private Random rand;
	
	
	public Domain() {
		this.rand = new Random();
	}
	
	
	public String init(Element domainElement) {
		this.termsList = new ArrayList<String>();
		Element elemName = domainElement.element("name");
		String name = elemName.getText();
		Element elemTerms = domainElement.element("terms");
		String termsStr = elemTerms.getText();
		StringTokenizer tokenizer = new StringTokenizer(termsStr);
		while( tokenizer.hasMoreTokens() ) {
			this.termsList.add(tokenizer.nextToken());
		}
		this.name = name;
		return name;
	}
	
	
	public String getName() {
		return this.name;
	}
	
	
	public String getRandomTerm() {
		int randPosition = this.randInt(0, this.termsList.size()-1);
		String term = this.termsList.get(randPosition);
		return term;
	}
	
	
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		String newLine = System.getProperty("line.separator");
		buffer.append("Name: " + this.name + newLine);
		for( int i = 0; i < this.termsList.size(); i++ ) {
			buffer.append(this.termsList.get(i) + " ");
			if( (i+1) % 5 == 0 )
				buffer.append(newLine);
		}
		return buffer.toString();
	}
	
	
    private int randInt(int min, int max) {
	    int randomNum = this.rand.nextInt((max - min) + 1) + min;
	    return randomNum;
	}
	

}





