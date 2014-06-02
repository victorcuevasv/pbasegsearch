package org.dataone.daks.pbasegsearch;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

public class DomainCatalog {

	
	List<String> genericNames;
	List<String> specificNames;
	Hashtable<String, Domain> genericHT;
	Hashtable<String, Domain> specificHT;
	private Random rand;
	
	
	public DomainCatalog(String genericFile, String specificFile) {
		this.rand = new Random();
		this.genericNames = new ArrayList<String>();
		this.specificNames = new ArrayList<String>();
		this.genericHT = new Hashtable<String, Domain>();
		this.specificHT = new Hashtable<String, Domain>();
		this.init(genericFile, true);
		this.init(specificFile, false);
	}
	
	
	public static void main(String args[]) {
		DomainCatalog catalog = new DomainCatalog(args[0], args[1]);
		System.out.println(catalog.toString());
	}
	
	
	private void init(String xmlFileName, boolean isGeneric) {
		Document document = this.getDocument(xmlFileName);
		Element root = document.getRootElement();
		for ( Iterator<Element> i = root.elementIterator("domain"); i.hasNext(); ) {
			Element domainElem = i.next();
			Domain domain = new Domain();
			String name = domain.init(domainElem);
			if( isGeneric ) {
				this.genericNames.add(name);
				this.genericHT.put(name, domain);
			}
			else {
				this.specificNames.add(name);
				this.specificHT.put(name, domain);
			}
		}
	}
	

	public String getRandomGenericTerm() {
		int randPosition = this.randInt(0, this.genericNames.size()-1);
		String genericName = this.genericNames.get(randPosition);
		Domain genericDomain = this.genericHT.get(genericName);
		return genericDomain.getRandomTerm();
	}
	
	
	public Domain getRandomGenericDomain() {
		int randPosition = this.randInt(0, this.genericNames.size()-1);
		String genericName = this.genericNames.get(randPosition);
		Domain genericDomain = this.genericHT.get(genericName);
		return genericDomain;
	}
	
	
	public Domain getRandomSpecificDomain() {
		int randPosition = this.randInt(0, this.specificNames.size()-1);
		String specificName = this.specificNames.get(randPosition);
		Domain specificDomain = this.specificHT.get(specificName);
		return specificDomain;
	}
	
	
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		String newLine = System.getProperty("line.separator");
		buffer.append("GENERIC DOMAINS: \n");
		for( String name: this.genericNames ) {
			Domain domain = this.genericHT.get(name);
			buffer.append(domain.toString());
			buffer.append(newLine);
		}
		buffer.append("SPECIFIC DOMAINS: \n");
		for( String name: this.specificNames ) {
			Domain domain = this.specificHT.get(name);
			buffer.append(domain.toString());
			buffer.append(newLine);
		}
		return buffer.toString();
	}
    
    
    private int randInt(int min, int max) {
	    int randomNum = this.rand.nextInt((max - min) + 1) + min;
	    return randomNum;
	}
    
    
    private Document getDocument(String xmlFileName) {
		Document document = null;
		SAXReader reader = new SAXReader();
		try {
			document = reader.read(xmlFileName);
		}
		catch (DocumentException e) {
			e.printStackTrace();
		}
		return document;
	}
	
	
}



