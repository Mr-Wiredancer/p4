/**
 * Implementation of a set-associative cache.
 * 
 * @author Mosharaf Chowdhury (http://www.mosharaf.com)
 * @author Prashanth Mohan (http://www.cs.berkeley.edu/~prmohan)
 * 
 * Copyright (c) 2012, University of California at Berkeley
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *  * Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of University of California, Berkeley nor the
 *    names of its contributors may be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 *    
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 *  ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 *  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 *  DISCLAIMED. IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY
 *  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 *  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 *  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 *  ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 *  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package edu.berkeley.cs162;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import javax.xml.parsers.*;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.*;

/**
 * A set-associate cache which has a fixed maximum number of sets (numSets).
 * Each set has a maximum number of elements (MAX_ELEMS_PER_SET).
 * If a set is full and another entry is added, an entry is dropped based on the eviction policy.
 */
public class KVCache implements KeyValueInterface, Debuggable {	
	private int numSets = 100;
	private int maxElemsPerSet = 10;
		
	private CacheSet[] sets;
	/**
	 * Creates a new LRU cache.
	 * @param cacheSize	the maximum number of entries that will be kept in this cache.
	 */
	public KVCache(int numSets, int maxElemsPerSet) {
		this.numSets = numSets;
		this.maxElemsPerSet = maxElemsPerSet;    
		this.sets = new CacheSet[numSets];
		
		for (int i = 0; i < numSets; i++){
			this.sets[i] = new CacheSet(maxElemsPerSet);
		}
		
	}

	/**
	 * Retrieves an entry from the cache.
	 * Assumes the corresponding set has already been locked for writing.
	 * @param key the key whose associated value is to be returned.
	 * @return the value associated to this key, or null if no value with this key exists in the cache.
	 */
	public String get(String key) {
		// Must be called before anything else
		AutoGrader.agCacheGetStarted(key);
		AutoGrader.agCacheGetDelay();
		
		DEBUG.debug("Cache receives a get request with key "+key);
		try {
			int setId = this.getSetId(key);
			String result = this.sets[setId].get(key);
			
			return result;
		} finally {
			AutoGrader.agCacheGetFinished(key);
		}
	}
	
	/**
	 * Called when a replacement happens
	 * @param key
	 * @param value
	 */
	public void replace(String key, String value){
		this.sets[getSetId(key)].replace(key, value);
	}
	
	/**
	 * Adds an entry to this cache.
	 * If an entry with the specified key already exists in the cache, it is replaced by the new entry.
	 * If the cache is full, an entry is removed from the cache based on the eviction policy
	 * Assumes the corresponding set has already been locked for writing.
	 * @param key	the key with which the specified value is to be associated.
	 * @param value	a value to be associated with the specified key.
	 */
	public void put(String key, String value) {
		// Must be called before anything else
		AutoGrader.agCachePutStarted(key, value);
		AutoGrader.agCachePutDelay();

		DEBUG.debug("Cache receives a put request with key "+key+" and value "+value);
		try {			
			this.sets[this.getSetId(key)].put(key, value);
		} finally {
			AutoGrader.agCachePutFinished(key, value);
		}
	}

	/**
	 * Removes an entry from this cache.
	 * Assumes the corresponding set has already been locked for writing.
	 * @param key	the key with which the specified value is to be associated.
	 * @throws KVException 
	 */
	public void del (String key) {
		// Must be called before anything else
		AutoGrader.agCacheGetStarted(key);
		AutoGrader.agCacheDelDelay();
		
		DEBUG.debug("Cache receives a del request with key "+key);
		try {			
			this.sets[this.getSetId(key)].del(key);
		} finally {
			AutoGrader.agCacheDelFinished(key);
		}
	}
	
	/**
	 * get the write lock of the corresponding set
	 * @param key
	 * @return	the write lock of the set that contains key.
	 */
	public WriteLock getWriteLock(String key) {
	  return this.sets[this.getSetId(key)].getWriteLock();
	}
	
	/**
	 * get the read lock of the corresponding set
	 * @param key
	 * @return the read lock of the set that contains key
	 */
	public ReadLock getReadLock(String key) {
		return this.sets[this.getSetId(key)].getReadLock();
	}
	
	/**
	 * 
	 * @param key
	 * @return	set of the key
	 */
	private int getSetId(String key) {
		return Math.abs(key.hashCode()) % numSets;
	}

	/**
	 * output the cache's content as XML
	 * @return XML representation of cache
	 */
  public String toXML() {
    DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder docBuilder = null;
		try {
			docBuilder = docFactory.newDocumentBuilder();
		} catch (ParserConfigurationException e) {
			DEBUG.debug("this should not happen");
			e.printStackTrace();
			return ""; // so that the rest doesn't break
		}
 
		// root element
		Document doc = docBuilder.newDocument();
		Element rootElement = doc.createElement("KVCache");
		doc.setXmlStandalone(true);
		doc.appendChild(rootElement);
		
		for (int i = 0; i<this.numSets; i++) {
			//add a set element
			CacheSet set = this.sets[i];
			
			Element setElement = doc.createElement("Set");
			setElement.setAttribute("Id", ""+i);
			
			//add the existent entries in a set
			for (int entryIndex = 0; entryIndex < set.entries.size(); entryIndex++) {
				CacheEntry e = set.entries.get(entryIndex);
				
				Element entryElement = doc.createElement("CacheEntry");
				boolean isReferenced = e.isReferred();
				entryElement.setAttribute("isReferenced", ""+isReferenced);
				entryElement.setAttribute("isValid", ""+true);
				
				Element keyElement = doc.createElement("Key");
				keyElement.appendChild(doc.createTextNode(e.getKey()));
				
				Element valueElement = doc.createElement("Value");
				valueElement.appendChild(doc.createTextNode(e.getValue()));
				
				entryElement.appendChild(keyElement);
				entryElement.appendChild(valueElement);
				
				setElement.appendChild(entryElement);
			}
			
			for (int j = set.entries.size(); j < this.maxElemsPerSet; j++){
				Element entryElement = doc.createElement("CacheEntry");
				entryElement.setAttribute("isReferenced", ""+false);
				entryElement.setAttribute("isValid", ""+false);
				
				Element keyElement = doc.createElement("Key");
				Element valueElement = doc.createElement("Value");

				entryElement.appendChild(keyElement);
				entryElement.appendChild(valueElement);
				
				setElement.appendChild(entryElement);
			}
			
			rootElement.appendChild(setElement);
		}
		
		TransformerFactory transformerFactory = TransformerFactory.newInstance();
		Transformer transformer = null;
		try {
			transformer = transformerFactory.newTransformer();
		} catch (TransformerConfigurationException e) {
			DEBUG.debug("this should not happen");
			e.printStackTrace();
			return ""; // so that the rest doesn't break
		}
		
		StringWriter writer = new StringWriter();
	
		DOMSource source= new DOMSource(doc);
		StreamResult result = new StreamResult(writer);
		
		try {
			transformer.transform(source, result);
		} catch (TransformerException e) {
			DEBUG.debug("this should not happen");
			e.printStackTrace();
			return ""; // so that the rest doesn't break
		}
		
		String xml = writer.toString();
		
    return xml;
  }
  
  /** CS162: Represents a single entry in the cache. */
  private class CacheEntry {
    private String value;
    private boolean isReferred = false;
    private String key;
    
    public CacheEntry(String key, String val) {
      this.value = val;
      this.key = key;
    }
    
    public void refer() {
      this.isReferred = true;
    }
    
    public boolean isReferred() {
      return this.isReferred;
    }
    
    public boolean shouldBeReplaced() {
      return !isReferred();
    }
  
    public String getKey() {
      return this.key;
    }
    
    public void miss() {
      this.isReferred = false;
    }
    
    public String getValue() {
      return this.value;
    }
    
    public void setValue(String val) {
      this.value = val;
    }
  }
  
  /** CS162: Represents a single set in the cache. Each set may contain multiple CacheEntries. */
  private class CacheSet{
    private ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private WriteLock writeLock;
    private ReadLock readLock;
    private final int MAX_NUM_ELEMENT;
    private LinkedList<CacheEntry> entries = new LinkedList<CacheEntry>();
    
    public CacheSet(int maxElementPerSet) {
      this.MAX_NUM_ELEMENT = maxElementPerSet;
      readLock = readWriteLock.readLock();
      writeLock = readWriteLock.writeLock();
    }
    
    public WriteLock getWriteLock() {
      return this.writeLock;
    }

    public ReadLock getReadLock() {
        return this.readLock;
      }
    
    /**
     * sequential search for the key. leave the caller to handle locking. 
     * @param key
     * @param value
     */
    public void put(String key, String value) {  
    	assert(writeLock.isHeldByCurrentThread());
	    for (int i = 0; i < entries.size(); i++) {
	      CacheEntry e = entries.get(i);
	      String k = e.getKey();
	      if (k.equals(key)){
	          e.setValue(value);
	          return;
	      }
	    }		
	    //the key is not in the cache. Needs replacement
	    this.replaceHelper(key, value);
    }
    
    /**
     * replace an old entry with <key, value>. Leave the caller to handle locking. 
     * @param key
     * @param value
     */
    public void replace(String key, String value) {
      this.replaceHelper(key, value);
    }
    
    /**
     * Linear search of the requested key.If they key is found, said the reference b. leave the caller to handle locking
     * @param key
     * @return value of the key; null if the key doesn't exist
     */
    public String get(String key) {
        for(int i = 0; i < entries.size(); i++) {
          CacheEntry e = entries.get(i);
          String k = e.getKey();
          if (k.equals(key)){
            if (!e.isReferred()){
              e.refer();
            }
            return e.getValue();
          }
        }
        return null;
    }
    
    /**
     * sequential serach for the key. If the key exists, remove the entry. leave the caller to handle locking
     * @param key
     */
    public void del(String key) {
    	assert(this.writeLock.isHeldByCurrentThread());
	    for (int i = 0; i < entries.size(); i++) {
	    	CacheEntry e = entries.get(i);
			String k = e.getKey();
			if (k.equals(key)){
			    entries.remove(i);
			    return;
			}
	    }
 
    }
    
    /**
     * Called when a replacement is needed. <key, value> is retreived from KVStore.
     * Assume writeLock is held by currentThread. And caller should handle the unlocking of writeLock
     * @param key 
     * @param value
     */
    private void replaceHelper(String key, String value) {    		
      assert(writeLock.isHeldByCurrentThread());
      
      //set is empty or not null yet
      if (entries.isEmpty() || (entries.size() < this.MAX_NUM_ELEMENT)) {
        entries.add(new CacheEntry(key, value));
        return;
      }
      
      while(true) {   								
        CacheEntry e = entries.removeFirst();

        //isRefeRred is false
        if (e.shouldBeReplaced()){
          entries.add(new CacheEntry(key, value));
          return;
        }
        
        //set the first element's isReferred to false and remove it the the end of the queue
        e.miss();
        entries.addLast(e);
      }
    }
  }
}
