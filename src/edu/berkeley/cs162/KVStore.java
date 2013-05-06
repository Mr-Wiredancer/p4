/**
 * Persistent Key-Value storage layer. Current implementation is transient, 
 * but assume to be backed on disk when you do your project.
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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.Hashtable;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;


/**
 * This is a dummy KeyValue Store. Ideally this would go to disk, 
 * or some other backing store. For this project, we simulate the disk like 
 * system using a manual delay.
 *
 */
public class KVStore implements KeyValueInterface, Debuggable {
	private Dictionary<String, String> store 	= null;
	
	public KVStore() {
		resetStore();
	}

	private void resetStore() {
		store = new Hashtable<String, String>();
	}
	
	/**
	 * Put the <key, value> pair in the store. Replace the old value if key is existent. 
	 */
	public synchronized void put(String key, String value) throws KVException {
		AutoGrader.agStorePutStarted(key, value);
		
		DEBUG.debug("Store receives a put request of key "+key+" and value "+value);
		try {
			putDelay();
			
			//sanity check on key and value
			CheckHelper.sanityCheckKeyValue(key, value);
			
			store.put(key, value);
		} finally {
			AutoGrader.agStorePutFinished(key, value);
		}
	}

	/**
	 * return the value of key if key exists; otherwise throw a kvexception
	 */
	public synchronized String get(String key) throws KVException {
		AutoGrader.agStoreGetStarted(key);
		
		DEBUG.debug("store receives a get request of key "+key);
		try {
			getDelay();
			
			//sanity check on key
			CheckHelper.sanityCheckKey(key);
			
			String retVal = this.store.get(key);
			if (retVal == null) {
				KVMessage msg = new KVMessage(KVMessage.RESPTYPE, "Does not exist");
				throw new KVException(msg);
			}
			return retVal;
		} finally {
			AutoGrader.agStoreGetFinished(key);
		}
	}
	
	/**
	 * Delete the value which is mapped to key. If the key does not exist, throw a KVException.
	 */
	public synchronized void del(String key) throws KVException {
		AutoGrader.agStoreDelStarted(key);

		DEBUG.debug("store receives a del request of key "+key);
		try {
			delDelay();
			
			//sanity check on key
			CheckHelper.sanityCheckKey(key);
			
			String val = this.store.remove(key);
			if (val==null)
				throw new KVException(new KVMessage(KVMessage.RESPTYPE, "Does not exist"));
		
		} finally {
			AutoGrader.agStoreDelFinished(key);
		}
	}
	
	private void getDelay() {
		AutoGrader.agStoreDelay();
	}
	
	private void putDelay() {
		AutoGrader.agStoreDelay();
	}
	
	private void delDelay() {
		AutoGrader.agStoreDelay();
	}
	
  public synchronized String toXML() throws KVException {
    return this.storeToXML();
  }        
  
  /**
   * helper method to output store as XML;
   * @return XML representation of store
   */
  private String storeToXML() {
    DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
    DocumentBuilder docBuilder = null;
    try {
      docBuilder = docFactory.newDocumentBuilder();
    } catch (ParserConfigurationException e) {
      //this should not happen
      DEBUG.debug("this should not happen");
      e.printStackTrace();
      return ""; //return so that the rest doesn't break 
    }

    // root element
    Document doc = docBuilder.newDocument();
    Element rootElement = doc.createElement("KVStore");
    doc.setXmlStandalone(true);
    doc.appendChild(rootElement);
    
    Enumeration<String> keys = this.store.keys();
    while (keys.hasMoreElements()) {
      String key = (String)keys.nextElement();
      String val = this.store.get(key);
      
      Element pairElement = doc.createElement("KVPair");
      
      Element keyElement = doc.createElement("Key");
      keyElement.appendChild(doc.createTextNode(key));
      
      Element valueElement = doc.createElement("Value");
      valueElement.appendChild(doc.createTextNode(val));
      
      pairElement.appendChild(keyElement);
      pairElement.appendChild(valueElement);

      rootElement.appendChild(pairElement);
    }
    
    TransformerFactory transformerFactory = TransformerFactory.newInstance();
    Transformer transformer = null;
    try {
      transformer = transformerFactory.newTransformer();
    } catch (TransformerConfigurationException e) {
      //this should not happen too
      DEBUG.debug("this should not happen");
      e.printStackTrace();
      return ""; //return so that the rest doesn't break 
    }
    
    StringWriter writer = new StringWriter();

    DOMSource  source= new DOMSource(doc);
    StreamResult result = new StreamResult(writer);
    
    try {
      transformer.transform(source, result);
    } catch (TransformerException e) {
      //this should not happen
      DEBUG.debug("this should not happen");
      e.printStackTrace();
      return ""; //return so that the rest doesn't break 
    }
    
    String xml = writer.toString();
    return xml;
  }

    /**
     * Dump the current state of store to corresponding file. This does not change the state of the store
     * @param fileName 
     */
    public synchronized void dumpToFile(String fileName) {
      try {
      PrintWriter out = new PrintWriter(fileName);
      out.print(this.storeToXML());
      out.close();
    } catch (FileNotFoundException e) {
      DEBUG.debug("file not found and couldnot create the file");
      e.printStackTrace();
    }
  }

  /**
   * STRICTLY checks the XML. The XML needs to have exactly the number of attributes and nodes and format as on the spec to pass the test
   * @param doc
   * @return return the new store dictionary
   * @throws KVException
   */
  private Dictionary<String, String> checkDocStruture(Document doc) throws KVException {
    NodeList nodes = doc.getChildNodes();
    
    if (nodes.getLength()!=1) {
      throw new KVException( new KVMessage (KVMessage.RESPTYPE, "IO Error"));
    }
    
    if (!doc.getFirstChild().getNodeName().equals("KVStore")) {
      throw new KVException( new KVMessage (KVMessage.RESPTYPE, "IO Error"));
    }
    
    return checkStoreNodeStructure(doc.getFirstChild());	
  }
  
  private Dictionary<String,String> checkStoreNodeStructure(Node storeNode) throws KVException {
    Dictionary<String, String> newStore = new Hashtable<String, String>();
    
    NodeList nodes = storeNode.getChildNodes();
    
    for (int i = 0; i < storeNode.getChildNodes().getLength(); i++){
      Node pair = nodes.item(i);
      if (!pair.getNodeName().equals("KVPair"))
        throw new KVException( new KVMessage (KVMessage.RESPTYPE, "IO Error"));
    
      checkPairNodeStructure(pair, newStore);
    }
    
    return newStore;
  }
  
  private void checkPairNodeStructure(Node pairNode, Dictionary<String, String> store) throws KVException {
    NodeList nodes = pairNode.getChildNodes();

    if (nodes.getLength()!=2){
      throw new KVException( new KVMessage (KVMessage.RESPTYPE, "IO Error"));
    }
    
    Node keyNode = nodes.item(0);
    if (!keyNode.getNodeName().equals("Key"))
      throw new KVException( new KVMessage (KVMessage.RESPTYPE, "IO Error"));
    String key = checkKeyNodeStructure(keyNode);
    
    Node valNode = nodes.item(1);
    if (!valNode.getNodeName().equals("Value"))
      throw new KVException( new KVMessage (KVMessage.RESPTYPE, "IO Error"));
    String value = checkValNodeStructure(valNode);
    
    store.put(key, value);
  }
  
  private String checkKeyNodeStructure(Node keyNode) throws KVException {
    NodeList nodes = keyNode.getChildNodes();
    
    if (nodes.getLength()!=1 || nodes.item(0).getNodeType() != Document.TEXT_NODE)
      throw new KVException( new KVMessage (KVMessage.RESPTYPE, "IO Error"));

    String key = nodes.item(0).getTextContent();
    
	CheckHelper.sanityCheckKey(key);
	
    return key;
  }
  
  private String checkValNodeStructure(Node valNode) throws KVException {
    NodeList nodes = valNode.getChildNodes();
  
    if (nodes.getLength()!=1
        || nodes.item(0).getNodeType()!=Document.TEXT_NODE)
      throw new KVException( new KVMessage (KVMessage.RESPTYPE, "IO Error"));

    String value = nodes.item(0).getTextContent();
    
	CheckHelper.sanityCheckValue(value);
	
    return value;
  }    
  
  /**
   * restore the state of the store to the one indicated by the file
   * @param fileName 
   * @throws KVException if file could not be opened or there is error when parsing the XML
   */
  public synchronized void restoreFromFile(String fileName) {
    //delete the original store whether the restore succeeds or not
    try {
      this.store = new Hashtable<String, String>();
      File fXmlFile = new File(fileName);
      DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
      DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
      Document doc = dBuilder.parse(fXmlFile);
     
      doc.getDocumentElement().normalize();
      
      Dictionary<String, String> newStore = checkDocStruture(doc);    		
      
      this.store = newStore;
    
    } catch (ParserConfigurationException e) {
      //this should not happen
      DEBUG.debug("this should not happen");
      e.printStackTrace();
    
    } catch (SAXException e) {
      //not a valid XML
      DEBUG.debug("this is not a valid xml");
      e.printStackTrace();
    
    } catch (IOException e) {
      //io error
      DEBUG.debug("io error");
      e.printStackTrace();
    
    } catch (KVException e) {
      DEBUG.debug(e.getMsg().getMessage());
      e.printStackTrace();
    }
  }
}
