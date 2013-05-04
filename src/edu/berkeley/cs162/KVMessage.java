/**
 * XML Parsing library for the key-value store
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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.net.Socket;
import java.net.SocketException;

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
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;


/**
 * This is the object that is used to generate messages the XML based messages 
 * for communication between clients and servers. 
 */
public class KVMessage implements Serializable, Debuggable {
	public static final String GETTYPE = "getreq";
	public static final String PUTTYPE = "putreq";
	public static final String DELTYPE = "delreq";
	public static final String RESPTYPE = "resp";
	public static final String READYTYPE = "ready";
	public static final String ABORTTYPE = "abort";
	public static final String COMMITTYPE = "commit";
	public static final String ACKTYPE = "ack";
	public static final String REGISTERTYPE = "register";
	public static final String IGNORENEXTTYPE = "ignoreNext";
  
	public static final int MAX_KEY_LENGTH = 256;
	public static final int MAX_VALUE_LENGTH = 256*1024;
	
	private static final long serialVersionUID = 6473128480951955693L;
	
	private String msgType = null;
	private String key = null;
	private String value = null;
	private String message = null;
    private String tpcOpId = null;    
	
	public final String getKey() {
		return key;
	}

	public final void setKey(String key) {
		this.key = key;
	}

	public final String getValue() {
		return value;
	}

	public final void setValue(String value) {
		this.value = value;
	}

	public final String getMessage() {
		return message;
	}

	public final void setMessage(String message) {
		this.message = message;
	}

	public String getMsgType() {
		return msgType;
	}
	
	public void setMsgType(String msgType) {
		this.msgType = msgType;
	}

	public String getTpcOpId() {
		return tpcOpId;
	}

	public void setTpcOpId(String tpcOpId) {
		this.tpcOpId = tpcOpId;
	}

	/* Solution from http://weblogs.java.net/blog/kohsuke/archive/2005/07/socket_xml_pitf.html */
	private class NoCloseInputStream extends FilterInputStream {
	    public NoCloseInputStream(InputStream in) {
	        super(in);
	    }
	    
	    public void close() {} // ignore close
	}
	
	/**
	 * See if the msgType is an acceptable type
	 * @param msgType
	 * @return true if the msgType is known; false otherwise
	 */
	private boolean isAcceptableMsgType(String msgType){
		return !msgType.equals(KVMessage.PUTTYPE) 
				&& !msgType.equals(KVMessage.GETTYPE)
				&& !msgType.equals(KVMessage.DELTYPE)
				&& !msgType.equals(KVMessage.RESPTYPE)
				&& !msgType.equals(KVMessage.READYTYPE)
				&& !msgType.equals(KVMessage.ABORTTYPE)
				&& !msgType.equals(KVMessage.COMMITTYPE)
				&& !msgType.equals(KVMessage.ACKTYPE)
				&& !msgType.equals(KVMessage.REGISTERTYPE)
				&& !msgType.equals(KVMessage.IGNORENEXTTYPE)
				;
	}
	
	/***
	 * 
	 * @param msgType
	 * @throws KVException of type "resp" with message "Message format incorrect" if msgType is unknown
	 */
	public KVMessage(String msgType) throws KVException {
		if ( this.isAcceptableMsgType(msgType) ){	
			DEBUG.debug("Could not recognize the msgType: "+msgType);
			throw new KVException( new KVMessage(KVMessage.RESPTYPE, "Message format incorrect"));
		}
		this.msgType = msgType;
	}
	
	public KVMessage(String msgType, String message) throws KVException {
		if ( !msgType.equals(KVMessage.RESPTYPE) && !msgType.equals(KVMessage.ABORTTYPE) && !msgType.equals(KVMessage.REGISTERTYPE) ){	
			DEBUG.debug("Only a resp/register/abort(voting) type can have a message field");
			throw new KVException(new KVMessage(KVMessage.RESPTYPE, "Message format incorrect"));
		}
		this.message = message;
		this.msgType = msgType;	
	}
	
	 /***
     * Parse KVMessage from incoming network connection
     * @param sock
     * @throws KVException if there is an error in parsing the message. The exception should be of type "resp and message should be :
     * a. "XML Error: Received unparseable message" - if the received message is not valid XML.
     * b. "Network Error: Could not receive data" - if there is a network error causing an incomplete parsing of the message.
     * c. "Message format incorrect" - if there message does not conform to the required specifications. Examples include incorrect message type. 
     */
	public KVMessage(InputStream input) throws KVException {
		this.msgHelper(input);
	}
	
	//initiate the KVMessage from a InputStream object
	private void msgHelper(InputStream input) throws KVException{
	  try {
	        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
	        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
	        Document doc = dBuilder.parse(input);
	        
	        doc.getDocumentElement().normalize();
	        
	        //check doc structure
	        checkDocStructure(doc);
	        
	        Node message = doc.getFirstChild();
	        
	        String type = checkKVMessageStructure(message);
	        this.msgType = type;
	        
	        NodeList nodes = message.getChildNodes();
	        //put or 2pc put
	        if (type.equals(KVMessage.PUTTYPE)) {
	          checkKeyNode(nodes.item(0));
	          checkValNode(nodes.item(1));
	          if (nodes.item(2)!=null){
	        	  checkTPCOpIdNode(nodes.item(2));
	          }
	        } else if(type.equals(KVMessage.GETTYPE)) {
	          checkKeyNode(nodes.item(0));
	        
	          //del or 2pc del
	        } else if(type.equals(KVMessage.DELTYPE)) {
	          checkKeyNode(nodes.item(0));
	          if (nodes.item(1)!=null){
	        	  this.checkTPCOpIdNode(nodes.item(1));
	          }
	         
	        } else if (type.equals(KVMessage.RESPTYPE)){
	          if (nodes.getLength() == 1) {
	            checkMessageNode(nodes.item(0));
	          } else if(nodes.getLength() == 2) {
	            checkKeyNode(nodes.item(0));
	            checkValNode(nodes.item(1));
	          } else {
	            throw new KVException(new KVMessage(KVMessage.RESPTYPE, "Message format incorrect"));
	          }
	        } else if(type.equals(KVMessage.REGISTERTYPE)){
	        	checkMessageNode(nodes.item(0));
	        } else if(type.equals(KVMessage.IGNORENEXTTYPE)){
	        	if  (nodes.getLength()!=0){
		            throw new KVException(new KVMessage(KVMessage.RESPTYPE, "Message format incorrect"));
	        	}
	        } else if(type.equals(KVMessage.ABORTTYPE)){
	        	//decision
	        	if (nodes.getLength()==1) {
	        		checkTPCOpIdNode(nodes.item(0));
	        	
	        	//vote
	        	}else if(nodes.getLength()==2) {
	        		checkMessageNode(nodes.item(0));
	        		checkTPCOpIdNode(nodes.item(1));
	        	}else{
		            throw new KVException(new KVMessage(KVMessage.RESPTYPE, "Message format incorrect"));
	        	}
	        } else if (type.equals(KVMessage.ACKTYPE)){
        		checkTPCOpIdNode(nodes.item(0));
	        } else if(type.equals(KVMessage.READYTYPE)){	        	
        		checkTPCOpIdNode(nodes.item(0));
	        }else if(type.equals(KVMessage.COMMITTYPE)){
        		checkTPCOpIdNode(nodes.item(0));
	        }
  		} catch (ParserConfigurationException e) {
  			//this should not happen
  			DEBUG.debug("This should not happen");
  			e.printStackTrace();
  			throw new KVException( new KVMessage(KVMessage.RESPTYPE, "Unknown Error: this should not happen") );
  		} catch (SAXException e) {
  			//not a valid XML
  			DEBUG.debug("Invalid XML");
  			e.printStackTrace();
  			throw new KVException( new KVMessage(KVMessage.RESPTYPE, "XML Error: Received unparseable message") );
  		
  		//IOException handles SocketTimeoutException
  		} catch (IOException e) {
  			//io error
  			DEBUG.debug("Could not receive data");
  			e.printStackTrace();
  			throw new KVException( new KVMessage(KVMessage.RESPTYPE, "Network Error: Could not receive data") );
  		} 
	}
	
	/**
	 * 
	 * @param sock Socket to receive from
	 * @throws KVException if there is an error in parsing the message. The exception should be of type "resp and message should be :
	 * a. "XML Error: Received unparseable message" - if the received message is not valid XML.
	 * b. "Network Error: Could not receive data" - if there is a network error causing an incomplete parsing of the message.
	 * c. "Message format incorrect" - if there message does not conform to the required specifications. Examples include incorrect message type. 
	 */
	public KVMessage(Socket sock) throws KVException {
		try {
		    InputStream in = sock.getInputStream();
		    this.msgHelper(new KVMessage.NoCloseInputStream(in));
		} catch (IOException e) {
  			throw new KVException( new KVMessage(KVMessage.RESPTYPE, "Network Error: Could not receive data") );
		}
	}

	/**
	 * 
	 * @param sock Socket to receive from
	 * @param timeout Give up after timeout milliseconds
	 * @throws KVException if there is an error in parsing the message. The exception should be of type "resp and message should be :
	 * a. "XML Error: Received unparseable message" - if the received message is not valid XML.
	 * b. "Network Error: Could not receive data" - if there is a network error causing an incomplete parsing of the message.
	 * c. "Message format incorrect" - if there message does not conform to the required specifications. Examples include incorrect message type. 
	 */
	public KVMessage(Socket sock, int timeout) throws KVException {
		try {
			sock.setSoTimeout(timeout);
		    InputStream in = sock.getInputStream();
			this.msgHelper(in);
		} catch (SocketException e) {
			e.printStackTrace();
		} catch (IOException e) {
  			throw new KVException( new KVMessage(KVMessage.RESPTYPE, "Network Error: Could not receive data") );
		}
	}
	
	/**
	 * Check messageNode structure( no attribute and a child of Document.TEXT_NODE). Will set this.message if the node is valid.
	 * @param keyNode
	 * @throws KVException
	 */
	private void checkMessageNode(Node messageNode) throws KVException {
		if (messageNode.getAttributes().getLength()!=0)
			throw new KVException( new KVMessage(KVMessage.RESPTYPE, "Message format incorrect") );

		NodeList nodes = messageNode.getChildNodes();
		if (nodes.getLength()!=1 || nodes.item(0).getNodeType()!=Document.TEXT_NODE)
			throw new KVException( new KVMessage(KVMessage.RESPTYPE, "Message format incorrect") );

		String message = messageNode.getFirstChild().getTextContent();
		this.message = message;
	}
	
	/**
	 * Check keynode structure( no attribute and a child of Document.TEXT_NODE). Will set this.key if the node is valid.
	 * @param keyNode
	 * @throws KVException
	 */
	private void checkKeyNode(Node keyNode) throws KVException {
		if (keyNode.getAttributes().getLength()!=0)
			throw new KVException( new KVMessage(KVMessage.RESPTYPE, "Message format incorrect") );

		NodeList nodes = keyNode.getChildNodes();
		if ( nodes.getLength()!=1 || nodes.item(0).getNodeType()!=Document.TEXT_NODE)
			throw new KVException( new KVMessage(KVMessage.RESPTYPE, "Message format incorrect") );

		String key = keyNode.getFirstChild().getTextContent();
		
		CheckHelper.sanityCheckKey(key);
		
		this.key = key;
	}
	
	/**
	 * Check TPCOpId Node structure( no attribute and a child of Document.TEXT_NODE). Will set this.value if the node is valid.
	 * @param keyNode
	 * @throws KVException
	 */
	private void checkTPCOpIdNode(Node tNode) throws KVException {
		if (tNode.getAttributes().getLength()!=0)
			throw new KVException( new KVMessage(KVMessage.RESPTYPE, "Message format incorrect") );

		NodeList nodes = tNode.getChildNodes();
		if (nodes.getLength()!=1 || nodes.item(0).getNodeType()!=Document.TEXT_NODE)
			throw new KVException( new KVMessage(KVMessage.RESPTYPE, "Message format incorrect") );

		String tpcOpId = tNode.getFirstChild().getTextContent();
		
		this.tpcOpId = tpcOpId;
	}
	
	/**
	 * Check valNode structure( no attribute and a child of Document.TEXT_NODE). Will set this.value if the node is valid.
	 * @param keyNode
	 * @throws KVException
	 */
	private void checkValNode(Node valNode) throws KVException {
		if (valNode.getAttributes().getLength()!=0)
			throw new KVException( new KVMessage(KVMessage.RESPTYPE, "Message format incorrect") );

		NodeList nodes = valNode.getChildNodes();
		if (nodes.getLength()!=1 || nodes.item(0).getNodeType()!=Document.TEXT_NODE)
			throw new KVException( new KVMessage(KVMessage.RESPTYPE, "Message format incorrect") );

		String val = valNode.getFirstChild().getTextContent();
		
		CheckHelper.sanityCheckValue(val);

		this.value = val;
	}
	
	/**
	 * Check attributes of KVMessage( should only have one attribute type, which is one of the 4 types),
	 * and check structure of KVMessage according to type (number and names of children).
	 * @param messageNode
	 * @return type of the KVmessage
	 * @throws KVException
	 */
	private String checkKVMessageStructure(Node messageNode) throws KVException {
		NamedNodeMap attrs = messageNode.getAttributes();
		
		if (attrs.getLength()!=1)
			throw new KVException(new KVMessage(KVMessage.RESPTYPE, "Message format incorrect") );

		Node attr = attrs.item(0);
		String attrName = attr.getNodeName();
		String attrValue = attr.getNodeValue();
		if (!attrName.equals("type"))
			throw new KVException(new KVMessage(KVMessage.RESPTYPE, "Message format incorrect") );
				
		if (attrValue.equals(KVMessage.PUTTYPE)) {
			checkPutTypeMessage(messageNode);
		} else if (attrValue.equals(KVMessage.GETTYPE)) {
			checkGetTypeMessage(messageNode);
		} else if(attrValue.equals(KVMessage.DELTYPE)) {
			checkDelTypeMessage(messageNode);
		} else if(attrValue.equals(KVMessage.RESPTYPE)) {
			checkRespTypeMessage(messageNode);
		} else if(attrValue.equals(KVMessage.READYTYPE)) {
			checkReadyTypeMessage(messageNode);
		} else if(attrValue.equals(KVMessage.ABORTTYPE)) {
			checkAbortTypeMessage(messageNode);		
		} else if(attrValue.equals(KVMessage.COMMITTYPE)) {
			checkCommitTypeMessage(messageNode);
		} else if(attrValue.equals(KVMessage.ACKTYPE)) {
			checkAckTypeMessage(messageNode);
		} else if(attrValue.equals(KVMessage.REGISTERTYPE)) {
			checkRegisterTypeMessage(messageNode);
		} else if(attrValue.equals(KVMessage.IGNORENEXTTYPE)) {
			checkIgnoreNextTypeMessage(messageNode);
		} else {
			throw new KVException( new KVMessage(KVMessage.RESPTYPE, "Message format incorrect") );
		}
		return attrValue;
	}
	
	/**
	 * check if messageNode is a valid ignoreNext message.
	 * @param messageNode
	 * @throws KVException
	 */
	private void checkIgnoreNextTypeMessage(Node messageNode) throws KVException {
		NodeList nodes = messageNode.getChildNodes();
		int l = nodes.getLength();	
		
		if (l!=0)
			throw new KVException( new KVMessage(KVMessage.RESPTYPE, "Message format incorrect") );	
	}
	
	/**
	 * check if messageNode is a valid register message.
	 * @param messageNode
	 * @throws KVException
	 */
	private void checkRegisterTypeMessage(Node messageNode) throws KVException {
		NodeList nodes = messageNode.getChildNodes();
		int l = nodes.getLength();	
		
		if (l!=1)
			throw new KVException( new KVMessage(KVMessage.RESPTYPE, "Message format incorrect") );

		Node message = nodes.item(0);

		if (!message.getNodeName().equals("Message"))
			throw new KVException( new KVMessage(KVMessage.RESPTYPE, "Message format incorrect") );
	}
	
	/**
	 * check if messageNode is a valid ack(registration, 2pc or ignorenext) message.
	 * @param messageNode
	 * @throws KVException
	 */
	private void checkAckTypeMessage(Node messageNode) throws KVException {
		NodeList nodes = messageNode.getChildNodes();
		int l = nodes.getLength();	
		
		if (l!=1)
			throw new KVException( new KVMessage(KVMessage.RESPTYPE, "Message format incorrect") );

		Node n = nodes.item(0);

		if (!n.getNodeName().equals("TPCOpId") && !n.getNodeName().equals("message"))
			throw new KVException( new KVMessage(KVMessage.RESPTYPE, "Message format incorrect") );
	}
	
	/**
	 * check if messageNode is a valid ready message.
	 * @param messageNode
	 * @throws KVException
	 */
	private void checkReadyTypeMessage(Node messageNode) throws KVException {
		NodeList nodes = messageNode.getChildNodes();
		int l = nodes.getLength();	
		
		if (l!=1)
			throw new KVException( new KVMessage(KVMessage.RESPTYPE, "Message format incorrect") );

		Node tpcOpId = nodes.item(0);

		if (!tpcOpId.getNodeName().equals("TPCOpId"))
			throw new KVException( new KVMessage(KVMessage.RESPTYPE, "Message format incorrect") );
	}
	
	/**
	 * check if messageNode is a valid commit message.
	 * @param messageNode
	 * @throws KVException
	 */
	private void checkCommitTypeMessage(Node messageNode) throws KVException {
		NodeList nodes = messageNode.getChildNodes();
		int l = nodes.getLength();	
		
		if (l!=1)
			throw new KVException( new KVMessage(KVMessage.RESPTYPE, "Message format incorrect") );

		Node tpcOpId = nodes.item(0);

		if (!tpcOpId.getNodeName().equals("TPCOpId"))
			throw new KVException( new KVMessage(KVMessage.RESPTYPE, "Message format incorrect") );
	}
	
	/**
	 * check if messageNode is a valid abort (voting/decision) message.
	 * @param messageNode
	 * @throws KVException
	 */
	private void checkAbortTypeMessage(Node messageNode) throws KVException {
		NodeList nodes = messageNode.getChildNodes();
		int l = nodes.getLength();	

		//desicion abort
		if (l==1){
			Node tpcOpId = nodes.item(0);
			
			if (!tpcOpId.getNodeName().equals("TPCOpId"))
				throw new KVException( new KVMessage(KVMessage.RESPTYPE, "Message format incorrect") );		

		//voting abort
		}else if(l==2){
			Node tpcOpId = nodes.item(1);
			Node msg = nodes.item(0);
			
			if (!msg.getNodeName().equals("Message") || !tpcOpId.getNodeName().equals("TPCOpId")) 
				throw new KVException( new KVMessage(KVMessage.RESPTYPE, "Message format incorrect") );
			
		}else{
			throw new KVException( new KVMessage(KVMessage.RESPTYPE, "Message format incorrect") );
		}
	}
	
	/**
	 * check if messageNode is a resp type message( either a messageNode or a keyNode & valueNode)
	 * @param messageNode
	 * @throws KVException
	 */
	private void checkRespTypeMessage(Node messageNode) throws KVException {
		NodeList nodes = messageNode.getChildNodes();
			
		if (nodes.getLength() == 1) {
			Node message = nodes.item(0);
			
			if (!message.getNodeName().equals("Message"))
				throw new KVException( new KVMessage(KVMessage.RESPTYPE, "Message format incorrect") );
		} else if (nodes.getLength() == 2) {
			Node keyNode = nodes.item(0);
			Node valNode = nodes.item(1);

			if (!keyNode.getNodeName().equals("Key") || !valNode.getNodeName().equals("Value"))
				throw new KVException( new KVMessage(KVMessage.RESPTYPE, "Message format incorrect") );
		} else {
			throw new KVException( new KVMessage(KVMessage.RESPTYPE, "Message format incorrect") );
		}
	}
	
	/**
	 * check if messageNode is a valid Del/2PCDel message.
	 * @param messageNode
	 * @throws KVException
	 */
	private void checkDelTypeMessage(Node messageNode) throws KVException {
		NodeList nodes = messageNode.getChildNodes();
		int l = nodes.getLength();	
		
		if (l!=1 && l!=2)
			throw new KVException( new KVMessage(KVMessage.RESPTYPE, "Message format incorrect") );

		Node key = nodes.item(0);
		
		if (!key.getNodeName().equals("Key"))
			throw new KVException( new KVMessage(KVMessage.RESPTYPE, "Message format incorrect") );

		if (l==2){
			Node tpcOpId = nodes.item(1);
			if (!tpcOpId.getNodeName().equals("TPCOpId"))
				throw new KVException( new KVMessage(KVMessage.RESPTYPE, "Message format incorrect") );
		}
	}
	
	/**
	 * check if messageNode is a valid Get message ( 1 keyNode )
	 * @param messageNode
	 * @throws KVException
	 */
	private void checkGetTypeMessage(Node messageNode) throws KVException {
    NodeList nodes = messageNode.getChildNodes();
		
		if (nodes.getLength()!=1)
			throw new KVException( new KVMessage(KVMessage.RESPTYPE, "Message format incorrect") );

		Node key = nodes.item(0);
		
		if (!key.getNodeName().equals("Key"))
			throw new KVException( new KVMessage(KVMessage.RESPTYPE, "Message format incorrect") );
	}
	
	/**
	 * Check if messageNode is a valid Put/2PC-put message.
	 * @param messageNode
	 * @throws KVException
	 */
	private void checkPutTypeMessage(Node messageNode) throws KVException {
		NodeList nodes = messageNode.getChildNodes();
		
		int l = nodes.getLength();
		
		if (l!=2 && l!=3)
			throw new KVException( new KVMessage(KVMessage.RESPTYPE, "Message format incorrect") );

		Node key = nodes.item(0);
		Node value = nodes.item(1);
		
		if (!key.getNodeName().equals("Key") || !value.getNodeName().equals("Value")){
			throw new KVException( new KVMessage(KVMessage.RESPTYPE, "Message format incorrect") );
		}
		
		if (l==3){
			Node tpcOpId = nodes.item(2);
			if (!tpcOpId.getNodeName().equals("TPCOpId"))
				throw new KVException( new KVMessage(KVMessage.RESPTYPE, "Message format incorrect") );
		}
	}
	
	/**
	 * Check encoding, XML version, and structure(number and names of children). 
	 * @param doc
	 * @throws KVException
	 */
	private void checkDocStructure(Document doc) throws KVException {
		//doc shoudl have only one child with name KVMessage	
		if (!doc.getXmlEncoding().equals("UTF-8") 
				|| !doc.getXmlVersion().equals("1.0")
				|| doc.getChildNodes().getLength()!=1 
				|| !doc.getFirstChild().getNodeName().equals("KVMessage"))
			throw new KVException( new KVMessage(KVMessage.RESPTYPE,"Message format incorrect"));
	}
	
	/**
	 * Copy constructor
	 * 
	 * @param kvm
	 */
	public KVMessage(KVMessage kvm) {
		this.msgType = kvm.msgType;
		this.key = kvm.key;
		this.value = kvm.value;
		this.message = kvm.message;
		this.tpcOpId = kvm.tpcOpId;
	}

	private void xmlBuildHelperPut(Element root, Document doc) throws KVException{
		CheckHelper.sanityCheckKeyValue(this.key, this.value);
		
		this.addKeyXml(root, doc);
		this.addValueXml(root, doc);
		//2PC put
		if (this.tpcOpId!=null){
			this.addTPCOpIdXml(root, doc);
		}
	}

	private void addMessageXml(Element root, Document doc){
		Element msgElement = doc.createElement("Message");
		msgElement.appendChild(doc.createTextNode(this.message));
		root.appendChild(msgElement);
	}

	private void addTPCOpIdXml(Element root, Document doc){
		Element tElement = doc.createElement("TPCOpId");
		tElement.appendChild(doc.createTextNode(this.tpcOpId));
		root.appendChild(tElement);
	}

	private void addValueXml(Element root, Document doc){
		Element valElement = doc.createElement("Value");
		valElement.appendChild(doc.createTextNode(this.value));
		root.appendChild(valElement);
	}
	
	private void addKeyXml(Element root, Document doc){
		Element keyElement = doc.createElement("Key");
		keyElement.appendChild(doc.createTextNode(this.key));
		root.appendChild(keyElement);
	}
	
	private void xmlBuildHelperGet(Element root, Document doc) throws KVException{
		CheckHelper.sanityCheckKey(this.key);
		
		this.addKeyXml(root, doc);
	}
	
	private void xmlBuildHelperAbort(Element root, Document doc) throws KVException{
		if (this.tpcOpId==null)
			throw new KVException(new KVMessage("Unknown Error: not enough data to build XML"));
		
		if (this.message!=null){
			this.addMessageXml(root, doc);
		}
		
		this.addTPCOpIdXml(root, doc);
	}
	
	private void xmlBuildHelperCommit(Element root, Document doc) throws KVException{		
		if (this.tpcOpId==null)
			throw new KVException(new KVMessage("Unknown Error: not enough data to build XML"));

		this.addTPCOpIdXml(root, doc);
	}
	
	private void xmlBuildHelperRegister(Element root, Document doc) throws KVException{	
		if (this.message==null)
			throw new KVException(new KVMessage("Unknown Error: not enough data to build XML"));
		this.addMessageXml(root, doc);
	}
	
	private void xmlBuildHelperReady(Element root, Document doc) throws KVException{
		if (this.tpcOpId==null)
			throw new KVException(new KVMessage("Unknown Error: not enough data to build XML"));
		
		this.addTPCOpIdXml(root, doc);
	}
	
	private void xmlBuildHelperAck(Element root, Document doc) throws KVException{
		if (this.tpcOpId==null)
			throw new KVException(new KVMessage("Unknown Error: not enough data to build XML"));
		
		this.addTPCOpIdXml(root, doc);
	}
	
	private void xmlBuildHelperDel(Element root, Document doc) throws KVException{
		CheckHelper.sanityCheckKey(this.key);
		
		this.addKeyXml(root, doc);
		
		//2pc del
		if (this.tpcOpId!=null){
			this.addTPCOpIdXml(root, doc);
		}
	}
	
	private void xmlBuildHelperResp(Element root, Document doc) throws KVException{
		if (this.message!=null) {
			this.addMessageXml(root, doc);
		} else {
			CheckHelper.sanityCheckKeyValue(this.key, this.value);
				
			this.addKeyXml(root, doc);
			this.addValueXml(root, doc);
		}
	}
	
	/**
	 * Generate the XML representation for this message.
	 * @return the XML String
	 * @throws KVException if not enough data is available to generate a valid KV XML message
	 */
	public String toXML() throws KVException {
	    DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder docBuilder = null;
		try {
			docBuilder = docFactory.newDocumentBuilder();
		} catch (ParserConfigurationException e) {
			DEBUG.debug("this should not happen");
			e.printStackTrace();
			throw new KVException( new KVMessage(KVMessage.RESPTYPE, "this should not happen"));
		}
		// root element
		Document doc = docBuilder.newDocument();
		Element rootElement = doc.createElement("KVMessage");
		rootElement.setAttribute("type", this.msgType);
		doc.setXmlStandalone(true);
		doc.appendChild(rootElement);
		
		if (this.msgType.equals(KVMessage.PUTTYPE)) {
			this.xmlBuildHelperPut(rootElement, doc);

		} else if(this.msgType.equals(KVMessage.GETTYPE)){
			this.xmlBuildHelperGet(rootElement, doc);
			
		} else if( this.msgType.equals(KVMessage.DELTYPE)) {
			this.xmlBuildHelperDel(rootElement, doc);

		} else if (this.msgType.equals(KVMessage.RESPTYPE)){
			this.xmlBuildHelperResp(rootElement, doc);
		} else if (this.msgType.equals(KVMessage.READYTYPE)){
			this.xmlBuildHelperReady(rootElement, doc);
		} else if (this.msgType.equals(KVMessage.ABORTTYPE)){
			this.xmlBuildHelperAbort(rootElement, doc);
		} else if (this.msgType.equals(KVMessage.COMMITTYPE)){
			this.xmlBuildHelperCommit(rootElement, doc);
		} else if (this.msgType.equals(KVMessage.ACKTYPE)){
			this.xmlBuildHelperAck(rootElement, doc);
		} else if (this.msgType.equals(KVMessage.REGISTERTYPE)){
			this.xmlBuildHelperRegister(rootElement, doc);
		} else if (this.msgType.equals(KVMessage.IGNORENEXTTYPE)){
			//do nothing
		}
    
		TransformerFactory transformerFactory = TransformerFactory.newInstance();
		Transformer transformer = null;
		try {
			transformer = transformerFactory.newTransformer();
		} catch (TransformerConfigurationException e) {
			DEBUG.debug("this should not happen either");
			e.printStackTrace();
			throw new KVException( new KVMessage(KVMessage.RESPTYPE, "this should not happen"));
		}
		
		StringWriter writer = new StringWriter();
	
		DOMSource  source= new DOMSource(doc);
		StreamResult result = new StreamResult(writer);
 
		try {
			transformer.transform(source, result);
		} catch (TransformerException e) {
			//this should not happen
			DEBUG.debug("this should not happen either");
			e.printStackTrace();
			throw new KVException( new KVMessage(KVMessage.RESPTYPE, "this should not happen"));
		}
				
		String xml = writer.toString();
		return xml;
	}
	
	public void sendMessage(Socket sock) throws KVException {
		String msg = this.toXML();
		try {
			PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
			out.println(msg);
			sock.shutdownOutput();
		} catch (IOException e) {
			e.printStackTrace();
			throw new KVException(new KVMessage(KVMessage.RESPTYPE, "Unkown Error: IO error in sendMessage"));
		}
	}
	
	public static void sendRespMsgIgnoringException(String message, Socket socket){
		try {
			KVMessage response = new KVMessage(KVMessage.RESPTYPE, message);
			response.sendMessageIgnoringException(socket);
		} catch (KVException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Calls sendMessage(sock), but ignores the errors
	 * @param sock
	 */
	public void sendMessageIgnoringException(Socket sock){
		try{
			this.sendMessage(sock);
		} catch (Exception e){
			//ignore this exception
		}
	}
	
	public void sendMessage(Socket sock, int timeout) throws KVException {
		/*
		 * As was pointed out, setting a timeout when sending the message (while would still technically work),
		 * is a bit silly. As such, this method will be taken out at the end of Spring 2013.
		 */
		// TODO: optional implement me
	}
}
