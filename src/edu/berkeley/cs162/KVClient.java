/**
 * Client component for generating load for the KeyValue store. 
 * This is also used by the Master server to reach the slave nodes.
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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;


/**
 * This class is used to communicate with (appropriately marshalling and unmarshalling) 
 * objects implementing the {@link KeyValueInterface}.
 *
 */
public class KVClient implements KeyValueInterface,Debuggable {

	private String server = null;
	private int port = 0;
	
	/**
	 * @param server is the DNS reference to the Key-Value server
	 * @param port is the port on which the Key-Value server is listening
	 */
	public KVClient(String server, int port) {
		this.server = server;
		this.port = port;
	}
	
	private Socket connectHost() throws KVException {
	    Socket socket;
	    try {
	      socket = new Socket(this.server, port);
	      return socket;
	      
	    //could not connect to the server/port tuple	
	    } catch (UnknownHostException e) {
	      DEBUG.debug("cannot connect to "+this.server+" with port "+this.port);
	      e.printStackTrace();
	      throw new KVException(new KVMessage(KVMessage.RESPTYPE, "Network Error: Could not connect"));
	    
	    //could not create the socket
	    } catch (IOException e) {
	      DEBUG.debug("cannot create a socket with "+this.server+" with port "+this.port);
	      e.printStackTrace();
	      throw new KVException(new KVMessage(KVMessage.RESPTYPE, "Network Error: Could not create socket"));
	    }
	}
	
	private void closeHost(Socket sock) throws KVException {
		try {
			sock.close();
		} catch (IOException e) {
			DEBUG.debug("cannot close "+this.server+" with port "+this.port);
			e.printStackTrace();
			throw new KVException(new KVMessage(KVMessage.RESPTYPE, "Unknown Error: Could not close the socket"));
		}
	}
	
	/**
	 * Send a put <key, value> request to server
	 * @param key
	 * @param value
	 * @throws KVException
	 */
	public void put(String key, String value) throws KVException {
		
		//sanity check of key and value
		CheckHelper.sanityCheckKeyValue(key, value);
				
		Socket sock = this.connectHost();
		
		//try to open inputstream and outputstream of the socket
		OutputStream out = null;
		InputStream in = null;
		try {
			out = sock.getOutputStream();
		} catch (IOException e1) {
			DEBUG.debug("cannot open outputstream");
			e1.printStackTrace();
			throw new KVException(new KVMessage(KVMessage.RESPTYPE, "Network Error: Could not send data"));
		}
		
		try {
			in = sock.getInputStream();
		} catch (IOException e1) {
			DEBUG.debug("cannot open inputstream");
			e1.printStackTrace();
			throw new KVException(new KVMessage(KVMessage.RESPTYPE, "Network Error: Could not receive data"));
		}
		
		KVMessage msg = new KVMessage(KVMessage.PUTTYPE);
		msg.setKey(key);
		msg.setValue(value);
				
		PrintWriter writer = new PrintWriter(out, true);
		writer.println(msg.toXML());
		try {
			sock.shutdownOutput();
		} catch (IOException e) {
			DEBUG.debug("could not close the outputstream");
			e.printStackTrace();
			throw new KVException(new KVMessage(KVMessage.RESPTYPE, "Unknown Error: Could not close the output stream of the socket"));
		}
		
		DEBUG.debug(String.format("Put request of <%s, %s> was sent, waiting for response", key, value));
		
		KVMessage response = new KVMessage(in);
		
		try {
			in.close();
		} catch (IOException e) {
			DEBUG.debug("could not close the input stream");
			throw new KVException(new KVMessage(KVMessage.RESPTYPE, "Unknown Error: Could not close the input stream of the socket"));
		}
		
		this.closeHost(sock);
	
		if (!response.getMessage().equals("Success")) {
			DEBUG.debug("Put request failed. Error message from server: "+ response.getMessage());
			throw new KVException(response);
		} else {
			DEBUG.debug("Put request succeeded");
		}
	}

	public String get(String key) throws KVException {
		
		//sanity check on key
		CheckHelper.sanityCheckKey(key);
		
		Socket sock = this.connectHost();
		
		OutputStream out = null;
		InputStream in = null;

		try {
			out = sock.getOutputStream();
		} catch (IOException e1) {
			DEBUG.debug("cannot open outputstream");
			e1.printStackTrace();
			throw new KVException(new KVMessage(KVMessage.RESPTYPE, "Network Error: Could not send data"));
		}
		
		try {
			in = sock.getInputStream();
		} catch (IOException e1) {
			DEBUG.debug("cannot open inputstream");
			e1.printStackTrace();
			throw new KVException(new KVMessage(KVMessage.RESPTYPE, "Network Error: Could not receive data"));
		}
		
		KVMessage msg = new KVMessage(KVMessage.GETTYPE);
		msg.setKey(key);
		
		PrintWriter writer = new PrintWriter(out, true);
		writer.println(msg.toXML());
		
		try {
			sock.shutdownOutput();
		} catch (IOException e) {
			DEBUG.debug("could not close output stream");
			e.printStackTrace();
			throw new KVException(new KVMessage(KVMessage.RESPTYPE, "Unknown Error: Could not close the output stream of the socket"));
		}
				
		DEBUG.debug(String.format("Get request of <%s> was sent, waiting for response", key));
		
		KVMessage response = new KVMessage(in);
		
		try {
			in.close();
		} catch (IOException e) {
			DEBUG.debug("cannot close input stream");
			e.printStackTrace();
			throw new KVException(new KVMessage(KVMessage.RESPTYPE, "Unknown Error: Could not close the input stream of the socket"));
		}
		this.closeHost(sock);
		
		if (response.getMessage()!=null) {
			DEBUG.debug("Get request failed. Error message from server: "+ response.getMessage());
			throw new KVException(response);
		} else {
			DEBUG.debug("Get request succeeded. Value is "+response.getValue());
			return response.getValue(); 
		}
	}
	
	public void del(String key) throws KVException {
		CheckHelper.sanityCheckKey(key);
		
		Socket sock = this.connectHost();
		
		OutputStream out = null;
		InputStream in = null;
		try {
			out = sock.getOutputStream();
		} catch (IOException e1) {
			DEBUG.debug("Could not open outputstream");
			e1.printStackTrace();
			throw new KVException(new KVMessage(KVMessage.RESPTYPE, "Network Error: Could not send data"));
		}
		
		try {
			in = sock.getInputStream();
		} catch (IOException e1) {
			DEBUG.debug("Could not open input stream");
			e1.printStackTrace();
			throw new KVException(new KVMessage(KVMessage.RESPTYPE, "Network Error: Could not receive data"));
		}
		
		KVMessage msg = new KVMessage(KVMessage.DELTYPE);
		msg.setKey(key);
		
		PrintWriter writer = new PrintWriter(out, true);
		writer.println(msg.toXML());

		try {
			sock.shutdownOutput();
		} catch (IOException e) {
			DEBUG.debug("could not close output stream");
			e.printStackTrace();
			throw new KVException(new KVMessage(KVMessage.RESPTYPE, "Unknown Error: Could not close the output stream of the socket"));
		}
		
		DEBUG.debug(String.format("Del request of <%s> was sent, waiting for response", key));
		
		KVMessage response = new KVMessage(in);
		
		try {
			in.close();
		} catch (IOException e) {
			DEBUG.debug("cannot close input stream");
			e.printStackTrace();
			throw new KVException(new KVMessage(KVMessage.RESPTYPE, "Unknown Error: Could not close the input stream of the socket"));
		}
		
		this.closeHost(sock);
	
		if (!response.getMessage().equals("Success")){
			DEBUG.debug("Del request failed. Error message from server: "+ response.getMessage());
			throw new KVException(response);
		} else {
			DEBUG.debug("Del request succeeded");
		}
	}
	
	/**
	 * send ignoreNext to the server
	 * @throws KVException
	 */
	public void ignoreNext() throws KVException {
		Socket sock = this.connectHost();
		
		OutputStream out = null;
		InputStream in = null;
		try {
			out = sock.getOutputStream();
		} catch (IOException e1) {
			DEBUG.debug("Could not open outputstream");
			e1.printStackTrace();
			throw new KVException(new KVMessage(KVMessage.RESPTYPE, "Network Error: Could not send data"));
		}
		
		try {
			in = sock.getInputStream();
		} catch (IOException e1) {
			DEBUG.debug("Could not open input stream");
			e1.printStackTrace();
			throw new KVException(new KVMessage(KVMessage.RESPTYPE, "Network Error: Could not receive data"));
		}
		
		KVMessage msg = new KVMessage(KVMessage.IGNORENEXTTYPE);
		
		PrintWriter writer = new PrintWriter(out, true);
		writer.println(msg.toXML());

		try {
			sock.shutdownOutput();
		} catch (IOException e) {
			DEBUG.debug("could not close output stream");
			e.printStackTrace();
			throw new KVException(new KVMessage(KVMessage.RESPTYPE, "Unknown Error: Could not close the output stream of the socket"));
		}
		
		DEBUG.debug(String.format("ignoreNext request was sent, waiting for response"));
		
		KVMessage response = new KVMessage(in);
		
		try {
			in.close();
		} catch (IOException e) {
			DEBUG.debug("cannot close input stream");
			e.printStackTrace();
			throw new KVException(new KVMessage(KVMessage.RESPTYPE, "Unknown Error: Could not close the input stream of the socket"));
		}
		
		this.closeHost(sock);
	
		if (!response.getMessage().equals("Success")){
			DEBUG.debug("IgnoreNext request failed. Error message from server: "+ response.getMessage());
			throw new KVException(response);
		} else {
			DEBUG.debug("IgnoreNext request succeeded");
		}
	}
}
