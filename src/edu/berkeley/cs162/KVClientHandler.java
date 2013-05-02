/**
 * Handle client connections over a socket interface
 * 
 * @author Mosharaf Chowdhury (http://www.mosharaf.com)
 * @author Prashanth Mohan (http://www.cs.berkeley.edu/~prmohan)
 *
 * Copyright (c) 2011, University of California at Berkeley
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
 *  DISCLAIMED. IN NO EVENT SHALL PRASHANTH MOHAN BE LIABLE FOR ANY
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
import java.net.Socket;

/**
 * This NetworkHandler will asynchronously handle the socket connections. 
 * It uses a threadpool to ensure that none of it's methods are blocking.
 *
 */
public class KVClientHandler implements NetworkHandler {
	private ThreadPool threadpool = null;
    private TPCMaster tpcMaster = null;
	
	public KVClientHandler(TPCMaster tpcMaster) {
		initialize(1, tpcMaster);
	}

	public KVClientHandler(int connections, TPCMaster tpcMaster) {
		initialize(connections, tpcMaster);
	}

	private void initialize(int connections, TPCMaster tpcMaster) {
		threadpool = new ThreadPool(connections);
        this.tpcMaster = tpcMaster; 
	}
	

	private class ClientHandler implements Runnable, Debuggable {
		private Socket client = null;
		private TPCMaster tpcMaster = null;
		
		private void handlePut(KVMessage msg){
			try {
				this.tpcMaster.performTPCOperation(msg, true);
			} catch (KVException e) {
				try {
					e.getMsg().sendMessage(this.client);
				} catch (KVException e1) {
					DEBUG.debug("error happens when trying to send back error message");
					e1.printStackTrace();
				}
				return;
			}
			
			try {
				KVMessage successMsg = new KVMessage(KVMessage.RESPTYPE, "Success");
				successMsg.sendMessage(this.client);

			} catch(KVException e) {
				DEBUG.debug("error happens when trying to send back success message");
				e.printStackTrace();
			}
		}
		
		private void handleDel(KVMessage msg){
			try {
				this.tpcMaster.performTPCOperation(msg, false);
			} catch (KVException e) {
				try {
					e.getMsg().sendMessage(this.client);
				} catch (KVException e1) {
					DEBUG.debug("error happens when trying to send back error message");
					e1.printStackTrace();
				}
				return;			
			}
			
			try {
				KVMessage successMsg = new KVMessage(KVMessage.RESPTYPE, "Success");
				successMsg.sendMessage(this.client);

			} catch(KVException e) {
				DEBUG.debug("error happens when trying to send back success message");
				e.printStackTrace();
			}
		}
		
		private void handleGet(KVMessage msg){
			String val = null;
			try {
				val = this.tpcMaster.handleGet(msg);
			} catch (KVException e) {
				try {
					e.getMsg().sendMessage(this.client);
				} catch (KVException e1) {
					DEBUG.debug("error happens when trying to send back error message");
					e1.printStackTrace();
				}
				return;
			}
		
			try{
				//successful get
				assert(val!=null);
				
				KVMessage successMsg = null;
				try {
					successMsg = new KVMessage(KVMessage.RESPTYPE);
				} catch (KVException e) {
					//silence this exception
					DEBUG.debug("this error should not happen");
					e.printStackTrace();
					return;
				}
				successMsg.setKey(msg.getKey());
				successMsg.setValue(val);
				successMsg.sendMessage(this.client);

			}catch (KVException e){
				DEBUG.debug("error happens when trying to send back success message");
				e.printStackTrace();
			}
		}
		
		@Override
		public void run() {
			if (!this.tpcMaster.hasFinishedRegistration()){
				try {
					new KVMessage(KVMessage.RESPTYPE, "Unknown Error: the master server has not finished registration yet").sendMessage(this.client);
				} catch (KVException e1) {
					DEBUG.debug("error happens when trying to send back error message");
					e1.printStackTrace();
				}
				return;
			}
			
			
			try {
				    
				KVMessage msg = new KVMessage(this.client);

				DEBUG.debug(this.hashCode()+msg.toXML());
				//get request
				if (msg.getMsgType().equals(KVMessage.GETTYPE)) {
					DEBUG.debug("Get a get request of key "+msg.getKey());
					handleGet(msg);
				
				//put request	
				} else if (msg.getMsgType().equals(KVMessage.PUTTYPE)) {
					DEBUG.debug(String.format("Get a put request of key %s and value %s", msg.getKey(), msg.getValue()));
					handlePut(msg);
					
				//del request	
				} else if (msg.getMsgType().equals(KVMessage.DELTYPE)) {
					DEBUG.debug("Get a del request of key "+msg.getKey());
					handleDel(msg);
			
				//resp request
				} else {
					try {
						new KVMessage(KVMessage.RESPTYPE, "Unknown Error: server received a response message").sendMessage(this.client);
					} catch (KVException e1) {
						DEBUG.debug("error happens when trying to send back error message");
						e1.printStackTrace();
					}
				}
			} catch (KVException e) {
				//exception when getting KVMessage from the socket's input stream
				try {
					e.getMsg().sendMessage(this.client);
				} catch (KVException e1) {
					DEBUG.debug("error happens when trying to send back error message");
					e1.printStackTrace();
				}
//			} catch (IOException e) {
//				DEBUG.debug("could not receive data");
//				e.printStackTrace();
//				try {
//					new KVMessage(KVMessage.RESPTYPE, "Network Error: Could not receive data").sendMessage(this.client);
//				} catch (KVException e1) {
//					DEBUG.debug("error happens when trying to send back error message");
//					e1.printStackTrace();
//				}
			}
		}
		
		public ClientHandler(Socket client) {
			this.client = client;
			this.tpcMaster = KVClientHandler.this.tpcMaster;
		}
	}
	
	/* (non-Javadoc)
	 * @see edu.berkeley.cs162.NetworkHandler#handle(java.net.Socket)
	 */
	@Override
	public void handle(Socket client) throws IOException {
		Runnable r = new ClientHandler(client);
		try {
			threadpool.addToQueue(r);
		} catch (InterruptedException e) {
			// Ignore this error
			return;
		}
	}
}
