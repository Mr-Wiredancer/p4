/**
 * Handle TPC connections over a socket interface
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
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

/**
 * Implements NetworkHandler to handle 2PC operation requests from the Master/
 * Coordinator Server
 *
 */
public class TPCMasterHandler implements NetworkHandler, Debuggable {
	private KVServer kvServer = null;
	private ThreadPool threadpool = null;
	private TPCLog tpcLog = null;
	
	private long slaveID = -1;
	
	// Used to handle the "ignoreNext" message
	private boolean ignoreNext = false;
	private WriteLock ignoreNextLock = new ReentrantReadWriteLock().writeLock();
	
	// States carried from the first to the second phase of a 2PC operation
//	private KVMessage originalMessage = null;
	private HashMap<String, KVMessage> waitingOperations = new HashMap<String, KVMessage>();
	private WriteLock waitingLock = new ReentrantReadWriteLock().writeLock();
//	private boolean aborted = true;	

	public TPCMasterHandler(KVServer keyserver) {
		this(keyserver, 1);
	}

	public TPCMasterHandler(KVServer keyserver, long slaveID) {
		this.kvServer = keyserver;
		this.slaveID = slaveID;
		threadpool = new ThreadPool(1);
	}

	public TPCMasterHandler(KVServer kvServer, long slaveID, int connections) {
		this.kvServer = kvServer;
		this.slaveID = slaveID;
		threadpool = new ThreadPool(connections);
	}

	private class ClientHandler implements Runnable {
		private KVServer keyserver = null;
		private Socket client = null;
		
		private void closeConn() {
			try {
				client.close();
			} catch (IOException e) {
			}
		}
		
		@Override
		public void run() {
			
			//check and update the waiting operations(ready state) if any (After server restarts)
			TPCMasterHandler.this.waitingLock.lock();
			if (TPCMasterHandler.this.tpcLog.hasInterruptedTpcOperations()){
				TPCMasterHandler.this.waitingOperations = TPCMasterHandler.this.tpcLog.getInterruptedTpcOperations();
			}
			TPCMasterHandler.this.waitingLock.unlock();

			// Receive message from client
			// Implement me	
			KVMessage msg = null;
			
			try {
				msg = new KVMessage(this.client);
			} catch (KVException e) {
				//cannot get a KVMessage from the socket, just timeout
				return;
			}

			// Parse the message and do stuff 
			String key = msg.getKey();
			
			if (msg.getMsgType().equals("putreq")) {
				handlePut(msg, key);
			}
			else if (msg.getMsgType().equals("getreq")) {
				handleGet(msg, key);
			}
			else if (msg.getMsgType().equals("delreq")) {
				handleDel(msg, key);
			} 
			else if (msg.getMsgType().equals("ignoreNext")) {
				// Set ignoreNext to true. PUT and DEL handlers know what to do.
				TPCMasterHandler.this.ignoreNextLock.lock();
				TPCMasterHandler.this.ignoreNext = true;
				TPCMasterHandler.this.ignoreNextLock.unlock();
				
				// Send back an acknowledgment
				KVMessage.sendRespMsgIgnoringException("Success",this.client);
			}
			else if (msg.getMsgType().equals("commit") || msg.getMsgType().equals("abort")) {
				TPCMasterHandler.this.waitingLock.lock();
				/*
				 * if originalMsg is null, it means that this slave server crashed,
				 * after successfully commit/abort the put/del request,
				 * but before sending back ACK to master.
				 */
				KVMessage originalMsg = TPCMasterHandler.this.waitingOperations.remove(msg.getTpcOpId()); 
				TPCMasterHandler.this.waitingLock.unlock();
				
				handleMasterResponse(msg, originalMsg, msg.getMsgType().equals("abort"));
			}
			
			// Finally, close the connection
			closeConn();
		}
		
		/**
		 * helper method to send back a commit vote
		 * @param msg
		 */
		private void sendReady(KVMessage msg){
			KVMessage commit;
			try {
				commit = new KVMessage(KVMessage.READYTYPE);
				commit.setTpcOpId(msg.getTpcOpId());
				commit.sendMessageIgnoringException(this.client);
			} catch (KVException e) {
				//ignore this exception
			}
		}

		/**
		 * helper method to send back an abort vote
		 * @param msg
		 */
		private void sendAbort(String msg, String tpcOpId){
			KVMessage abort;
			try {
				abort = new KVMessage(KVMessage.ABORTTYPE);
				abort.setTpcOpId(tpcOpId);
				abort.setMessage(msg);
				abort.sendMessageIgnoringException(this.client);
			} catch (KVException e) {
				//ignore this exception
			}
		}
		
		/**
		 * helper method to send back an ack vote
		 * @param msg
		 */
		private void sendAck(KVMessage msg){
			KVMessage ack;
			try {
				ack = new KVMessage(KVMessage.ACKTYPE);
				ack.setTpcOpId(msg.getTpcOpId());
				ack.sendMessageIgnoringException(this.client);
			} catch (KVException e) {
				//ignore this exception
			}
		}
		
 		private void handleGet(KVMessage msg, String key) {
 			AutoGrader.agGetStarted(slaveID);
			
 			try{
 				KVMessage response = null;
 				try {
					String val = this.keyserver.get(key);
					response = new KVMessage(KVMessage.RESPTYPE);
					response.setKey(key);
					response.setValue(val);
					
				} catch (KVException e) {
					// TODO Auto-generated catch block
					response = e.getMsg();
				}
 				
 				//sendback message
 				try {
					response.sendMessage(this.client);
				} catch (KVException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
 			}finally{
 				AutoGrader.agGetFinished(slaveID);
 			}
 		}
		
		private void handlePut(KVMessage msg, String key) {
			AutoGrader.agTPCPutStarted(slaveID, msg, key);
			try{
			
				TPCMasterHandler.this.tpcLog.appendAndFlush(msg);
				
				TPCMasterHandler.this.waitingLock.lock();
				TPCMasterHandler.this.waitingOperations.put(msg.getTpcOpId(), msg);
				TPCMasterHandler.this.waitingLock.unlock();
				
				TPCMasterHandler.this.ignoreNextLock.lock();
				try{
					if (TPCMasterHandler.this.ignoreNext) {
						this.sendAbort("Unknown Error: Ignore this 2PC operation", msg.getTpcOpId());
						TPCMasterHandler.this.ignoreNext = false;
						return;
					}
				}finally{
					TPCMasterHandler.this.ignoreNextLock.unlock();
				}
				
				try {
					CheckHelper.sanityCheckKeyValue(key, msg.getValue());
					this.sendReady(msg);
				
				} catch (KVException e) {
					this.sendAbort(e.getMsg().getMessage(), msg.getTpcOpId());
				}				
			}finally{
				AutoGrader.agTPCPutFinished(slaveID, msg, key);
			}
		}
 		
		private void handleDel(KVMessage msg, String key) {
			AutoGrader.agTPCDelStarted(slaveID, msg, key);
			try{
				TPCMasterHandler.this.tpcLog.appendAndFlush(msg);
				
				TPCMasterHandler.this.waitingLock.lock();
				TPCMasterHandler.this.waitingOperations.put(msg.getTpcOpId(), msg);
				TPCMasterHandler.this.waitingLock.unlock();
				
				TPCMasterHandler.this.ignoreNextLock.lock();
				try{
					if (TPCMasterHandler.this.ignoreNext) {
						this.sendAbort("Unknown Error: Ignore this 2pc operation", msg.getTpcOpId());
						TPCMasterHandler.this.ignoreNext = false;
						return;
					}
				}finally{
					TPCMasterHandler.this.ignoreNextLock.unlock();
				}
				
				try {
					CheckHelper.sanityCheckKey(key);

					if (this.keyserver.hasKey(key)){
						this.sendReady(msg);
					}
				} catch (KVException e) {
					this.sendAbort(e.getMsg().getMessage(), msg.getTpcOpId());
				}
			}finally{
				AutoGrader.agTPCDelFinished(slaveID, msg, key);
			}
		}

		/**
		 * Second phase of 2PC
		 * 
		 * @param masterResp Global decision taken by the master
		 * @param origMsg Message from the actual client (received via the coordinator/master)
		 * @param origAborted Did this slave server abort it in the first phase 
		 */
		private void handleMasterResponse(KVMessage masterResp, KVMessage origMsg, boolean origAborted) {
			AutoGrader.agSecondPhaseStarted(slaveID, origMsg, origAborted);
			
			try{
				TPCMasterHandler.this.tpcLog.appendAndFlush(masterResp);
				String id = masterResp.getTpcOpId();
				
				//no message of the tpcopid is waiting or the global decision is abort
				if (origMsg==null || masterResp.getMsgType().equals(KVMessage.ABORTTYPE)) {
					this.sendAck(masterResp);
					return;
				}
			
				//do the actual operation
				if (origMsg.getMsgType().equals(KVMessage.PUTTYPE)){
					try {
						this.keyserver.put(origMsg.getKey(), origMsg.getValue());
					} catch (KVException e) {
						return;
					}
				} else {
					try{
						this.keyserver.del(origMsg.getKey());
					} catch (KVException e){
						return;
					}
				}
				
				//send ack
				this.sendAck(masterResp);
			}finally{
				AutoGrader.agSecondPhaseFinished(slaveID, origMsg, origAborted);
			}
		}
		
		public ClientHandler(KVServer keyserver, Socket client) {
			this.keyserver = keyserver;
			this.client = client;
		}
	}

	@Override
	public void handle(Socket client) throws IOException {
		AutoGrader.agReceivedTPCRequest(slaveID);
		Runnable r = new ClientHandler(kvServer, client);
		try {
			threadpool.addToQueue(r);
		} catch (InterruptedException e) {
			// TODO: HANDLE ERROR
//			return; comment out this so that it'll always call agFinishedTPCRequest
		}		
		AutoGrader.agFinishedTPCRequest(slaveID);
	}

	/**
	 * Set TPCLog after it has been rebuilt
	 * @param tpcLog
	 */
	public void setTPCLog(TPCLog tpcLog) {
		this.tpcLog  = tpcLog;
	}

	/**
	 * Registers the slave server with the coordinator
	 * 
	 * @param masterHostName
	 * @param servr KVServer used by this slave server (contains the hostName and a random port)
	 * @throws UnknownHostException
	 * @throws IOException
	 * @throws KVException
	 */
	public void registerWithMaster(String masterHostName, SocketServer server) throws UnknownHostException, IOException, KVException {
		AutoGrader.agRegistrationStarted(slaveID);
		try{
			Socket master = new Socket(masterHostName, 9090);
			KVMessage regMessage = new KVMessage("register", slaveID + "@" + server.getHostname() + ":" + server.getPort());
			regMessage.sendMessage(master);
			
			// Receive master response. 
			// Response should always be success, except for Exceptions. Throw away.
			KVMessage response = new KVMessage(master.getInputStream());
			DEBUG.debug("Message from master: "+response.getMessage());
			master.close();
		}finally{
			AutoGrader.agRegistrationFinished(slaveID);
		}
	}
}
