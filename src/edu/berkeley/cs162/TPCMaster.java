/**
 * Master for Two-Phase Commits
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
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TPCMaster implements Debuggable {
	
	/**
	 * Implements NetworkHandler to handle registration requests from 
	 * SlaveServers.
	 * 
	 */
	private class TPCRegistrationHandler implements NetworkHandler {

		private ThreadPool threadpool = null;

		public TPCRegistrationHandler() {
			// Call the other constructor
			this(1);	
		}

		public TPCRegistrationHandler(int connections) {
			threadpool = new ThreadPool(connections);	
		}

		@Override
		public void handle(Socket client) throws IOException {
			Runnable r = new RegistrationHandler(client);
			try {
				threadpool.addToQueue(r);
			} catch (InterruptedException e) {
				// Ignore this error
				return;
			}
		}
		
		private class RegistrationHandler implements Runnable {
			
			private Socket client = null;

			public RegistrationHandler(Socket client) {
				this.client = client;
			}
		
			@Override
			public void run() {
				KVMessage msg=null;
				try {
					msg = new KVMessage(client);
				} catch (KVException e) {
					e.getMsg().sendMessageIgnoringException(this.client);
					return;
				}
				
				if (!msg.getMsgType().equals(KVMessage.REGISTERTYPE)){
					KVMessage.sendRespMsgIgnoringException("Unknown Error: cannot recognize the message type", this.client);
					return;
				}
								
				SlaveInfo slaveInfo=null;
				try {
					slaveInfo = new SlaveInfo(msg.getMessage());
				} catch (KVException e) {
					System.out.println("error when parsig: "+msg.getMessage());
					e.getMsg().sendMessageIgnoringException(this.client);
					return;
				}
					
				TPCMaster.this.slaveInfosLock.lock();
				try{
					TPCMaster.this.slaveInfos.put(slaveInfo.getSlaveID(), slaveInfo);
					if (TPCMaster.this.slaveInfos.size()>TPCMaster.this.numSlaves){
						TPCMaster.this.slaveInfos.remove(slaveInfo.getSlaveID());
						KVMessage.sendRespMsgIgnoringException("Unknown Error: master already has enough slave servers", this.client);
						return;
					}
				}finally{
					TPCMaster.this.slaveInfosLock.unlock();
				}
				
				//send back message
				KVMessage.sendRespMsgIgnoringException(String.format("Successfully registered %s@%s:%s", slaveInfo.slaveID, slaveInfo.hostName, slaveInfo.port), this.client);

			}
		}	
	}
	
	/**
	 *  Data structure to maintain information about SlaveServers
	 *
	 */
	private class SlaveInfo {
		// 64-bit globally unique ID of the SlaveServer
		private long slaveID = -1;
		// Name of the host this SlaveServer is running on
		private String hostName = null;
		// Port which SlaveServer is listening to
		private int port = -1;

		/**
		 * 
		 * @param slaveInfo as "SlaveServerID@HostName:Port"
		 * @throws KVException
		 */
		public SlaveInfo(String slaveInfo) throws KVException {
			// implement me
			//regular expression
			String pattern = "(-?\\d+)@(.+):(\\d+)";
			Pattern p = Pattern.compile(pattern);
			Matcher m = p.matcher(slaveInfo);
			if (!m.matches()){
				throw new KVException(new KVMessage(KVMessage.RESPTYPE, "Unknown Error: could not parse the slave info"));
			}
			try{
				this.slaveID = Long.parseLong(m.group(1));
				this.hostName = m.group(2);
				this.port = Integer.parseInt(m.group(3));
			} catch (Exception e){
				throw new KVException(new KVMessage(KVMessage.RESPTYPE, "Unknown Error: could not recoginze the slave info"));
			}
		}
		
		
		public long getSlaveID() {
			return slaveID;
		}
		
		/**
		 * Connect to the host and return as Socket object
		 * @return the created socket
		 * @throws KVException if could not connect or could not create a socket
		 */
		public Socket connectHost() throws KVException {
		    Socket socket;
		    try {
		      socket = new Socket(this.hostName, port);
		      return socket;
		      
		    //could not connect to the server/port tuple	
		    } catch (UnknownHostException e) {
		      DEBUG.debug("TPCMaster cannot connect to "+this.hostName+" with port "+this.port);
		      e.printStackTrace();
		      throw new KVException(new KVMessage(KVMessage.RESPTYPE, "Network Error: Could not connect"));
		    
		    //could not create the socket
		    } catch (IOException e) {
		      DEBUG.debug("TPCMaster cannot create a socket with "+this.hostName+" with port "+this.port);
		      e.printStackTrace();
		      throw new KVException(new KVMessage(KVMessage.RESPTYPE, "Network Error: Could not create socket"));
		    }
		}
		
		public void closeHost(Socket sock) throws KVException {
			try {
				sock.close();
			} catch (IOException e) {
				DEBUG.debug("TPCMaster cannot close the connection to "+this.hostName+" with port "+this.port);
				e.printStackTrace();
				throw new KVException(new KVMessage(KVMessage.RESPTYPE, "Unknown Error: Could not close the socket"));
			}
		}
	}
	
	// Timeout value used during 2PC operations
	private static final int TIMEOUT_MILLISECONDS = 5000;
	
	// Cache stored in the Master/Coordinator Server
	private KVCache masterCache = new KVCache(100, 10);
	
	// Registration server that uses TPCRegistrationHandler
	private SocketServer regServer = null;

	// Number of slave servers in the system
	private int numSlaves = -1;
	
	// ID of the next 2PC operation
	private Long tpcOpId = 0L;
	
	//slaveInfos of slave servers
	private TreeMap<Long, SlaveInfo> slaveInfos = new TreeMap<Long, SlaveInfo>(new UnsignedLongComparator());
	private WriteLock slaveInfosLock = new ReentrantReadWriteLock().writeLock();
	
	/**
	 * Creates TPCMaster
	 * 
	 * @param numSlaves number of expected slave servers to register
	 * @throws Exception
	 */
	public TPCMaster(int numSlaves) {
		// Using SlaveInfos from command line just to get the expected number of SlaveServers 
		this.numSlaves = numSlaves;

		// Create registration server
		regServer = new SocketServer("localhost", 9090);
		regServer.addHandler(new TPCRegistrationHandler());
	}
	
	/**
	 * Calculates tpcOpId to be used for an operation. In this implementation
	 * it is a long variable that increases by one for each 2PC operation. 
	 * 
	 * @return 
	 */
	private String getNextTpcOpId() {
		tpcOpId++;
		return tpcOpId.toString();		
	}
	
	/**
	 * Start registration server in a separate thread
	 */
	public void run() {
		AutoGrader.agTPCMasterStarted();
		
		Runnable r = new Runnable(){
			public void run(){
				try {
					regServer.connect();
					regServer.run();
					DEBUG.debug("Starting registration server "+regServer.hostname+" on "+regServer.port);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		};
		
		new Thread(r).start();
		AutoGrader.agTPCMasterFinished();
	}
	
	/**
	 * stop the registration server
	 */
	public void stop(){
		regServer.stop();
	}
	
	/**
	 * Converts Strings to 64-bit longs
	 * Borrowed from http://stackoverflow.com/questions/1660501/what-is-a-good-64bit-hash-function-in-java-for-textual-strings
	 * Adapted from String.hashCode()
	 * @param string String to hash to 64-bit
	 * @return
	 */
	private long hashTo64bit(String string) {
		// Take a large prime
		long h = 1125899906842597L; 
		int len = string.length();

		for (int i = 0; i < len; i++) {
			h = 31*h + string.charAt(i);
		}
		return h;
	}
	
	/**
	 * Compares two longs as if they were unsigned (Java doesn't have unsigned data types except for char)
	 * Borrowed from http://www.javamex.com/java_equivalents/unsigned_arithmetic.shtml
	 * @param n1 First long
	 * @param n2 Second long
	 * @return is unsigned n1 less than unsigned n2
	 */
	private boolean isLessThanUnsigned(long n1, long n2) {
		return (n1 < n2) ^ ((n1 < 0) != (n2 < 0));
	}
	
	private boolean isLessThanEqualUnsigned(long n1, long n2) {
		return isLessThanUnsigned(n1, n2) || n1 == n2;
	}	
	
	private class UnsignedLongComparator implements Comparator<Long>{

		@Override
		public int compare(Long arg0, Long arg1) {
			if (TPCMaster.this.isLessThanUnsigned(arg0, arg1)) return -1;
			
			if (arg0==arg1) return 0;
			
			return 1;
		}
		
	}

	/**
	 * Find first/primary replica location
	 * @param key
	 * @return
	 */
	private SlaveInfo findFirstReplica(String key) {
		// 64-bit hash of the key
		long hashedKey = hashTo64bit(key.toString());

		this.slaveInfosLock.lock();
		
		//get the value of the least key greater or equal to the hashedkey
		Map.Entry<Long, SlaveInfo> entry = this.slaveInfos.ceilingEntry(hashedKey);
		
		SlaveInfo slaveInfo = null;
		//if it is null, the hashed key is stored in the 1st slave(lowest ID)
		if (entry==null){
			slaveInfo = this.slaveInfos.firstEntry().getValue();
		}else{
			slaveInfo = entry.getValue();
		}
			
		this.slaveInfosLock.unlock();
		
		return slaveInfo;
	}
	
	/**
	 * Find the successor of firstReplica to put the second replica
	 * @param firstReplica
	 * @return
	 */
	private SlaveInfo findSuccessor(SlaveInfo firstReplica) {
		// implement me
		long keyFirstReplica = firstReplica.getSlaveID();
		
		this.slaveInfosLock.lock();

		Map.Entry<Long, SlaveInfo> entry = this.slaveInfos.ceilingEntry(keyFirstReplica+1);

		SlaveInfo slaveInfo = null;
		
		//if it is null, the successor is stored the 1st slave(lowest ID)
		if (entry==null){
			slaveInfo = this.slaveInfos.firstEntry().getValue();
		}else{
			slaveInfo = entry.getValue();
		}
		
		this.slaveInfosLock.unlock();

		return slaveInfo;
	}
	
	/**
	 * return true if the master finished registration
	 * @return true if numSlaves matches the size of slaveInfos(registration finished)
	 */
	public boolean hasFinishedRegistration(){
		this.slaveInfosLock.lock();
		
		try{
			return this.numSlaves==this.slaveInfos.size();
		}finally{
			this.slaveInfosLock.unlock();
		}
	}
	
	/**
	 * Synchronized method to perform 2PC operations one after another
	 * You will need to remove the synchronized declaration if you wish to attempt the extra credit
	 * 
	 * @param msg
	 * @param isPutReq
	 * @throws KVException
	 */
	public void performTPCOperation(KVMessage msg, boolean isPutReq) throws KVException {
		AutoGrader.agPerformTPCOperationStarted(isPutReq);
		WriteLock lock = this.masterCache.getWriteLock(msg.getKey());
		lock.lock();
		
		// implement me
		try{
			String key = msg.getKey();
			String value = msg.getValue();
			
			if (isPutReq){
				this.masterCache.put(key, value);
			}else{
				this.masterCache.del(key);
			}
			
			SlaveInfo primary = this.findFirstReplica(key);
			SlaveInfo secondary = this.findSuccessor(primary);
			
			String[] errors = new String [2];errors[0] = null; errors[1] = null;
			msg.setTpcOpId(this.getNextTpcOpId());
	
			//1st phase
			Runnable r1 = new RunnableVoteRequest(primary, msg, errors, true);
			Runnable r2 = new RunnableVoteRequest(secondary, msg, errors, false);
			
			Thread t1 = new Thread(r1), t2 = new Thread(r2);
			t1.start(); t2.start();
			try {
				t1.join();
				t2.join();
			} catch (InterruptedException e) {
				//ignore
			} 
			
			String e1 = errors[0];
			String e2 = errors[1];
			
			//2nd phase, block until we got ack from both slaves
			Runnable r3 = new RunnableSendDecision(msg, true, e1==null && e2==null);
			Runnable r4 = new RunnableSendDecision(msg, false, e1==null && e2==null);
			
			Thread t3 = new Thread(r3), t4 = new Thread(r4);
			t3.start(); t4.start();
			try {
				t3.join();
				t4.join();
			} catch (InterruptedException e) {
				//ignore
			} 
						
			if (e1!=null || e2!=null){
				String message = "";
			
				if (e1!=null && e2!=null){
					message = e1+"\n"+e2;
				}else if(e1!=null){
					message = e1;
				}else{
					message = e2;
				}
				throw new KVException(new KVMessage(KVMessage.RESPTYPE, message));
			}
		}finally{
			lock.unlock();
			AutoGrader.agPerformTPCOperationFinished(isPutReq);
		}
	}
	
	/**
	 * send vote request to slave. normally return only if the response is ready
	 * @param slave
	 * @param msg
	 * @throws KVException if there is any error or the vote is not ready
	 */
	private void sendVoteRequest(SlaveInfo slave, KVMessage msg) throws KVException{
		Socket sock = slave.connectHost();
		try {
			sock.setSoTimeout(TPCMaster.TIMEOUT_MILLISECONDS);
		} catch (SocketException e) {
			throw new KVException(new KVMessage(KVMessage.RESPTYPE, "Unknown Error: could not set timeout of socket"));
		}
		msg.sendMessage(sock);
		
		KVMessage response = new KVMessage(sock);
		
		try{
			slave.closeHost(sock);
		} catch (KVException e){
			//silence this
		}
		
		if (response.getMsgType().equals(KVMessage.ABORTTYPE)){
			throw new KVException(response);
		}
	}
	
	private class RunnableVoteRequest implements Runnable{
		SlaveInfo slaveInfo;
		KVMessage msg; 
		String[] errors;
		boolean isPrimary;
		
		public RunnableVoteRequest(SlaveInfo slaveInfo, KVMessage msg, String[] errors, boolean isPrimary){
			super();
			this.slaveInfo = slaveInfo;
			this.msg = msg;
			this.errors = errors;
			this.isPrimary = isPrimary;
		}
		
		@Override
		public void run() {
			try {
				TPCMaster.this.sendVoteRequest(slaveInfo, msg);
			} catch (KVException e) {
				// TODO Auto-generated catch block
				if (isPrimary){
					errors[0] = e.getMsg().getMessage();
				}else{
					errors[1] = e.getMsg().getMessage();
				}
			}
		}
		
	}
	
	private class RunnableSendDecision implements Runnable{
		KVMessage msg; 
		boolean isPrimary;
		boolean isCommit;
		
		public RunnableSendDecision(KVMessage msg, boolean isPrimary, boolean isCommit){
			this.msg = msg;
			this.isPrimary = isPrimary;
			this.isCommit = isCommit;
		}

		
		@Override
		public void run() {
			TPCMaster.this.sendDecision(msg, isPrimary, isCommit);
		}
		
	}	
	
	/**
	 * Send decision to primary/secondary slave server and keep trying until slave returns a success message
	 * @param request the KVMessage from the client, with tpcopid set
	 * @param isPrimary indicates whether to send to primary server or not
	 * @param isCommit indicates whether this is a commit or not
	 */
	private void sendDecision(KVMessage request, boolean isPrimary, boolean isCommit){
		SlaveInfo slave = null;
		KVMessage msg = null;
		String key = request.getKey();
		try {
			if (isCommit){
				msg = new KVMessage(KVMessage.COMMITTYPE);
			}else{
				msg = new KVMessage(KVMessage.ABORTTYPE);
			}
		} catch (KVException e1) {
			//this cannot happen
			e1.printStackTrace();
		}
		msg.setTpcOpId(request.getTpcOpId());
		
		while (true){
			//get the updated slave information
			if (isPrimary){
				slave = this.findFirstReplica(key);
			}else{
				slave = this.findSuccessor(this.findFirstReplica(key));
			}
			
			try {
				Socket sock  = slave.connectHost();
				sock.setSoTimeout(TPCMaster.TIMEOUT_MILLISECONDS);
				msg.sendMessage(sock);
				KVMessage response = new KVMessage(sock);
				slave.closeHost(sock);
				//return upon success
				if (response.getMsgType().equals(KVMessage.ACKTYPE) 
						&& response.getTpcOpId().equals(msg.getTpcOpId())){
					return ;
				}
				
			} catch (KVException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SocketException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
		}
	}

	/**
	 * Perform GET operation in the following manner:
	 * - Try to GET from first/primary replica
	 * - If primary succeeded, return Value
	 * - If primary failed, try to GET from the other replica
	 * - If secondary succeeded, return Value
	 * - If secondary failed, return KVExceptions from both replicas
	 * 
	 * @param msg Message containing Key to get
	 * @return Value corresponding to the Key
	 * @throws KVException
	 */
	public String handleGet(KVMessage msg) throws KVException {
		AutoGrader.aghandleGetStarted();	
		WriteLock l = this.masterCache.getWriteLock(msg.getKey());
		l.lock();
		try{
			String cacheResult = this.masterCache.get(msg.getKey());
			
			if (cacheResult!=null){
				return cacheResult;
			}
			
			msg.setTpcOpId(this.getNextTpcOpId());
			
			//trying primary
			String [] values = new String[2]; values[0] = null; values[1] = null;
			String [] errors = new String[2]; errors[0] = null; errors[1] = null;
			
			SlaveInfo primary = this.findFirstReplica(msg.getKey());
			SlaveInfo secondary = this.findSuccessor(primary);

			Runnable r1 = new RunnableGet(primary, msg, values,errors, true);
			Runnable r2 = new RunnableGet(secondary, msg, values, errors, false);
			
			Thread t1 = new Thread(r1), t2 = new Thread(r2);
			t1.start(); t2.start();
			try {
				t1.join();
				t2.join();
			} catch (InterruptedException e) {
				//ignore
			} 
			
			if (values[0]!=null){
				this.masterCache.replace(msg.getKey(), values[0]);
				return values[0];
			} else if (values[1]!=null){
				this.masterCache.replace(msg.getKey(), values[1]);
				return values[1];
			} else {
				String e1 = errors[0];
				String e2 = errors[1];
				
				String message = "";
				
				if (e1!=null && e2!=null){
					message = e1+"\n"+e2;
				}else if(e1!=null){
					message = e1;
				}else{
					message = e2;
				}
				
				throw new KVException(new KVMessage(KVMessage.RESPTYPE, message));
			}
			
		}finally{
			l.unlock();
			AutoGrader.aghandleGetFinished();
		}
	}
	
	private class RunnableGet implements Runnable{
		SlaveInfo slaveInfo;
		KVMessage msg;
		boolean isPrimary;
		String [] values, errors;
		
		public RunnableGet(SlaveInfo slaveInfo, KVMessage msg, String[] values, String[] errors, boolean isPrimary){
			super();
			this.slaveInfo = slaveInfo;
			this.msg = msg;
			this.isPrimary = isPrimary;
			this.values = values;
			this.errors = errors;
		}
		
		@Override
		public void run() {			
			try {
				Socket sock = slaveInfo.connectHost();
				sock.setSoTimeout(TPCMaster.TIMEOUT_MILLISECONDS);
				msg.setTpcOpId(TPCMaster.this.getNextTpcOpId());
				msg.sendMessage(sock);
				KVMessage response  = new KVMessage(sock);
				
				//return upon success
				if (response.getMessage()==null){
					String val = response.getValue();
					if (isPrimary){
						values[0] = val;
					}else{
						values[1] = val;
					}
				}else{
					if (isPrimary){
						errors[0] = String.format("@%s:=%s", slaveInfo.getSlaveID(), response.getMessage());
					} else{
						errors[1] = String.format("@%s:=%s", slaveInfo.getSlaveID(), response.getMessage());
					}
				}
			}catch(KVException e){
				if (isPrimary){
					errors[0] = String.format("@%s:=%s", slaveInfo.getSlaveID(), e.getMsg().getMessage());					
				}else{
					errors[1] = String.format("@%s:=%s", slaveInfo.getSlaveID(), e.getMsg().getMessage());
				}
			}catch (SocketException e) {
				//should not happen
				if (isPrimary){
					errors[0] = String.format("@%s:=%s", slaveInfo.getSlaveID(), "this should not happen");
				}else{
					errors[1] = String.format("@%s:=%s", slaveInfo.getSlaveID(), "this should not happen");
				}
				e.printStackTrace();
			}
		}
		
	}
}
