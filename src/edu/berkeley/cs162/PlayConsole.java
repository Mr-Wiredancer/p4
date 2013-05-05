package edu.berkeley.cs162;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;

/**
 * A console which lets u to play around proj4
 * 
 * If you're using mac/ubuntu/other unix/linux and wanna run from command line
 * 1. go to cs162 directory and run:
 *		"ls -r *.java | grep -v Test | xargs javac" (compile all java files without "Test" substring in filenames)
 *
 * 2. go to src directory and run:
 *		"java edu/berkeley/cs162/PlayConsole"
 *
 * If you're using windows, just click "run" in eclipse
 * @author amos0528
 *
 */
public class PlayConsole implements Debuggable {
	public static SocketServer server = null;
	public static TPCMaster  master = null;	
	public static ArrayList<SocketServer> slaves = new ArrayList<SocketServer>();

	
	private static class SlaveServerThread extends Thread implements Debuggable {
		long slaveId;
		
		public SlaveServerThread(long slaveId) {
			super();
			this.slaveId = slaveId;
			this.setName("SlaveServer"+this.slaveId);
		}
		
		public void run() {
			KVServer keyServer = null;
			SocketServer myServer = null;
			try{
				DEBUG.debug("Binding SlaveServer:");
				keyServer = new KVServer(100, 10);
				myServer = new SocketServer(InetAddress.getLocalHost().getHostAddress());
				TPCMasterHandler handler = new TPCMasterHandler(keyServer, slaveId);
				myServer.addHandler(handler);
				myServer.connect();
				
				// Create TPCLog
				String logPath = slaveId + "@" + myServer.getHostname();
				TPCLog tpcLog = new TPCLog(logPath, keyServer);
				
				// Load from disk and rebuild logs
				tpcLog.rebuildKeyServer();
				
				// Set log for TPCMasterHandler
				handler.setTPCLog(tpcLog);
				
				// Register with the Master. Assuming it always succeeds (not catching).
				handler.registerWithMaster("localhost", myServer);
				
				DEBUG.debug("Starting SlaveServer at " + myServer.getHostname() + ":" + myServer.getPort());
				myServer.run();
				PlayConsole.slaves.add(myServer);
			} catch(Exception e){
				DEBUG.debug("Error occurs, shutting down");
				myServer.stop();
			}
		}	
	}
	
	private static class MasterServerThread extends Thread implements Debuggable {
		int numSlave;
				
		public MasterServerThread(int numSlave) {
			super();
			this.numSlave = numSlave;
			this.setName("MasterServer");
		}
		
		public void run() {
			// Create TPCMaster
			TPCMaster tpcMaster = new TPCMaster(this.numSlave);
			tpcMaster.run();
			PlayConsole.master = tpcMaster;
			
			// Create KVClientHandler
			DEBUG.debug("Binding Master:");
			try {
				SocketServer server = new SocketServer(InetAddress.getLocalHost().getHostAddress(), 8080);
				NetworkHandler handler = new KVClientHandler(tpcMaster);
				server.addHandler(handler);
				server.connect();
				DEBUG.debug("Starting Master "+server.getHostname()+" on "+server.getPort());
				server.run();
				PlayConsole.server = server;
			} catch ( Exception e ) {
				DEBUG.debug("Could not start the master server");
			}

		}	
	}
	
	public static void main(String [] args) {
		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
		String input = null;
		int numSlave = 0;
		
		System.out.println("Enter the number of slaves you want to create:");
		try {
			while ((input=reader.readLine())!=null) {
				try{
					numSlave = Integer.parseInt(input);
					break;
				}catch(Exception e){
					System.out.println("Please enter an integer");
				}
			}
		} catch (IOException e1) {
			handleQuit();
			System.exit(1);
		}
		
		Thread.currentThread().setName("PlayConsole");
		MasterServerThread st = new MasterServerThread(numSlave);
		st.start();
		
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e1) {
		}
		System.out.println("-------------------------------------------------\nCommand List:\n" +
				"	slave <slaveId>\n" +
				"		create a new slave server of <slaveId> and try to register on master\n" +
				"	put <key> <value>\n" +
				"		send a put request to master\n" +
				"	get <key>\n" +
				"		send a get request to master\n" +
				"	del <key>\n" +
				"		send a del request to master\n" +
				"	ignoreNext <address> <port>\n" +
				"		send a ignoreNext request to the designated server on <address>:<port>\n" +
				"	quit\n" +
				"		quit the most powerful, enjoyable, interesting, amazing play console in the world\n" +
				"	slaves\n" +
				"		list the slaveIDs of all the slaves" +
				"-------------------------------------------------");
		try {
			KVClient kc = new KVClient(InetAddress.getLocalHost().getHostAddress(), 8080);
			
			while ((input=reader.readLine())!=null) {
				if (input.equals("quit")) {
					handleQuit();
					System.exit(0);
				}
				
				String [] inputs = input.split(" ");
				
				if (inputs.length < 1 || inputs.length>3) {
					System.out.println("could not recognize ur command");
					continue;
				}
				
				String command = inputs[0];
				if (command.equals("slave")) {
					if (inputs.length!=2){
						System.out.println("could not recognize ur command");
						continue;
					}		
					try{
						long slaveID = Long.parseLong(inputs[1]);

						new SlaveServerThread(slaveID).start();
					}catch(Exception e){
						System.out.println("<slaveID> has to be a long integer");
					}	
				}
				else if (command.equals("put")) {
					handlePut(kc, inputs);
				
				} else if(command.equals("get")) {
					handleGet(kc, inputs);
				} else if(command.equals("del")) {
					handleDel(kc, inputs);
				}else if (command.equals("ignoreNext")){
					handleIgnoreNext(inputs);
				} else {
					System.out.println("could not recognize ur command");
				}
			}
		} catch(Exception e) {
			System.out.println("Something wrong with the play console");
			handleQuit();
			System.exit(1);
		}
	}
	
	private static void handleIgnoreNext(String[] inputs) {
		if (inputs.length!=2){
			System.out.println("could not recognize ur command");
			return;
		}
		try{
			long slaveId = Long.parseLong(inputs[1]);
			String host = PlayConsole.master.getSlaveHost(slaveId);
			int port = PlayConsole.master.getSlavePort(slaveId);
			if (host==null){
				System.out.println("could not find this slave");
				return;
			}
			KVClient kc = new KVClient(host, port);
			kc.ignoreNext();
		}catch(KVException e){
		}
		
	}

	private static void handleDel(KVClient kc, String[] inputs) {
		if (inputs.length!=2){
			System.out.println("could not recognize ur command");
			return;
		}
		try{
		kc.del(inputs[1]);	
		}catch(KVException e){
		}
	}

	private static void handleGet(KVClient kc, String[] inputs) {
		if (inputs.length!=2){
			System.out.println("could not recognize ur command");
			return;
		}
		try{
		kc.get(inputs[1]);	
		}catch(KVException e){
		}
	}

	private static void handlePut(KVClient kc, String[] inputs) {
		if (inputs.length!=3){
			System.out.println("could not recognize ur command");
			return;
		}
		try{
		kc.put(inputs[1],inputs[2]);
		}catch (KVException e){
		}
	}

	public static void handleQuit() {
		System.out.println("quiting");
		if (PlayConsole.server!=null) {
			PlayConsole.server.stop();	
		}
		if (PlayConsole.master!=null){
			PlayConsole.master.stop();
		}
		for (SocketServer s : PlayConsole.slaves){
			s.stop();
		}
	}
}
