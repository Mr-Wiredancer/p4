package edu.berkeley.cs162;

import static org.junit.Assert.*;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;

import org.junit.Test;

public class TPCLogTest {
/**
 * TODO:
 * 1 orderTest:
 * 	example: orderTest1()
 * 2 abortTest:
 * 	example: abortTest1()
 * 3 commitTest:
 * 	example: orderTest1()
 * 4 mixedTest:
 * 	
 */
	
	@Test
	public void simpleTest() {
		KVServer kvServer = new KVServer(10, 10);
		TPCLog tpcLog = new TPCLog("logFile1", kvServer);
		
		try {
			tpcLog.rebuildKeyServer();
			
			KVMessage msg1 = new KVMessage(KVMessage.PUTTYPE);
			msg1.setKey("key1");
			msg1.setValue("value1");
			msg1.setTpcOpId("1001");
			KVMessage msg2 = new KVMessage(KVMessage.COMMITTYPE);
			msg2.setTpcOpId("1001");
			
			//a message not executed
			KVMessage msg3 = new KVMessage(KVMessage.PUTTYPE);
			msg3.setKey("key3");
			msg3.setValue("value3");
			msg3.setTpcOpId("1002");
			
			tpcLog.appendAndFlush(msg1);
			tpcLog.appendAndFlush(msg2);
			tpcLog.appendAndFlush(msg3);
			
			KVServer kvServer2 = new KVServer(10, 10);
			TPCLog tpcLog2 = new TPCLog("logFile1", kvServer2);
			tpcLog2.rebuildKeyServer();
			assertEquals(kvServer2.get("key1"), "value1");
			try{
				kvServer2.get("key3");
			} catch (KVException e){
				assertEquals(e.getMsg().getMessage(), "Does not exist");
			}
			
			HashMap<String, KVMessage> ops = tpcLog2.getInterruptedTpcOperations();
			assertEquals(ops.size(), 1);
			assertEquals(ops.get("1002").getKey(), "key3");
		} catch (KVException e) {
			fail();
		}
		
	}
	
  @Test 
  /**
   * Build from a empty log file. store and cache should all be empty. intterrruptedOperations should be null and entries should be empty
   */
  	public void testBuildEmpty(){
	  	KVServer kvServer = new KVServer(10, 10);
	  	TPCLog tpcLog = new TPCLog("emptyLog", kvServer);

		try {
			tpcLog.rebuildKeyServer();
			assertEquals(tpcLog.getInterruptedTpcOperations(), null);
			assertEquals(tpcLog.getEntries().size(), 0);
		
			try{
				kvServer.get("key1");
				assertEquals(1,2);
			} catch (KVException e){
				assertEquals(1,1);
			}
	
			try{
				kvServer.get("key3");
				assertEquals(1,2);
			} catch (KVException e){
				assertEquals(1,1);
			}		
		
		} catch (KVException e) {
			fail();
		}
  	}
  
  @Test 
  /**
   * Build from a non-empty log file. 
   */
  	public void testBuildNotEmpty(){
		try {
			KVMessage msg1 = new KVMessage(KVMessage.PUTTYPE);
			msg1.setKey("key1");
			msg1.setValue("value1");
			msg1.setTpcOpId("1001");
			KVMessage msg2 = new KVMessage(KVMessage.PUTTYPE);
			msg2.setKey("key2");
			msg2.setValue("value2");
			msg2.setTpcOpId("1002");
			KVMessage msg3 = new KVMessage(KVMessage.PUTTYPE);
			msg3.setKey("key3");
			msg3.setValue("value3");
			msg3.setTpcOpId("1003");
			KVMessage msg4 = new KVMessage(KVMessage.PUTTYPE);
			msg4.setKey("key4");
			msg4.setValue("value4");
			msg4.setTpcOpId("1004");
			KVMessage msg5 = new KVMessage(KVMessage.PUTTYPE);
			msg5.setKey("key5");
			msg5.setValue("value5");
			msg5.setTpcOpId("1005");
			KVMessage msg6 = new KVMessage(KVMessage.PUTTYPE);
			msg6.setKey("key6");
			msg6.setValue("value6");
			msg6.setTpcOpId("1006");
			
			KVMessage msg7 = new KVMessage(KVMessage.DELTYPE);
			msg7.setKey("key7");
			msg7.setTpcOpId("1007");
			
			KVMessage msg8 = new KVMessage(KVMessage.PUTTYPE);
			msg8.setKey("key8");
			msg8.setValue("value8");
			msg8.setTpcOpId("1008");
			KVMessage msg9 = new KVMessage(KVMessage.DELTYPE);
			msg9.setKey("key8");
			msg9.setTpcOpId("1009");
			
			KVMessage msg10 = new KVMessage(KVMessage.PUTTYPE);
			msg10.setKey("key10");
			msg10.setValue("value10");
			msg10.setTpcOpId("10010");
			
			KVMessage commit1 = new KVMessage(KVMessage.COMMITTYPE);
			commit1.setTpcOpId("1001");
			KVMessage commit2 = new KVMessage(KVMessage.COMMITTYPE);
			commit2.setTpcOpId("1002");
			KVMessage commit3 = new KVMessage(KVMessage.COMMITTYPE);
			commit3.setTpcOpId("1003");
			KVMessage commit4 = new KVMessage(KVMessage.COMMITTYPE);
			commit4.setTpcOpId("1004");
			KVMessage commit5 = new KVMessage(KVMessage.COMMITTYPE);
			commit5.setTpcOpId("1005");
			KVMessage commit6 = new KVMessage(KVMessage.COMMITTYPE);
			commit6.setTpcOpId("1006");
			
			KVMessage commit7 = new KVMessage(KVMessage.ABORTTYPE);
			commit7.setTpcOpId("1007");
			KVMessage commit8 = new KVMessage(KVMessage.COMMITTYPE);
			commit8.setTpcOpId("1008");
			KVMessage commit9 = new KVMessage(KVMessage.COMMITTYPE);
			commit9.setTpcOpId("1009");
			
			KVMessage commit10 = new KVMessage(KVMessage.ABORTTYPE);
			commit10.setTpcOpId("10010");
			
			ArrayList<KVMessage> entries = new ArrayList<KVMessage>();
			entries.add(msg1);
			entries.add(msg2);
			entries.add(msg3);
			entries.add(msg4);
			entries.add(msg5);
			entries.add(msg6);
			entries.add(msg7);
			entries.add(msg8);
			entries.add(msg9);
			entries.add(msg10);
			entries.add(commit4);
			entries.add(commit1);
			entries.add(commit7);
			entries.add(commit8);
			entries.add(commit9);
			entries.add(commit5);
			entries.add(commit10);
			entries.add(commit6);
			entries.add(commit2);
			entries.add(commit3);
			
			
			
			String fileName = "notEmptyLog";
			this.writeEntries(fileName, entries);

			KVServer kvServer = new KVServer(10, 10);
			TPCLog tpcLog = new TPCLog(fileName, kvServer);
			tpcLog.rebuildKeyServer();

			for (int i = 1; i<=6; i++){
				assertEquals(kvServer.get("key"+i), "value"+i);
			}
			
			try{
				kvServer.get("key8");
				assertEquals(1,2);
			} catch (KVException e){
				assertEquals(1,1);
			}
			
			try{
				kvServer.get("key10");
				assertEquals(1,2);
			} catch (KVException e){
				assertEquals(1,1);
			}
			
			try{
				kvServer.get("key7");
				assertEquals(1,2);
			} catch (KVException e){
				assertEquals(1,1);
			}
		
		} catch (KVException e) {
			fail();
		}
  	}
  
  	/**
  	 * helper method to write a log file used by rebuildKeyserver
  	 * @param fileName
  	 * @param entries
  	 */
	public void writeEntries(String fileName, ArrayList<KVMessage> entries){
		ObjectOutputStream outputStream = null;
		
  		try {
			outputStream = new ObjectOutputStream(new FileOutputStream(fileName));
			outputStream.writeObject(entries);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (outputStream != null) {
					outputStream.flush();
					outputStream.close();
				}
			} catch (IOException e) {				
				e.printStackTrace();
			}
		}	  
	}
  
  	@Test
  	/**
  	 * order: put1, put2, commit2, commit1
  	 */
  	public void orderTest1(){
		try {
			KVMessage msg1 = new KVMessage(KVMessage.PUTTYPE);
			msg1.setKey("key1");
			msg1.setValue("value1");
			msg1.setTpcOpId("1001");
			KVMessage msg2 = new KVMessage(KVMessage.PUTTYPE);
			msg2.setKey("key1");
			msg2.setValue("value2");
			msg2.setTpcOpId("1002");
			
			KVMessage commit1 = new KVMessage(KVMessage.COMMITTYPE);
			commit1.setTpcOpId("1001");
			KVMessage commit2 = new KVMessage(KVMessage.COMMITTYPE);
			commit2.setTpcOpId("1002");
			
			ArrayList<KVMessage> entries = new ArrayList<KVMessage>();
			entries.add(msg1);
			entries.add(msg2);
			entries.add(commit2);
			entries.add(commit1);
			
			String fileName = "orderTest1";
			
			this.writeEntries(fileName, entries);
			
			KVServer kvServer = new KVServer(10, 10);
			TPCLog tpcLog = new TPCLog(fileName, kvServer);
			
			tpcLog.rebuildKeyServer();
			
			assertEquals(kvServer.get("key1"), "value1");

		} catch (KVException e) {
			fail();
		}
  	}
  	  	
  	@Test
  	public void abortTest1(){
  		try {
			KVMessage msg1 = new KVMessage(KVMessage.PUTTYPE);
			msg1.setKey("key1");
			msg1.setValue("value1");
			msg1.setTpcOpId("1001");
			KVMessage msg2 = new KVMessage(KVMessage.PUTTYPE);
			msg2.setKey("key1");
			msg2.setValue("value2");
			msg2.setTpcOpId("1002");
			
			KVMessage commit1 = new KVMessage(KVMessage.ABORTTYPE);
			commit1.setTpcOpId("1001");
			KVMessage commit2 = new KVMessage(KVMessage.ABORTTYPE);
			commit2.setTpcOpId("1002");
			
			ArrayList<KVMessage> entries = new ArrayList<KVMessage>();
			entries.add(msg1);
			entries.add(msg2);
			entries.add(commit2);
			entries.add(commit1);
			
			String fileName = "abortTest1";
			
			this.writeEntries(fileName, entries);
			
			KVServer kvServer = new KVServer(10, 10);
			TPCLog tpcLog = new TPCLog(fileName, kvServer);
			
			tpcLog.rebuildKeyServer();
			
			try{
				kvServer.get("key1");
				assertEquals(1,2);
			} catch (KVException e){
				assertEquals(1,1);
			}
			
			assertEquals(tpcLog.getEntries().size(), 4);
			assertEquals(tpcLog.getInterruptedTpcOperations(), null);
		} catch (KVException e) {
			fail();
		}
  	}
  


}
