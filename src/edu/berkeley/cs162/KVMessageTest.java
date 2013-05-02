package edu.berkeley.cs162;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.junit.Test;

public class KVMessageTest {
	@Test
	public void constructorFromInputTest(){
		try {
			KVMessage msg = new KVMessage(KVMessage.GETTYPE);
			msg.setKey("key1");
			msg.setValue("val1");
      assertEquals("key1", msg.getKey());
      assertEquals("val1", msg.getValue());
		} catch(KVException e) {
			fail();
		}
	}
	
  @Test
  public void testKVMessageConstructionFromCorrectXML() {
    try {
    	String resp1 = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><KVMessage type=\"resp\"><Message>this is a test msg</Message></KVMessage>";
		ByteArrayInputStream in = new ByteArrayInputStream(resp1.getBytes());
			
		KVMessage newMsg = new KVMessage(in);
		assertEquals(newMsg.getMsgType(), KVMessage.RESPTYPE);
		assertEquals(newMsg.getMessage(), "this is a test msg");
		assertEquals(newMsg.getValue(), null);
		assertEquals(newMsg.getKey(), null);
		
		String tpcPut = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><KVMessage type=\"putreq\"><Key>key</Key><Value>value</Value><TPCOpId>10010101</TPCOpId></KVMessage>";
		in = new ByteArrayInputStream(tpcPut.getBytes());
		newMsg = new KVMessage(in);
		assertEquals(newMsg.getMsgType(), KVMessage.PUTTYPE);
		assertEquals(newMsg.getValue(), "value");
		assertEquals(newMsg.getKey(), "key");
		assertEquals(newMsg.getTpcOpId(), "10010101");

		String tpcDel = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><KVMessage type=\"delreq\"><Key>key</Key><TPCOpId>10010101</TPCOpId></KVMessage>";
		in = new ByteArrayInputStream(tpcDel.getBytes());
		newMsg = new KVMessage(in);
		assertEquals(newMsg.getMsgType(), KVMessage.DELTYPE);
		assertEquals(newMsg.getKey(), "key");
		assertEquals(newMsg.getTpcOpId(), "10010101");

		String ready = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><KVMessage type=\"ready\"><TPCOpId>10010101</TPCOpId></KVMessage>";
		in = new ByteArrayInputStream(ready.getBytes());
		newMsg = new KVMessage(in);
		assertEquals(newMsg.getMsgType(), KVMessage.READYTYPE);
		assertEquals(newMsg.getTpcOpId(), "10010101");
		
		String abort = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><KVMessage type=\"abort\"><Message>testing</Message><TPCOpId>10010101</TPCOpId></KVMessage>";
		in = new ByteArrayInputStream(abort.getBytes());
		newMsg = new KVMessage(in);
		assertEquals(newMsg.getMessage(), "testing");
		assertEquals(newMsg.getMsgType(), KVMessage.ABORTTYPE);
		assertEquals(newMsg.getTpcOpId(), "10010101");
		
		String abortdec = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><KVMessage type=\"abort\"><TPCOpId>10010101</TPCOpId></KVMessage>";
		in = new ByteArrayInputStream(abortdec.getBytes());
		newMsg = new KVMessage(in);
		assertEquals(newMsg.getMessage(), null);
		assertEquals(newMsg.getMsgType(), KVMessage.ABORTTYPE);
		assertEquals(newMsg.getTpcOpId(), "10010101");
		
		String commit = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><KVMessage type=\"commit\"><TPCOpId>10010101</TPCOpId></KVMessage>";
		in = new ByteArrayInputStream(commit.getBytes());
		newMsg = new KVMessage(in);
		assertEquals(newMsg.getMessage(), null);
		assertEquals(newMsg.getMsgType(), KVMessage.COMMITTYPE);
		assertEquals(newMsg.getTpcOpId(), "10010101");
		
		String ack = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><KVMessage type=\"ack\"><TPCOpId>10010101</TPCOpId></KVMessage>";
		in = new ByteArrayInputStream(ack.getBytes());
		newMsg = new KVMessage(in);
		assertEquals(newMsg.getMessage(), null);
		assertEquals(newMsg.getMsgType(), KVMessage.ACKTYPE);
		assertEquals(newMsg.getTpcOpId(), "10010101");
		
		String register = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><KVMessage type=\"register\"><Message>10010101</Message></KVMessage>";
		in = new ByteArrayInputStream(register.getBytes());
		newMsg = new KVMessage(in);
		assertEquals(newMsg.getTpcOpId(), null);
		assertEquals(newMsg.getMsgType(), KVMessage.REGISTERTYPE);
		assertEquals(newMsg.getMessage(), "10010101");
		
		String ignoreNext = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><KVMessage type=\"ignoreNext\"/>";
		in = new ByteArrayInputStream(ignoreNext.getBytes());
		newMsg = new KVMessage(in);
		assertEquals(newMsg.getTpcOpId(), null);
		assertEquals(newMsg.getMsgType(), KVMessage.IGNORENEXTTYPE);
		assertEquals(newMsg.getMessage(), null);
    }
    catch (KVException e) {
    	e.printStackTrace();
      fail();
    }
  }
  
  @Test
  public void testToXML() {
     try {
      KVMessage msg = new KVMessage(KVMessage.GETTYPE);
			msg.setKey("key1");
			msg.setValue("val1");
		      assertEquals("key1", msg.getKey());
		      assertEquals("val1", msg.getValue());
      
			String xml = msg.toXML();
			InputStream in = new ByteArrayInputStream(xml.getBytes());			
			KVMessage newMsg = new KVMessage(in);
			assertEquals(newMsg.getKey(), "key1");
     }
     catch (KVException e) {
      fail();
     }
  }
  
	@Test(expected = KVException.class)
	public void getTest1() throws KVException {
		KVMessage msg = new KVMessage(KVMessage.GETTYPE);
		msg.toXML();
	}
	
	@Test
	public void buildXMLFailureTest(){
		try {
			KVMessage msg = new KVMessage(KVMessage.GETTYPE);
			msg.toXML();
		} catch(KVException e) {
			assertEquals(e.getMsg().getMessage(), "Unknown Error: the key is null");
		}
		
		try {
			KVMessage msg = new KVMessage(KVMessage.GETTYPE);
			msg.setKey("key1");
			System.out.println(msg.toXML());
		} catch(KVException e) {
			fail();
		}
		
		try {
			KVMessage msg = new KVMessage(KVMessage.PUTTYPE);
			msg.setValue("value1");
			msg.toXML();
		} catch(KVException e) {
			assertEquals(e.getMsg().getMessage(), "Unknown Error: the key is null");
		}
		
		try {
			KVMessage msg = new KVMessage(KVMessage.PUTTYPE);
			msg.setMessage("value1");
			msg.toXML();
		} catch(KVException e) {
			assertEquals(e.getMsg().getMessage(), "Unknown Error: the key is null");
		}
		
		try{
			KVMessage msg = new KVMessage(KVMessage.DELTYPE);
			msg.setKey("key1");
			msg.setTpcOpId("123123123");
		} catch(KVException e){
			fail();
		}
		
		try {
			KVMessage msg = new KVMessage(KVMessage.RESPTYPE);
			msg.setMessage("value1");
			msg.toXML();
		} catch(KVException e) {
			fail();
		}
		
		try {
			KVMessage msg = new KVMessage(KVMessage.GETTYPE);
			msg.setKey("1234567812345678123456781234567812345678123456781234567812345678123456781234567812345678123456781234567812345678123456781234567812345678123456781234567812345678123456781234567812345678123456781234567812345678123456781234567812345678123456781234567812345678123456781234567812345678123456781234567812345678123456781234567812345678123456781234567812345678123456781234567812345678123456781234567812345678123456781234567812345678123456781234567812345678123456781234567812345678123456781234567812345678123456781234567812345678123456781234567812345678123456781234567812345678123456781234567812345678123456781234567812345678123456781234567812345678123456781234567812345678123456781234567812345678123456781234567812345678123456781234567812345678");
			msg.toXML();
		} catch(KVException e) {
			assertEquals(e.getMsg().getMessage(), "Oversized key");
		}
	}
}
