package edu.berkeley.cs162;

import static org.junit.Assert.*;

import org.junit.Test;

public class KVCacheTest {

	@Test
	public void testReplacement1() {
		KVCache cache = new KVCache(2,2);
		cache.getWriteLock("key1").lock();
		cache.put("key1", "val1");
		cache.getWriteLock("key1").unlock();
		
		cache.getWriteLock("key2").lock();
		cache.put("key2", "val2");
		cache.getWriteLock("key2").unlock();
		
		cache.getWriteLock("key3").lock();
		cache.put("key3", "val3");
		cache.getWriteLock("key3").unlock();
	
		cache.getWriteLock("key4").lock();
		cache.put("key4", "val4");
		cache.getWriteLock("key4").unlock();

		cache.getReadLock("key1").lock();
		cache.get("key1"); // this should set key1's reference bit
		cache.getReadLock("key1").unlock();
		
		cache.getWriteLock("key5").lock();
		cache.put("key5", "val5");//should replace key3
		cache.getWriteLock("key5").unlock();
		
		cache.getReadLock("key3").lock();
		assertEquals(cache.get("key3"),null);
		cache.getReadLock("key3").unlock();	
	}
	
	@Test
	public void testReplacement3(){
		KVCache cache = new KVCache(1, 2);
		cache.getWriteLock("key1").lock();
		cache.put("key1", "value1");
		cache.getWriteLock("key1").unlock();
		
		cache.getWriteLock("key2").lock();
		cache.put("key2", "value2");		
		cache.getWriteLock("key2").unlock();

		cache.getReadLock("key1").lock();
		cache.get("key1");
		cache.getReadLock("key1").unlock();
		
		cache.getReadLock("key2").lock();
		cache.get("key2");
		cache.getReadLock("key2").unlock();
		
		assertEquals(cache.toXML(),"<?xml version=\"1.0\" encoding=\"UTF-8\"?><KVCache><Set Id=\"0\"><CacheEntry isReferenced=\"true\" isValid=\"true\"><Key>key1</Key><Value>value1</Value></CacheEntry><CacheEntry isReferenced=\"true\" isValid=\"true\"><Key>key2</Key><Value>value2</Value></CacheEntry></Set></KVCache>");
		
		cache.getWriteLock("key3").lock();
		cache.put("key3", "value3");
		cache.getWriteLock("key3").unlock();
		
		System.out.println(cache.toXML());
		assertEquals(cache.toXML(),"<?xml version=\"1.0\" encoding=\"UTF-8\"?><KVCache><Set Id=\"0\"><CacheEntry isReferenced=\"false\" isValid=\"true\"><Key>key2</Key><Value>value2</Value></CacheEntry><CacheEntry isReferenced=\"false\" isValid=\"true\"><Key>key3</Key><Value>value3</Value></CacheEntry></Set></KVCache>");			
	}	
	
	@Test
	public void testReplacement2(){
		KVCache cache = new KVCache(1, 4);

		cache.getWriteLock("key1").lock();
		cache.put("key1", "val1");
		cache.getWriteLock("key1").unlock();
		
		cache.getWriteLock("key2").lock();
		cache.put("key2", "val2");
		cache.getWriteLock("key2").unlock();
		
		cache.getWriteLock("key3").lock();
		cache.put("key3", "val3");
		cache.getWriteLock("key3").unlock();
		
		cache.getWriteLock("key4").lock();
		cache.put("key4", "val4");
		cache.getWriteLock("key4").unlock();

		cache.get("key1"); // this should set key1's reference bit
		cache.get("key3");

		cache.getWriteLock("key5").lock();
		cache.put("key5", "val5");//should replace key3
		cache.getWriteLock("key5").unlock();
		
		assertEquals(cache.get("key2"),null);
		
		cache.get("key3");
		cache.get("key1");
		
  		cache.getWriteLock("key6").lock();
		cache.put("key6", "val6");
		cache.getWriteLock("key6").unlock();
		
		assertEquals(cache.get("key4"), null);
		assertEquals(cache.get("key5"), "val5");

	}
  
  @Test
  public void replacementTest3() {
  }
}
