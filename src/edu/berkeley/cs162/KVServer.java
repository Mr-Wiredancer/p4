/**
 * Slave Server component of a KeyValue store
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

import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

/**
 * This class defines the slave key value servers. Each individual KVServer 
 * would be a fully functioning Key-Value server. For Project 3, you would 
 * implement this class. For Project 4, you will have a Master Key-Value server 
 * and multiple of these slave Key-Value servers, each of them catering to a 
 * different part of the key namespace.
 *
 */
public class KVServer implements KeyValueInterface {
	private KVStore dataStore = null;
	private KVCache dataCache = null;
	
	private static final int MAX_KEY_SIZE = 256;
	private static final int MAX_VAL_SIZE = 256 * 1024;
	
	/**
	 * @param numSets number of sets in the data Cache.
	 */
	public KVServer(int numSets, int maxElemsPerSet) {
		dataStore = new KVStore();
		dataCache = new KVCache(numSets, maxElemsPerSet);

		AutoGrader.registerKVServer(dataStore, dataCache);
	}
	
	/**
	 * Tries to put <key, value> in the server. First call cache's put(will replace if necessary) and then server's put.
	 * @param key
	 * @param value
	 * @throws KVException when key or value or both didn't pass sanity check. 
	 */
	public void put(String key, String value) throws KVException {
		// Must be called before anything else
		AutoGrader.agKVServerPutStarted(key, value);
	
		try {
			//sanity check on key and value
			CheckHelper.sanityCheckKeyValue(key, value);
		
			WriteLock lock = this.dataCache.getWriteLock(key);
			lock.lock();
			this.dataCache.put(key, value);
			lock.unlock();
			this.dataStore.put(key, value);

		} finally {
			AutoGrader.agKVServerPutFinished(key, value);
		}
	}
	
	/**
	 * Tries to get the value mapped to key. First cal cache's GET and then store's GET. Will do an entry replacement if necessary
	 * @param key
	 * @throws KVException when the key is not in store. Or key doesn't pass sanity check.
	 */
	public String get (String key) throws KVException {
		// Must be called before anything else
		AutoGrader.agKVServerGetStarted(key);
		
		try {
			//sanity check on key
			CheckHelper.sanityCheckKey(key);
			
			//try to get the value in cache
			WriteLock lock = this.dataCache.getWriteLock(key);
			lock.lock();
			String cacheValue = this.dataCache.get(key);
			lock.unlock();
			if (cacheValue!=null) {
				return cacheValue; //directly return the value if the value is in cache
			}
			
			//key is not in cache, try to get the value in the store
			String storeResult = this.dataStore.get(key); //this will throw KVException if key is not in store
			this.dataCache.replace(key, storeResult);
			return storeResult;

		} finally {
			AutoGrader.agKVServerGetFinished(key);
		}
	}
	
	/**
	 * Delete the value mapped to key. It calles the cache's DEL and then store's DEL
	 * @param key
	 * @throws KVException when key is not in store. Or key doesn't pass sanity check.
	 */
	public void del (String key) throws KVException {
		// Must be called before anything else
		AutoGrader.agKVServerDelStarted(key);

		try {
			//sanity check on key
			CheckHelper.sanityCheckKey(key);
			
			WriteLock lock = this.dataCache.getWriteLock(key);
			lock.lock();
			this.dataCache.del(key);
			lock.unlock();
			this.dataStore.del(key); //will throw KVException if is not in  
		
		} finally {
			AutoGrader.agKVServerDelFinished(key);
		}
	}
	
	/**
	 * Only for testing
	 * @return XML representation of store
	 */
	public String dumpStore() {
		try {
			return this.dataStore.toXML();
		} catch (KVException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Only for testing
	 * @return XML representation of cache
	 */
	public String dumpCache(){
		return this.dataCache.toXML();
	}
	
	public boolean hasKey (String key) {
		try {
			this.dataStore.get(key);
			return true;
		} catch (KVException e) {
			return false;
		}
	}
}
