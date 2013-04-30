package edu.berkeley.cs162;

/**
 * 
 * @author amos0528
 *
 */
public class CheckHelper implements Debuggable {
	
	/**
	 * sanity check on key and value. check null, empty and length
	 * @param key
	 * @param value
	 * @throws KVException
	 */
	public static void sanityCheckKeyValue(String key, String value) throws KVException{
		sanityCheckKey(key);
		sanityCheckValue(value);
	}
	
	/**
	 * check if the key is null, longer than max length or empty
	 * @param key
	 * @throws KVException
	 */
	public static void sanityCheckKey(String key) throws KVException{
		if (key==null){
			DEBUG.debug("The key is null");
			throw new KVException( new KVMessage(KVMessage.RESPTYPE, "Unknown Error: the key is null") );
		}
		int l = key.length();
		if ( l > KVMessage.MAX_KEY_LENGTH){
			DEBUG.debug("key is oversized");
			throw new KVException(new KVMessage(KVMessage.RESPTYPE, "Oversized key"));
		}
		if( l==0 ){
			DEBUG.debug(" key is empty ");
			throw new KVException( new KVMessage(KVMessage.RESPTYPE, "Unknown Error: the key is empty") );
		}
	}

	/**
	 * check if the value null, longer than max length or empty
	 * @param value
	 * @throws KVException
	 */
	public static void sanityCheckValue(String value) throws KVException{
		if (value==null){
			DEBUG.debug("The value is null");
			throw new KVException( new KVMessage(KVMessage.RESPTYPE, "Unknown Error: the key is null") );
		}
		int l = value.length();
		if ( l > KVMessage.MAX_VALUE_LENGTH){
			DEBUG.debug("The value is oversized");
			throw new KVException(new KVMessage(KVMessage.RESPTYPE, "Oversized value"));
		}
		if( l==0 ){
			DEBUG.debug("The value is empty ");
			throw new KVException( new KVMessage(KVMessage.RESPTYPE, "Unknown Error: the value is empty") );
		}
	}
	
}
