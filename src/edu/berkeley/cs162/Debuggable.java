package edu.berkeley.cs162;

public interface Debuggable {
	
	public static DefaultDebuggable DEBUG = new DefaultDebuggable();
				
	class DefaultDebuggable implements Debuggable{ 
	
		public void debug(String s){
			System.out.println(Thread.currentThread().getName()+": "+s);
		}
	}
}
