package edu.berkeley.cs162;

import static org.junit.Assert.*;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

public class TPCMasterTest {

	@Test
	public void slaveInfoParsingTest() {
		String pattern = "(\\d+)@(.+):(\\d+)";
		
		String slaveInfo = "123192831@132:242:32.13:8080";
		Pattern p = Pattern.compile(pattern);
		Matcher m = p.matcher(slaveInfo);
		assertEquals(m.matches(), true);
		assertEquals(m.group(1), "123192831");
		assertEquals(m.group(2), "132:242:32.13");
		assertEquals(m.group(3), "8080");
		
		slaveInfo = "98792873@localhost:::::8080";
		m = Pattern.compile(pattern).matcher(slaveInfo);
		assertEquals(m.matches(), true);
		assertEquals(m.group(1), "98792873");
		assertEquals(m.group(2), "localhost::::");
		assertEquals(m.group(3), "8080");
		
		slaveInfo = "98792873@@@@lo:ca:lh@@ost:::::8080";
		m = Pattern.compile(pattern).matcher(slaveInfo);
		assertEquals(m.matches(), true);
		assertEquals(m.group(1), "98792873");
		assertEquals(m.group(2), "@@@lo:ca:lh@@ost::::");
		assertEquals(m.group(3), "8080");
		
		slaveInfo = "123@localhost@8080";
		m = Pattern.compile(pattern).matcher(slaveInfo);
		assertEquals(m.matches(), false);
		
		slaveInfo = "123@localhost:8a80";
		m = Pattern.compile(pattern).matcher(slaveInfo);
		assertEquals(m.matches(), false);
		
		slaveInfo = "12aa3@localhost:8080";
		m = Pattern.compile(pattern).matcher(slaveInfo);
		assertEquals(m.matches(), false);
		
		slaveInfo = "12aa3@localhost:8080";
		m = Pattern.compile(pattern).matcher(slaveInfo);
		assertEquals(m.matches(), false);
		
		slaveInfo = "@localhost:";
		m = Pattern.compile(pattern).matcher(slaveInfo);
		assertEquals(m.matches(), false);
		
		slaveInfo = "@localhost:80";
		m = Pattern.compile(pattern).matcher(slaveInfo);
		assertEquals(m.matches(), false);
		
		slaveInfo = "123@localhost:";
		m = Pattern.compile(pattern).matcher(slaveInfo);
		assertEquals(m.matches(), false);
	}

}
