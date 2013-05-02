package edu.berkeley.cs162;

import static org.junit.Assert.*;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

public class TPCMasterTest {

	@Test
	public void test() {
		String slaveInfo = "123192831:localhost:8080";
		String pattern = "(\\d+)@(.+):(\\d+)";
		Pattern p = Pattern.compile(pattern);
		Matcher m = p.matcher(slaveInfo);
		System.out.println(m.matches());
		m.find();
		System.out.println(m.group(1));
		}

}
