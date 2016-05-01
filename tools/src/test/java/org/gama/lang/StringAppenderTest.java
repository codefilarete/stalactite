package org.gama.lang;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Guillaume Mary
 */
public class StringAppenderTest {
	
	@Test
	public void testCat() {
		// test all kind of cat signature
		StringAppender testInstance = new StringAppender();
		testInstance.cat("a").cat("b").cat("c", "d").cat("e", "f", "g");
		assertEquals("abcdefg", testInstance.toString());
	}
	
	@Test
	public void testWrap() {
		// simple use case
		StringAppender testInstance = new StringAppender();
		testInstance.cat("a").wrap("#", "$");
		assertEquals("#a$", testInstance.toString());
		
		// test on empty appender
		testInstance = new StringAppender();
		testInstance.wrap("#", "$");
		assertEquals("#$", testInstance.toString());
	}
	
	@Test
	public void testCcat() {
		StringAppender testInstance = new StringAppender();
		testInstance.ccat(1, 2, "a", ",");
		assertEquals("1,2,a", testInstance.toString());
	}
	
	@Test
	public void testCutTail() {
		StringAppender testInstance = new StringAppender();
		testInstance.cat("snake tail").cutTail(5);
		assertEquals("snake", testInstance.toString());
	}
	
	@Test
	public void testCutHead() {
		StringAppender testInstance = new StringAppender();
		testInstance.cat("headache").cutHead(4);
		assertEquals("ache", testInstance.toString());
	}
	
	@Test
	public void testCutAndInsert() {
		StringAppender testInstance = new StringAppender();
		testInstance.cat("headache").cutHead(4).catAt(0, "sstom").cutHead(1);
		assertEquals("stomache", testInstance.toString());
	}
}