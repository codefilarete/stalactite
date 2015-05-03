package org.gama.lang;

import static org.junit.Assert.*;

import org.testng.annotations.Test;


/**
 * @author Guillaume Mary
 */
public class StringsTest {
	
	@Test
	public void testIsEmpty() throws Exception {
		assertFalse(Strings.isEmpty("a"));
		assertTrue(Strings.isEmpty(""));
		assertTrue(Strings.isEmpty(null));
	}
	
	@Test
	public void testCapitalize() throws Exception {
		assertEquals("Bonjour", Strings.capitalize("bonjour"));
		assertEquals("BONJOUR", Strings.capitalize("BONJOUR"));
		assertEquals("", Strings.capitalize(""));
		assertEquals(null, Strings.capitalize(null));
	}
	
	@Test
	public void testHead() throws Exception {
		assertEquals("sn", Strings.head("snake", 2));
		assertEquals("sna", Strings.head("snake", 3));
		assertEquals("snake", Strings.head("snake", 42));
		assertEquals("", Strings.head("snake", -42));
		assertEquals(null, Strings.head(null, 2));
	}
	
	@Test
	public void testTail() throws Exception {
		assertEquals("ke", Strings.tail("snake", 2));
		assertEquals("ake", Strings.tail("snake", 3));
		assertEquals("snake", Strings.tail("snake", 42));
		assertEquals("", Strings.tail("snake", -42));
		assertEquals(null, Strings.tail(null, 2));
	}
}