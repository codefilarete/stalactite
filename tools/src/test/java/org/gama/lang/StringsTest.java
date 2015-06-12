package org.gama.lang;

import org.testng.annotations.Test;

import static org.junit.Assert.*;


/**
 * @author Guillaume Mary
 */
public class StringsTest {
	
	@Test
	public void testIsEmpty() {
		assertFalse(Strings.isEmpty("a"));
		assertTrue(Strings.isEmpty(""));
		assertTrue(Strings.isEmpty(null));
	}
	
	@Test
	public void testCapitalize() {
		assertEquals("Bonjour", Strings.capitalize("bonjour"));
		assertEquals("BONJOUR", Strings.capitalize("BONJOUR"));
		assertEquals("", Strings.capitalize(""));
		assertEquals(null, Strings.capitalize(null));
	}
	
	@Test
	public void testHead() {
		assertEquals("sn", Strings.head("snake", 2));
		assertEquals("sna", Strings.head("snake", 3));
		assertEquals("snake", Strings.head("snake", 42));
		assertEquals("", Strings.head("snake", -42));
		assertEquals(null, Strings.head(null, 2));
	}
	
	@Test
	public void testTail() {
		assertEquals("ke", Strings.tail("snake", 2));
		assertEquals("ake", Strings.tail("snake", 3));
		assertEquals("snake", Strings.tail("snake", 42));
		assertEquals("", Strings.tail("snake", -42));
		assertEquals(null, Strings.tail(null, 2));
	}
	
	@Test
	public void testRepeat() {
		String s10 = "aaaaaaaaaa";
		String s5 = "bbbbb";
		String s = "c";
		// we know if the optimization is good as we give pre-concatenated Strings different from each other
		assertEquals("ccc", Strings.repeat(3, s).toString());
		assertEquals("cccccccc", Strings.repeat(8, s).toString());
		assertEquals("ccc", Strings.repeat(3, s, s10, s5).toString());
		assertEquals("bbbbb", Strings.repeat(5, s, s10, s5).toString());
		assertEquals("bbbbbccc", Strings.repeat(8, s, s10, s5).toString());
		assertEquals("aaaaaaaaaacc", Strings.repeat(12, s, s10, s5).toString());
		assertEquals("aaaaaaaaaabbbbbcc", Strings.repeat(17, s, s10, s5).toString());
		assertEquals("aaaaaaaaaaaaaaaaaaaabbbbbcc", Strings.repeat(27, s, s10, s5).toString());
		// inverting order of pre-concatenated Strings may work also but lessly optimized
		assertEquals("bbbbbbbbbbbbbbbbbbbbbbbbbcc", Strings.repeat(27, s, s5, s10).toString());
	}
}