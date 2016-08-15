package org.gama.reflection;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Guillaume Mary
 */
public class ArrayAccessorTest {
	
	@Test
	public void testGet() {
		ArrayAccessor<String> testInstance = new ArrayAccessor<>();
		String[] sample = { "a", "b", "c" };
		
		testInstance.setIndex(0);
		assertEquals("a", testInstance.get(sample));
		testInstance.setIndex(1);
		assertEquals("b", testInstance.get(sample));
		testInstance.setIndex(2);
		assertEquals("c", testInstance.get(sample));
	}
	
	@Test(expected = ArrayIndexOutOfBoundsException.class)
	public void testGet_ArrayIndexOutOfBoundsException() {
		ArrayAccessor<String> testInstance = new ArrayAccessor<>();
		String[] sample = { "a", "b", "c" };
		
		testInstance.setIndex(-1);
		assertEquals("a", testInstance.get(sample));
	}
}