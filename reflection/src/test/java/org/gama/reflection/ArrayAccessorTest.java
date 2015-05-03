package org.gama.reflection;

import static org.junit.Assert.*;

import org.testng.annotations.Test;

/**
 * @author Guillaume Mary
 */
public class ArrayAccessorTest {
	
	@Test
	public void testGet() throws Exception {
		ArrayAccessor<String> testInstance = new ArrayAccessor<>();
		String[] sample = { "a", "b", "c" };
		
		testInstance.setIndex(0);
		assertEquals("a", testInstance.get(sample));
		testInstance.setIndex(1);
		assertEquals("b", testInstance.get(sample));
		testInstance.setIndex(2);
		assertEquals("c", testInstance.get(sample));
	}
	
	@Test(expectedExceptions = ArrayIndexOutOfBoundsException.class)
	public void testGet_ArrayIndexOutOfBoundsException() throws Exception {
		ArrayAccessor<String> testInstance = new ArrayAccessor<>();
		String[] sample = { "a", "b", "c" };
		
		testInstance.setIndex(-1);
		assertEquals("a", testInstance.get(sample));
	}
}