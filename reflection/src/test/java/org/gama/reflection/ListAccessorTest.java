package org.gama.reflection;

import java.util.List;

import org.gama.lang.collection.Arrays;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Guillaume Mary
 */
public class ListAccessorTest {
	
	@Test
	public void testGet() throws Exception {
		ListAccessor<List<String>, String> testInstance = new ListAccessor<>();
		List<String> sample = Arrays.asList("a", "b", "c");
		
		testInstance.setIndex(0);
		assertEquals("a", testInstance.get(sample));
		testInstance.setIndex(1);
		assertEquals("b", testInstance.get(sample));
		testInstance.setIndex(2);
		assertEquals("c", testInstance.get(sample));
	}
	
	@Test(expected = ArrayIndexOutOfBoundsException.class)
	public void testGet_ArrayIndexOutOfBoundsException() throws Exception {
		ListAccessor<List<String>, String> testInstance = new ListAccessor<>();
		List<String> sample = Arrays.asList("a", "b", "c");
		
		testInstance.setIndex(-1);
		assertEquals("a", testInstance.get(sample));
	}
}