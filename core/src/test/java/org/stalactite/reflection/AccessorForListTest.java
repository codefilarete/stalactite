package org.stalactite.reflection;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.stalactite.lang.collection.Arrays;
import org.testng.annotations.Test;

/**
 * @author Guillaume Mary
 */
public class AccessorForListTest {
	
	@Test
	public void testGet() throws Exception {
		AccessorForList<List<String>, String> testInstance = new AccessorForList<>();
		List<String> sample = Arrays.asList("a", "b", "c");
		
		testInstance.setIndex(0);
		assertEquals("a", testInstance.get(sample));
		testInstance.setIndex(1);
		assertEquals("b", testInstance.get(sample));
		testInstance.setIndex(2);
		assertEquals("c", testInstance.get(sample));
	}
	
	@Test(expectedExceptions = ArrayIndexOutOfBoundsException.class)
	public void testGet_ArrayIndexOutOfBoundsException() throws Exception {
		AccessorForList<List<String>, String> testInstance = new AccessorForList<>();
		List<String> sample = Arrays.asList("a", "b", "c");
		
		testInstance.setIndex(-1);
		assertEquals("a", testInstance.get(sample));
	}
}