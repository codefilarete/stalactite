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
		AccessorForList testInstance = new AccessorForList();
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
		AccessorForList testInstance = new AccessorForList();
		List<String> sample = Arrays.asList("a", "b", "c");
		
		testInstance.setIndex(-1);
		assertEquals("a", testInstance.get(sample));
	}
	
	@Test
	public void testSet() throws Exception {
		AccessorForList testInstance = new AccessorForList();
		List<String> sample = Arrays.asList("a", "b", "c");
		
		testInstance.setIndex(0);
		testInstance.set(sample, "x");
		assertEquals(sample, Arrays.asList("x", "b", "c"));
		testInstance.setIndex(1);
		testInstance.set(sample, "y");
		assertEquals(sample, Arrays.asList("x", "y", "c"));
		testInstance.setIndex(2);
		testInstance.set(sample, "z");
		assertEquals(sample, Arrays.asList("x", "y", "z"));
	}
	
	@Test(expectedExceptions = ArrayIndexOutOfBoundsException.class)
	public void testSet_ArrayIndexOutOfBoundsException() throws Exception {
		AccessorForList testInstance = new AccessorForList();
		List<String> sample = Arrays.asList("a", "b", "c");
		
		testInstance.setIndex(-1);
		testInstance.set(sample, "x");
	}
}