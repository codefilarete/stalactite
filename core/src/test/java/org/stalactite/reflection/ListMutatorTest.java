package org.stalactite.reflection;

import static org.junit.Assert.*;

import java.util.List;

import org.stalactite.lang.collection.Arrays;
import org.testng.annotations.Test;

/**
 * @author Guillaume Mary
 */
public class ListMutatorTest {
	
	@Test
	public void testSet() throws Exception {
		ListMutator<List<String>, String> testInstance = new ListMutator<>();
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
		ListMutator<List<String>, String> testInstance = new ListMutator<>();
		List<String> sample = Arrays.asList("a", "b", "c");
		
		testInstance.setIndex(-1);
		testInstance.set(sample, "x");
	}
}