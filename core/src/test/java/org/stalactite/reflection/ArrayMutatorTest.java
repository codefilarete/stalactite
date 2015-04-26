package org.stalactite.reflection;

import static org.junit.Assert.*;

import org.testng.annotations.Test;

/**
 * @author Guillaume Mary
 */
public class ArrayMutatorTest {
	
	@Test
	public void testSet() throws Exception {
		ArrayMutator<String> testInstance = new ArrayMutator<>();
		String[] sample = { "a", "b", "c" };
		
		testInstance.setIndex(0);
		testInstance.set(sample, "x");
		assertArrayEquals(sample, new String[]{"x", "b", "c"});
		testInstance.setIndex(1);
		testInstance.set(sample, "y");
		assertArrayEquals(sample, new String[]{"x", "y", "c"});
		testInstance.setIndex(2);
		testInstance.set(sample, "z");
		assertArrayEquals(sample, new String[]{"x", "y", "z"});
	}
	
	@Test(expectedExceptions = ArrayIndexOutOfBoundsException.class)
	public void testSet_ArrayIndexOutOfBoundsException() throws Exception {
		ArrayMutator<String> testInstance = new ArrayMutator<>();
		String[] sample = { "a", "b", "c" };
		
		testInstance.setIndex(-1);
		testInstance.set(sample, "x");
	}
}