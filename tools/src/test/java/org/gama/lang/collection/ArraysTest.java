package org.gama.lang.collection;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Guillaume Mary
 */
public class ArraysTest {
	
	@Test
	public void asList() throws Exception {
		List<String> newList = Arrays.asList("a", "c", "b");
		assertEquals(java.util.Arrays.asList("a", "c", "b"), newList);
		assertTrue(Arrays.asList().isEmpty());
		// the list is modifiable
		assertTrue(Arrays.asList().add("a"));
	}
	
	@Test
	public void asSet() throws Exception {
		Set<String> newSet = Arrays.asSet("a", "c", "b");
		assertTrue(newSet.containsAll(java.util.Arrays.asList("a", "c", "b")));
		// order is preserved
		assertEquals(java.util.Arrays.asList("a", "c", "b"), new ArrayList<>(newSet));
		assertTrue(Arrays.asSet().isEmpty());
		// the set is modifiable
		assertTrue(Arrays.asList().add("a"));
	}
	
	@Test
	public void isEmpty() throws Exception {
		assertTrue(Arrays.isEmpty(new Object[0]));
		assertTrue(Arrays.isEmpty(null));
		assertFalse(Arrays.isEmpty(new Object[1]));
	}
	
	@Test
	public void get() throws Exception {
		assertEquals("a", Arrays.get(0).apply(new String[] { "a", "b", "c" }));
		assertEquals("b", Arrays.get(1).apply(new String[] { "a", "b", "c" }));
		assertEquals("c", Arrays.get(2).apply(new String[] { "a", "b", "c" }));
	}
	
	@Test
	public void first() throws Exception {
		Function<Object[], Object> first = Arrays::first;
		assertEquals("a", first.apply(new String[] { "a", "b", "c" }));
	}
	
	@Test
	public void last() throws Exception {
		Function<Object[], Object> last = Arrays::last;
		assertEquals("c", last.apply(new String[] { "a", "b", "c" }));
	}
	
}