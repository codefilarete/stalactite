package org.gama.lang.collection;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.testng.annotations.Test;

/**
 * @author Guillaume Mary
 */
public class ReverseListIteratorTest {
	
	@Test
	public void testIteration() throws Exception {
		List<String> toReverse = Arrays.asList("a", "b", "c");
		List<String> expected = Arrays.asList("c", "b", "a");
		Iterator<String> iterator = new ReverseListIterator<>(toReverse);
		PairIterator<String, String> pairIterator = new PairIterator<>(iterator, expected.iterator());
		while (pairIterator.hasNext()) {
			Entry<String, String> next = pairIterator.next();
			assertEquals(next.getKey(), next.getValue());
		}
	}
	
	@Test
	public void testIteration_empty() throws Exception {
		Iterator<?> iterator = new ReverseListIterator<>(Arrays.asList());
		assertFalse(iterator.hasNext());
	}
	
	@Test
	public void testRemove() throws Exception {
		ArrayList<String> toModify = new ArrayList<>(Arrays.asList("a", "b", "c"));
		Iterator<String> iterator = new ReverseListIterator<>(toModify);
		iterator.next();
		iterator.remove();
		assertEquals(Arrays.asList("a", "b"), toModify);
		assertEquals("b", iterator.next());
	}
}