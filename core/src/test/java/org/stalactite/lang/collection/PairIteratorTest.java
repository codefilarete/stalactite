package org.stalactite.lang.collection;


import static org.testng.Assert.*;

import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.NoSuchElementException;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class PairIteratorTest {
	
	private static final String NEXT_NO_SUCH_ELEMENT_EXCEPTION_DATA = "testNext_NoSuchElementExceptionData";
	
	@Test
	public void testHasNext() throws Exception {
		PairIterator<Integer, String> testInstance = new PairIterator<>(Arrays.asList(1,2,3), Arrays.asList("a", "b"));
		assertTrue(testInstance.hasNext());
		assertEquals(testInstance.next(), new SimpleEntry<>(1, "a"));
		assertTrue(testInstance.hasNext());
		assertEquals(testInstance.next(), new SimpleEntry<>(2, "b"));
		assertFalse(testInstance.hasNext());
	}
	
	@DataProvider(name = NEXT_NO_SUCH_ELEMENT_EXCEPTION_DATA)
	public Object[][] testNext_NoSuchElementExceptionData() {
		PairIterator<Integer, String> startedIterator = new PairIterator<>(Arrays.asList(1), Arrays.asList("a"));
		startedIterator.next();
		return new Object[][] {
				{ startedIterator },
				{ new PairIterator<>(Arrays.asList(1), Arrays.asList()) },
				{ new PairIterator<>(Arrays.asList(), Arrays.asList("a")) },
				{ new PairIterator<>(Arrays.asList(), Arrays.asList()) },
		};
	}
	
	@Test(expectedExceptions = NoSuchElementException.class, dataProvider = NEXT_NO_SUCH_ELEMENT_EXCEPTION_DATA)
	public void testNext_NoSuchElementException(PairIterator<Integer, String> testInstance) throws Exception {
		testInstance.next();
	}
	
	@Test
	public void testRemove() throws Exception {
		List<Integer> integers = Arrays.asList(1, 2, 3);
		List<String> strings = Arrays.asList("a", "b");
		PairIterator<Integer, String> testInstance = new PairIterator<>(integers, strings);
		testInstance.hasNext();
		testInstance.next();
		testInstance.remove();
		assertEquals(integers, Arrays.asList(2, 3));
		assertEquals(strings, Arrays.asList("b"));
	}
	
	@Test
	public void testInfiniteIterator() throws Exception {
		List<Integer> integers = Arrays.asList(1, 2, 3);
		List<String> strings = Arrays.asList("a");
		PairIterator<Integer, String> testInstance = new PairIterator<>(integers.iterator(), new PairIterator.InfiniteIterator<>(strings.iterator()));
		assertTrue(testInstance.hasNext());
		assertEquals(testInstance.next(), new SimpleEntry<>(1, "a"));
		assertTrue(testInstance.hasNext());
		assertEquals(testInstance.next(), new SimpleEntry<>(2, null));
		assertTrue(testInstance.hasNext());
		assertEquals(testInstance.next(), new SimpleEntry<>(3, null));
		assertFalse(testInstance.hasNext());
	}
}