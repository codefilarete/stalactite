package org.gama.lang.collection;


import java.util.AbstractMap;
import java.util.List;
import java.util.NoSuchElementException;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.*;

/**
 * @author Guillaume Mary
 */
@RunWith(DataProviderRunner.class)
public class PairIteratorTest {
	
	@Test
	public void testHasNext() throws Exception {
		PairIterator<Integer, String> testInstance = new PairIterator<>(Arrays.asList(1,2,3), Arrays.asList("a", "b"));
		assertTrue(testInstance.hasNext());
		assertEquals(testInstance.next(), new AbstractMap.SimpleEntry<>(1, "a"));
		assertTrue(testInstance.hasNext());
		assertEquals(testInstance.next(), new AbstractMap.SimpleEntry<>(2, "b"));
		assertFalse(testInstance.hasNext());
	}
	
	@DataProvider
	public static Object[][] testNext_NoSuchElementExceptionData() {
		PairIterator<Integer, String> startedIterator = new PairIterator<>(Arrays.asList(1), Arrays.asList("a"));
		startedIterator.next();
		return new Object[][] {
				{ startedIterator },
				{ new PairIterator<>(Arrays.asList(1), Arrays.asList()) },
				{ new PairIterator<>(Arrays.asList(), Arrays.asList("a")) },
				{ new PairIterator<>(Arrays.asList(), Arrays.asList()) },
		};
	}
	
	@Test(expected = NoSuchElementException.class)
	@UseDataProvider("testNext_NoSuchElementExceptionData")
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
		assertEquals(testInstance.next(), new AbstractMap.SimpleEntry<>(1, "a"));
		assertTrue(testInstance.hasNext());
		assertEquals(testInstance.next(), new AbstractMap.SimpleEntry<>(2, null));
		assertTrue(testInstance.hasNext());
		assertEquals(testInstance.next(), new AbstractMap.SimpleEntry<>(3, null));
		assertFalse(testInstance.hasNext());
	}
}