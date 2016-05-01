package org.gama.lang.collection;

import java.util.Iterator;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.*;

/**
 * @author Guillaume Mary
 */
public class SteppingIteratorTest {
	
	@Test
	public void testHasNext() throws Exception {
		Iterator<String> iterator = mock(Iterator.class);
		SteppingIterator testInstance = new SteppingIterator<String>(iterator, 10) {
			@Override
			protected void onStep() {
				
			}
		};
		testInstance.hasNext();
		verify(iterator).hasNext();
		testInstance.hasNext();
		verify(iterator, Mockito.times(2)).hasNext();
		testInstance.hasNext();
		verify(iterator, Mockito.times(3)).hasNext();
	}
	
	@Test
	public void testNext() throws Exception {
		Iterator<String> iterator = mock(Iterator.class);
		SteppingIterator testInstance = new SteppingIterator<String>(iterator, 10) {
			@Override
			protected void onStep() {
				
			}
		};
		testInstance.next();
		verify(iterator).next();
		testInstance.next();
		verify(iterator, Mockito.times(2)).next();
		testInstance.next();
		verify(iterator, Mockito.times(3)).next();
	}
	
	@Test
	public void testRemove() throws Exception {
		Iterator<String> iterator = mock(Iterator.class);
		SteppingIterator testInstance = new SteppingIterator<String>(iterator, 10) {
			@Override
			protected void onStep() {
				
			}
		};
		testInstance.remove();
		verify(iterator).remove();
		testInstance.remove();
		verify(iterator, Mockito.times(2)).remove();
		testInstance.remove();
		verify(iterator, Mockito.times(3)).remove();
	}
	
	@Test
	public void testOnStep() throws Exception {
		Iterator<String> iterator = mock(Iterator.class);
		when(iterator.hasNext()).thenReturn(true);
		final int[] i= new int[1];
		SteppingIterator testInstance = new SteppingIterator<String>(iterator, 2) {
			@Override
			protected void onStep() {
				i[0]++;
			}
		};
		testInstance.hasNext();
		Assert.assertEquals(0, i[0]);
		testInstance.next();
		testInstance.hasNext();
		Assert.assertEquals(0, i[0]);
		testInstance.next();
		testInstance.hasNext();
		Assert.assertEquals(1, i[0]);
		testInstance.next();
		testInstance.hasNext();
		Assert.assertEquals(1, i[0]);
		testInstance.next();
		testInstance.hasNext();
		Assert.assertEquals(2, i[0]);
		
		when(iterator.hasNext()).thenReturn(false);
		testInstance.next();
		testInstance.hasNext();
		Assert.assertEquals(3, i[0]);
	}
}