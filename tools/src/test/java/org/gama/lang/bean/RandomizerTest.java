package org.gama.lang.bean;

import java.util.List;
import java.util.TreeSet;

import org.gama.lang.collection.Arrays;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Guillaume
 */
public class RandomizerTest {
	
	@Test
	public void testGetElementsByIndex_listInput() {
		TreeSet<Integer> indexes = new TreeSet<>();
		indexes.addAll(Arrays.asList(0, 2, 8));
		List<String> elementsByIndex = Randomizer.getElementsByIndex(Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i"), indexes);
		assertEquals(Arrays.asList("a", "c", "i"), elementsByIndex);
	}
	
	@Test
	public void testGetElementsByIndex_setInput() {
		TreeSet<Integer> indexes = new TreeSet<>();
		indexes.addAll(Arrays.asList(0, 2, 8));
		List<String> elementsByIndex = Randomizer.getElementsByIndex(Arrays.asSet("a", "b", "c", "d", "e", "f", "g", "h", "i"), indexes);
		assertEquals(Arrays.asList("a", "c", "i"), elementsByIndex);
	}
	
}