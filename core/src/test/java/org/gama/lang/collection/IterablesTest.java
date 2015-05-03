package org.gama.lang.collection;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;

import java.util.*;

import org.gama.lang.collection.Iterables.Filter;
import org.gama.lang.collection.Iterables.Finder;
import org.gama.lang.collection.Iterables.IVisitor;
import org.testng.Assert;
import org.testng.annotations.Test;

public class IterablesTest {
	
	@Test
	public void testFirst_iterable() {
		List<String> strings = Arrays.asList("a", "b");
		assertEquals(Iterables.first(strings), "a");
		// test sur nullité
		Assert.assertNull(Iterables.first(Arrays.asList()));
		Assert.assertNull(Iterables.first((Iterable) null));
	}
	
	@Test
	public void testFirst_map() {
		Map<String, Integer> aMap = new LinkedHashMap<>();
		aMap.put("d", 25);
		aMap.put("a", 14);
		assertEquals(Iterables.first(aMap), new LinkedHashMap.SimpleEntry<>("d", 25));
		// test sur nullité
		Assert.assertNull(Iterables.first(new HashMap()));
		Assert.assertNull(Iterables.first((Map) null));
	}
	
	@Test
	public void testFirstValue_map() {
		Map<String, Integer> aMap = new LinkedHashMap<>();
		aMap.put("d", 25);
		aMap.put("a", 14);
		assertEquals((int) Iterables.firstValue(aMap), 25);
		// test sur nullité
		Assert.assertNull(Iterables.first(new HashMap()));
		Assert.assertNull(Iterables.first((Map) null));
	}
	
	@Test
	public void testCopy() {
		// test de contenu
		Set<String> aSet = new LinkedHashSet<>();
		aSet.add("d");
		aSet.add("a");
		assertEquals(Iterables.copy(aSet), Arrays.asList("d", "a"));
		// test que la copie n'est pas la même instance que l'original
		assertNotSame(Iterables.copy(aSet), aSet);
		// test sur cas limite: contenu vide
		assertEquals(Iterables.copy(Arrays.asList()), Arrays.asList());
	}
	
	@Test
	public void testVisit_iterable() {
		// création d'un visitor qui parcourt tout
		IVisitor iVisitorMock = mock(IVisitor.class);
		when(iVisitorMock.pursue()).thenReturn(true);
		when(iVisitorMock.visit(any())).thenReturn(new Object());
		
		// test basique de comptage de passage dans la méthode principale
		List strings = Arrays.asList("a", "b");
		List visitResult = Iterables.visit(strings, iVisitorMock);
		verify(iVisitorMock, times(2)).visit(any());
		assertEquals(visitResult.size(), 2);
		
		// test sur liste vide
		reset(iVisitorMock);
		visitResult = Iterables.visit(Arrays.asList(), iVisitorMock);
		verify(iVisitorMock, times(0)).visit(any());
		assertEquals(visitResult.size(), 0);
		
		// test sur nullité
		Assert.assertNull(Iterables.visit((Iterable) null, iVisitorMock));
	}
	
	@Test
	public void testVisit_iterator() {
		// création d'un visitor qui parcourt tout
		IVisitor iVisitorMock = mock(IVisitor.class);
		when(iVisitorMock.pursue()).thenReturn(true);
		
		// test basique de comptage de passage dans la méthode principale
		List strings = Arrays.asList("a", "b");
		List visitResult = Iterables.visit(strings.iterator(), iVisitorMock);
		verify(iVisitorMock, times(2)).visit(any());
		assertEquals(visitResult.size(), 2);
		
		// test sur liste vide
		reset(iVisitorMock);
		visitResult = Iterables.visit(Arrays.asList().iterator(), iVisitorMock);
		verify(iVisitorMock, times(0)).visit(any());
		assertEquals(visitResult.size(), 0);
	}
	
	@Test
	public void testFilter_Filter() {
		// création d'un visitor qui parcourt tout
		Filter<String> iVisitorMock = spy(new Filter<String>() {
			@Override
			public boolean accept(String s) {
				return "b".equals(s);
			}
		});
		
		// test basique de comptage de passage dans la méthode principale
		List<String> strings = Arrays.asList("a", "b", "c", "b", "d");
		List visitResult = Iterables.filter(strings, iVisitorMock);
		verify(iVisitorMock, times(5)).visit((String) any());
		assertEquals(visitResult, Arrays.asList("b", "b"));
	}
	
	@Test
	public void testFilter_Finder() {
		// création d'un visitor qui parcourt tout
		Finder<String> iVisitorMock = spy(new Finder<String>() {
			@Override
			public boolean accept(String s) {
				return "b".equals(s);
			}
		});
		
		// test basique de comptage de passage dans la méthode principale
		List<String> strings = Arrays.asList("a", "b", "c", "b", "d");
		String visitResult = Iterables.filter(strings, iVisitorMock);
		verify(iVisitorMock, times(2)).visit((String) any());
		assertEquals(visitResult, "b");
	}
}