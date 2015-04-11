package org.stalactite.reflection;

import static org.testng.AssertJUnit.assertEquals;

import org.stalactite.lang.Reflections;
import org.testng.annotations.Test;

/**
 * @author Guillaume Mary
 */
public class AccessorByFieldTest {
	
	@Test
	public void testGet() throws Exception {
		AccessorByField testInstance = new AccessorByField(Reflections.getField(Toto.class, "a"));
		Toto toto = new Toto();
		toto.a = 42;
		assertEquals(42, testInstance.get(toto));
	}
	
	@Test
	public void testSet() throws Exception {
		AccessorByField testInstance = new AccessorByField(Reflections.getField(Toto.class, "a"));
		Toto toto = new Toto();
		testInstance.set(toto, 42);
		assertEquals(42, toto.a);
	}
	
	private static class Toto {
		private int a;
	}
	
}