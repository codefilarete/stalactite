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
		AccessorByField<Toto, Integer> testInstance = new AccessorByField<>(Reflections.getField(Toto.class, "a"));
		Toto toto = new Toto();
		toto.a = 42;
		assertEquals((Object) 42, testInstance.get(toto));
	}
	
	private static class Toto {
		private int a;
	}
	
}