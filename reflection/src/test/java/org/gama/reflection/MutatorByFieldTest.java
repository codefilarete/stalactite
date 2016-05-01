package org.gama.reflection;

import org.gama.lang.Reflections;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Guillaume Mary
 */
public class MutatorByFieldTest {
	
	@Test
	public void testSet() throws Exception {
		MutatorByField<Toto, Integer> testInstance = new MutatorByField<>(Reflections.findField(Toto.class, "a"));
		Toto toto = new Toto();
		testInstance.set(toto, 42);
		assertEquals(42, toto.a);
	}
	
	private static class Toto {
		private int a;
	}
	
}