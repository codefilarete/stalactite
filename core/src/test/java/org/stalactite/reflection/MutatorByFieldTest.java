package org.stalactite.reflection;

import static org.testng.AssertJUnit.*;

import org.stalactite.lang.Reflections;
import org.testng.annotations.Test;

/**
 * @author Guillaume Mary
 */
public class MutatorByFieldTest {
	
	@Test
	public void testSet() throws Exception {
		FieldMutator<Toto, Integer> testInstance = new FieldMutator<>(Reflections.getField(Toto.class, "a"));
		Toto toto = new Toto();
		testInstance.set(toto, 42);
		assertEquals(42, toto.a);
	}
	
	private static class Toto {
		private int a;
	}
	
}