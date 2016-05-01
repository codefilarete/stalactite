package org.gama.reflection;

import org.gama.lang.Reflections;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Guillaume Mary
 */
public class MutatorByMethodTest {
	
	@Test
	public void testForProperty() throws Exception {
		MutatorByMethod testInstance = Accessors.mutatorByMethod(Toto.class, "a");
		assertEquals(testInstance.getSetter(), Reflections.findMethod(Toto.class, "setA", Integer.TYPE));
	}
	
	@Test
	public void testSet() throws Exception {
		MutatorByMethod<Toto, Integer> testInstance = new MutatorByMethod<>(Reflections.findMethod(Toto.class, "setA", Integer.TYPE));
		Toto toto = new Toto();
		testInstance.set(toto, 42);
		assertEquals(42, toto.a);
	}
	
	private static class Toto {
		private int a;
		
		public int getA() {
			return a;
		}
		
		public void setA(int a) {
			this.a = a;
		}
	}
	
}