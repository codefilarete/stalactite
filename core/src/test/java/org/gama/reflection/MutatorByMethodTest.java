package org.gama.reflection;

import static org.testng.AssertJUnit.*;

import org.gama.lang.Reflections;
import org.testng.annotations.Test;

/**
 * @author Guillaume Mary
 */
public class MutatorByMethodTest {
	
	@Test
	public void testForProperty() throws Exception {
		MutatorByMethod testInstance = Accessors.mutatorByMethod(Toto.class, "a");
		assertEquals(testInstance.getSetter(), Reflections.getMethod(Toto.class, "setA", Integer.TYPE));
	}
	
	@Test
	public void testSet() throws Exception {
		MutatorByMethod<Toto, Integer> testInstance = new MutatorByMethod<>(Reflections.getMethod(Toto.class, "setA", Integer.TYPE));
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