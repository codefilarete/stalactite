package org.gama.reflection;

import static org.testng.AssertJUnit.*;

import org.gama.lang.Reflections;
import org.testng.annotations.Test;

/**
 * @author Guillaume Mary
 */
public class AccessorByMethodTest {
	
	@Test
	public void testForProperty() throws Exception {
		AccessorByMethod testInstance = Accessors.accessorByMethod(Toto.class, "a");
		assertEquals(testInstance.getGetter(), Reflections.getMethod(Toto.class, "getA"));
	}
	
	@Test
	public void testGet() throws Exception {
		AccessorByMethod<Toto, Integer> testInstance = new AccessorByMethod<>(Reflections.getMethod(Toto.class, "getA"));
		Toto toto = new Toto();
		toto.a = 42;
		assertEquals((Object) 42, testInstance.get(toto));
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