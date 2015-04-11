package org.stalactite.reflection;

import static org.testng.AssertJUnit.assertEquals;

import org.stalactite.lang.Reflections;
import org.testng.annotations.Test;

/**
 * @author Guillaume Mary
 */
public class AccessorByMethodTest {
	
	@Test
	public void testGet() throws Exception {
		AccessorByMethod testInstance = new AccessorByMethod(Reflections.getMethod(Toto.class, "getA"), Reflections.getMethod(Toto.class, "setA"));
		Toto toto = new Toto();
		toto.a = 42;
		assertEquals(42, testInstance.get(toto));
	}
	
	@Test
	public void testSet() throws Exception {
		AccessorByMethod testInstance = new AccessorByMethod(Reflections.getMethod(Toto.class, "getA"), Reflections.getMethod(Toto.class, "setA"));
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