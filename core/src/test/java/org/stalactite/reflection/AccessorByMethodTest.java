package org.stalactite.reflection;

import org.stalactite.lang.Reflections;
import static org.testng.AssertJUnit.*;
import org.testng.annotations.Test;

/**
 * @author Guillaume Mary
 */
public class AccessorByMethodTest {
	
	@Test
	public void testForProperty() throws Exception {
		AccessorByMethod accessorByMethodForFieldA = AccessorByMethod.forProperty(Toto.class, "a");
		AccessorByMethod testInstance = new AccessorByMethod(Reflections.getMethod(Toto.class, "getA"), Reflections.getMethod(Toto.class, "setA", Integer.TYPE));
		testInstance.getGetter().equals(accessorByMethodForFieldA.getGetter());
		testInstance.getSetter().equals(accessorByMethodForFieldA.getSetter());
	}
	
	@Test
	public void testGet() throws Exception {
		AccessorByMethod testInstance = new AccessorByMethod(Reflections.getMethod(Toto.class, "getA"), Reflections.getMethod(Toto.class, "setA", Integer.TYPE));
		Toto toto = new Toto();
		toto.a = 42;
		assertEquals(42, testInstance.get(toto));
	}
	
	@Test
	public void testSet() throws Exception {
		AccessorByMethod testInstance = new AccessorByMethod(Reflections.getMethod(Toto.class, "getA"), Reflections.getMethod(Toto.class, "setA", Integer.TYPE));
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