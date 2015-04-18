package org.stalactite.lang;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.testng.annotations.Test;

/**
 * @author Guillaume Mary
 */
public class ReflectionsTest {
	
	@Test
	public void testGetDefaultConstructor() throws Exception {
		Constructor<Toto> defaultConstructor = Reflections.getDefaultConstructor(Toto.class);
		assertNotNull(defaultConstructor);
	}
	
	@Test
	public void testGetField() throws Exception {
		Field fieldA = Reflections.getField(Toto.class, "a");
		assertNotNull(fieldA);
		assertEquals("a", fieldA.getName());
		
		Field fieldB = Reflections.getField(Toto.class, "b");
		assertNotNull(fieldB);
		assertEquals("b", fieldB.getName());
	}
	
	@Test
	public void testGetField_inheritance() throws Exception {
		Field fieldA = Reflections.getField(Tata.class, "a");
		assertNotNull(fieldA);
		assertEquals("a", fieldA.getName());
		
		Field fieldB = Reflections.getField(Tata.class, "b");
		assertNotNull(fieldB);
		assertEquals("b", fieldB.getName());
		assertEquals(Tata.class, fieldB.getDeclaringClass());
	}
	
	@Test
	public void testGetMethod() throws Exception {
		Method totoMethod = Reflections.getMethod(Toto.class, "toto");
		assertNotNull(totoMethod);
		assertEquals(0, totoMethod.getParameterTypes().length);
		
		Method toto2Method = Reflections.getMethod(Toto.class, "toto2");
		assertNotNull(toto2Method);
		assertEquals(0, toto2Method.getParameterTypes().length);
		
		Method totoMethodWithParams = Reflections.getMethod(Toto.class, "toto", Integer.TYPE);
		assertNotNull(totoMethodWithParams);
		assertEquals(1, totoMethodWithParams.getParameterTypes().length);
		
		Method toto2MethodWithParams = Reflections.getMethod(Toto.class, "toto2", Integer.TYPE);
		assertNotNull(toto2MethodWithParams);
		assertEquals(1, toto2MethodWithParams.getParameterTypes().length);
	}
	
	private static class Toto {
		private int a;
		private String b;
		
		private void toto() {
		}
		
		private void toto(int a) {
		}
		
		// méthodes toto2() déclarées en ordre inverse des toto() pour tester la robustesse au jdk
		// (open jdk ne renvoie pas dans le même ordre)  
		private void toto2(int a) {
		}
		
		private void toto2() {
		}
	}
	
	private static class Tata extends Toto {
		private String b;
	}
}