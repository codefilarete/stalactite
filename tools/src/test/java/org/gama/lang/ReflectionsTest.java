package org.gama.lang;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Guillaume Mary
 */
@RunWith(DataProviderRunner.class)
public class ReflectionsTest {
	
	@Test
	public void testGetDefaultConstructor() {
		Constructor<Toto> defaultConstructor = Reflections.getDefaultConstructor(Toto.class);
		assertNotNull(defaultConstructor);
	}
	
	@DataProvider
	public static Object[][] testGetFieldData() {
		return new Object[][] {
				{ Toto.class, "a", Toto.class },
				{ Toto.class, "b", Toto.class },
				// inheritance test
				{ Tutu.class, "a", Toto.class },
				{ Tutu.class, "b", Tata.class },
		};
	}
	
	@Test
	@UseDataProvider("testGetFieldData")
	public void testGetField(Class<Toto> fieldClass, String fieldName, Class expectedDeclaringClass) {
		Field field = Reflections.findField(fieldClass, fieldName);
		assertNotNull(field);
		assertEquals(fieldName, field.getName());
		assertEquals(expectedDeclaringClass, field.getDeclaringClass());
	}
	
	@DataProvider
	public static Object[][] testGetMethodData() {
		return new Object[][] {
				{ Toto.class, "toto", null, Toto.class, 0 },
				{ Toto.class, "toto2", null, Toto.class, 0 },
				// with parameter
				{ Toto.class, "toto", Integer.TYPE, Toto.class, 1 },
				{ Toto.class, "toto2", Integer.TYPE, Toto.class, 1 },
				// inheritance test
				{ Tutu.class, "toto", null, Toto.class, 0 },
				{ Tutu.class, "toto2", null, Toto.class, 0 },
				{ Tutu.class, "toto", Integer.TYPE, Toto.class, 1 },
				{ Tutu.class, "toto2", Integer.TYPE, Toto.class, 1 },
		};
	}
	
	@Test
	@UseDataProvider("testGetMethodData")
	public void testGetMethod(Class<Toto> methodClass, String methodName, Class parameterType, Class expectedDeclaringClass, int exectedParameterCount) {
		Method method;
		if (parameterType == null) {
			method = Reflections.findMethod(methodClass, methodName);
		} else {
			method = Reflections.findMethod(methodClass, methodName, parameterType);
		}
		assertNotNull(method);
		assertEquals(methodName, method.getName());
		assertEquals(expectedDeclaringClass, method.getDeclaringClass());
		assertEquals(exectedParameterCount, method.getParameterTypes().length);
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
	
	private static class Titi extends Tata {
		// no field, no method, for no member traversal check
	}
	
	private static class Tutu extends Titi {
		// no field, no method, for no member traversal check
	}
}