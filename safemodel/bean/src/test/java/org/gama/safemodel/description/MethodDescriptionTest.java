package org.gama.safemodel.description;

import java.lang.reflect.Method;

import org.gama.lang.Reflections;
import org.junit.Test;

import static org.gama.safemodel.description.MethodDescription.method;
import static org.junit.Assert.assertEquals;

/**
 * @author Guillaume Mary
 */
public class MethodDescriptionTest {
	
	@Test
	public void testMethod_methodFound() throws NoSuchMethodException {
		Method expectedMethod = TestMethodTargetClass.class.getDeclaredMethod("toto");
		
		MethodDescription<String> methodDescription = method(TestMethodTargetClass.class, "toto", String.class);
		
		assertEquals(expectedMethod, methodDescription.getMethod());
	}
	
	/**
	 * {@link MethodDescription#method} must find inherited method to fullfill abstract class inheritance use case
	 */
	@Test
	public void testMethod_inheritance_methodFound() throws NoSuchMethodException {
		Method expectedMethod = TestMethodTargetClass.class.getDeclaredMethod("toto");
		
		MethodDescription<String> methodDescription = method(TestMethodTargetClass_extended.class, "toto", String.class);
		
		assertEquals(expectedMethod, methodDescription.getMethod());
	}
	
	@Test(expected = Reflections.MemberNotFoundException.class)
	public void testMethod_methodNotFound() {
		method(TestMethodTargetClass.class, "tata", String.class);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void testMethod_wrongType() {
		method(TestMethodTargetClass.class, "toto", Long.class);
	}
	
	private static class TestMethodTargetClass {
		
		private String toto() {
			return "";
		}
		
	}
	
	private static class TestMethodTargetClass_extended extends TestMethodTargetClass {
		
	}
	
}