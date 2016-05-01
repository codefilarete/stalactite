package org.gama.safemodel.description;

import java.lang.reflect.Field;

import org.gama.lang.Reflections;
import org.junit.Test;

import static org.gama.safemodel.description.FieldDescription.field;
import static org.junit.Assert.assertEquals;

/**
 * @author Guillaume Mary
 */
public class FieldDescriptionTest {
	
	@Test
	public void testField_fieldFound() throws NoSuchFieldException {
		Field expectedField = TestFieldTargetClass.class.getDeclaredField("toto");
		
		FieldDescription<String> fieldDescription = field(TestFieldTargetClass.class, "toto", String.class);
		
		assertEquals(expectedField, fieldDescription.getField());
	}
	
	/**
	 * {@link FieldDescription#field} must find inherited field to fullfill abstract class inheritance use case
	 */
	@Test
	public void testField_inheritance_fieldFound() throws NoSuchFieldException {
		Field expectedField = TestFieldTargetClass.class.getDeclaredField("toto");
		
		FieldDescription<String> fieldDescription = field(TestFieldTargetClass_extended.class, "toto", String.class);
		
		assertEquals(expectedField, fieldDescription.getField());
	}
	
	@Test(expected = Reflections.MemberNotFoundException.class)
	public void testField_fieldNotFound() {
		field(TestFieldTargetClass.class, "tata", String.class);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void testField_wrongType() {
		field(TestFieldTargetClass.class, "toto", Long.class);
	}
	
	private static class TestFieldTargetClass {
		
		private String toto;
		
	}
	
	private static class TestFieldTargetClass_extended extends TestFieldTargetClass {
		
	}
	
}