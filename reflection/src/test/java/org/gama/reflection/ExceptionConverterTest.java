package org.gama.reflection;

import org.gama.lang.Reflections;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Guillaume Mary
 */
public class ExceptionConverterTest {
	
	@Test
	public void testConvertException_wrongConversion() throws Throwable {
		Field field_a = Reflections.findField(Toto.class, "a");
		assertNotNull(field_a);
		field_a.setAccessible(true);
		
		MutatorByField<Toto, Object> accessorByField = new MutatorByField<>(field_a);
		
		Toto target = new Toto();
		Throwable thrownThrowable = null;
		try {
			accessorByField.set(target, 0L);
		} catch (IllegalArgumentException e) {
			thrownThrowable = e;
		}
		
		assertTrue(thrownThrowable.getMessage().contains("can't be used with"));
	}
	
	@Test
	public void testConvertException_missingField() throws Throwable {
		Field field_a = Reflections.findField(Toto.class, "a");
		assertNotNull(field_a);
		field_a.setAccessible(true);
		
		MutatorByField<Tata, Object> mutatorByField = new MutatorByField<>(field_a);
		
		Tata target = new Tata();
		Throwable thrownThrowable = null;
		try {
			mutatorByField.set(target, 0L);
		} catch (IllegalArgumentException e) {
			thrownThrowable = e;
		}
		
		assertTrue(thrownThrowable.getMessage().contains("doesn't have field"));
	}
	
	
	private static class Toto {
		
		private Integer a; 
	}
	
	private static class Tata {
		
	}
}