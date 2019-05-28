package org.gama.sql.binder;

import java.io.Serializable;

import org.gama.sql.dml.SQLStatement.BindingException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Guillaume Mary
 */
class ParameterBinderRegistryTest {
	
	@Test
	void getBinder_notFound() {
		ParameterBinderRegistry testInstance = new ParameterBinderRegistry();
		assertEquals(DefaultParameterBinders.STRING_BINDER, testInstance.getBinder(String.class));
	}
	
	@Test
	void getBinder_notRegistered_throwsException() {
		ParameterBinderRegistry testInstance = new ParameterBinderRegistry();
		BindingException thrownException = assertThrows(BindingException.class, () -> testInstance.getBinder(Object.class));
		assertEquals("No binder found for type j.l.Object", thrownException.getMessage());
	}
	
	@Test
	void getBinder_findCompliantBinder() {
		ParameterBinderRegistry testInstance = new ParameterBinderRegistry();
		// just to be sure that nobody corrupted this test by adding StringBuilder in the default registry
		assertFalse(testInstance.getBindersPerType().keySet().contains(StringBuilder.class));
		
		NullAwareParameterBinder<CharSequence> expectedBinder = new NullAwareParameterBinder<>(new LambdaParameterBinder<>(
				DefaultParameterBinders.STRING_BINDER, String::toString, CharSequence::toString));
		testInstance.register(CharSequence.class, expectedBinder);
		assertEquals(expectedBinder, testInstance.getBinder(StringBuilder.class));
		assertTrue(testInstance.getBindersPerType().keySet().contains(StringBuilder.class));
	}
	
	@Test
	void getBinder_multipleCompliantBindersFound_throwsException() {
		ParameterBinderRegistry testInstance = new ParameterBinderRegistry();
		NullAwareParameterBinder<CharSequence> charSequenceBinder = new NullAwareParameterBinder<>(new LambdaParameterBinder<>(
				DefaultParameterBinders.STRING_BINDER, String::toString, CharSequence::toString));
		testInstance.register(CharSequence.class, charSequenceBinder);
		NullAwareParameterBinder<Serializable> serializableBinder = new NullAwareParameterBinder<>(new LambdaParameterBinder<>(
				DefaultParameterBinders.STRING_BINDER, Serializable::toString, Serializable::toString));
		testInstance.register(Serializable.class, serializableBinder);
		BindingException thrownException = assertThrows(BindingException.class, () -> testInstance.getBinder(StringBuilder.class));
		assertEquals("Multiple binders found for j.l.StringBuilder, please register a dedicated one : [j.l.CharSequence, j.i.Serializable]",
				thrownException.getMessage());
	}
}