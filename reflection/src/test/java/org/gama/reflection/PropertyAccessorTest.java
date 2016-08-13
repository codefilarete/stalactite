package org.gama.reflection;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.gama.lang.Reflections;
import org.gama.reflection.model.City;
import org.gama.reflection.model.Phone;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;

/**
 * @author Guillaume Mary
 */
public class PropertyAccessorTest {
	
	@Test
	public void testOf_fieldInput() {
		Field numberField = Reflections.findField(Phone.class, "number");
		PropertyAccessor<Phone, String> numberAccessor = PropertyAccessor.of(numberField);
		assertEquals(new AccessorByField<>(numberField), numberAccessor.getAccessor());
		assertEquals(new MutatorByField<>(numberField), numberAccessor.getMutator());
	}
	
	@Test
	public void testOf_methodInput() {
		Field numberField = Reflections.findField(Phone.class, "number");
		Method numberGetter = Reflections.findMethod(Phone.class, "getNumber");
		PropertyAccessor<Phone, String> numberAccessor = PropertyAccessor.of(numberGetter);
		assertEquals(new AccessorByMethod<>(numberGetter), numberAccessor.getAccessor());
		// As there's no setter for "number" field, the mutator is an field one, not a method one
		assertEquals(new MutatorByField<>(numberField), numberAccessor.getMutator());
		
		
		Method nameGetter = Reflections.findMethod(City.class, "getName");
		Method nameSetter = Reflections.findMethod(City.class, "setName", String.class);
		PropertyAccessor<City, String> nameAccessor = PropertyAccessor.of(nameGetter);
		assertEquals(new AccessorByMethod<>(nameGetter), nameAccessor.getAccessor());
		// As a setter exists for "name" field, the mutator is a method one, not a field one
		assertEquals(new MutatorByMethod<>(nameSetter), nameAccessor.getMutator());
	}
	
	@Rule
	public ExpectedException expectedException = ExpectedException.none();
	
	@Test
	public void testOf_nonConventionalMethodInput_exceptionThrown() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("Field wrapper org.gama.reflection.model.City.name doesn't feet encapsulation naming convention");
		Method nameGetter = Reflections.findMethod(City.class, "name");
		PropertyAccessor.of(nameGetter);
	}
	
}