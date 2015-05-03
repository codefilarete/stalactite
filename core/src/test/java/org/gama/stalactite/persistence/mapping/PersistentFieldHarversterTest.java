package org.gama.stalactite.persistence.mapping;

import static org.testng.Assert.*;

import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.List;

import org.gama.lang.collection.Arrays;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class PersistentFieldHarversterTest {
	
	public static final String NEXT_METHODS_DATA = "testNextMethodsData";
	
	@DataProvider(name = NEXT_METHODS_DATA)
	private Object[][] testNextMethodsData() {
		return new Object[][] {
				{ X.class, Arrays.asList("f1") },
				{ Y.class, Arrays.asList("f2", "f1") },
				{ Z.class, Arrays.asList("f2", "f2", "f1") }
		};
	}
	
	@Test(dataProvider = NEXT_METHODS_DATA)
	public void testGetFields(Class clazz, List<String> expectedFields) throws Exception {
		PersistentFieldHarverster testInstance = new PersistentFieldHarverster();
		Iterable<Field> fields = testInstance.getFields(clazz);
		Iterator<Field> fieldsIterator = fields.iterator();
		assertTrue(fieldsIterator.hasNext());
		for (String expectedField : expectedFields) {
			assertEquals(fieldsIterator.next().getName(), expectedField);
		}
		assertFalse(fieldsIterator.hasNext());
	}
	
	static class X { private String f1; }
	
	static class Y extends X { private String f2; }
	
	static class Z extends Y { private String f2; }
	
}