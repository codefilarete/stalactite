package org.gama.stalactite.persistence.mapping;

import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.List;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.gama.lang.collection.Arrays;
import org.junit.Test;
import org.junit.runner.RunWith;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Guillaume Mary
 */
@RunWith(DataProviderRunner.class)
public class PersistentFieldHarversterTest {
	
	@DataProvider
	public static Object[][] testNextMethodsData() {
		return new Object[][] {
				{ X.class, Arrays.asList("f1") },
				{ Y.class, Arrays.asList("f2", "f1") },
				{ Z.class, Arrays.asList("f2", "f2", "f1") }
		};
	}
	
	@Test
	@UseDataProvider("testNextMethodsData")
	public void testGetFields(Class clazz, List<String> expectedFields) throws Exception {
		PersistentFieldHarverster testInstance = new PersistentFieldHarverster();
		Iterable<Field> fields = testInstance.getFields(clazz);
		Iterator<Field> fieldsIterator = fields.iterator();
		assertTrue(fieldsIterator.hasNext());
		for (String expectedField : expectedFields) {
			assertEquals(expectedField, fieldsIterator.next().getName());
		}
		assertFalse(fieldsIterator.hasNext());
	}
	
	static class X { private String f1; }
	
	static class Y extends X { private String f2; }
	
	static class Z extends Y { private String f2; }
	
}