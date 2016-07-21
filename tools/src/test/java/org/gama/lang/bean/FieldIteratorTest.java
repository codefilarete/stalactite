package org.gama.lang.bean;

import java.lang.reflect.Field;
import java.util.List;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

/**
 * @author Guillaume Mary
 */
@RunWith(DataProviderRunner.class)
public class FieldIteratorTest {
	
	@DataProvider
	public static Object[][] testNextMethodsData() throws NoSuchFieldException {
		Field xf1 = X.class.getDeclaredField("f1");
		Field yf2 = Y.class.getDeclaredField("f2");
		Field zf2 = Z.class.getDeclaredField("f2");
		return new Object[][] {
				{ X.class, Arrays.asList(xf1) },
				{ Y.class, Arrays.asList(yf2, xf1) },
				{ Z.class, Arrays.asList(zf2, yf2, xf1) },
				{ NoField.class, Arrays.asList(xf1) }
		};
	}
	
	@Test
	@UseDataProvider("testNextMethodsData")
	public void testNextMethods(Class clazz, List<Field> expectedFields) throws Exception {
		FieldIterator testInstance = new FieldIterator(clazz);
		assertEquals(expectedFields, Iterables.visit(testInstance, new Iterables.ForEach<Field, Field>() {
			
			@Override
			public Field visit(Field field) {
				return field;
			}
		}));
	}
	
	static class X {
		private Object f1;
	}
	
	static class Y extends X {
		private Object f2;
	}
	
	static class Z extends Y {
		private Object f2;
	}
	
	static class NoField extends X {
	}
}