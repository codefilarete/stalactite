package org.gama.lang;


import org.gama.lang.Reflections.FieldIterator;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.PairIterator;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class FieldIteratorTest {
	
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
	public void testNextMethods(Class clazz, List<String> expectedFields) throws Exception {
		FieldIterator testInstance = new FieldIterator(clazz);
		PairIterator<String, Field> expectationComparator = new PairIterator.UntilBothIterator<>(expectedFields.iterator(), testInstance);
		while(expectationComparator.hasNext()) {
			Map.Entry<String, Field> next = expectationComparator.next();
			assertEquals(next.getKey(), next.getValue().getName());
		}
	}
	
	static class X { private String f1; }
	
	static class Y extends X { private String f2; }
	
	static class Z extends Y { private String f2; }
}