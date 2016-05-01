package org.gama.stalactite.persistence.sql.ddl;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

/**
 * @author Guillaume Mary
 */
@RunWith(DataProviderRunner.class)
public class JavaTypeToSqlTypeMappingTest {
	
	@DataProvider
	public static Object[][] testSingletonGetTypeNameData() {
		JavaTypeToSqlTypeMapping testInstance1 = new JavaTypeToSqlTypeMapping();
		testInstance1.put(String.class, "VARCHAR");
		JavaTypeToSqlTypeMapping testInstance2 = new JavaTypeToSqlTypeMapping();
		testInstance2.put(String.class, 10, "CHAR($l)");
		return new Object[][] {
				new Object[] { testInstance1, String.class, null, "VARCHAR" },
				new Object[] { testInstance2, String.class, 10, "CHAR(10)" },
		};
	}
	
	@Test
	@UseDataProvider("testSingletonGetTypeNameData")
	public void testGetTypeName_WithSingleton(JavaTypeToSqlTypeMapping testInstance, Class javaType, Integer size, String expected) throws Exception {
		assertEquals(expected, testInstance.getTypeName(javaType, size));
	}
	
	@DataProvider
	public static Object[][] testMultiGetTypeNameData() {
		JavaTypeToSqlTypeMapping testInstance = new JavaTypeToSqlTypeMapping();
		testInstance.put(String.class, "VARCHAR");
		testInstance.put(String.class, 10, "CHAR($l)");
		return new Object[][] {
				new Object[] { testInstance, String.class, null, "VARCHAR" },
				new Object[] { testInstance, String.class, 5, "CHAR(5)" },
				new Object[] { testInstance, String.class, 10, "CHAR(10)" },
				new Object[] { testInstance, String.class, 20, "VARCHAR" },
		};
	}
	
	@Test
	@UseDataProvider("testMultiGetTypeNameData")
	public void testGetTypeName(JavaTypeToSqlTypeMapping testInstance, Class javaType, Integer size, String expected) throws Exception {
		assertEquals(expected, testInstance.getTypeName(javaType, size));
	}
}