package org.gama.stalactite.persistence.sql.ddl;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class JavaTypeToSqlTypeMappingTest {
	
	public static final String SINGLETON_GET_TYPE_NAME_DATA = "singletonGetTypeNameData";
	public static final String MULTI_GET_TYPE_NAME_DATA = "multiGetTypeNameData";
	
	@BeforeMethod
	public void setUp() {
	}
	
	@DataProvider(name = SINGLETON_GET_TYPE_NAME_DATA)
	public Object[][] testSingletonGetTypeNameData() {
		JavaTypeToSqlTypeMapping testInstance1 = new JavaTypeToSqlTypeMapping();
		testInstance1.put(String.class, "VARCHAR");
		JavaTypeToSqlTypeMapping testInstance2 = new JavaTypeToSqlTypeMapping();
		testInstance2.put(String.class, 10, "CHAR($l)");
		return new Object[][] {
				new Object[] { testInstance1, String.class, null, "VARCHAR" },
				new Object[] { testInstance2, String.class, 10, "CHAR(10)" },
		};
	}
	
	@Test(dataProvider = SINGLETON_GET_TYPE_NAME_DATA)
	public void testGetTypeName_WithSingleton(JavaTypeToSqlTypeMapping testInstance, Class javaType, Integer size, String expected) throws Exception {
		Assert.assertEquals(testInstance.getTypeName(javaType, size), expected);
	}
	
	@DataProvider(name = MULTI_GET_TYPE_NAME_DATA)
	public Object[][] testMultiGetTypeNameData() {
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
	
	@Test(dataProvider = MULTI_GET_TYPE_NAME_DATA)
	public void testGetTypeName(JavaTypeToSqlTypeMapping testInstance, Class javaType, Integer size, String expected) throws Exception {
//		System.out.println(testInstance.getJavaTypeToSQLType());
		Assert.assertEquals(testInstance.getTypeName(javaType, size), expected);
	}
}