package org.gama.stalactite.persistence.sql.ddl;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

/**
 * @author Guillaume Mary
 */
@RunWith(DataProviderRunner.class)
public class JavaTypeToSqlTypeMappingTest {
	
	@DataProvider
	public static Object[][] testGetTypeName_withSingleton() {
		JavaTypeToSqlTypeMapping testInstance1 = new JavaTypeToSqlTypeMapping();
		testInstance1.put(String.class, "VARCHAR");
		JavaTypeToSqlTypeMapping testInstance2 = new JavaTypeToSqlTypeMapping();
		testInstance2.put(String.class, 10, "CHAR($l)");
		return new Object[][] {
				{ testInstance1, String.class, null, "VARCHAR" },
				{ testInstance2, String.class, 10, "CHAR(10)" },
		};
	}
	
	@Test
	@UseDataProvider
	public void testGetTypeName_withSingleton(JavaTypeToSqlTypeMapping testInstance, Class javaType, Integer size, String expected) {
		assertEquals(expected, testInstance.getTypeName(javaType, size));
	}
	
	@DataProvider
	public static Object[][] testGetTypeName() {
		JavaTypeToSqlTypeMapping testInstance = new JavaTypeToSqlTypeMapping();
		testInstance.put(CharSequence.class, "VARCHAR");
		testInstance.put(String.class, "VARCHAR(255)");
		testInstance.put(String.class, 10, "CHAR($l)");
		return new Object[][] {
				{ testInstance, String.class, null, "VARCHAR(255)" },
				{ testInstance, String.class, 5, "CHAR(5)" },
				{ testInstance, String.class, 10, "CHAR(10)" },
				{ testInstance, String.class, 20, "VARCHAR(255)" },
				{ testInstance, CharSequence.class, null, "VARCHAR" },
				{ testInstance, CharSequence.class, 20, "VARCHAR" },
		};
	}
	
	@Test
	@UseDataProvider
	public void testGetTypeName(JavaTypeToSqlTypeMapping testInstance, Class javaType, Integer size, String expected) {
		assertEquals(expected, testInstance.getTypeName(javaType, size));
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void testGetTypeName_unkonwnType_exceptionIsThrown() {
		JavaTypeToSqlTypeMapping testInstance = new JavaTypeToSqlTypeMapping();
		testInstance.getTypeName(Object.class, null);
	}
	
	@DataProvider
	public static Object[][] testGetTypeName_column() {
		Table toto = new Table("toto");
		Column<String> columnA = toto.addColumn("a", String.class);
		
		JavaTypeToSqlTypeMapping testInstance1 = new JavaTypeToSqlTypeMapping();
		testInstance1.put(String.class, "VARCHAR(255)");
		JavaTypeToSqlTypeMapping testInstance2 = new JavaTypeToSqlTypeMapping();
		testInstance2.put(columnA, "CLOB");
		
		JavaTypeToSqlTypeMapping testInstanceWithOverrideByColumn = new JavaTypeToSqlTypeMapping();
		testInstanceWithOverrideByColumn.put(String.class, "VARCHAR(255)");
		testInstanceWithOverrideByColumn.put(columnA, "CLOB");
		
		return new Object[][] {
				{ testInstance1, columnA, "VARCHAR(255)" },
				{ testInstance2, columnA, "CLOB" },
				{ testInstanceWithOverrideByColumn, columnA, "CLOB" },
		};
	}
	
	@Test
	@UseDataProvider
	public void testGetTypeName_column(JavaTypeToSqlTypeMapping testInstance, Column column, String expected) {
		assertEquals(expected, testInstance.getTypeName(column));
	}
	
	@Test(expected = NullPointerException.class)	// because we can't create a column with null type, if possible addd a safe guard in getTypeName
	public void testGetTypeName_columnWithNullType_exceptionIsThrown() {
		Table toto = new Table("toto");
		toto.addColumn("a", null);
	}
}