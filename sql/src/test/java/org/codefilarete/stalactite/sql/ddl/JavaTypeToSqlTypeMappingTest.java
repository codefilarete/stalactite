package org.codefilarete.stalactite.sql.ddl;

import java.time.Month;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Guillaume Mary
 */
class JavaTypeToSqlTypeMappingTest {
	
	static Object[][] getTypeName_withSingleton() {
		JavaTypeToSqlTypeMapping testInstance1 = new JavaTypeToSqlTypeMapping();
		testInstance1.put(String.class, "VARCHAR");
		JavaTypeToSqlTypeMapping testInstance2 = new JavaTypeToSqlTypeMapping();
		testInstance2.put(String.class, "CHAR($l)", 10);
		return new Object[][] {
				{ testInstance1, String.class, null, "VARCHAR" },
				{ testInstance2, String.class, 10, "CHAR(10)" },
		};
	}
	
	@ParameterizedTest
	@MethodSource("getTypeName_withSingleton")
	void getTypeName_withSingleton(JavaTypeToSqlTypeMapping testInstance, Class javaType, Integer size, String expected) {
		assertThat(testInstance.getTypeName(javaType, size)).isEqualTo(expected);
	}
	
	static Object[][] getTypeName() {
		JavaTypeToSqlTypeMapping testInstance = new JavaTypeToSqlTypeMapping();
		testInstance.put(CharSequence.class, "VARCHAR");
		testInstance.put(String.class, "TEXT");
		testInstance.put(String.class, "CHAR($l)", 10);
		testInstance.put(String.class, "VARCHAR($l)", 100);
		testInstance.put(Enum.class, "myEnumType");	// a entry for Enum must be registered for make enum types work 
		testInstance.put(Month.class, "monthType");	// a entry for Enum must be registered for make enum types work 
		return new Object[][] {
				{ testInstance, String.class, null, "TEXT" },
				{ testInstance, String.class, 5, "CHAR(5)" },
				{ testInstance, String.class, 10, "CHAR(10)" },
				{ testInstance, String.class, 50, "VARCHAR(50)" },
				{ testInstance, String.class, 100, "VARCHAR(100)" },
				{ testInstance, String.class, 101, "TEXT" },
				{ testInstance, CharSequence.class, null, "VARCHAR" },
				{ testInstance, CharSequence.class, 20, "VARCHAR" },
				// testing interface inheritance
				{ testInstance, StringBuilder.class, null, "VARCHAR" },
				{ testInstance, StringBuilder.class, 20, "VARCHAR" },
				// testing enum
				{ testInstance, TimeUnit.class, null, "myEnumType" },
				{ testInstance, Month.class, null, "monthType" },
		};
	}
	
	@ParameterizedTest
	@MethodSource("getTypeName")
	void getTypeName(JavaTypeToSqlTypeMapping testInstance, Class javaType, Integer size, String expected) {
		assertThat(testInstance.getTypeName(javaType, size)).isEqualTo(expected);
	}
	
	@Test
	void getTypeName_unknownType_returnsNull() {
		JavaTypeToSqlTypeMapping testInstance = new JavaTypeToSqlTypeMapping();
		assertThat(testInstance.getTypeName(Object.class)).isNull();
	}
	
}