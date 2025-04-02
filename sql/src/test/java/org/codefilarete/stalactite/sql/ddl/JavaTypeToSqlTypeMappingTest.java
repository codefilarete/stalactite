package org.codefilarete.stalactite.sql.ddl;

import java.time.Month;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.sql.ddl.Size.fixedPoint;
import static org.codefilarete.stalactite.sql.ddl.Size.length;

/**
 * @author Guillaume Mary
 */
class JavaTypeToSqlTypeMappingTest {
	
	static Object[][] getTypeName_withSingleton() {
		JavaTypeToSqlTypeMapping testInstance1 = new JavaTypeToSqlTypeMapping();
		testInstance1.put(String.class, "VARCHAR");
		JavaTypeToSqlTypeMapping testInstance2 = new JavaTypeToSqlTypeMapping();
		testInstance2.put(String.class, "CHAR($l)", length(10));
		return new Object[][] {
				{ testInstance1, String.class, null, "VARCHAR" },
				{ testInstance2, String.class, length(10), "CHAR(10)" },
		};
	}
	
	@ParameterizedTest
	@MethodSource("getTypeName_withSingleton")
	void getTypeName_withSingleton(JavaTypeToSqlTypeMapping testInstance, Class javaType, Size size, String expected) {
		assertThat(testInstance.getTypeName(javaType, size)).isEqualTo(expected);
	}
	
	static Object[][] getTypeName() {
		JavaTypeToSqlTypeMapping testInstance = new JavaTypeToSqlTypeMapping();
		testInstance.put(CharSequence.class, "VARCHAR");
		testInstance.put(String.class, "TEXT");
		testInstance.put(String.class, "CHAR($l)", length(10));
		testInstance.put(String.class, "VARCHAR($l)", length(100));
		testInstance.put(Double.class, "decimal(10, 2)");
		testInstance.put(Double.class, "decimal($p, $s)", fixedPoint(Integer.MAX_VALUE, Integer.MAX_VALUE));
		testInstance.put(Float.class, "decimal($p)", fixedPoint(Integer.MAX_VALUE));
		testInstance.put(Enum.class, "myEnumType");	// a entry for Enum must be registered for make enum types work 
		testInstance.put(Month.class, "monthType");	// a entry for Enum must be registered for make enum types work 
		return new Object[][] {
				{ testInstance, String.class, null, "TEXT" },
				{ testInstance, String.class, length(5), "CHAR(5)" },
				{ testInstance, String.class, length(10), "CHAR(10)" },
				{ testInstance, String.class, length(50), "VARCHAR(50)" },
				{ testInstance, String.class, length(100), "VARCHAR(100)" },
				{ testInstance, String.class, length(101), "TEXT" },
				{ testInstance, Double.class, fixedPoint(7, 3), "decimal(7, 3)" },
				{ testInstance, Double.class, null, "decimal(10, 2)" },
				{ testInstance, Float.class, fixedPoint(10), "decimal(10)" },
				{ testInstance, CharSequence.class, null, "VARCHAR" },
				{ testInstance, CharSequence.class, length(20), "VARCHAR" },
				// testing interface inheritance
				{ testInstance, StringBuilder.class, null, "VARCHAR" },
				{ testInstance, StringBuilder.class, length(20), "VARCHAR" },
				// testing enum
				{ testInstance, TimeUnit.class, null, "myEnumType" },
				{ testInstance, Month.class, null, "monthType" },
		};
	}
	
	@ParameterizedTest
	@MethodSource("getTypeName")
	void getTypeName(JavaTypeToSqlTypeMapping testInstance, Class javaType, Size size, String expected) {
		assertThat(testInstance.getTypeName(javaType, size)).isEqualTo(expected);
	}
	
	@Test
	void getTypeName_unknownType_returnsNull() {
		JavaTypeToSqlTypeMapping testInstance = new JavaTypeToSqlTypeMapping();
		assertThat(testInstance.getTypeName(Object.class)).isNull();
	}
	
}