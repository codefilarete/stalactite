package org.gama.stalactite.sql.ddl;

import java.util.concurrent.TimeUnit;

import org.gama.stalactite.sql.dml.SQLStatement.BindingException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * @author Guillaume Mary
 */
public class JavaTypeToSqlTypeMappingTest {
	
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
	
	@ParameterizedTest
	@MethodSource("testGetTypeName_withSingleton")
	public void testGetTypeName_withSingleton(JavaTypeToSqlTypeMapping testInstance, Class javaType, Integer size, String expected) {
		assertThat(testInstance.getTypeName(javaType, size)).isEqualTo(expected);
	}
	
	public static Object[][] testGetTypeName() {
		JavaTypeToSqlTypeMapping testInstance = new JavaTypeToSqlTypeMapping();
		testInstance.put(CharSequence.class, "VARCHAR");
		testInstance.put(String.class, "VARCHAR(255)");
		testInstance.put(String.class, 10, "CHAR($l)");
		testInstance.put(Enum.class, "myEnumType");	// a entry for Enum must be registered for make enum types work 
		return new Object[][] {
				{ testInstance, String.class, null, "VARCHAR(255)" },
				{ testInstance, String.class, 5, "CHAR(5)" },
				{ testInstance, String.class, 10, "CHAR(10)" },
				{ testInstance, String.class, 20, "VARCHAR(255)" },
				{ testInstance, CharSequence.class, null, "VARCHAR" },
				{ testInstance, CharSequence.class, 20, "VARCHAR" },
				// testing interface inheritance
				{ testInstance, StringBuilder.class, null, "VARCHAR" },
				{ testInstance, StringBuilder.class, 20, "VARCHAR" },
				// testing enum
				{ testInstance, TimeUnit.class, null, "myEnumType" },
		};
	}
	
	@ParameterizedTest
	@MethodSource("testGetTypeName")
	public void testGetTypeName(JavaTypeToSqlTypeMapping testInstance, Class javaType, Integer size, String expected) {
		assertThat(testInstance.getTypeName(javaType, size)).isEqualTo(expected);
	}
	
	@Test
	public void testGetTypeName_unkonwnType_exceptionIsThrown() {
		JavaTypeToSqlTypeMapping testInstance = new JavaTypeToSqlTypeMapping();
		assertThatExceptionOfType(BindingException.class).isThrownBy(() -> testInstance.getTypeName(Object.class, null));
	}
	
}