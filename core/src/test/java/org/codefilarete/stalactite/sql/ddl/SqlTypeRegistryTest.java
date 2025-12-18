package org.codefilarete.stalactite.sql.ddl;

import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * @author Guillaume Mary
 */
class SqlTypeRegistryTest {
	
	public static Object[][] getTypeName_column() {
		Table toto = new Table("toto");
		Column<Table, String> columnA = toto.addColumn("a", String.class);
		
		SqlTypeRegistry testInstance1 = new SqlTypeRegistry(new JavaTypeToSqlTypeMapping());
		testInstance1.put(String.class, "VARCHAR(255)");
		SqlTypeRegistry testInstance2 = new SqlTypeRegistry();
		testInstance2.put(columnA, "CLOB");
		
		SqlTypeRegistry testInstanceWithOverrideByColumn = new SqlTypeRegistry();
		testInstanceWithOverrideByColumn.put(String.class, "VARCHAR(255)");
		testInstanceWithOverrideByColumn.put(columnA, "CLOB");
		
		return new Object[][] {
				{ testInstance1, columnA, "VARCHAR(255)" },
				{ testInstance2, columnA, "CLOB" },
				{ testInstanceWithOverrideByColumn, columnA, "CLOB" },
		};
	}
	
	@ParameterizedTest
	@MethodSource("getTypeName_column")
	void getTypeName_column(SqlTypeRegistry testInstance, Column column, String expected) {
		assertThat(testInstance.getTypeName(column)).isEqualTo(expected);
	}
}