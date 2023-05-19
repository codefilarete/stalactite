package org.codefilarete.stalactite.sql.ddl;

import java.util.Collections;

import org.codefilarete.stalactite.sql.HSQLDBDialect.HSQLDBDMLNameProvider;
import org.codefilarete.stalactite.sql.ddl.DDLAppender;
import org.codefilarete.stalactite.sql.ddl.DDLTableGenerator;
import org.codefilarete.stalactite.sql.ddl.SqlTypeRegistry;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;

public class HSQLDBDDLTableGenerator extends DDLTableGenerator {
	
	public HSQLDBDDLTableGenerator(SqlTypeRegistry typeMapping) {
		super(typeMapping, new HSQLDBDMLNameProvider(Collections.emptyMap()));
	}
	
	@Override
	protected String getSqlType(Column column) {
		String sqlType = super.getSqlType(column);
		if (column.isAutoGenerated()) {
			sqlType += " GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)";
		}
		return sqlType;
	}
	
	/**
	 * Overridden to implement HSQLDB "unique" keyword
	 */
	@Override
	protected void generateCreatePrimaryKey(PrimaryKey primaryKey, DDLAppender sqlCreateTable) {
		sqlCreateTable.cat(", unique (")
				.ccat(primaryKey.getColumns(), ", ")
				.cat(")");
	}
}