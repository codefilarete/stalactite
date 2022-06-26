package org.codefilarete.stalactite.sql;

import java.util.Collections;

import org.codefilarete.stalactite.sql.ddl.DDLTableGenerator;
import org.codefilarete.stalactite.sql.ddl.SqlTypeRegistry;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.ForeignKey;
import org.codefilarete.stalactite.sql.ddl.structure.Index;
import org.codefilarete.tool.StringAppender;

/**
 * @author Guillaume Mary
 */
public class MySQLDDLTableGenerator extends DDLTableGenerator {
	
	public MySQLDDLTableGenerator(SqlTypeRegistry typeMapping) {
		super(typeMapping, new MySQLDMLNameProvier(Collections.emptyMap()));
	}
	
	/**
	 * Overriden to change "drop constraint" into "drop foreign key", MySQL specific
	 */
	@Override
	public String generateDropForeignKey(ForeignKey foreignKey) {
		StringAppender sqlCreateTable = new StringAppender("alter table ", dmlNameProvider.getName(foreignKey.getTable()),
			" drop foreign key ", foreignKey.getName());
		return sqlCreateTable.toString();
	}
	
	@Override
	public String generateDropIndex(Index index) {
		StringAppender sqlDropColumn = new StringAppender("alter table ", dmlNameProvider.getName(index.getTable()),
			" drop index ", index.getName());
		return sqlDropColumn.toString();
	}
	
	@Override
	protected String getSqlType(Column column) {
		String sqlType = super.getSqlType(column);
		if (column.isAutoGenerated()) {
			sqlType += " auto_increment";
		}
		return sqlType;
	}
}
