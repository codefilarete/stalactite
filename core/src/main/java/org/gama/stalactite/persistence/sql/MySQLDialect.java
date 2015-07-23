package org.gama.stalactite.persistence.sql;

import org.gama.lang.StringAppender;
import org.gama.stalactite.persistence.sql.ddl.DDLSchemaGenerator;
import org.gama.stalactite.persistence.sql.ddl.DDLTableGenerator;
import org.gama.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.Table;

/**
 * @author Guillaume Mary
 */
public class MySQLDialect extends Dialect {
	
	public MySQLDialect(JavaTypeToSqlTypeMapping javaTypeToSqlTypeMapping) {
		super(javaTypeToSqlTypeMapping);
	}
	
	public MySQLDialect(JavaTypeToSqlTypeMapping javaTypeToSqlTypeMapping, ColumnBinderRegistry columnBinderRegistry) {
		super(javaTypeToSqlTypeMapping, columnBinderRegistry);
	}
	
	@Override
	public DDLSchemaGenerator getDDLSchemaGenerator(Iterable<Table> tablesToCreate) {
		return new DDLSchemaGenerator(tablesToCreate, getJavaTypeToSqlTypeMapping()) {
			@Override
			protected DDLTableGenerator newDDLTableGenerator(JavaTypeToSqlTypeMapping typeMapping) {
				return new DDLTableGenerator(getJavaTypeToSqlTypeMapping()) {
					/**
					 * Overriden to change "drop constraint" into "drop foreign key", MySQL specific
					 */
					@Override
					public String generateDropForeignKey(Table.ForeignKey foreignKey) {
						StringAppender sqlCreateTable = new StringAppender("alter table ", foreignKey.getTable().getName(),
								" drop foreign key ", foreignKey.getName());
						return sqlCreateTable.toString();
					}
				};
			}
		};
	}
}
