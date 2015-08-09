package org.gama.stalactite.persistence.sql;

import org.gama.lang.StringAppender;
import org.gama.stalactite.persistence.sql.ddl.DDLSchemaGenerator;
import org.gama.stalactite.persistence.sql.ddl.DDLTableGenerator;
import org.gama.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.Table;

import java.util.Date;

/**
 * @author Guillaume Mary
 */
public class MySQLDialect extends Dialect {
	
	public MySQLDialect() {
		this(new MySQLTypeMapping());
	}
	
	public MySQLDialect(JavaTypeToSqlTypeMapping javaTypeToSqlTypeMapping) {
		super(javaTypeToSqlTypeMapping);
	}
	
	public MySQLDialect(JavaTypeToSqlTypeMapping javaTypeToSqlTypeMapping, ColumnBinderRegistry columnBinderRegistry) {
		super(javaTypeToSqlTypeMapping, columnBinderRegistry);
	}
	
	@Override
	public DDLSchemaGenerator getDdlSchemaGenerator() {
		DDLSchemaGenerator ddlSchemaGenerator = super.getDdlSchemaGenerator();
		ddlSchemaGenerator.setDdlTableGenerator(new DDLTableGenerator(getJavaTypeToSqlTypeMapping()) {
			/**
			 * Overriden to change "drop constraint" into "drop foreign key", MySQL specific
			 */
			@Override
			public String generateDropForeignKey(Table.ForeignKey foreignKey) {
				StringAppender sqlCreateTable = new StringAppender("alter table ", foreignKey.getTable().getName(),
						" drop foreign key ", foreignKey.getName());
				return sqlCreateTable.toString();
			}
		});
		return ddlSchemaGenerator;
	}
	
	public static class MySQLTypeMapping extends JavaTypeToSqlTypeMapping {
		
		public MySQLTypeMapping() {
			super();
			put(Boolean.class, "bit");
			put(Boolean.TYPE, "bit");
			put(Double.class, "double");
			put(Double.TYPE, "double");
			put(Float.class, "float");
			put(Float.TYPE, "float");
			put(Long.class, "bigint");
			put(Long.TYPE, "bigint");
			put(Integer.class, "integer");
			put(Integer.TYPE, "integer");
			put(Date.class, "timestamp null");	// null allows nullable in MySQL, else current time is inserted by default
			put(String.class, "varchar(255)");
			put(String.class, 16383, "varchar($l)");
		}
	}
}
