package org.gama.stalactite.persistence.sql;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Date;

import org.gama.lang.Retryer;
import org.gama.lang.StringAppender;
import org.gama.lang.exception.Exceptions;
import org.gama.stalactite.persistence.sql.ddl.DDLSchemaGenerator;
import org.gama.stalactite.persistence.sql.ddl.DDLTableGenerator;
import org.gama.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.ForeignKey;

/**
 * Dialect specialization for MySQL:
 * - drop foreign key SQL statement
 * - null on timestamp column else current time is default value
 * - Retryer to handle "lock wait timeout" that appears sometimes even is low concurrency
 * 
 * @author Guillaume Mary
 */
public class MySQLDialect extends Dialect {
	
	public MySQLDialect() {
		this(new MySQLTypeMapping());
	}
	
	public MySQLDialect(JavaTypeToSqlTypeMapping javaTypeToSqlTypeMapping) {
		this(javaTypeToSqlTypeMapping, new ColumnBinderRegistry());
	}
	
	public MySQLDialect(JavaTypeToSqlTypeMapping javaTypeToSqlTypeMapping, ColumnBinderRegistry columnBinderRegistry) {
		super(javaTypeToSqlTypeMapping, columnBinderRegistry);
		
		setWriteOperationRetryer(new Retryer(3, 200) {
			@Override
			protected boolean shouldRetry(Throwable t) {
				return Exceptions.findExceptionInHierarchy(t, SQLException.class, "Lock wait timeout exceeded; try restarting transaction") != null;
			}
		});
	}
	
	@Override
	public DDLSchemaGenerator getDdlSchemaGenerator() {
		DDLSchemaGenerator ddlSchemaGenerator = super.getDdlSchemaGenerator();
		ddlSchemaGenerator.setDdlTableGenerator(new DDLTableGenerator(getJavaTypeToSqlTypeMapping()) {
			/**
			 * Overriden to change "drop constraint" into "drop foreign key", MySQL specific
			 */
			@Override
			public String generateDropForeignKey(ForeignKey foreignKey) {
				StringAppender sqlCreateTable = new StringAppender("alter table ", foreignKey.getTable().getName(),
						" drop foreign key ", foreignKey.getName());
				return sqlCreateTable.toString();
			}
		});
		return ddlSchemaGenerator;
	}
	
	public static class MySQLTypeMapping extends DefaultTypeMapping {
		
		public MySQLTypeMapping() {
			super();
			put(Integer.class, "int");
			put(Integer.TYPE, "int");
			put(Date.class, "timestamp null");	// null allows nullable in MySQL, else current time is inserted by default
			put(LocalDateTime.class, "timestamp null");
			put(String.class, "varchar(255)");
		}
	}
}
