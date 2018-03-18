package org.gama.stalactite.persistence.sql;

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Set;

import org.gama.lang.Retryer;
import org.gama.lang.StringAppender;
import org.gama.lang.collection.Arrays;
import org.gama.lang.exception.Exceptions;
import org.gama.stalactite.persistence.sql.ddl.DDLSchemaGenerator;
import org.gama.stalactite.persistence.sql.ddl.DDLTableGenerator;
import org.gama.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.ForeignKey;
import org.gama.stalactite.persistence.structure.Index;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.builder.DMLNameProvider;

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
	public DDLSchemaGenerator newDdlSchemaGenerator() {
		DDLSchemaGenerator ddlSchemaGenerator = super.newDdlSchemaGenerator();
		ddlSchemaGenerator.setDdlTableGenerator(new MySQLDDLTableGenerator(getJavaTypeToSqlTypeMapping()));
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
	
	public static class MySQLDDLTableGenerator extends DDLTableGenerator {
		
		public MySQLDDLTableGenerator(JavaTypeToSqlTypeMapping typeMapping) {
			super(typeMapping, new MySQLDMLNameProvier(Collections.emptyMap()));
		}
		
		/**
		 * Overriden to change "drop constraint" into "drop foreign key", MySQL specific
		 */
		@Override
		public String generateDropForeignKey(ForeignKey foreignKey) {
			StringAppender sqlCreateTable = new StringAppender("alter table ", dmlNameProvider.getSimpleName(foreignKey.getTable()),
					" drop foreign key ", foreignKey.getName());
			return sqlCreateTable.toString();
		}
		
		@Override
		public String generateDropIndex(Index index) {
			StringAppender sqlDropColumn = new StringAppender("alter table ", dmlNameProvider.getSimpleName(index.getTable()),
					" drop index ", index.getName());
			return sqlDropColumn.toString();
		}
	}
	
	public static class MySQLDMLNameProvier extends DMLNameProvider {
		
		/** MySQL keywords to be escape. TODO: to be completed */
		public static final Set<String> KEYWORDS = Collections.unmodifiableSet(Arrays.asTreeSet(String.CASE_INSENSITIVE_ORDER, "key", "keys",
				"table", "index", "group", "cursor",
				"interval", "in"
		));
		
		public MySQLDMLNameProvier(Map<Table, String> tableAliases) {
			super(tableAliases);
		}
		
		@Override
		public String getSimpleName(@Nonnull Column column) {
			if (KEYWORDS.contains(column.getName())) {
				return "`" + column.getName() + "`";
			}
			return super.getSimpleName(column);
		}
		
		@Override
		public String getSimpleName(Table table) {
			if (KEYWORDS.contains(table.getName())) {
				return "`" + super.getSimpleName(table) + "`";
			}
			return super.getSimpleName(table);
		}
	}
}
