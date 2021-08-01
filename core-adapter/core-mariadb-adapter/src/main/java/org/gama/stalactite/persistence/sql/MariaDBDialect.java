package org.gama.stalactite.persistence.sql;

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.gama.lang.Retryer;
import org.gama.lang.StringAppender;
import org.gama.lang.collection.Arrays;
import org.gama.lang.exception.Exceptions;
import org.gama.stalactite.persistence.sql.ddl.DDLTableGenerator;
import org.gama.stalactite.persistence.sql.ddl.SqlTypeRegistry;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.ForeignKey;
import org.gama.stalactite.persistence.structure.Index;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.builder.DMLNameProvider;
import org.gama.stalactite.sql.binder.MariaDBTypeMapping;

/**
 * Dialect specialization for MariaDB:
 * - drop foreign key SQL statement
 * - null on timestamp column else current time is default value
 * - Retryer to handle "lock wait timeout" that appears sometimes even is low concurrency
 * 
 * @author Guillaume Mary
 */
public class MariaDBDialect extends Dialect {
	
	public MariaDBDialect() {
		super(new MariaDBTypeMapping(), new ColumnBinderRegistry());
		
		setWriteOperationRetryer(new Retryer(3, 200) {
			@Override
			protected boolean shouldRetry(Throwable t) {
				return Exceptions.findExceptionInCauses(t, SQLException.class, "Lock wait timeout exceeded; try restarting transaction") != null;
			}
		});
	}
	
	@Override
	public DDLTableGenerator newDdlTableGenerator() {
		return new MariaDBDDLTableGenerator(getSqlTypeRegistry());
	}

	public static class MariaDBDDLTableGenerator extends DDLTableGenerator {
		
		public MariaDBDDLTableGenerator(SqlTypeRegistry typeMapping) {
			super(typeMapping, new MariaDBDMLNameProvier(Collections.emptyMap()));
		}
		
		/**
		 * Overriden to change "drop constraint" into "drop foreign key", MariaDB specific
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
		
		@Override
		protected String getSqlType(Column column) {
			String sqlType = super.getSqlType(column);
			if (column.isAutoGenerated()) {
				sqlType += " auto_increment";
			}
			return sqlType;
		}
	}
	
	public static class MariaDBDMLNameProvier extends DMLNameProvider {
		
		/** MariaDB keywords to be escape. TODO: to be completed */
		public static final Set<String> KEYWORDS = Collections.unmodifiableSet(Arrays.asTreeSet(String.CASE_INSENSITIVE_ORDER, "key", "keys",
				"table", "index", "group", "cursor",
				"interval", "in"
		));
		
		public MariaDBDMLNameProvier(Map<Table, String> tableAliases) {
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