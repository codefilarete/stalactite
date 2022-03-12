package org.codefilarete.stalactite.persistence.sql;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.function.ThrowingBiFunction;
import org.codefilarete.stalactite.persistence.sql.ddl.DDLAppender;
import org.codefilarete.stalactite.persistence.sql.ddl.DDLTableGenerator;
import org.codefilarete.stalactite.persistence.sql.ddl.SqlTypeRegistry;
import org.codefilarete.stalactite.persistence.sql.dml.WriteOperationFactory;
import org.codefilarete.stalactite.persistence.structure.Column;
import org.codefilarete.stalactite.persistence.structure.PrimaryKey;
import org.codefilarete.stalactite.persistence.structure.Table;
import org.codefilarete.stalactite.query.builder.DMLNameProvider;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.binder.HSQLDBTypeMapping;
import org.codefilarete.stalactite.sql.dml.HSQLDBWriteOperation;
import org.codefilarete.stalactite.sql.dml.SQLStatement;
import org.codefilarete.stalactite.sql.dml.WriteOperation;
import org.codefilarete.stalactite.sql.dml.WriteOperation.RowCountListener;

/**
 * @author Guillaume Mary
 */
public class HSQLDBDialect extends Dialect { 
	
	public HSQLDBDialect() {
		super(new HSQLDBTypeMapping());
	}
	
	@Override
	protected HSQLDBDDLTableGenerator newDdlTableGenerator() {
		return new HSQLDBDDLTableGenerator(getSqlTypeRegistry());
	}
	
	public static class HSQLDBDDLTableGenerator extends DDLTableGenerator {
		
		public HSQLDBDDLTableGenerator(SqlTypeRegistry typeMapping) {
			super(typeMapping, new HSQLDBDMLNameProvier(Collections.emptyMap()));
		}
		
		@Override
		protected String getSqlType(Column column) {
			String sqlType = super.getSqlType(column);
			if (column.isAutoGenerated()) {
				sqlType += " GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)";
			}
			return sqlType;
		}
		
		/** Overriden to implement HSQLDB "unique" keyword */
		@Override
		protected void generateCreatePrimaryKey(PrimaryKey primaryKey, DDLAppender sqlCreateTable) {
			sqlCreateTable.cat(", unique (")
					.ccat(primaryKey.getColumns(), ", ")
					.cat(")");
		}
	}
	
	@Override
	protected WriteOperationFactory newWriteOperationFactory() {
		return new HSQLDBWriteOperationFactory();
	}
	
	static class HSQLDBWriteOperationFactory extends WriteOperationFactory {
		
		@Override
		protected <ParamType> WriteOperation<ParamType> createInstance(SQLStatement<ParamType> sqlGenerator,
																	   ConnectionProvider connectionProvider,
																	   ThrowingBiFunction<Connection, String, PreparedStatement, SQLException> statementProvider,
																	   RowCountListener rowCountListener) {
			return new HSQLDBWriteOperation<ParamType>(sqlGenerator, connectionProvider, rowCountListener) {
				@Override
				protected void prepareStatement(Connection connection) throws SQLException {
					this.preparedStatement = statementProvider.apply(connection, getSQL());
				}
			};
		}
		
	} 
	
	public static class HSQLDBDMLNameProvier extends DMLNameProvider {
		
		/** HSQLDB keywords to be escape. TODO: to be completed */
		public static final Set<String> KEYWORDS = Collections.unmodifiableSet(Arrays.asTreeSet(String.CASE_INSENSITIVE_ORDER));
		
		public HSQLDBDMLNameProvier(Map<Table, String> tableAliases) {
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
