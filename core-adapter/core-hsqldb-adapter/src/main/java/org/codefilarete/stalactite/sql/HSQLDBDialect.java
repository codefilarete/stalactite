package org.codefilarete.stalactite.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.codefilarete.stalactite.mapping.id.sequence.DatabaseSequenceSelectBuilder;
import org.codefilarete.stalactite.query.builder.DMLNameProvider;
import org.codefilarete.stalactite.query.model.Fromable;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ddl.HSQLDBDDLTableGenerator;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.HSQLDBWriteOperation;
import org.codefilarete.stalactite.sql.statement.SQLStatement;
import org.codefilarete.stalactite.sql.statement.WriteOperation;
import org.codefilarete.stalactite.sql.statement.WriteOperation.RowCountListener;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory;
import org.codefilarete.stalactite.sql.statement.binder.HSQLDBParameterBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.HSQLDBTypeMapping;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.function.ThrowingBiFunction;

/**
 * @author Guillaume Mary
 */
public class HSQLDBDialect extends DefaultDialect { 
	
	public HSQLDBDialect() {
		super(new HSQLDBTypeMapping(), new HSQLDBParameterBinderRegistry());
	}
	
	@Override
	protected DMLNameProviderFactory newDMLNameProviderFactory() {
		return HSQLDBDMLNameProvider::new;
	}
	
	@Override
	protected HSQLDBDDLTableGenerator newDdlTableGenerator() {
		return new HSQLDBDDLTableGenerator(getSqlTypeRegistry(), HSQLDBDMLNameProvider::new);
	}
	
	@Override
	protected WriteOperationFactory newWriteOperationFactory() {
		return new HSQLDBWriteOperationFactory();
	}
	
	@Override
	public DatabaseSequenceSelectBuilder getDatabaseSequenceSelectBuilder() {
		return sequenceName -> "CALL NEXT VALUE FOR " + sequenceName;
	}
	
	@Override
	public boolean supportsTupleCondition() {
		return true;
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
	
	public static class HSQLDBDMLNameProvider extends DMLNameProvider {
		
		/** HSQLDB keywords to be escape. TODO: to be completed */
		public static final Set<String> KEYWORDS = Collections.unmodifiableSet(Arrays.asTreeSet(String.CASE_INSENSITIVE_ORDER));
		
		public HSQLDBDMLNameProvider(Function<Fromable, String> tableAliaser) {
			super(tableAliaser);
		}
		
		public HSQLDBDMLNameProvider(Map<Table, String> tableAliases) {
			super(tableAliases);
		}
		
		@Override
		public String getSimpleName(Selectable<?> column) {
			if (KEYWORDS.contains(column.getExpression())) {
				return "`" + column.getExpression() + "`";
			}
			return super.getSimpleName(column);
		}
		
		@Override
		public String getName(Fromable table) {
			if (KEYWORDS.contains(table.getName())) {
				return "`" + super.getName(table) + "`";
			}
			return super.getName(table);
		}
	}
	
}
