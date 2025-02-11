package org.codefilarete.stalactite.sql;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.codefilarete.stalactite.mapping.id.sequence.DatabaseSequenceSelectBuilder;
import org.codefilarete.stalactite.query.builder.DMLNameProvider;
import org.codefilarete.stalactite.query.model.Fromable;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ddl.DDLTableGenerator;
import org.codefilarete.stalactite.sql.ddl.SqlTypeRegistry;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.statement.binder.PostgreSQLParameterBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.PostgreSQLTypeMapping;
import org.codefilarete.tool.collection.Arrays;

/**
 * @author Guillaume Mary
 */
public class PostgreSQLDialect extends DefaultDialect {
	
	private final PostgreSQLSequenceSelectBuilder postgreSQLSequenceSelectBuilder = new PostgreSQLSequenceSelectBuilder();
	
	public PostgreSQLDialect() {
		super(new PostgreSQLTypeMapping(), new PostgreSQLParameterBinderRegistry());
	}
	
	@Override
	protected DMLNameProviderFactory newDMLNameProviderFactory() {
		return PostgreSQLDMLNameProvider::new;
	}
	
	@Override
	public DDLTableGenerator newDdlTableGenerator() {
		return new PostgreSQLDDLTableGenerator(getSqlTypeRegistry(), PostgreSQLDMLNameProvider::new);
	}
	
	@Override
	public DatabaseSequenceSelectBuilder getDatabaseSequenceSelectBuilder() {
		return postgreSQLSequenceSelectBuilder;
	}
	
	public static class PostgreSQLDMLNameProvider extends DMLNameProvider {
		
		/** PostgreSQL keywords to be escaped. TODO: to be completed */
		public static final Set<String> KEYWORDS = Collections.unmodifiableSet(Arrays.asTreeSet(String.CASE_INSENSITIVE_ORDER));
		
		public PostgreSQLDMLNameProvider(Map<? extends Fromable, String> tableAliases) {
			super(tableAliases);
		}
		
		public PostgreSQLDMLNameProvider(Function<Fromable, String> tableAliaser) {
			super(tableAliaser);
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

	public static class PostgreSQLDDLTableGenerator extends DDLTableGenerator {

		public PostgreSQLDDLTableGenerator(SqlTypeRegistry typeMapping, DMLNameProviderFactory dmlNameProviderFactory) {
			super(typeMapping, dmlNameProviderFactory);
		}

		@Override
		protected String getSqlType(Column column) {
			String sqlType;
			if (column.isAutoGenerated()) {
				sqlType = " SERIAL";
			} else {
				sqlType = super.getSqlType(column);
			}
			return sqlType;
		}
	}
	
	@Override
	public boolean supportsTupleCondition() {
		return true;
	}
}
