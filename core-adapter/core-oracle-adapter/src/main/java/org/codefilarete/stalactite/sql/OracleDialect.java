package org.codefilarete.stalactite.sql;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.LongSupplier;

import org.codefilarete.stalactite.mapping.id.sequence.DatabaseSequenceSelectBuilder;
import org.codefilarete.stalactite.query.builder.DMLNameProvider;
import org.codefilarete.stalactite.query.model.Fromable;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ddl.OracleDDLTableGenerator;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.ColumnParameterizedSQL;
import org.codefilarete.stalactite.sql.statement.GeneratedKeysReader;
import org.codefilarete.stalactite.sql.statement.WriteOperation;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory;
import org.codefilarete.stalactite.sql.statement.binder.OracleParameterBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.OracleTypeMapping;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;

/**
 * @author Guillaume Mary
 */
public class OracleDialect extends DefaultDialect { 
	
	public OracleDialect() {
		super(new OracleTypeMapping(), new OracleParameterBinderRegistry());
	}
	
	@Override
	protected OracleDDLTableGenerator newDdlTableGenerator() {
		return new OracleDDLTableGenerator(getSqlTypeRegistry(), OracleDMLNameProvider::new);
	}
	
	@Override
	public boolean supportsTupleCondition() {
		return true;
	}
	
	@Override
	protected WriteOperationFactory newWriteOperationFactory() {
		return new OracleWriteOperationFactory();
	}
	
	/**
	 * Overridden to return dedicated Oracle generated keys reader because Oracle reads them from a specific column
	 * <strong>Only supports Integer</strong>
	 */
	@Override
	public <I> GeneratedKeysReader<I> buildGeneratedKeysReader(String keyName, Class<I> columnType) {
		return (GeneratedKeysReader<I>) new OracleGeneratedKeysReader(keyName);
	}
	
	@Override
	protected DMLNameProviderFactory newDMLNameProviderFactory() {
		return OracleDMLNameProvider::new;
	}
	
	@Override
	public DatabaseSequenceSelectBuilder getDatabaseSequenceSelectBuilder() {
		return sequenceName -> "select " + sequenceName + " from dual";
	}
	
	public static class OracleDMLNameProvider extends DMLNameProvider {
		
		/** Oracle keywords to be escaped. TODO: to be completed */
		public static final Set<String> KEYWORDS = Collections.unmodifiableSet(Arrays.asTreeSet(String.CASE_INSENSITIVE_ORDER));
		
		public OracleDMLNameProvider(Map<? extends Fromable, String> tableAliases) {
			super(tableAliases);
		}
		
		public OracleDMLNameProvider(Function<Fromable, String> tableAliaser) {
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
	
	/**
	 * {@link WriteOperationFactory} appropriate for Oracle : mainly indicates what columns must be retrieved while
	 * some generated key is expected.
	 * 
	 * @author Guillaume Mary
	 * @see OracleGeneratedKeysReader
	 */
	static class OracleWriteOperationFactory extends WriteOperationFactory {
		
		@Override
		public <T extends Table<T>> WriteOperation<Column<T, ?>> createInstanceForInsertion(ColumnParameterizedSQL<T> sqlGenerator,
																								 ConnectionProvider connectionProvider,
																								 LongSupplier expectedRowCount) {
			// Looking for autogenerated column (identifier policy is "after insertion") : it will be added to PreparedStatement descriptor
			Set<? extends Column<?, ?>> columns = ((ColumnParameterizedSQL<?>) sqlGenerator).getColumnIndexes().keySet();
			Column<?, ?> column = Iterables.find(columns, Column::isAutoGenerated);
			if (column != null) {
				return createInstance(sqlGenerator, connectionProvider,
						// Oracle requires passing the column name to be retrieved in the generated keys, else it gives back the RowId
						(connection, sql) -> connection.prepareStatement(sql, new String[] { column.getName() }), expectedRowCount);
			} else {
				// no autogenerated column => standard behavior
				return super.createInstanceForInsertion(sqlGenerator, connectionProvider, expectedRowCount);
			}
		}
	}
}
