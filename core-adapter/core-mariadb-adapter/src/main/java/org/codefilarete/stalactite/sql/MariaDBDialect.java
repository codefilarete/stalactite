package org.codefilarete.stalactite.sql;

import org.codefilarete.stalactite.mapping.id.sequence.DatabaseSequenceSelector;
import org.codefilarete.stalactite.sql.ddl.DDLTableGenerator;
import org.codefilarete.stalactite.sql.ddl.MariaDBDDLTableGenerator;
import org.codefilarete.stalactite.sql.ddl.structure.Sequence;
import org.codefilarete.stalactite.sql.statement.GeneratedKeysReader;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory;
import org.codefilarete.stalactite.sql.statement.binder.MariaDBParameterBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.MariaDBTypeMapping;

/**
 * Dialect specialization for MariaDB:
 * - drop foreign key SQL statement
 * - null on timestamp column else current time is default value
 * - Retryer to handle "lock wait timeout" that sometimes appears even in low concurrency
 * 
 * @author Guillaume Mary
 */
public class MariaDBDialect extends DefaultDialect {
	
	private final MariaDBSequenceSelectorFactory sequenceSelectorFactory = new MariaDBSequenceSelectorFactory();
	
	public MariaDBDialect() {
		super(new MariaDBTypeMapping(), new MariaDBParameterBinderRegistry());
	}
	
	/**
	 * Overridden to return dedicated MySQL generated keys reader because MySQL reads them from a specific column
	 * <strong>Only supports Integer</strong>
	 */
	@Override
	public <I> GeneratedKeysReader<I> buildGeneratedKeysReader(String keyName, Class<I> columnType) {
		return (GeneratedKeysReader<I>) new MariaDBGeneratedKeysReader();
	}
	
	@Override
	protected DMLNameProviderFactory newDMLNameProviderFactory() {
		return MariaDBDMLNameProvider::new;
	}
	
	@Override
	public DDLTableGenerator newDdlTableGenerator() {
		return new MariaDBDDLTableGenerator(getSqlTypeRegistry(), MariaDBDMLNameProvider::new);
	}
	
	@Override
	protected WriteOperationFactory newWriteOperationFactory() {
		return new MariaDBWriteOperationFactory();
	}
	
	@Override
	public DatabaseSequenceSelectorFactory getDatabaseSequenceSelectorFactory() {
		return sequenceSelectorFactory;
	}
	
	@Override
	public boolean supportsTupleCondition() {
		return true;
	}
	
	private class MariaDBSequenceSelectorFactory implements DatabaseSequenceSelectorFactory {
		
		@Override
		public DatabaseSequenceSelector create(Sequence databaseSequence, ConnectionProvider connectionProvider) {
			return new DatabaseSequenceSelector(databaseSequence, "select next value for " + databaseSequence.getAbsoluteName(), getReadOperationFactory(), connectionProvider);
		}
	}
}
