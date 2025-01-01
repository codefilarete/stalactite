package org.codefilarete.stalactite.sql;

import org.codefilarete.stalactite.sql.ddl.DDLTableGenerator;
import org.codefilarete.stalactite.sql.ddl.MariaDBDDLTableGenerator;
import org.codefilarete.stalactite.sql.statement.GeneratedKeysReader;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
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
	
	public MariaDBDialect() {
		super(new MariaDBTypeMapping(), new ColumnBinderRegistry());
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
		return new MariaDBDDLTableGenerator(getSqlTypeRegistry());
	}
	
	@Override
	protected WriteOperationFactory newWriteOperationFactory() {
		return new MariaDBWriteOperationFactory();
	}
	
	@Override
	public boolean supportsTupleCondition() {
		return true;
	}
}
