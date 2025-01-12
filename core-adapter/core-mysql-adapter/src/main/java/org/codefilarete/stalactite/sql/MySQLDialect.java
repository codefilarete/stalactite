package org.codefilarete.stalactite.sql;

import org.codefilarete.stalactite.sql.ddl.DDLTableGenerator;
import org.codefilarete.stalactite.sql.ddl.MySQLDDLTableGenerator;
import org.codefilarete.stalactite.sql.statement.GeneratedKeysReader;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory;
import org.codefilarete.stalactite.sql.statement.binder.MySQLParameterBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.MySQLTypeMapping;

/**
 * Dialect specialization for MySQL:
 * - drop foreign key SQL statement
 * - null on timestamp column else current time is default value
 * - Retryer to handle "lock wait timeout" that appears sometimes even in low concurrency
 * 
 * @author Guillaume Mary
 */
public class MySQLDialect extends DefaultDialect {
	
	public MySQLDialect() {
		super(new MySQLTypeMapping(), new MySQLParameterBinderRegistry());
	}

	/**
	 * Overridden to return dedicated MySQL generated keys reader because MySQL reads them from a specific column
	 * <strong>Only supports Integer</strong>
	 */
	@Override
	public <I> GeneratedKeysReader<I> buildGeneratedKeysReader(String keyName, Class<I> columnType) {
		return (GeneratedKeysReader<I>) new MySQLGeneratedKeysReader();
	}
	
	@Override
	protected DMLNameProviderFactory newDMLNameProviderFactory() {
		return MySQLDMLNameProvider::new;
	}
	
	@Override
	public DDLTableGenerator newDdlTableGenerator() {
		return new MySQLDDLTableGenerator(getSqlTypeRegistry());
	}
	
	@Override
	protected WriteOperationFactory newWriteOperationFactory() {
		return new MySQLWriteOperationFactory();
	}
	
	@Override
	public boolean supportsTupleCondition() {
		return true;
	}
}
