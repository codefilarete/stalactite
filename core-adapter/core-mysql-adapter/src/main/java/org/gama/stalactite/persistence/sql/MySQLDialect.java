package org.gama.stalactite.persistence.sql;

import org.gama.stalactite.persistence.sql.ddl.DDLTableGenerator;
import org.gama.stalactite.persistence.sql.dml.WriteOperationFactory;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.sql.binder.MySQLTypeMapping;
import org.gama.stalactite.sql.dml.GeneratedKeysReader;

/**
 * Dialect specialization for MySQL:
 * - drop foreign key SQL statement
 * - null on timestamp column else current time is default value
 * - Retryer to handle "lock wait timeout" that appears sometimes even in low concurrency
 * 
 * @author Guillaume Mary
 */
public class MySQLDialect extends Dialect {
	
	public MySQLDialect() {
		super(new MySQLTypeMapping(), new ColumnBinderRegistry());
	}

	/**
	 * Overriden to return dedicated MySQL generated keys reader because MySQL reads them from a specific column
	 * <strong>Only supports Integer</strong>
	 */
	@Override
	public <I> GeneratedKeysReader<I> buildGeneratedKeysReader(String keyName, Class<I> columnType) {
		return (GeneratedKeysReader<I>) new MySQLGeneratedKeysReader();
	}
	
	@Override
	public DDLTableGenerator newDdlTableGenerator() {
		return new MySQLDDLTableGenerator(getSqlTypeRegistry());
	}
	
	@Override
	protected WriteOperationFactory newWriteOperationFactory() {
		return new MySQLWriteOperationFactory();
	}
	
}
