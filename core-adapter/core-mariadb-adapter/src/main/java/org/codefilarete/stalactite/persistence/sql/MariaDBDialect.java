package org.codefilarete.stalactite.persistence.sql;

import org.codefilarete.stalactite.persistence.sql.ddl.DDLTableGenerator;
import org.codefilarete.stalactite.persistence.sql.statement.WriteOperationFactory;
import org.codefilarete.stalactite.persistence.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.MariaDBTypeMapping;

/**
 * Dialect specialization for MariaDB:
 * - drop foreign key SQL statement
 * - null on timestamp column else current time is default value
 * - Retryer to handle "lock wait timeout" that sometimes appears even in low concurrency
 * 
 * @author Guillaume Mary
 */
public class MariaDBDialect extends Dialect {
	
	public MariaDBDialect() {
		super(new MariaDBTypeMapping(), new ColumnBinderRegistry());
	}
	
	@Override
	public DDLTableGenerator newDdlTableGenerator() {
		return new MariaDBDDLTableGenerator(getSqlTypeRegistry());
	}
	
	@Override
	protected WriteOperationFactory newWriteOperationFactory() {
		return new MariaDBWriteOperationFactory();
	}
	
}
