package org.gama.stalactite.persistence.sql;

import org.gama.stalactite.persistence.sql.ddl.DDLTableGenerator;
import org.gama.stalactite.persistence.sql.dml.ReadOperationFactory;
import org.gama.stalactite.sql.ConnectionProvider;
import org.gama.stalactite.sql.binder.DerbyTypeMapping;
import org.gama.stalactite.sql.dml.DerbyReadOperation;
import org.gama.stalactite.sql.dml.GeneratedKeysReader;
import org.gama.stalactite.sql.dml.ReadOperation;
import org.gama.stalactite.sql.dml.SQLStatement;

/**
 * @author Guillaume Mary
 */
public class DerbyDialect extends Dialect {

	public DerbyDialect() {
		super(new DerbyTypeMapping());
	}

	/**
	 * Overriden to return dedicated Derby generated keys reader because Derby as a special management
	 * <strong>Only supports Integer</strong>
	 */
	@Override
	public <I> GeneratedKeysReader<I> buildGeneratedKeysReader(String keyName, Class<I> columnType) {
		return (GeneratedKeysReader<I>) new DerbyGeneratedKeysReader();
	}

	@Override
	protected DDLTableGenerator newDdlTableGenerator() {
		return new DerbyTableGenerator(getSqlTypeRegistry());
	}
	
	@Override
	protected ReadOperationFactory newReadOperationFactory() {
		return new DerbyReadOperationFactory();
	}
	
	public static class DerbyReadOperationFactory extends ReadOperationFactory {
		
		@Override
		public <ParamType> ReadOperation<ParamType> createInstance(SQLStatement<ParamType> sqlGenerator, ConnectionProvider connectionProvider) {
			return new DerbyReadOperation<>(sqlGenerator, connectionProvider);
		}
	}
}
