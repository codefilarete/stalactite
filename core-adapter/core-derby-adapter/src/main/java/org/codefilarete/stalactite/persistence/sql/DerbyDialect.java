package org.codefilarete.stalactite.persistence.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.stream.LongStream;

import org.codefilarete.tool.function.ThrowingBiFunction;
import org.codefilarete.stalactite.persistence.sql.ddl.DDLTableGenerator;
import org.codefilarete.stalactite.persistence.sql.dml.ReadOperationFactory;
import org.codefilarete.stalactite.persistence.sql.dml.WriteOperationFactory;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.binder.DerbyTypeMapping;
import org.codefilarete.stalactite.sql.dml.DerbyReadOperation;
import org.codefilarete.stalactite.sql.dml.GeneratedKeysReader;
import org.codefilarete.stalactite.sql.dml.ReadOperation;
import org.codefilarete.stalactite.sql.dml.SQLStatement;
import org.codefilarete.stalactite.sql.dml.WriteOperation;
import org.codefilarete.stalactite.sql.dml.WriteOperation.RowCountListener;

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
	
	@Override
	protected WriteOperationFactory newWriteOperationFactory() {
		return new DerbyWriteOperationFactory();
	}
	
	public static class DerbyReadOperationFactory extends ReadOperationFactory {
		
		@Override
		public <ParamType> ReadOperation<ParamType> createInstance(SQLStatement<ParamType> sqlGenerator, ConnectionProvider connectionProvider) {
			return new DerbyReadOperation<>(sqlGenerator, connectionProvider);
		}
	}
	
	public static class DerbyWriteOperationFactory extends WriteOperationFactory {
		
		@Override
		protected <ParamType> WriteOperation<ParamType> createInstance(SQLStatement<ParamType> sqlGenerator,
																	   ConnectionProvider connectionProvider,
																	   ThrowingBiFunction<Connection, String, PreparedStatement, SQLException> statementProvider,
																	   RowCountListener rowCountListener) {
			return new DerbyWriteOperation<ParamType>(sqlGenerator, connectionProvider, rowCountListener) {
				@Override
				protected void prepareStatement(Connection connection) throws SQLException {
					this.preparedStatement = statementProvider.apply(connection, getSQL());
				}
			};
		}
	}
	
	/**
	 * Made package-private to be visible by {@link DerbyGeneratedKeysReader}
	 * @param <ParamType>
	 */
	static class DerbyWriteOperation<ParamType> extends WriteOperation<ParamType> {
		
		/** Updated row count of the last executed batch statement */
		private long updatedRowCount = 0;
		
		public DerbyWriteOperation(SQLStatement<ParamType> sqlGenerator, ConnectionProvider connectionProvider, RowCountListener rowCountListener) {
			super(sqlGenerator, connectionProvider, rowCountListener);
		}
		
		public long getUpdatedRowCount() {
			return updatedRowCount;
		}
		
		protected long[] doExecuteBatch() throws SQLException {
			long[] rowCounts = super.doExecuteBatch();
			this.updatedRowCount = LongStream.of(rowCounts).sum();
			return rowCounts;
		}
	}
}
