package org.codefilarete.stalactite.sql.statement.binder;

import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.codefilarete.stalactite.sql.statement.binder.DefaultResultSetReaders.BINARYSTREAM_READER;

/**
 * Specialized {@link ParameterBinder}s for HSQLDB.
 * 
 * @author Guillaume Mary
 */
public final class HSQLDBParameterBinders {
	
	/**
	 * Specialization is made by overriding {@link PreparedStatement#setBinaryStream(int, InputStream)} for null values because it fails
	 * with "org.hsqldb.HsqlException: Invalid argument in JDBC call"
	 * HsqlDB 2.3.2
	 */
	public static final ParameterBinder<InputStream> BINARYSTREAM_BINDER = new LambdaParameterBinder<>(BINARYSTREAM_READER, new PreparedStatementWriter<InputStream>() {
		@Override
		public void set(PreparedStatement p, int i, InputStream v) throws SQLException {
			if (v == null) {
				p.setObject(i, null);
			} else {
				p.setBinaryStream(i, v);
			}
		}
		
		@Override
		public Class<InputStream> getType() {
			return InputStream.class;
		}
	});
	
	private HSQLDBParameterBinders() {}
}
