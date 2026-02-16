package org.codefilarete.stalactite.sql.oracle.statement.binder;

import java.io.IOException;
import java.sql.Blob;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;

import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.statement.binder.NullAwareParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.PreparedStatementWriter;
import org.codefilarete.tool.io.IOs;

/**
 * @author Guillaume Mary
 */
public final class OracleParameterBinders {
	
	/**
	 * As of JDBC4, SQLite doesn't support setBinaryStream, therefore we use setBytes(..)
	 */
	public static final ParameterBinder<Blob> BLOB_BINDER = new NullAwareParameterBinder<>(
			DefaultParameterBinders.BLOB_BINDER, new BlobWriter());
	// Can also be done that wrapping setBinaryStream(..), but it doesn't call setBlob(..) which is preferred to match Binder philosophy
	// DefaultParameterBinders.BINARYSTREAM_BINDER.wrap(inputStream -> new InMemoryBlobSupport(IOs.toByteArray(inputStream)), Blob::getBinaryStream);
	
	/**
	 * Oracle native support for {@link ZonedDateTime}
	 */
	public static final ParameterBinder<ZonedDateTime> ZONED_DATE_TIME_BINDER = new NullAwareParameterBinder<>(
			new JdbcTypeResultSetReader<>(ZonedDateTime.class),
			new JdbcTypePreparedStatementWriter<>(ZonedDateTime.class, JDBCType.TIMESTAMP_WITH_TIMEZONE));
	
	/**
	 * Oracle native support for {@link OffsetDateTime}
	 */
	public static final ParameterBinder<OffsetDateTime> OFFSET_DATE_TIME_BINDER = new NullAwareParameterBinder<>(
			new JdbcTypeResultSetReader<>(OffsetDateTime.class),
			new JdbcTypePreparedStatementWriter<>(OffsetDateTime.class, JDBCType.TIMESTAMP_WITH_TIMEZONE));
	
	/**
	 * Dedicated writer for Oracle Blobs that handles non Oracle Blob to avoid a ClassCastException meaning that
	 * Oracle only support incoming oracle.sql.BLOB
	 * 
	 * @author Guillaume Mary
	 */
	private static class BlobWriter implements PreparedStatementWriter<Blob> {
		
		@Override
		public Class<Blob> getType() {
			return Blob.class;
		}
		
		@Override
		public void set(PreparedStatement preparedStatement, int valueIndex, Blob blob) throws SQLException {
			oracle.sql.BLOB oracleBlob;
			if (blob instanceof oracle.sql.BLOB) {
				oracleBlob = (oracle.sql.BLOB) blob;
			} else {
				oracleBlob = (oracle.sql.BLOB) preparedStatement.getConnection().createBlob();
				try {
					// note that setBytes(byte[]) is not supported
					oracleBlob.setBytes(1, IOs.toByteArray(blob.getBinaryStream()));
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
			preparedStatement.setBlob(valueIndex, oracleBlob);
			// note that we shouldn't free the blob here since transaction is not yet complete
			// oracleBlob.free();
		}
	}
	
	private OracleParameterBinders() {
	}
}
