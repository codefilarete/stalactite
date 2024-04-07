package org.codefilarete.stalactite.sql.statement.binder;

import java.io.IOException;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.SQLException;

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
