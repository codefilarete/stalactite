package org.codefilarete.stalactite.sql.statement.binder;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.Blob;

import org.codefilarete.tool.io.IOs;

import static org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders.BYTES_BINDER;

/**
 * @author Guillaume Mary
 */
public final class SQLiteParameterBinders {
	
	/**
	 * As of JDBC4, SQLite doesn't support setBinaryStream, therefore we use setBytes(..)
	 */
	public static final ParameterBinder<InputStream> BINARYSTREAM_BINDER = new NullAwareParameterBinder<>(BYTES_BINDER.wrap(ByteArrayInputStream::new, IOs::toByteArray));
	
	/**
	 * As of JDBC4, SQLite doesn't support setBinaryStream, therefore we use setBytes(..)
	 */
	public static final ParameterBinder<Blob> BLOB_BINDER = new NullAwareParameterBinder<>(BYTES_BINDER.wrap(InMemoryBlobSupport::new, blob -> IOs.toByteArray(blob.getBinaryStream())));
	
	private SQLiteParameterBinders() {
	}
}
