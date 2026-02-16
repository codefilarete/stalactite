package org.codefilarete.stalactite.sql.postgresql.statement.binder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;

import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.statement.binder.DefaultResultSetReaders;
import org.codefilarete.stalactite.sql.statement.binder.NullAwareParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.NullAwarePreparedStatementWriter;
import org.codefilarete.stalactite.sql.statement.binder.NullAwareResultSetReader;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.tool.io.IOs;
import org.postgresql.PGConnection;
import org.postgresql.largeobject.LargeObject;
import org.postgresql.largeobject.LargeObjectManager;

/**
 * @author Guillaume Mary
 */
public final class PostgreSQLParameterBinders {
	
	/**
	 * Equivalent to {@link DefaultParameterBinders#BLOB_BINDER} but uses PostgreSQL {@link LargeObjectManager} to write {@link Blob}
	 * as described in PostGreSQL documentation
	 */
	public static final ParameterBinder<Blob> BLOB_BINDER = new NullAwareParameterBinder<>(new NullAwareResultSetReader<>(DefaultResultSetReaders.BLOB_READER),
																						   new NullAwarePreparedStatementWriter<>((preparedStatement, valueIndex, value) -> {
				// Note that column is expected to be of type OID
				// see https://jdbc.postgresql.org/documentation/head/binary-data.html#binary-data-example
				PGConnection connection = preparedStatement.getConnection().unwrap(PGConnection.class);
				LargeObjectManager lobj = connection.getLargeObjectAPI();
				long oid = lobj.createLO(LargeObjectManager.READ | LargeObjectManager.WRITE);
				try (LargeObject obj = lobj.open(oid, LargeObjectManager.WRITE)) {
					try (OutputStream outputStream = obj.getOutputStream();
						InputStream binaryStream = value.getBinaryStream()) {
						IOs.copy(binaryStream, outputStream, 2048);
					} catch (IOException e) {
						throw new SQLException("Blob can't be copied as a PostgreSQL LargeObject", e);
					}
				}
				preparedStatement.setLong(valueIndex, oid);
			}));
	
	private PostgreSQLParameterBinders() {
	}
}
