package org.gama.sql.binder;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.Arrays;

import org.gama.lang.Nullable;
import org.gama.lang.io.IOs;

import static org.gama.sql.binder.DefaultPreparedStatementWriters.BINARYSTREAM_WRITER;
import static org.gama.sql.binder.DefaultPreparedStatementWriters.BLOB_INPUTSTREAM_WRITER;
import static org.gama.sql.binder.DefaultPreparedStatementWriters.BLOB_WRITER;
import static org.gama.sql.binder.DefaultPreparedStatementWriters.BYTES_WRITER;

/**
 * @author Guillaume Mary
 */
public final class DerbyParameterBinders {
	
	/**
	 * Equivalent to {@link DefaultParameterBinders#BINARYSTREAM_BINDER} but makes a copy of the read binary stream from ResultSet, else later reading fails
	 * with "java.io.IOException: The object is already closed",
	 * even it the connection is still open and the ResultSet hasn't changed of row. Looks weird.
	 * Looks like https://issues.apache.org/jira/browse/DERBY-6341 but I don't get where I'm wrong (don't see where I read twice the ResultSet).
	 */
	public static final ParameterBinder<InputStream> BINARYSTREAM_BINDER = new LambdaParameterBinder<>((resultSet, columnName) ->
			Nullable.nullable(resultSet.getBinaryStream(columnName)).mapThrower(inputStream -> {
				try {
					return IOs.toByteArrayInputStream(inputStream);
				} catch (IOException e) {
					throw new SQLException(e);
				}
			}).get(), BINARYSTREAM_WRITER);
	
	public static final ParameterBinder<InputStream> BLOB_INPUTSTREAM_BINDER = new LambdaParameterBinder<>((resultSet, columnName) ->
			Nullable.nullable(resultSet.getBinaryStream(columnName)).mapThrower(inputStream -> {
				try {
					return IOs.toByteArrayInputStream(inputStream);
				} catch (IOException e) {
					throw new SQLException(e);
				}
			}).get(), BLOB_INPUTSTREAM_WRITER);
	
	/**
	 * Equivalent to {@link DefaultParameterBinders#BYTES_BINDER} but makes a copy of the read binary stream from ResultSet, else later reading fails
	 * with "Stream or LOB value cannot be retrieved more than once".
	 */
	public static final ParameterBinder<byte[]> BYTES_BINDER = new LambdaParameterBinder<>((resultSet, columnName) ->
			Nullable.nullable(resultSet.getBytes(columnName)).mapThrower(bytes -> Arrays.copyOf(bytes, bytes.length)).get(), BYTES_WRITER);
	
	/**
	 * Equivalent to {@link DefaultParameterBinders#BLOB_BINDER} but makes a copy of the read binary stream from ResultSet, else later reading fails
	 * with "Stream or LOB value cannot be retrieved more than once".
	 */
	public static final ParameterBinder<Blob> BLOB_BINDER = new LambdaParameterBinder<>((resultSet, columnName) ->
			Nullable.nullable(resultSet.getBlob(columnName)).mapThrower(blob -> new InMemoryBlobSupport(blob.getBytes(1, (int) blob.length()))).get(), BLOB_WRITER);
	
	private DerbyParameterBinders() {
	}
}
