package org.codefilarete.stalactite.sql.sqlite.statement.binder;

import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderRegistry;

import java.io.InputStream;
import java.sql.Blob;

/**
 * @author Guillaume Mary
 */
public class SQLiteParameterBinderRegistry extends ParameterBinderRegistry {
	
	@Override
	protected void registerParameterBinders() {
		super.registerParameterBinders();
		register(InputStream.class, SQLiteParameterBinders.BINARYSTREAM_BINDER);
		register(Blob.class, SQLiteParameterBinders.BLOB_BINDER);
	}
}
