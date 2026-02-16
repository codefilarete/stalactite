package org.codefilarete.stalactite.sql.postgresql.statement.binder;

import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderRegistry;

import java.sql.Blob;

/**
 * @author Guillaume Mary
 */
public class PostgreSQLParameterBinderRegistry extends ParameterBinderRegistry {
	
	@Override
	protected void registerParameterBinders() {
		super.registerParameterBinders();
		// overwriting Blob mapping due to PostgreSQL way of handling Blobs
		register(Blob.class, PostgreSQLParameterBinders.BLOB_BINDER);
	}
}
