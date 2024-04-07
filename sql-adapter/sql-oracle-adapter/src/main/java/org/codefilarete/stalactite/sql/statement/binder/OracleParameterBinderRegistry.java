package org.codefilarete.stalactite.sql.statement.binder;

import java.sql.Blob;

/**
 * @author Guillaume Mary
 */
public class OracleParameterBinderRegistry extends ParameterBinderRegistry {
	
	@Override
	protected void registerParameterBinders() {
		super.registerParameterBinders();
		// for now no override of default binders seems necessary for Oracle
		register(Blob.class, OracleParameterBinders.BLOB_BINDER);
	}
}
