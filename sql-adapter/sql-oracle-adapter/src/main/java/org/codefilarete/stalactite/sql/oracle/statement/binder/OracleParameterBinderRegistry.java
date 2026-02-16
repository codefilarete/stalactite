package org.codefilarete.stalactite.sql.oracle.statement.binder;

import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderRegistry;

import java.sql.Blob;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;

/**
 * @author Guillaume Mary
 */
public class OracleParameterBinderRegistry extends ParameterBinderRegistry {
	
	@Override
	protected void registerParameterBinders() {
		super.registerParameterBinders();
		// for now no override of default binders seems necessary for Oracle
		register(Blob.class, OracleParameterBinders.BLOB_BINDER);
		// Oracle supports natively ZonedDateTime and OffsetDateTime persistence
		register(ZonedDateTime.class, OracleParameterBinders.ZONED_DATE_TIME_BINDER);
		register(OffsetDateTime.class, OracleParameterBinders.OFFSET_DATE_TIME_BINDER);
	}
}
