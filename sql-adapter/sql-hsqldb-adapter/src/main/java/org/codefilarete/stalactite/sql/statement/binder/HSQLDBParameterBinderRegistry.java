package org.codefilarete.stalactite.sql.statement.binder;

import java.io.InputStream;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;

/**
 * @author Guillaume Mary
 */
public class HSQLDBParameterBinderRegistry extends ParameterBinderRegistry {
	
	@Override
	protected void registerParameterBinders() {
		super.registerParameterBinders();
		// overwriting InputStream mapping due to HSQLDB usage variation
		register(InputStream.class, HSQLDBParameterBinders.BINARYSTREAM_BINDER);
		register(ZonedDateTime.class, HSQLDBParameterBinders.ZONED_DATE_TIME_BINDER);
		register(OffsetDateTime.class, HSQLDBParameterBinders.OFFSET_DATE_TIME_BINDER);
	}
}
