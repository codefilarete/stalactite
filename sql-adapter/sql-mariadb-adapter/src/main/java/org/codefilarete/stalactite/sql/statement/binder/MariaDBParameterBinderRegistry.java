package org.codefilarete.stalactite.sql.statement.binder;

import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderRegistry;

/**
 * @author Guillaume Mary
 */
public class MariaDBParameterBinderRegistry extends ParameterBinderRegistry {
	
	@Override
	protected void registerParameterBinders() {
		super.registerParameterBinders();
		// for now no override of default binders seems necessary for MariaDB
	}
}
