package org.codefilarete.stalactite.sql.binder;

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
