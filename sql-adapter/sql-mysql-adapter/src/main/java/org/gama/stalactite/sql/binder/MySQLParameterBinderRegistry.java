package org.gama.stalactite.sql.binder;

/**
 * @author Guillaume Mary
 */
public class MySQLParameterBinderRegistry extends ParameterBinderRegistry {
	
	@Override
	protected void registerParameterBinders() {
		super.registerParameterBinders();
		// for now no override of default binders seems necessary for MySQL
	}
}
