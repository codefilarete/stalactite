package org.codefilarete.stalactite.engine;

import org.codefilarete.stalactite.sql.DMLNameProviderFactory;
import org.codefilarete.stalactite.sql.ddl.SqlTypeRegistry;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderIndex;

/**
 * @author Guillaume Mary
 */
public interface SQLOperationsFactoriesBuilder {
	
	SQLOperationsFactories build(ParameterBinderIndex<Column, ParameterBinder> columnBinderRegistry, DMLNameProviderFactory dmlNameProviderFactory, SqlTypeRegistry sqlTypeRegistry);
}
