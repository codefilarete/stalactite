package org.codefilarete.stalactite.sql;

import org.codefilarete.stalactite.sql.statement.GeneratedKeysReader;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderRegistry;

/**
 * @author Guillaume Mary
 */
public interface GeneratedKeysReaderFactory {
	
	<I> GeneratedKeysReader<I> build(String keyName, Class<I> columnType);
	
	class DefaultGeneratedKeysReaderFactory implements GeneratedKeysReaderFactory {
		private final ParameterBinderRegistry parameterBinderRegistry;
		
		public DefaultGeneratedKeysReaderFactory(ParameterBinderRegistry parameterBinderRegistry) {
			this.parameterBinderRegistry = parameterBinderRegistry;
		}
		
		@Override
		public <I> GeneratedKeysReader<I> build(String keyName, Class<I> columnType) {
			return new GeneratedKeysReader<>(keyName, parameterBinderRegistry.getBinder(columnType));
		}
	}
}
