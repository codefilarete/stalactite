package org.codefilarete.stalactite.sql.binder;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * @author Guillaume Mary
 */
public interface PreparedStatementWriterIndex<K, BINDER extends PreparedStatementWriter> extends PreparedStatementWriterProvider<K> {
	
	/**
	 * @return all available keys
	 */
	Set<K> keys();
	
	/**
	 * Aims at being used for iterating over all keys and values instead of calling {@link #getWriter(Object)} for each element of {@link #keys()}
	 * @return all the key + value pairs of this index
	 */
	Set<Entry<K, BINDER>> all();
	
	/**
	 * Short way of getting a {@link PreparedStatementWriterProvider} from a Map
	 * @param parameterBinders the source of {@link PreparedStatementWriter}
	 * @return a {@link PreparedStatementWriterProvider} backed by the Map
	 */
	static <K, BINDER extends PreparedStatementWriter> PreparedStatementWriterIndex<K, BINDER> fromMap(Map<K, BINDER> parameterBinders) {
		return new PreparedStatementWriterIndexFromMap<>(parameterBinders);
	}
	
	/**
	 * A simple {@link ParameterBinderIndex} that takes its values from a {@link Map}
	 *
	 * @author Guillaume Mary
	 */
	class PreparedStatementWriterIndexFromMap<ParamType, BINDER extends PreparedStatementWriter> extends PreparedStatementWriterProviderFromMap<ParamType, BINDER> implements PreparedStatementWriterIndex<ParamType, BINDER> {
		
		public PreparedStatementWriterIndexFromMap(Map<ParamType, BINDER> parameterBinders) {
			super(parameterBinders);
		}
		
		@Override
		public Set<ParamType> keys() {
			return getParameterBinders().keySet();
		}
		
		@Override
		public Set<Entry<ParamType, BINDER>> all() {
			return getParameterBinders().entrySet();
		}
	}
}
