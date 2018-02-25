package org.gama.sql.binder;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * @author Guillaume Mary
 */
public interface PreparedStatementWriterIndex<K> extends PreparedStatementWriterProvider<K> {
	
	/**
	 * @return all available keys
	 */
	Set<K> keys();
	
	/**
	 * Aims at being used for iterating over all keys and values instead of calling {@link #getWriter(Object)} for each element of {@link #keys()}
	 * @return all the key + value pairs of this index
	 */
	Set<? extends Entry<K, ? extends PreparedStatementWriter>> all();
	
	/**
	 * Short way of getting a {@link PreparedStatementWriterProvider} from a Map
	 * @param parameterBinders the source of {@link PreparedStatementWriter}
	 * @return a {@link PreparedStatementWriterProvider} backed by the Map
	 */
	static <K> PreparedStatementWriterIndex<K> fromMap(Map<K, ? extends PreparedStatementWriter> parameterBinders) {
		return new PreparedStatementWriterIndexFromMap<>(parameterBinders);
	}
	
	/**
	 * A simple {@link ParameterBinderIndex} that takes its values from a {@link Map}
	 *
	 * @author Guillaume Mary
	 */
	class PreparedStatementWriterIndexFromMap<ParamType> extends PreparedStatementWriterProviderFromMap<ParamType> implements PreparedStatementWriterIndex<ParamType> {
		
		public PreparedStatementWriterIndexFromMap(Map<ParamType, ? extends PreparedStatementWriter> parameterBinders) {
			super(parameterBinders);
		}
		
		@Override
		public Set<ParamType> keys() {
			return getParameterBinders().keySet();
		}
		
		@Override
		public Set<? extends Entry<ParamType, ? extends PreparedStatementWriter>> all() {
			return getParameterBinders().entrySet();
		}
	}
}
