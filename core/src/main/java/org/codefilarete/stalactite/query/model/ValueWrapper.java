package org.codefilarete.stalactite.query.model;

import org.codefilarete.stalactite.query.model.operator.SQLFunction;

/**
 * A wrapper to make raw value (String, Integer, etc) looks like {@link org.codefilarete.stalactite.query.model.operator.SQLFunction} and have
 * a unified shape.
 * 
 * @param <V> value type, can be a simple clas as String or Integer or a complex one as {@link org.codefilarete.stalactite.query.model.operator.SQLFunction}
 * @author Guillaume Mary
 */
public interface ValueWrapper<V> {
	
	void setValue(V value);
	
	V getValue();
	
	class RawValueWrapper<V> implements ValueWrapper<V> {
		
		private V value;
		
		public RawValueWrapper() {
		}
		
		public RawValueWrapper(V value) {
			this.value = value;
		}
		
		@Override
		public void setValue(V value) {
			this.value = value;
		}
		
		@Override
		public V getValue() {
			return value;
		}
	}
	
	/**
	 * 
	 * @param <V> concrete value type (type supported by the SQL function)
	 * @param <W> SQL argument type of the function
	 * @author Guillaume Mary
	 */
	class SQLFunctionWrapper<F extends SQLFunction<V>, V, W extends ValueWrapper<V>> implements ValueWrapper<V> {
		
		private final F function;
		private W value;
		
		public SQLFunctionWrapper(F function) {
			this.function = function;
		}
		
		public SQLFunctionWrapper(F function, W value) {
			this.function = function;
			this.value = value;
		}
		
		public SQLFunctionWrapper(F function, V value) {
			this(function, (W) new RawValueWrapper<>(value));
		}
		
		public F getFunction() {
			return function;
		}
		
		@Override
		public void setValue(V value) {
			this.value.setValue(value);
		}
		
		@Override
		public V getValue() {
			return value.getValue();
		}
	}
}
