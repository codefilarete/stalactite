package org.gama.sql.dml;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.gama.sql.binder.ParameterBinder;
import org.gama.sql.binder.ParameterBinderIndex;

/**
 * Parent class that defines methods for applying values to {@link PreparedStatement} that is supposed to be built
 * with the {@link #getSQL()} method. Mainly used by {@link SQLOperation} subclasses.
 * 
 * @see SQLOperation
 * 
 * @author Guillaume Mary
 */
public abstract class SQLStatement<ParamType> {
	
	protected final Map<ParamType, Object> values = new HashMap<>(5);
	
	protected final ParameterBinderIndex<ParamType> parameterBinderProvider;
	protected final Set<ParamType> indexes;
	
	protected SQLStatement(Map<ParamType, ParameterBinder> parameterBinders) {
		this(ParameterBinderIndex.fromMap(parameterBinders));
	}
	
	protected SQLStatement(ParameterBinderIndex<ParamType> parameterBinderProvider) {
		this.parameterBinderProvider = parameterBinderProvider;
		this.indexes = parameterBinderProvider.keys();
	}
	
	/**
	 * Set values to be given to the {@link PreparedStatement}. Values are not applied to {@link PreparedStatement}.
	 * 
	 * @see #applyValues(PreparedStatement) 
	 * @param values
	 */
	public void setValues(Map<ParamType, Object> values) {
		if (values.containsKey(null)) {
			throw new IllegalArgumentException("Parameter should not be null (parameter's value = " + values.get(null) + ")");
		}
		this.values.putAll(values);
	}
	
	/**
	 * Set a particular value to be given to the {@link PreparedStatement}. Values are not applied to {@link PreparedStatement}.
	 * 
	 * @see #applyValues(PreparedStatement) 
	 * @param index
	 * @param value
	 */
	public void setValue(ParamType index, Object value) {
		if (index == null) {
			throw new IllegalArgumentException("Parameter should not be null (parameter's value = " + value + ")");
		}
		this.values.put(index, value);
	}
	
	/**
	 * Expected to gives the SQL run in the {@link PreparedStatement}
	 * @return
	 */
	public abstract String getSQL();
	
	/**
	 * Calls right setXXX method (according to {@link ParameterBinder} given at contructor) on the given
	 * {@link PreparedStatement}. Called by {@link SQLOperation} classes.
	 * 
	 * @param statement
	 */
	public void applyValues(PreparedStatement statement) {
		Set<ParamType> paramTypes = values.keySet();
		if (!paramTypes.containsAll(indexes)) {
			HashSet<ParamType> missingIndexes = new HashSet<>(indexes);
			missingIndexes.removeAll(paramTypes);
			throw new IllegalArgumentException("Missing value for indexes " + missingIndexes + " in values " + values + " for \"" + getSQL() + "\"");
		}
		for (Entry<ParamType, Object> indexToValue : values.entrySet()) {
			try {
				doApplyValue(indexToValue.getKey(), indexToValue.getValue(), statement);
			} catch (Throwable t) {
				throw new RuntimeException("Error while applying value " + indexToValue.getValue() + " on parameter " + indexToValue.getKey()
						+ " on statement \"" + getSQL() + "\"", t);
			}
		}
	}
	
	public ParameterBinder<Object> getParameterBinder(ParamType parameter) {
		return parameterBinderProvider.getBinder(parameter);
	}
	
	/**
	 * Applies a value of a parameter to a statement. Implementation is let to children classes because it depends
	 * on {@link ParamType} and type of value.
	 * 
	 * @param key the parameter
	 * @param value the value of the parameter
	 * @param statement the statement to use
	 */
	protected abstract void doApplyValue(ParamType key, Object value, PreparedStatement statement);
	
	/**
	 * Applies a value at an index of a statement according to a binder. Accessible from children classes.
	 * 
	 * @param index the index of the parameter
	 * @param value the value of the parameter
	 * @param paramBinder the binder of the parameter on the statement
	 * @param statement the statement to use
	 */
	protected void doApplyValue(int index, Object value, ParameterBinder<Object> paramBinder, PreparedStatement statement) {
		try {
			paramBinder.set(index, value, statement);
		} catch (SQLException e) {
			throw new RuntimeException("Error while setting value " + value + " for parameter " + index + " on statement " + getSQL(), e);
		}
	}
	
}
