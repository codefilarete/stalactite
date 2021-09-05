package org.gama.stalactite.sql.dml;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.gama.lang.collection.Iterables;
import org.gama.stalactite.sql.binder.ParameterBinder;
import org.gama.stalactite.sql.binder.PreparedStatementWriter;
import org.gama.stalactite.sql.binder.PreparedStatementWriterIndex;
import org.gama.stalactite.sql.binder.PreparedStatementWriterProvider;

/**
 * Parent class that defines methods for applying values to {@link PreparedStatement} that is supposed to be built
 * with the {@link #getSQL()} method. Mainly used by {@link SQLOperation} subclasses.
 * 
 * @author Guillaume Mary
 * @see SQLOperation
 */
public abstract class SQLStatement<ParamType> {
	
	protected final Map<ParamType, Object> values = new HashMap<>(5);
	
	protected final PreparedStatementWriterProvider<ParamType> parameterBinderProvider;
	/** Set of keys/parameters/indexes available in the statement */
	protected final Set<ParamType> expectedParameters;
	
	/**
	 * 
	 * @param parameterBinders expected to be the exact necessary binders of every parameter in the SQL order (no more, no less).
	 * Checked by {@link #assertValuesAreApplyable()}
	 */
	protected SQLStatement(Map<? extends ParamType, ? extends PreparedStatementWriter> parameterBinders) {
		this(PreparedStatementWriterIndex.fromMap(parameterBinders));
	}
	
	/**
	 *
	 * @param parameterBinderProvider expected to be the exact necessary binders of every parameter in the SQL order (no more, no less).
	 * Checked by {@link #assertValuesAreApplyable()}
	 */
	protected SQLStatement(PreparedStatementWriterIndex<? extends ParamType, ? extends PreparedStatementWriter> parameterBinderProvider) {
		this.parameterBinderProvider = (PreparedStatementWriterProvider<ParamType>) parameterBinderProvider;
		this.expectedParameters = (Set<ParamType>) parameterBinderProvider.keys();
	}
	
	/**
	 * Set values to be given to the {@link PreparedStatement}. Values are not applied to {@link PreparedStatement}.
	 * Not expected to be called by external API
	 * 
	 * @see #applyValues(PreparedStatement) 
	 * @param values
	 */
	public void setValues(Map<ParamType, ?> values) {
		this.values.putAll(values);
	}
	
	/**
	 * Set a particular value to be given to the {@link PreparedStatement}. Values are not applied to {@link PreparedStatement}.
	 * 
	 * @see #applyValues(PreparedStatement) 
	 * @param index value key in statement, usually an int or a String depending on internal statement type 
	 * @param value value to set for index
	 */
	public void setValue(ParamType index, Object value) {
		this.values.put(index, value);
	}
	
	/**
	 * @return a non-modifiable copy of values (because subclasses may not support direct modifications and it's even not encouraged by this class)
	 */
	public Map<ParamType, Object> getValues() {
		return Collections.unmodifiableMap(values);
	}
	
	/**
	 * Gives original sql. Essentially used for logging
	 * 
	 * @return the sql given at construction time
	 */
	public String getSQLSource() {
		return getSQL();
	}
	
	/**
	 * Expected to give the SQL run in the {@link PreparedStatement}
	 * @return the SQL run in the {@link PreparedStatement}
	 */
	public abstract String getSQL();
	
	/**
	 * Calls right setXXX method (according to {@link ParameterBinder} given at contructor) on the given
	 * {@link PreparedStatement}. Called by {@link SQLOperation} classes.
	 * 
	 * @param statement target for values
	 */
	public void applyValues(PreparedStatement statement) {
		assertValuesAreApplyable();
		for (Entry<ParamType, Object> indexToValue : values.entrySet()) {
			try {
				doApplyValue(indexToValue.getKey(), indexToValue.getValue(), statement);
			} catch (RuntimeException | OutOfMemoryError e) {
				// NB: in case of BindingException it will be wrapped with some more friendly parameter name
				// because the original one may use only index (see doApplyValue(..))
				throw new BindingException(indexToValue.getValue(), indexToValue.getKey(), getSQL(), e);
			}
		}
	}
	
	public void assertValuesAreApplyable() {
		Set<ParamType> paramTypes = values.keySet();
		// looking for missing values
		Set<ParamType> indexDiff = Iterables.minus(expectedParameters, paramTypes);
		if (!indexDiff.isEmpty()) {
			throw new IllegalArgumentException("Missing value for parameters " + indexDiff + " in values " + values + " in \"" + getSQL() + "\"");
		}
		// Looking for undefined binder
		Set<ParamType> missingParameters = values.keySet().stream().filter(paramType -> parameterBinderProvider.doGetWriter(paramType) == null).collect(Collectors.toSet());
		if (!missingParameters.isEmpty()) {
			throw new IllegalArgumentException("Missing binder for " + missingParameters + " for values " + values + " in \"" + getSQL() + "\"");
		}
	}
	
	public PreparedStatementWriter<Object> getParameterBinder(ParamType parameter) {
		return parameterBinderProvider.getWriter(parameter);
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
	protected <T> void doApplyValue(int index, T value, PreparedStatementWriter<T> paramBinder, PreparedStatement statement) {
		try {
			paramBinder.set(statement, index, value);
		} catch (SQLException e) {
			throw new BindingException(value, index, getSQL(), e);
		}
	}
	
	public static class BindingException extends RuntimeException {
		
		public BindingException(String message) {
			super(message);
		}
		
		public BindingException(String message, Throwable cause) {
			super(message, cause);
		}
		
		public BindingException(Object value, Object paramId, String sql) {
			this("Error while setting value " + value + " for parameter " + paramId + " on statement " + sql);
		}
		
		public BindingException(Object value, Object paramId, String sql, Throwable cause) {
			this(value, paramId, sql);
			initCause(cause);
		}
	}
}
