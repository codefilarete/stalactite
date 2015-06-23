package org.gama.sql.dml;

import org.gama.sql.binder.ParameterBinder;

import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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
	
	protected final Map<ParamType, ParameterBinder> parameterBinders;
	protected final Set<ParamType> indexes;
	
	protected SQLStatement(Map<ParamType, ParameterBinder> parameterBinders) {
		this.parameterBinders = parameterBinders;
		this.indexes = parameterBinders.keySet();
	}
	
	/**
	 * Set values to be given to the {@link PreparedStatement}. Values are not applied to {@link PreparedStatement}.
	 * 
	 * @see #applyValues(PreparedStatement) 
	 * @param values
	 */
	public void setValues(Map<ParamType, Object> values) {
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
		this.values.put(index, value);
	}
	
	/**
	 * Expected to gives the SQL run in the {@link PreparedStatement}
	 * @return
	 */
	public abstract String getSQL();
	
	/**
	 * Calls right setXXX method (accoding to {@link ParameterBinder} given at contructor) on the given
	 * {@link PreparedStatement}. Called by {@link SQLOperation} classes.
	 * 
	 * @param statement
	 */
	public abstract void applyValues(PreparedStatement statement);
}
