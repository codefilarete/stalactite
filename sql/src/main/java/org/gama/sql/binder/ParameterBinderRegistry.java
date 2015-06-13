package org.gama.sql.binder;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.WeakHashMap;

/**
 * Registry for {@link ParameterBinder}s according to their binding class.
 *
 * @author Guillaume Mary
 */
public class ParameterBinderRegistry {
	
	private final WeakHashMap<Class, ParameterBinder> parameterBinders = new WeakHashMap<>();
	
	public ParameterBinderRegistry() {
		registerParameterBinders();
	}
	
	public WeakHashMap<Class, ParameterBinder> getParameterBinders() {
		return parameterBinders;
	}
	
	public <T> void register(Class<T> clazz, ParameterBinder<T> parameterBinder) {
		parameterBinders.put(clazz, parameterBinder);
	}
	
	protected void registerParameterBinders() {
		register(String.class, new StringBinder());
		register(Double.class, new DoubleBinder());
		register(Double.TYPE, new DoubleBinder());
		register(Float.class, new FloatBinder());
		register(Float.TYPE, new FloatBinder());
		register(Long.class, new LongBinder());
		register(Long.TYPE, new LongBinder());
		register(Integer.class, new IntegerBinder());
		register(Integer.TYPE, new IntegerBinder());
		register(Date.class, new DateBinder());
		register(Boolean.class, new BooleanBinder());
		register(Boolean.TYPE, new BooleanBinder());
	}
	
	/**
	 * Read the <t>columnName</t> retrieved by <t>resultSet</t>
	 *
	 * @param columnName the name of the column to be read
	 * @param expectedClass the expected type of return object
	 * @param resultSet the result set to read
	 * @return <t>columnName</t> content, typed as <t>expectedClass</t>
	 * @throws SQLException
	 * @throws UnsupportedOperationException if <t>expectedClass</t> is unknown from registry
	 */
	public <T> T get(String columnName, Class<T> expectedClass, ResultSet resultSet) throws SQLException {
		if (!isNull(columnName, resultSet)) {
			return getNotNull(columnName, expectedClass, resultSet);
		} else {
			return null;
		}
	}
	
	protected boolean isNull(String columnName, ResultSet resultSet) throws SQLException {
		// wasNull est utilisable également, mais getObject(String) est également supporté. protected au cas où.
		return resultSet.getObject(columnName) == null;
	}
	
	protected <T> T getNotNull(String columnName, Class<T> expectedClass, ResultSet resultSet) throws SQLException {
		T toReturn;
		ParameterBinder<T> parameterBinder = getBinder(expectedClass);
		if (parameterBinder != null) {
			toReturn = parameterBinder.get(columnName, resultSet);
		} else {
			throwMissingBinderException(expectedClass);
			return null;	// unreachable code
		}
		return toReturn;
	}
	
	public <T> void set(int index, T value, PreparedStatement preparedStatement) throws SQLException {
		if (value == null) {
			// Attention, pas mal de débat autour de l'utilisation de setObject(int, null) sur le Net, mais finalement
			// il semble que ça soit de plus en plus supporté.
			preparedStatement.setObject(index, null);
		} else {
			ParameterBinder<T> parameterBinder = (ParameterBinder<T>) getBinder(value.getClass());
			if (parameterBinder != null) {
				parameterBinder.set(index, value, preparedStatement);
			} else {
				throwMissingBinderException(value.getClass());
			}
		}
	}
	
	public <T> ParameterBinder<T> getBinder(Class<T> clazz) {
		return getParameterBinders().get(clazz);
	}
	
	private void throwMissingBinderException(Class clazz) {
		throw new UnsupportedOperationException("No parameter binder found for type " + clazz.getName());
	}
	
}
