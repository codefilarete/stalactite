package org.stalactite.persistence.sql.dml.binder;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.WeakHashMap;

import javax.annotation.Nonnull;

import org.stalactite.persistence.structure.Table;

/**
 * Classe d'accès aux méthodes PreparedStatement.setXXX(..) et ResultSet.getXXX(..) en fonction du type des colonnes
 * écrites ou lues.
 * Les méthodes {@link #setNotNull(int, Object, Table.Column, PreparedStatement)} et {@link #getNotNull(Table.Column, ResultSet)}
 * peuvent être surchargées pour implémenter le codage/décodage d'un type particulier.
 * Les méthodes {@link #getBinder(Object, Table.Column)} et {@link #getBinder(Table.Column)} peuvent être
 * adaptées pour aiguiller une colonne vers un type particulier.
 * 
 * @author mary
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
		register(Float.class, new FloatBinder());
		register(Long.class, new LongBinder());
		register(Integer.class, new IntegerBinder());
		register(Date.class, new DateBinder());
	}

	/**
	 * Lit la colonne <t>column</t> ramenée par <t>resultSet</t>
	 * 
	 * @param column
	 * @param resultSet
	 * @return le contenu de la colonne <t>column</t>, typé en fonction de <t>column</t>
	 * @throws SQLException
	 */
	public Object get(Table.Column column, ResultSet resultSet) throws SQLException {
		if (!isNull(column, resultSet)) {
			return getNotNull(column, resultSet);
		} else {
			return null;
		}
	}
	
	protected boolean isNull(Table.Column column, ResultSet resultSet) throws SQLException {
		return resultSet.getObject(column.getName()) == null;
	}

	protected Object getNotNull(Table.Column column, ResultSet resultSet) throws SQLException {
		String columnName = column.getName();
		Object toReturn;
		ParameterBinder parameterBinder = getBinder(column);
		if (parameterBinder != null) {
			toReturn = parameterBinder.get(columnName, resultSet);
		} else {
			throwMissingBinderException(column);
			return null;	// unreachable code
		}
		return toReturn;
	}
	
	public ParameterBinder getBinder(Table.Column column) {
		return getBinder(column.getJavaType());
	}

	public ParameterBinder getBinder(Class clazz) {
		return getParameterBinders().get(clazz);
	}

	private void throwMissingBinderException(Table.Column column) {
		throw new UnsupportedOperationException("No parameter binder found for column " + column.getAbsoluteName());
	}

}
