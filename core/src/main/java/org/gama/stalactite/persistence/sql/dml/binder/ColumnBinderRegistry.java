package org.gama.stalactite.persistence.sql.dml.binder;

import org.gama.sql.binder.ParameterBinder;
import org.gama.sql.binder.ParameterBinderRegistry;
import org.gama.stalactite.persistence.structure.Table;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Class to simplify access to method PreparedStatement.setXXX(..) and ResultSet.getXXX(..) according to Column type.
 * Methods {@link #getBinder(Class)} and {@link #getBinder(Table.Column)} can be overriden to implement a better match
 * on a more adapted {@link ParameterBinder}.
 * 
 * @author mary
 */
public class ColumnBinderRegistry {
	
	private ParameterBinderRegistry parameterBinderRegistry = new ParameterBinderRegistry();
	
	public ColumnBinderRegistry() {
	}

	public <T> void register(Class<T> clazz, ParameterBinder<T> parameterBinder) {
		this.parameterBinderRegistry.register(clazz, parameterBinder);
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
		try {
			return parameterBinderRegistry.get(column.getName(), column.getJavaType(), resultSet);
		} catch (UnsupportedOperationException e) {
			// transforming the exception for message rewriting
			throwMissingBinderException(column);
			// unreachable
			return null;
		}
	}

	public ParameterBinder getBinder(Table.Column column) {
		return getBinder(column.getJavaType());
	}

	public ParameterBinder getBinder(Class clazz) {
		return this.parameterBinderRegistry.getBinder(clazz);
	}

	private void throwMissingBinderException(Table.Column column) {
		throw new UnsupportedOperationException("No parameter binder found for column " + column.getAbsoluteName() + " (type " + column.getJavaType() + ")");
	}
}
