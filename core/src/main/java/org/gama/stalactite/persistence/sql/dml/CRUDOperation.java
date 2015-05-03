package org.gama.stalactite.persistence.sql.dml;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;

import org.gama.stalactite.ILogger;
import org.gama.stalactite.Logger;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Iterables.ForEach;
import org.gama.lang.exception.Exceptions;
import org.gama.stalactite.persistence.engine.PersistenceContext;
import org.gama.stalactite.persistence.mapping.PersistentValues;
import org.gama.stalactite.persistence.sql.dml.binder.ParameterBinder;
import org.gama.stalactite.persistence.sql.dml.binder.ParameterBinderRegistry;
import org.gama.stalactite.persistence.structure.Table.Column;

/**
 * Class for basic CRUD operations, not for complex select with joins nor update with subselect
 * 
 * @author mary
 */
public abstract class CRUDOperation {
	
	protected static final ILogger LOGGER = Logger.getLogger(CRUDOperation.class);
	
	private final String sql;
	
	private PreparedStatement statement;
	
	/**
	 * 
	 * @param sql
	 */
	public CRUDOperation(String sql) {
		this.sql = sql;
	}
	
	public String getSql() {
		return sql;
	}
	
	public PreparedStatement getStatement() {
		return statement;
	}

	/**
	 * Récupère les {@link ParameterBinder} de chaque couple colonne-indice à partir de la {@link ParameterBinderRegistry}.
	 * Méthode appelée par les classes filles à partir de leur correspondance colonne-indice.
	 * 
	 * @param colToIndexes la partie "upsert" ou where d'une opération SQL
	 * @return les {@link ParameterBinder} de chaque colonne-indice
	 */
	protected Map<Column, Map.Entry<Integer, ParameterBinder>> getBinders(Map<Column, Integer> colToIndexes) {
		HashMap<Column, Map.Entry<Integer, ParameterBinder>> binders = new HashMap<>(colToIndexes.size());
		ParameterBinderRegistry parameterBinderRegistry = PersistenceContext.getCurrent().getDialect().getParameterBinderRegistry();
		for (Map.Entry<Column, Integer> colToIndex : colToIndexes.entrySet()) {
			Column column = colToIndex.getKey();
			Integer value = colToIndex.getValue();
			ParameterBinder parameterBinder = parameterBinderRegistry.getBinder(column);
			binders.put(column, new AbstractMap.SimpleEntry<>(value, parameterBinder));
		}
		return binders;
	}
	
	protected void applyUpsertValues(Map<Column, Map.Entry<Integer, ParameterBinder>> colToIndexes, PersistentValues values) throws SQLException {
		LOGGER.trace("upsert values {}", values.getUpsertValues());
		applyValues(colToIndexes, values.getUpsertValues());
	}
	
	protected void applyWhereValues(Map<Column, Map.Entry<Integer, ParameterBinder>> colToIndexes, PersistentValues values) throws SQLException {
		LOGGER.trace("where values {}", values.getWhereValues());
		applyValues(colToIndexes, values.getWhereValues());
	}
	
	protected void applyValues(Map<Column, Map.Entry<Integer, ParameterBinder>> colToIndexes, Map<Column, Object> colToValues) throws SQLException {
		for (Map.Entry<Column, Object> colToValue : colToValues.entrySet()) {
			set(colToIndexes, colToValue.getKey(), colToValue.getValue());
		}
	}
	
	protected void set(Map<Column, Map.Entry<Integer, ParameterBinder>> colToIndexes, Column column, Object value) throws SQLException {
		Map.Entry<Integer, ParameterBinder> indexBinder = colToIndexes.get(column);
		if (indexBinder == null) {
			throw new IllegalArgumentException("No parameter found for column " + (column == null ? null : column.getAbsoluteName()));
		} else {
			try {
				bind(indexBinder.getKey(), value, indexBinder.getValue());
			} catch (NullPointerException e) {
				if (indexBinder.getKey() == null) {
					throw new NullPointerException("Null index for column " + column.getAbsoluteName() + " (" + column.getJavaType().getSimpleName() + ") on " + getSql());
				} else if (indexBinder.getValue() == null) {
					throw new NullPointerException("No parameter binder for column " + column.getAbsoluteName() + " (" + column.getJavaType().getSimpleName() + ") on " + getSql());
				} else {
					throw e;
				}
			}
		}
	}

	protected void bind(int valueIndex, Object value, ParameterBinder binder) throws SQLException {
		binder.set(valueIndex, value, statement);
	}
	
	/**
	 * Apply upsert and where values of PersistentValues to internal PreparedStatement created from Connection.
	 * Don't execute statement as we don't know if it's a select or not so return would be different.
	 * 
	 * @param values values to set to PreparedStatement
	 * @param connection a JDBC connection to create a PreparedStatement
	 * @throws SQLException
	 */
	public void apply(@Nonnull PersistentValues values, @Nonnull Connection connection) throws SQLException {
		apply(Arrays.asList(values), connection);
	}
	
	/**
	 * Massive version of {@link #apply(PersistentValues, Connection)}. Dedicated to non-select statement since each
	 * PersistentValues is added as batch to the preparedStatement, so use {@link PreparedStatement#executeBatch()} afterward.
	 * 
	 * @param values values to set to PreparedStatement
	 * @param connection a JDBC connection to create a PreparedStatement
	 * @throws SQLException
	 */
	public void apply(@Nonnull Iterable<PersistentValues> values, @Nonnull Connection connection) throws SQLException {
		prepare(connection);
		try {
			LOGGER.debug(getSql());
			Iterables.visit(values, new ForEach<PersistentValues, Void>() {
				@Override
				public Void visit(PersistentValues values) {
					try {
						applyValues(values);
					} catch (SQLException e) {
						Exceptions.throwAsRuntimeException(e);
					}
					return null;
				}
			});
		} catch (RuntimeException e) {
			// rethrow SQLException that was thrown as RuntimeException in loop
			Throwable t = e.getCause();
			if (t instanceof SQLException) {
				throw (SQLException) t;
			} else {
				throw e;
			}
		}
	}
	
	protected abstract void applyValues(PersistentValues values) throws SQLException;
	
	protected void prepare(Connection connection) throws SQLException {
		if (statement == null) {
			statement = connection.prepareStatement(sql);
		}
	}
}
