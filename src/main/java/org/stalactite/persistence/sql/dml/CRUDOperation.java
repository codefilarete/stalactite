package org.stalactite.persistence.sql.dml;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import javax.annotation.Nonnull;

import org.stalactite.ILogger;
import org.stalactite.Logger;
import org.stalactite.lang.collection.Arrays;
import org.stalactite.lang.collection.Iterables;
import org.stalactite.lang.collection.Iterables.ForEach;
import org.stalactite.lang.exception.Exceptions;
import org.stalactite.persistence.engine.PersistenceContext;
import org.stalactite.persistence.mapping.PersistentValues;
import org.stalactite.persistence.structure.Table.Column;

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
	
	protected void applyUpsertValues(Map<Column, Integer> colToIndexes, PersistentValues values) throws SQLException {
		LOGGER.trace("upsert values {}", values.getUpsertValues());
		applyValues(colToIndexes, values.getUpsertValues());
	}
	
	protected void applyWhereValues(Map<Column, Integer> colToIndexes, PersistentValues values) throws SQLException {
		LOGGER.trace("where values {}", values.getWhereValues());
		applyValues(colToIndexes, values.getWhereValues());
	}
	
	protected void applyValues(Map<Column, Integer> colToIndexes, Map<Column, Object> colToValuesMap) throws SQLException {
		for (Map.Entry<Column, Object> colToValues : colToValuesMap.entrySet()) {
			set(colToIndexes, colToValues.getKey(), colToValues.getValue());
		}
	}
	
	protected void set(Map<Column, Integer> colToIndexes, Column column, Object value) throws SQLException {
		Integer index = colToIndexes.get(column);
		if (index == null) {
			throw new IllegalArgumentException();
		} else {
			set(index, value, column);
		}
	}

	/**
	 * Fixe au statement la valeur value du paramètre valueIndex.
	 * La difficulté réside dans le choix de la bonne méthode Statement.setXXX(..) à appeler en fonction du type
	 * de la valeur. Un algorithme relativement simple à base de if instanceof est possible mais parfois insuffisant pour des
	 * cas fins en fonction des tables ou des colonnes, c'est pourquoi l'information column est passée en paramètre à titre
	 * informatif car elle permet de savoir qu'elle est exactement la colonne cible, ce que ne permet pas valueIndex.
	 * 
	 * Cette implémentation délègue ce traitement au Dialect courant (via {@link PersistenceContext#getCurrent()} ce qui permet
	 * une meilleure adaptation en fonction de la base de donnée cible.
	 * 
	 * @param valueIndex l'index du paramètre à fixer, à utiliser en premier paramètre de Statement.setXXX(..)
	 * @param value la valeur à passer en second paramètre de Statement.setXXX(..)
	 * @param column la colonne ciblée par valueIndex, autorise un ciblage plus fin que le type de value, informatif
	 * @throws SQLException
	 */
	public void set(int valueIndex, Object value, Column column) throws SQLException {
		PersistenceContext.getCurrent().getDialect().getJdbcParameterBinder().set(valueIndex, value, column, statement);
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
