package org.stalactite.persistence.sql.dml;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

import javax.annotation.Nonnull;

import org.stalactite.persistence.structure.Table;

/**
 * Classe d'accès aux méthodes PreparedStatement.setXXX(..) et ResultSet.getXXX(..) en fonction du type des colonnes
 * écrites ou lues.
 * Les méthodes {@link #setNotNull(int, Object, Class, PreparedStatement)} et {@link #getNotNull(Table.Column, ResultSet)}
 * peuvent être surchargées pour implémenter le codage/décodage d'un type particulier.
 * Les méthodes {@link #determineSetType(Object, Table.Column)} et {@link #determineGetType(Table.Column)} peuvent être
 * adaptées pour aiguiller une colonne vers un type particulier.
 * 
 * @author mary
 */
public class JDBCParameterBinder {
	
	public JDBCParameterBinder() {
		
	}
	
	/**
	 * Méthode chargée d'appliquer la valeur <t>value</t> à l'index <t>valueIndex</t> sur <t>statement</t>.
	 * 
	 * 
	 * @param valueIndex l'index du paramètre à fixer, à utiliser en premier paramètre de <t>Statement.setXXX(..)</t>
	 * @param value la valeur à passer en second paramètre de <t>Statement.setXXX(..)</t>
	 * @param column la colonne ciblée par <t>valueIndex</t>, autorise un ciblage plus fin que le type de value, informatif
	 * @param statement le PreparedStatement à utiliser
	 * @throws SQLException
	 */
	public void set(int valueIndex, Object value, Table.Column column, PreparedStatement statement) throws SQLException {
		Class setType = determineSetType(value, column);
		if (value == null) {
			setNull(valueIndex, setType, statement);
			// TODO: à implémenter avec un parsing de la requête, cf AbstractQueryImpl#expandParameterList
//		} else if (value instanceof Collection) {
//			Collection listParam = (Collection) value;
//			statement.setParameterList(valueIndex, listParam);
		} else {
			setNotNull(valueIndex, value, setType, statement);
		}
	}

	protected Class determineSetType(Object value, Table.Column column) {
		return column.getJavaType();
	}

	/**
	 * Cas spécial pour les valeurs null. Implémenté par {@link PreparedStatement#setObject(int, Object)} avec null en
	 * 2nd paramètre, mais tous les Drivers JDBC ne supportent pas cette façon de faire et nécessitent d'utiliser
	 * {@link PreparedStatement#setNull(int, int)}.
	 * A surcharger en fonction du support de setObject(index, null).
	 *
	 * @param valueIndex l'indice auquel il faut mettre null, 1er paramètre de setXXX(..)
	 * @param setType à titre informatif, permet d'avoir le type Java de la valeur null
	 * @param statement le PreparedStatement sur lequel on veut fixer null
	 * @throws SQLException
	 */
	protected void setNull(int valueIndex, Class setType, PreparedStatement statement) throws SQLException {
		statement.setObject(valueIndex, null);
	}

	protected void setNotNull(int valueIndex, @Nonnull Object value, Class setType, PreparedStatement statement) throws SQLException {
		if (Double.class.isAssignableFrom(setType)) {
			Double doubleParam = (Double) value;
			statement.setDouble(valueIndex, doubleParam);
		} else if (Float.class.isAssignableFrom(setType)) {
			Float floatParam = (Float) value;
			statement.setFloat(valueIndex, floatParam);
		} else if (Integer.class.isAssignableFrom(setType)) {
			Integer integerParam = (Integer) value;
			statement.setInt(valueIndex, integerParam);
		} else if (Long.class.isAssignableFrom(setType)) {
			Long longParam = (Long) value;
			statement.setLong(valueIndex, longParam);
		} else if (Date.class.isAssignableFrom(setType)) {
			Date dateParam = (Date) value;
			statement.setTimestamp(valueIndex, new Timestamp(dateParam.getTime()));
		} else if (String.class.isAssignableFrom(setType)) {
			String strParam = (String) value;
			statement.setString(valueIndex, strParam);
		} else {
			throw new UnsupportedOperationException("Type "+value.getClass() + " is not implemented");
		}
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
		Class getType = determineGetType(column);
		if (Double.class.isAssignableFrom(getType)) {
			toReturn = resultSet.getDouble(columnName);
		} else if (Float.class.isAssignableFrom(getType)) {
			toReturn = resultSet.getFloat(columnName);
		} else if (Integer.class.isAssignableFrom(getType)) {
			toReturn = resultSet.getInt(columnName);
		} else if (Long.class.isAssignableFrom(getType)) {
			toReturn = resultSet.getLong(columnName);
		} else if (Date.class.isAssignableFrom(getType)) {
			toReturn = new Date(resultSet.getTimestamp(columnName).getTime());
		} else if (String.class.isAssignableFrom(getType)) {
			toReturn = resultSet.getString(columnName);
		} else {
			throw new UnsupportedOperationException("Type " + getType + " is not implemented");
		}
		return toReturn;
	}

	protected Class determineGetType(Table.Column column) {
		return column.getJavaType();
	}
}
