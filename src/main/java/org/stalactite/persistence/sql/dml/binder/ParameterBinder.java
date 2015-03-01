package org.stalactite.persistence.sql.dml.binder;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author mary
 */
public interface ParameterBinder<T> {
	
	/**
	 * Lit la colonne <t>columnName</t> ramenée par <t>resultSet</t>
	 * 
	 * @param columnName
	 * @param resultSet
	 * @return le contenu de la colonne <t>columnName</t>, typé en fonction de <t>column</t>
	 * @throws SQLException
	 */
	T get(String columnName, ResultSet resultSet) throws SQLException;

	/**
	 * Méthode chargée d'appliquer la valeur <t>value</t> à l'index <t>valueIndex</t> sur <t>statement</t>.
	 * 
	 * @param valueIndex l'index du paramètre à fixer, à utiliser en premier paramètre de <t>Statement.setXXX(..)</t>
	 * @param value la valeur à passer en second paramètre de <t>Statement.setXXX(..)</t>
	 * @param statement le PreparedStatement à utiliser
	 * @throws SQLException
	 */
	void set(int valueIndex, T value, PreparedStatement statement) throws SQLException;
}
