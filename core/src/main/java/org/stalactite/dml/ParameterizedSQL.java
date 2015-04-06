package org.stalactite.dml;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.stalactite.dml.ExpandableSQL.ExpandableParameter;
import org.stalactite.dml.SQLParameterParser.ParsedSQL;
import org.stalactite.persistence.engine.PersistenceContext;
import org.stalactite.persistence.sql.Dialect;

/**
 * @author mary
 */
public class ParameterizedSQL {
	
	private final String originalSQL;
	
	private final Map<String, Object> values = new HashMap<>();
	
	private final ParsedSQL parsedSQL;

	public ParameterizedSQL(String originalSQL) {
		this.originalSQL = originalSQL;
		SQLParameterParser sqlParameterParser = new SQLParameterParser(this.originalSQL);
		this.parsedSQL = sqlParameterParser.parse();
	}
	
	public void set(String param, Object o) {
		this.values.put(param, o);
	}
	
	public int executeWrite() throws SQLException {
		PreparedStatement preparedStatement = prepareStatement();
		return preparedStatement.executeUpdate();
	}

	public ResultSet executeRead() throws SQLException {
		PreparedStatement preparedStatement = prepareStatement();
		return preparedStatement.executeQuery();
	}

	private PreparedStatement prepareStatement() throws SQLException {
		PersistenceContext currentPersistenceContext = PersistenceContext.getCurrent();
		ExpandableSQL expandableSQL = new ExpandableSQL(parsedSQL, values);
		PreparedStatement preparedStatement = currentPersistenceContext.getTransactionManager().getCurrentConnection().prepareStatement(expandableSQL.getPreparedSQL());
		Dialect currentDialect = currentPersistenceContext.getDialect();
		for (ExpandableParameter expandableParameter : expandableSQL.getExpandableParameters()) {
			for (Entry<Integer, Object> indexToValue : expandableParameter) {
				currentDialect.getParameterBinderRegistry().set(indexToValue.getKey(), indexToValue.getValue(), preparedStatement);
			}
		}
		return preparedStatement;
	}

}
