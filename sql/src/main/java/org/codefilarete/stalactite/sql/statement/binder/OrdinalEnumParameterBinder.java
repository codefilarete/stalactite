package org.codefilarete.stalactite.sql.statement.binder;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Enum {@link ParameterBinder} based on ordinal position of enums
 * 
 * @author Guillaume Mary
 */
public class OrdinalEnumParameterBinder<E extends Enum<E>> extends AbstractEnumParameterBinder<E> {
	
	/** Small index to optimize search of enum per ordinal when reading {@link ResultSet} */
	private final Map<Integer, E> enumPerOrdinal;
	
	public OrdinalEnumParameterBinder(Class<E> enumType) {
		super(enumType);
		enumPerOrdinal = new HashMap<>(enumType.getEnumConstants().length, 1);
		for (E enumConstant : enumType.getEnumConstants()) {
			enumPerOrdinal.put(enumConstant.ordinal(), enumConstant);
		}
	}
	
	@Override
	public Class<E> getType() {
		return enumType;
	}
	
	@Override
	public E doGet(ResultSet resultSet, String columnName) throws SQLException {
		return enumPerOrdinal.get(resultSet.getInt(columnName));
	}
	
	@Override
	public void set(PreparedStatement preparedStatement, int valueIndex, E value) throws SQLException {
		preparedStatement.setInt(valueIndex, value.ordinal());
	}
}
