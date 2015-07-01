package org.gama.sql.dml;

import org.gama.sql.binder.ParameterBinder;
import org.gama.sql.dml.SQLParameterParser.ParsedSQL;

import java.sql.PreparedStatement;
import java.util.Map;

/**
 * Class that applies values to {@link PreparedStatement} according to SQL that contains named parameters.
 * Supports parameters expansion when passing {@link Iterable} as value of a parameter.
 * 
 * @author Guillaume Mary
 */
public class ParameterizedSQL extends AbstractParameterizedSQL<String> {
	
	public ParameterizedSQL(String originalSQL, Map<String, ParameterBinder> parameterBinders) {
		this(new SQLParameterParser(originalSQL).parse(), parameterBinders);
	}
	
	public ParameterizedSQL(ParsedSQL parsedSQL, Map<String, ParameterBinder> parameterBinders) {
		super(parsedSQL, parameterBinders);
	}
	
	@Override
	protected Map<String, Integer> getValuesSizes(Map<String, Object> values) {
		return ExpandableSQL.sizes(values);
	}
	
	@Override
	protected String getParameterName(String parameter) {
		return parameter;
	}
}
