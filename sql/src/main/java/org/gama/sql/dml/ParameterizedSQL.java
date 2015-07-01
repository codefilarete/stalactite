package org.gama.sql.dml;

import org.gama.sql.binder.ParameterBinder;
import org.gama.sql.dml.ExpandableSQL.ExpandableParameter;
import org.gama.sql.dml.SQLParameterParser.ParsedSQL;

import java.sql.PreparedStatement;
import java.util.Map;

/**
 * Class that applies values to {@link PreparedStatement} according to SQL that contains named parameters.
 * Supports parameters expansion when passing {@link Iterable} as value of a parameter.
 * 
 * @author Guillaume Mary
 */
public class ParameterizedSQL extends ExpandableStatement<String> {
	
	private final ParsedSQL parsedSQL;
	private ExpandableSQL expandableSQL;
	
	public ParameterizedSQL(String originalSQL, Map<String, ParameterBinder> parameterBinders) {
		this(new SQLParameterParser(originalSQL).parse(), parameterBinders);
	}
	
	public ParameterizedSQL(ParsedSQL parsedSQL, Map<String, ParameterBinder> parameterBinders) {
		super(null, parameterBinders);
		this.parsedSQL = parsedSQL;
	}
	
	@Override
	public String getSQL() {
		ensureExpandableSQL(values);
		return expandableSQL.getPreparedSQL();
	}
	
	protected void ensureExpandableSQL(Map<String, Object> values) {
		if (expandableSQL == null) {
			expandableSQL = new ExpandableSQL(this.parsedSQL, getValuesSizes(values));
		}
	}
	
	@Override
	protected int[] getIndexes(String parameter) {
		ExpandableParameter expandableParameter = expandableSQL.getExpandableParameters().get(getParameterName(parameter));
		return expandableParameter.getMarkIndexes();
	}
	
	protected Map<String, Integer> getValuesSizes(Map<String, Object> values) {
		return ExpandableSQL.sizes(values);
	}
	
	@Override
	protected String getParameterName(String parameter) {
		return parameter;
	}
}
