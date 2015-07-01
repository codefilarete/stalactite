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
public abstract class AbstractParameterizedSQL<ParamType> extends ExpandableStatement<ParamType> {
	
	private final ParsedSQL parsedSQL;
	private ExpandableSQL expandableSQL;
	
	public AbstractParameterizedSQL(String originalSQL, Map<ParamType, ParameterBinder> parameterBinders) {
		this(new SQLParameterParser(originalSQL).parse(), parameterBinders);
	}
	
	public AbstractParameterizedSQL(ParsedSQL parsedSQL, Map<ParamType, ParameterBinder> parameterBinders) {
		super(null, parameterBinders);
		this.parsedSQL = parsedSQL;
	}
	
	@Override
	public String getSQL() {
		ensureExpandableSQL(values);
		return expandableSQL.getPreparedSQL();
	}
	
	protected void ensureExpandableSQL(Map<ParamType, Object> values) {
		if (expandableSQL == null) {
			expandableSQL = new ExpandableSQL(this.parsedSQL, getValuesSizes(values));
		}
	}
	
	@Override
	protected int[] getIndexes(ParamType parameter) {
		ExpandableParameter expandableParameter = expandableSQL.getExpandableParameters().get(getParameterName(parameter));
		return expandableParameter.getMarkIndexes();
	}
	
	protected abstract Map<String, Integer> getValuesSizes(Map<ParamType, Object> values);
}
