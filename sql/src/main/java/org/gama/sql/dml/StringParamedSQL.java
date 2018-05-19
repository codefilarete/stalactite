package org.gama.sql.dml;

import java.sql.PreparedStatement;
import java.util.Map;

import org.gama.sql.binder.PreparedStatementWriter;
import org.gama.sql.binder.PreparedStatementWriterIndex;
import org.gama.sql.dml.ExpandableSQL.ExpandableParameter;
import org.gama.sql.dml.SQLParameterParser.ParsedSQL;

/**
 * Class that applies values to {@link PreparedStatement} according to SQL that contains named parameters.
 * Supports parameters expansion when passing {@link Iterable} as value of a parameter.
 * 
 * @author Guillaume Mary
 */
public class StringParamedSQL extends ExpandableStatement<String> {
	
	private final ParsedSQL parsedSQL;
	private ExpandableSQL expandableSQL;
	private boolean expandableSQLExpired = false;
	
	public StringParamedSQL(String originalSQL, Map<String, ? extends PreparedStatementWriter> parameterBinders) {
		this(new SQLParameterParser(originalSQL).parse(), parameterBinders);
	}
	
	public StringParamedSQL(ParsedSQL parsedSQL, Map<String, ? extends PreparedStatementWriter> parameterBinders) {
		super(null, parameterBinders);
		this.parsedSQL = parsedSQL;
	}
	
	public StringParamedSQL(String originalSQL, PreparedStatementWriterIndex<String, ? extends PreparedStatementWriter> parameterBinderProvider) {
		this(new SQLParameterParser(originalSQL).parse(), parameterBinderProvider);
	}
	
	public StringParamedSQL(ParsedSQL parsedSQL, PreparedStatementWriterIndex<String, ? extends PreparedStatementWriter> parameterBinderProvider) {
		super(null, parameterBinderProvider);
		this.parsedSQL = parsedSQL;
	}
	
	@Override
	public void setValues(Map<String, ?> values) {
		super.setValues(values);
		expandableSQLExpired = true;
	}
	
	@Override
	public void setValue(String index, Object value) {
		super.setValue(index, value);
		expandableSQLExpired = true;
	}
	
	@Override
	public String getSQLSource() {
		return this.parsedSQL.toString();
	}
	
	@Override
	public String getSQL() {
		ensureExpandableSQL(values);
		return expandableSQL.getPreparedSQL();
	}
	
	protected void ensureExpandableSQL(Map<String, Object> values) {
		if (expandableSQL == null || expandableSQLExpired) {
			expandableSQL = new ExpandableSQL(this.parsedSQL, getValuesSizes(values));
			expandableSQLExpired = false;
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
