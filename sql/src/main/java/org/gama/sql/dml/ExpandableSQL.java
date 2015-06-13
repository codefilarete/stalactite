package org.gama.sql.dml;

import org.gama.lang.Strings;
import org.gama.lang.collection.ArrayIterator;
import org.gama.sql.dml.SQLParameterParser.Parameter;
import org.gama.sql.dml.SQLParameterParser.ParsedSQL;

import java.util.*;

/**
 * Class that creates a PreparedStatement from an SQL String with named parameters. Eases index of named parameters as
 * it manages SQL expansion for parameters that have Collection value: '?' are added as many as necessary.
 * Kind of wrapper over {@link ParsedSQL}, delegates parsing to {@link SQLParameterParser}.
 * 
 * @author Guillaume Mary
 */
public class ExpandableSQL {

	private final List<ExpandableParameter> expandableParameters;
	private final ParsedSQL parsedSQL;

	private String preparedSQL;
	private final Map<String, Object> values;

	public ExpandableSQL(ParsedSQL parsedSQL, Map<String, Object> values) {
		this.parsedSQL = parsedSQL;
		this.values = values;
		this.expandableParameters = new ArrayList<>(this.parsedSQL.getSqlSnippets().size() / 2);
		convertParsedParametersToExpandableParameters();
	}

	private void convertParsedParametersToExpandableParameters() {
		ExpandableParameter precedingParameter = null;
		StringBuilder preparedSQLBuilder = new StringBuilder();
		for (Object sqlSnippet : parsedSQL.getSqlSnippets()) {
			if (sqlSnippet instanceof Parameter) {
				String parameterName = ((Parameter) sqlSnippet).getName();
				if (!values.containsKey(parameterName)) {
					throw new IllegalArgumentException("Parameter " + parameterName + " is not set");
				} else {
					Object value = values.get(parameterName);
					ExpandableParameter snippetToAdd = new ExpandableParameter(value, parameterName, precedingParameter);
					this.expandableParameters.add(snippetToAdd);
					snippetToAdd.catParameterizedSQL(preparedSQLBuilder);
					// prepare next iteration
					precedingParameter = snippetToAdd;
				}
			} else {
				// sql snippet
				preparedSQLBuilder.append(sqlSnippet);
			}
		}
		this.preparedSQL = preparedSQLBuilder.toString();
	}

	public List<ExpandableParameter> getExpandableParameters() {
		return expandableParameters;
	}

	public String getPreparedSQL() {
		return preparedSQL;
	}
	
	/**
	 * Class that represents a named parameter in a SQL statement.
	 * It allows extension of single parameter mark (coded as a question mark '?') to multiple ones in case of
	 * Collection value. This is done on the {@link #catParameterizedSQL(StringBuilder)} method.
	 */
	public static class ExpandableParameter implements Iterable<Integer> {

		private static final String SQL_PARAMETER_MARK = "?";

		private static final String SQL_PARAMETER_MARK_1 = SQL_PARAMETER_MARK + ", ";
		
		private static final String SQL_PARAMETER_MARK_10 =
				SQL_PARAMETER_MARK_1 + SQL_PARAMETER_MARK_1 +
						SQL_PARAMETER_MARK_1 + SQL_PARAMETER_MARK_1 +
						SQL_PARAMETER_MARK_1 + SQL_PARAMETER_MARK_1 +
						SQL_PARAMETER_MARK_1 + SQL_PARAMETER_MARK_1 +
						SQL_PARAMETER_MARK_1 + SQL_PARAMETER_MARK_1;

		private static final String SQL_PARAMETER_MARK_100 =
				SQL_PARAMETER_MARK_10 + SQL_PARAMETER_MARK_10 +
						SQL_PARAMETER_MARK_10 + SQL_PARAMETER_MARK_10 +
						SQL_PARAMETER_MARK_10 + SQL_PARAMETER_MARK_10 +
						SQL_PARAMETER_MARK_10 + SQL_PARAMETER_MARK_10 +
						SQL_PARAMETER_MARK_10 + SQL_PARAMETER_MARK_10;
		
		/** The parameter value */
		private final int valueSize;
		/** The parameter name */
		private final String parameterName;
		/** Preceding parameter, for index computation, overall used in case of Collection valued parameters */
		private final ExpandableParameter precedingParameter;
		/** PreparedStatement parameter mark count for this parameter, 1 for single value, N for Collection value */
		private final int markCount;
		/** Index of the parameter in the PreparedStatement, the first one for Collection value */
		private Integer index;

		private ExpandableParameter(Object value, String parameterName, ExpandableParameter precedingParameter) {
			this.valueSize = value instanceof Collection ? ((Collection) value).size() : 1;
			this.parameterName = parameterName;
			this.precedingParameter = precedingParameter;
			if (value instanceof Collection) {
				this.markCount = ((Collection) value).size();
				if (this.markCount == 0) {
					// this case will throw an invalid SQL statement: "in ()" => we throw one before.
					throw new IllegalArgumentException("Empty collection for sql parameter " + parameterName);
				}
			} else {
				this.markCount = 1;
			}
		}

		public String getParameterName() {
			return parameterName;
		}
		
		/**
		 * Gives all indexes of this instance.
		 * 
		 * @return a one-sized array for single value parameter, a n-sized array for n-sized collection value parameter
		 */
		public Integer[] getMarkIndexes() {
			Integer[] indexes = new Integer[markCount];
			int startIndex = getFirstIndex();
			for (int i = 0; i < markCount; i++) {
				indexes[i] = startIndex + i;
			}
			return indexes;
		}
		
		/**
		 * Compute (from preceding parameter) and return the parameter index.
		 * 
		 * @return the parameter index
		 */
		public int getFirstIndex() {
			if (this.index == null) {
				if (this.precedingParameter != null) {
					this.index = this.precedingParameter.getFirstIndex() + this.precedingParameter.markCount;
				} else {
					this.index = 1;
				}
			}
			return this.index;
		}

		public StringBuilder catParameterizedSQL(StringBuilder stringBuilder) {
			if (valueSize > 1) {
				return expandParameters(stringBuilder);
			} else {
				return stringBuilder.append(SQL_PARAMETER_MARK);
			}
		}
		
		/**
		 * Convert the single valued parameter to a multiple one: add as many '?' as necessary 
		 * @return the changed sql
		 * @param stringBuilder
		 */
		protected StringBuilder expandParameters(StringBuilder stringBuilder) {
			StringBuilder sqlParameters = Strings.repeat(stringBuilder, valueSize, SQL_PARAMETER_MARK_1, SQL_PARAMETER_MARK_100, SQL_PARAMETER_MARK_10);
			sqlParameters.setLength(sqlParameters.length() - 2);
			return sqlParameters;
		}
		
		@Override
		public Iterator<Integer> iterator() {
			return new ArrayIterator<>(getMarkIndexes());
		}
	}
}
