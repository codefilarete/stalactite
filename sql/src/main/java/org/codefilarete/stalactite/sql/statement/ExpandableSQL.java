package org.codefilarete.stalactite.sql.statement;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.codefilarete.stalactite.sql.statement.SQLParameterParser.Parameter;
import org.codefilarete.stalactite.sql.statement.SQLParameterParser.ParsedSQL;
import org.codefilarete.tool.Strings;

/**
 * Class that helps to adapt an SQL String with named parameters to a prepared statement, according to the values of those parameters :
 * It manages SQL expansion for parameters that have Collection value ('?' are added as many as necessary)
 * Kind of wrapper over {@link ParsedSQL}, delegates parsing to {@link SQLParameterParser}.
 * 
 * @author Guillaume Mary
 */
public class ExpandableSQL {
	
	public static Map<String, Integer> sizes(Map<String, Object> values) {
		Map<String, Integer> toReturn = new HashMap<>();
		for (Entry<String, Object> entry : values.entrySet()) {
			toReturn.put(entry.getKey(), entry.getValue() instanceof Collection ? ((Collection) entry.getValue()).size() : 1);
		}
		return toReturn;
	}

	private final Map<String, ExpandableParameter> expandableParameters;
	private final ParsedSQL parsedSQL;
	private final Map<String, Integer> parameterComponentTypeSize;
	
	private String preparedSQL;

	public ExpandableSQL(ParsedSQL parsedSQL, Map<String, Integer> parameterValuesSize) {
		this(parsedSQL, parameterValuesSize, Collections.emptyMap());
	}
	
	public ExpandableSQL(ParsedSQL parsedSQL, Map<String, Integer> parameterValuesSize, Map<String, Integer> parameterComponentTypeSize) {
		this.parsedSQL = parsedSQL;
		this.parameterComponentTypeSize = parameterComponentTypeSize;
		this.expandableParameters = new HashMap<>(this.parsedSQL.getSqlSnippets().size() / 2);
		convertParsedParametersToExpandableParameters(parameterValuesSize);
	}
	
	private void convertParsedParametersToExpandableParameters(Map<String, Integer> parameterValuesSize) {
		StringBuilder preparedSQLBuilder = new StringBuilder();
		// index for prepared statement mark (?), used to compute mark indexes of parameters
		int markIndex = 1;
		for (Object sqlSnippet : parsedSQL.getSqlSnippets()) {
			if (sqlSnippet instanceof Parameter) {
				String parameterName = ((Parameter) sqlSnippet).getName();
				Integer valueSize = parameterValuesSize.get(parameterName);
				if (valueSize == null) {
					throw new IllegalArgumentException("Size is not given for parameter " + parameterName + " hence expansion is not possible");
				} else {
					buildParameter(parameterName, valueSize, markIndex, preparedSQLBuilder);
					markIndex += valueSize;
				}
			} else {
				// sql snippet
				preparedSQLBuilder.append(sqlSnippet);
			}
		}
		this.preparedSQL = preparedSQLBuilder.toString();
	}
	
	private void buildParameter(String parameterName, int valueSize, int firstIndex, StringBuilder preparedSQLBuilder) {
		ExpandableParameter expandableParameter = this.expandableParameters.computeIfAbsent(parameterName, name -> {
			Integer componentTypeSize = parameterComponentTypeSize.get(name);
			if (componentTypeSize != null) {
				return new ExpandableCompositeParameter(name, valueSize, componentTypeSize);
			} else {
				return new ExpandableParameter(name, valueSize);
			}
		});
		expandableParameter.buildMarkIndexes(firstIndex);
		expandableParameter.expandParameters(preparedSQLBuilder);
	}
	
	public Map<String, ExpandableParameter> getExpandableParameters() {
		return expandableParameters;
	}

	public String getPreparedSQL() {
		return preparedSQL;
	}
	
	/**
	 * Class that represents a named parameter in a SQL statement.
	 * It allows extension of the single parameter mark (coded as a question mark '?') to multiple ones in case of a
	 * Collection value. This is done in the {@link #expandParameters(StringBuilder)} method.
	 */
	public static class ExpandableParameter {

		public static final String SQL_PARAMETER_MARK = "?";
		
		public static final String SQL_PARAMETER_SEPARATOR = ", ";
		
		public static final String SQL_PARAMETER_MARK_1 = SQL_PARAMETER_MARK + SQL_PARAMETER_SEPARATOR;
		
		public static final String SQL_PARAMETER_MARK_10 =
				SQL_PARAMETER_MARK_1 + SQL_PARAMETER_MARK_1 +
						SQL_PARAMETER_MARK_1 + SQL_PARAMETER_MARK_1 +
						SQL_PARAMETER_MARK_1 + SQL_PARAMETER_MARK_1 +
						SQL_PARAMETER_MARK_1 + SQL_PARAMETER_MARK_1 +
						SQL_PARAMETER_MARK_1 + SQL_PARAMETER_MARK_1;
		
		public static final String SQL_PARAMETER_MARK_100 =
				SQL_PARAMETER_MARK_10 + SQL_PARAMETER_MARK_10 +
						SQL_PARAMETER_MARK_10 + SQL_PARAMETER_MARK_10 +
						SQL_PARAMETER_MARK_10 + SQL_PARAMETER_MARK_10 +
						SQL_PARAMETER_MARK_10 + SQL_PARAMETER_MARK_10 +
						SQL_PARAMETER_MARK_10 + SQL_PARAMETER_MARK_10;
		
		/** The parameter name */
		private final String parameterName;
		/** PreparedStatement parameter mark count for this parameter, 1 for single value, N for Collection value */
		protected final int markCount;
		/** Index of the parameter in the PreparedStatement, the first one for Collection value */
		private int[] indexes;
		
		private ExpandableParameter(String parameterName, int markCount) {
			this.parameterName = parameterName;
			this.markCount = markCount;
		}

		public String getParameterName() {
			return parameterName;
		}
		
		/**
		 * Gives all indexes of this instance.
		 * 
		 * @return a one-sized array for single value parameter, a n-sized array for n-sized collection value parameter
		 */
		public int[] getMarkIndexes() {
			return indexes;
		}
		
		private void buildMarkIndexes(int startIndex) {
			int offset;
			if (indexes == null) {
				offset = 0;
				indexes = new int[markCount];
			} else {
				// parameter already has indexes: we keep them (what a nice array copy !)
				offset = indexes.length;
				int[] newIndexes = new int[offset + markCount];
				System.arraycopy(indexes, 0, newIndexes, 0, offset);
				indexes = newIndexes;
			}
			// build mark indexes
			for (int i=0; i < markCount; i++) {
				indexes[i+offset] = startIndex++;
			}
		}
		
		/**
		 * Convert the single valued parameter to a multiple one: add as many '?' as necessary 
		 * @return the changed sql
		 * @param stringBuilder
		 */
		protected StringBuilder expandParameters(StringBuilder stringBuilder) {
			StringBuilder sqlParameters = Strings.repeat(stringBuilder, markCount, SQL_PARAMETER_MARK_1, SQL_PARAMETER_MARK_100, SQL_PARAMETER_MARK_10);
			sqlParameters.setLength(sqlParameters.length() - 2);
			return sqlParameters;
		}
	}
	
	public static class ExpandableCompositeParameter extends ExpandableParameter {
		
		private final int componentTypeMarkCount;
		
		private ExpandableCompositeParameter(String parameterName, int objectCount, int componentTypeMarkCount) {
			super(parameterName, objectCount);
			this.componentTypeMarkCount = componentTypeMarkCount;
		}
		
		@Override
		protected StringBuilder expandParameters(StringBuilder stringBuilder) {
			for (int i = 0; i < markCount; i++) {
				stringBuilder.append("(");
				Strings.repeat(stringBuilder, componentTypeMarkCount, SQL_PARAMETER_MARK_1, SQL_PARAMETER_MARK_100, SQL_PARAMETER_MARK_10);
				stringBuilder.setLength(stringBuilder.length() - 2);
				stringBuilder.append("), ");
			}
			stringBuilder.setLength(stringBuilder.length() - 2);
			return stringBuilder;
		}
	}
}
