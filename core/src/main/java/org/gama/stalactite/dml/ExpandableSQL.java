package org.gama.stalactite.dml;

import java.util.*;
import java.util.Map.Entry;

import org.gama.stalactite.dml.SQLParameterParser.Parameter;
import org.gama.stalactite.dml.SQLParameterParser.ParsedSQL;
import org.gama.lang.StringAppender;
import org.gama.lang.collection.ArrayIterator;
import org.gama.lang.collection.PairIterator;

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
					preparedSQLBuilder.append(snippetToAdd.toParameterizedSQL());
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
	 * Collection value. This is done on the {@link #toParameterizedSQL()} method.
	 */
	public static class ExpandableParameter implements Iterable<Entry<Integer, Object>> {

		private static final String SQL_PARAMETER_MARK = "?";

		private static final String SQL_PARAMETER_MARK_1 = SQL_PARAMETER_MARK + ", ";
		
		private static final int SQL_PARAMETER_MARK_1_LENGTH = SQL_PARAMETER_MARK_1.length();
		
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
		private final Object value;
		/** The parameter name */
		private final String parameterName;
		/** Preceding parameter, for index computation, overall used in case of Collection valued parameters */
		private final ExpandableParameter precedingParameter;
		/** PreparedStatement parameter mark count for this parameter, 1 for single value, N for Collection value */
		private final int markCount;
		/** Index of the parameter in the PreparedStatement, the first one for Collection value */
		private Integer index;

		private ExpandableParameter(Object value, String parameterName, ExpandableParameter precedingParameter) {
			this.value = value;
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
			int startIndex = getIndex();
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
		public int getIndex() {
			if (this.index == null) {
				if (this.precedingParameter != null) {
					this.index = this.precedingParameter.getIndex() + this.precedingParameter.markCount;
				} else {
					this.index = 1;
				}
			}
			return this.index;
		}
		
		/**
		 * 
		 * @return an iterator over index-value parameters
		 */
		@Override
		public Iterator<Entry<Integer, Object>> iterator() {
			Iterator<Object> valueIterator;
			if (value instanceof Iterable) {
				valueIterator = ((Iterable<Object>) value).iterator();
			} else {
				valueIterator = Collections.singletonList(value).iterator();
			}
			return new PairIterator<>(new ArrayIterator<>(getMarkIndexes()), valueIterator);
		}

		public String toParameterizedSQL() {
			if (value instanceof Collection) {
				return expandParameters();
			} else {
				return SQL_PARAMETER_MARK;
			}
		}
		
		/**
		 * Convert the single valued parameter to a multiple one: add as many '?' as necessary 
		 * @return the changed sql
		 */
		protected String expandParameters() {
			Collection values = (Collection) this.value;
			int collectionSize = values.size();
			StringAppender sqlParameter = new StringAppender(collectionSize * SQL_PARAMETER_MARK_1_LENGTH);
			// on optimise en traitant la concaténation par bloc, empirique mais sans doute plus efficace que 1 par 1
			int packet100Count = collectionSize / 100;
			for (int i = 0; i < packet100Count; i++) {
				sqlParameter.cat(SQL_PARAMETER_MARK_100);
			}
			int packetMod100 = collectionSize % 100;
			int packet10Count = packetMod100 / 10;
			for (int i = 0; i < packet10Count; i++) {
				sqlParameter.cat(SQL_PARAMETER_MARK_10);
			}
			int packet1Count = packetMod100 % 10;
			for (int i = 0; i < packet1Count; i++) {
				sqlParameter.cat(SQL_PARAMETER_MARK_1);
			}
			return sqlParameter.cutTail(2).toString();
		}
	}
}
