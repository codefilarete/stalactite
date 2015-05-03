package org.stalactite.dml;

import java.util.*;
import java.util.Map.Entry;

import org.stalactite.dml.SQLParameterParser.Parameter;
import org.stalactite.dml.SQLParameterParser.ParsedSQL;
import org.gama.lang.StringAppender;
import org.gama.lang.collection.ArrayIterator;
import org.gama.lang.collection.PairIterator;

/**
 * Classe qui permet la création d'un ordre SQL de type PreparedStatement à partir du résultat du parsing d'un ordre
 * SQL paramétré. Facilite la gestion des index des paramètres dans le PreparedStatement.
 * Gère également l'extension de l'ordre SQL en cas de valeur de type Collection, cas pour lequel on ajoute autant
 * de '?' que nécessaire et on adapte le calcul des index.
 * 
 * @author mary
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

	public static class ExpandableParameter implements Iterable<Entry<Integer, Object>> {

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

		private final Object value;
		private final String parameterName;
		private final ExpandableParameter precedingParameter;
		private final int markCount;
		private Integer index;

		private ExpandableParameter(Object value, String parameterName, ExpandableParameter precedingParameter) {
			this.value = value;
			this.parameterName = parameterName;
			this.precedingParameter = precedingParameter;
			if (value instanceof Collection) {
				this.markCount = ((Collection) value).size();
				if (this.markCount == 0) {
					// génèrera un SQL sans doute invalide: "in ()" => on lève une exception avant
					throw new IllegalArgumentException("Empty collection for sql parameter " + parameterName);
				}
			} else {
				this.markCount = 1;
			}
		}

		public String getParameterName() {
			return parameterName;
		}

		public Integer[] getMarkIndexes() {
			Integer[] indexes = new Integer[markCount];
			int startIndex = getIndex();
			for (int i = 0; i < markCount; i++) {
				indexes[i] = startIndex + i;
			}
			return indexes;
		}

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

		@Override
		public Iterator<Entry<Integer, Object>> iterator() {
			Iterator<Object> valueIterator;
			if (value instanceof Iterable) {
				valueIterator = ((Iterable) value).iterator();
			} else {
				valueIterator = Collections.singletonList(value).iterator();
			}
			return new PairIterator<>(new ArrayIterator<>(getMarkIndexes()), valueIterator);
		}

		public String toParameterizedSQL() {
			if (value instanceof Collection) {
				StringAppender sqlParameter = new StringAppender(((Collection) value).size() * SQL_PARAMETER_MARK_1.length());
				int collectionSize = ((Collection) value).size();
				// on optimise en traitant la concaténation par bloc, empirique mais sans doute plus efficace que 1 par 1
				int packet100 = collectionSize / 100;
				for (int i = 0; i < packet100; i++) {
					sqlParameter.cat(SQL_PARAMETER_MARK_100);
				}
				int packetMod100 = collectionSize % 100;
				int packet10 = packetMod100 / 10;
				for (int i = 0; i < packet10; i++) {
					sqlParameter.cat(SQL_PARAMETER_MARK_10);
				}
				int packet1 = packetMod100 % 10;
				for (int i = 0; i < packet1; i++) {
					sqlParameter.cat(SQL_PARAMETER_MARK_1);
				}
				sqlParameter.cutTail(2);
				return sqlParameter.toString();
			} else {
				return SQL_PARAMETER_MARK;
			}
		}
	}
}
