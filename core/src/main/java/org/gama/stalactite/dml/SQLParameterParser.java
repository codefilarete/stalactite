package org.gama.stalactite.dml;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.gama.lang.collection.EntryFactoryHashMap;

/**
 * @author mary
 */
public class SQLParameterParser {
	
	public static final Pattern IN_PATTERN = Pattern.compile("\\s+in\\s*\\(\\s*");
	
	private static final Set<Character> PARAMETER_NAME_ALLOWED_CHARACTER = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
			'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
			'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
			'_'
	)));
	
	private final ParsedSQL parsedSQL;
	private final int sqlLength;
	private final String sql;
	private int currentPos;
	private char currentChar;
	private StringBuilder sqlSnippet;

	public SQLParameterParser(String sql) {
		this.sql = sql;
		this.sqlLength = sql.length();
		this.currentPos = -1;
		this.parsedSQL = new ParsedSQL();
	}

	public ParsedSQL parse() {
		this.sqlSnippet = new StringBuilder(50);
		while (currentPos < sqlLength) {
			doUntil(':', new ParserDelegate() {
				@Override
				public void onRead() {
					sqlSnippet.append(currentChar);
				}
				
				@Override
				public void onCharFound() {
					parsedSQL.addSqlSnippet(sqlSnippet.toString());
					sqlSnippet.setLength(0);
					readParam();
				}
			});
			// onCharFound() n'est pas appelé en fin de chaîne: il faut donc terminer ce qu'on a commencé
			if (currentPos == sqlLength && sqlSnippet.length() != 0) {
				parsedSQL.addSqlSnippet(sqlSnippet.toString());
			}
		}
		return parsedSQL;
	}

	private void readParam() {
		final StringBuilder paramName = new StringBuilder();
		doWhile(PARAMETER_NAME_ALLOWED_CHARACTER, new ParserDelegate() {
			@Override
			public void onRead() {
				paramName.append(currentChar);
			}

			@Override
			public void onCharFound() {
				// détection d'un in pour avoir un paramètre dédié aux Collections ('?' extensible)
				Matcher inMatcher = IN_PATTERN.matcher(sqlSnippet.toString());
				if (inMatcher.find()) {
					parsedSQL.addCollectionParam(paramName.toString());
				} else {
					parsedSQL.addParam(paramName.toString());
				}
				// on a consommé un charatère en trop, on le rend pour l'appelant
				unread();
			}
		});
		// onCharFound() n'est pas appelé en fin de chaîne: il faut donc terminer ce qu'on a commencé
		if (currentPos == sqlLength) {
			if (paramName.length() == 0) {
				throw new IllegalArgumentException("Parameter name can't be empty at position " + currentPos);
			} else {
				parsedSQL.addParam(paramName.toString());
			}
		}
	}
	
	private void doUntil(char c, ParserDelegate delegate) {
		boolean charFound = true;
		while (charFound && read()) {
			charFound = (c != currentChar);
			if (charFound) {
				delegate.onRead();
			}
		}
		if (!charFound) {
			delegate.onCharFound();
		}
	}
	
	private void doWhile(Set<Character> c, ParserDelegate delegate) {
		boolean charFound = true;
		while (charFound && read()) {
			charFound = c.contains(currentChar);
			if (charFound) {
				delegate.onRead();
			}
		}
		if (!charFound) {
			delegate.onCharFound();
		}
	}
	
	private boolean read() {
		if (++currentPos < sqlLength) {
			currentChar = sql.charAt(currentPos);
			return true;
		} else {
			return false;
		}
	}
	
	private boolean unread() {
		if (--currentPos >= 0) {
			currentChar = sql.charAt(currentPos);
			return true;
		} else {
			return false;
		}
	}
	
	private interface ParserDelegate {
		void onRead();

		void onCharFound();
	}

	public static class ParsedSQL {

		private List<Object /* sql or Parameter */> sqlSnippets = new ArrayList<>(10);
		
		private Map<String, Parameter> parametersMap = new EntryFactoryHashMap<String, Parameter>() {
			@Override
			public Parameter createInstance(String input) {
				return new Parameter(input);
			}
		};

		public ParsedSQL() {
		}

		public ParsedSQL(List<Object /* sql or Parameter */> sqlSnippets, Map<String, Parameter> parametersMap) {
			this.sqlSnippets = sqlSnippets;
			this.parametersMap = parametersMap;
		}

		public List<Object> getSqlSnippets() {
			return sqlSnippets;
		}

		public Map<String, Parameter> getParametersMap() {
			return parametersMap;
		}

		public void addParam(String paramName) {
			Parameter parameter = this.parametersMap.get(paramName);
			// il est impératif de mettre la même instance dans la Map et dans la liste pour l'algo d'extension en List
			this.sqlSnippets.add(parameter);
		}

		public void addCollectionParam(String paramName) {
			Parameter parameter = this.parametersMap.get(paramName);
			if (!(parameter instanceof CollectionParameter)) {
				parameter = new CollectionParameter(paramName);
				this.parametersMap.put(paramName, parameter);
			}
			// il est impératif de mettre la même instance dans la Map et dans la liste pour l'algo d'extension en List
			this.sqlSnippets.add(parameter);
		}

		public void addSqlSnippet(String sqlSnippet) {
			this.sqlSnippets.add(sqlSnippet);
		}
	}
	
	public static class Parameter {

		private final String name;
		
		public Parameter(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}

		/** Implémenté pour les JUnits, non nécessaire en prod car non mis dans Map ni Set */
		@Override
		public boolean equals(Object obj) {
			return obj instanceof Parameter && ((Parameter) obj).name.equals(this.name);
		}

		/** Implémenté pour les JUnits, non nécessaire en prod car non mis dans Map ni Set */
		@Override
		public int hashCode() {
			return this.name.hashCode();
		}
	}
	
	public static class CollectionParameter extends Parameter {
		
		public CollectionParameter(String name) {
			super(name);
		}
		
	}
}
