package org.gama.stalactite.dml;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.gama.lang.collection.EntryFactoryHashMap;

/**
 * Parser for SQL String with named parameters.
 * Named parameters must begin with a colon (:) followed by any word Character (upper or lower alphabetical letter, or
 * a digit, or an underscore) [a-Z0-9_].
 * 
 * Implementation is quite naive.
 * 
 * @author Guillaume Mary
 */
public class SQLParameterParser {
	
	/** Pattern to detect 'in' keywords and created CollectionParameter instead of Parameter  */
	public static final Pattern IN_PATTERN = Pattern.compile("\\s+in\\s*\\(\\s*");
	
	private static final Set<Character> PARAMETER_NAME_ALLOWED_CHARACTERS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
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
			// onCharFound() is not called on String end: we must end what we began
			if (currentPos == sqlLength && sqlSnippet.length() != 0) {
				parsedSQL.addSqlSnippet(sqlSnippet.toString());
			}
		}
		return parsedSQL;
	}

	private void readParam() {
		final StringBuilder paramName = new StringBuilder();
		doWhile(PARAMETER_NAME_ALLOWED_CHARACTERS, new ParserDelegate() {
			@Override
			public void onRead() {
				paramName.append(currentChar);
			}

			@Override
			public void onCharFound() {
				// 'in' detection to have a dedicated Collection parameter (expandable '?')
				Matcher inMatcher = IN_PATTERN.matcher(sqlSnippet.toString());
				if (inMatcher.find()) {
					parsedSQL.addCollectionParam(paramName.toString());
				} else {
					parsedSQL.addParam(paramName.toString());
				}
				// we read an extra Character, so we give it back for caller
				unread();
			}
		});
		// onCharFound() is not called on String end: we must end what we began
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
	
	/**
	 * Consume next Character from sql. Put it in currentChar field.
	 * @return true if a Character was read, false if end of sql is reached
	 */
	private boolean read() {
		if (++currentPos < sqlLength) {
			currentChar = sql.charAt(currentPos);
			return true;
		} else {
			return false;
		}
	}
	
	/**
	 * Read previous Character from sql. Put it in currentChar field.
	 * @return true if a Character was read, false if beginning of sql is reached
	 */
	private boolean unread() {
		if (--currentPos >= 0) {
			currentChar = sql.charAt(currentPos);
			return true;
		} else {
			return false;
		}
	}
	
	/**
	 * Allows actions to be done during parsing phases.
	 * @see #doUntil(char, ParserDelegate)
	 * @see #doWhile(Set, ParserDelegate) 
	 */
	private interface ParserDelegate {
		void onRead();

		void onCharFound();
	}
	
	/**
	 * Returned object by the {@link SQLParameterParser#parse()} method. Represents a list of SQL code and named
	 * parameters.
	 */
	public static class ParsedSQL {
		
		/** SQL elements: sql code (String) and named parameters (Parameter) */
		private List<Object /* String or Parameter */> sqlSnippets = new ArrayList<>(10);
		
		/** Parameters mapped on their names */
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
			// we must put the same instance into the Map as into the List for the expansion algorithm
			this.sqlSnippets.add(parameter);
		}

		public void addCollectionParam(String paramName) {
			Parameter parameter = this.parametersMap.get(paramName);
			if (!(parameter instanceof CollectionParameter)) {
				parameter = new CollectionParameter(paramName);
				this.parametersMap.put(paramName, parameter);
			}
			// we must put the same instance into the Map as into the List the for expansion algorithm
			this.sqlSnippets.add(parameter);
		}

		public void addSqlSnippet(String sqlSnippet) {
			this.sqlSnippets.add(sqlSnippet);
		}
	}
	
	/**
	 * Named parameter representation
	 */
	public static class Parameter {

		private final String name;
		
		public Parameter(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}

		/** Implemented for unit tests, unecessary for production use since never put into Map nor Set */
		@Override
		public boolean equals(Object obj) {
			return obj instanceof Parameter && ((Parameter) obj).name.equals(this.name);
		}
		
		/** Implemented for unit tests, unecessary for production use since never put into Map nor Set */
		@Override
		public int hashCode() {
			return this.name.hashCode();
		}
	}
	
	/**
	 * Specialized Parameter to mark one for Collection (preceded by 'in' keyword)
	 */
	public static class CollectionParameter extends Parameter {
		
		public CollectionParameter(String name) {
			super(name);
		}
		
	}
}
