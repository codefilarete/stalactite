package org.gama.stalactite.sql.dml;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.gama.lang.StringAppender;
import org.gama.lang.collection.Arrays;

/**
 * Parser for SQL String with named parameters.
 * Named parameters must begin with a colon (:) followed by any word Character (upper or lower alphabetical letter, or
 * a digit, or an underscore) [a-Z0-9_].
 * <p>
 * Implementation is quite naive.
 *
 * @author Guillaume Mary
 */
public class SQLParameterParser {
	
	/** Pattern to detect 'in' keywords and created CollectionParameter instead of Parameter */
	public static final Pattern IN_PATTERN = Pattern.compile("\\s+in\\s*\\(\\s*");
	
	/** Allowed characters for a parameter, quite arbitrary */
	private static final Set<Character> PARAMETER_NAME_ALLOWED_CHARACTERS = Collections.unmodifiableSet(Arrays.asHashSet(
			'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
			'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
			'_', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'
	));
	
	/** Marker of a delimited text */
	private static final char SIMPLE_QUOTE = '\'';
	/** Marker of a delimited text */
	private static final char DOUBLE_QUOTE = '\"';
	/** Marker of a parameter */
	private static final char SEMI_COLON = ':';
	
	private static final Set<Character> SPECIAL_SYMBOLS = Collections.unmodifiableSet(Arrays.asHashSet(SEMI_COLON, SIMPLE_QUOTE, DOUBLE_QUOTE));
	
	private static final Set<Character> TEXT_MARKS = Collections.unmodifiableSet(Arrays.asHashSet(SIMPLE_QUOTE, DOUBLE_QUOTE));
	
	private final ParsedSQL parsedSQL;
	private final int sqlLength;
	private final String sql;
	private int currentPos;
	private char currentChar;
	/** Current sql snippet: either a sql portion or a parameter. Is "closed" (reset) on next sql snippet */
	private StringBuilder sqlSnippet;
	
	public SQLParameterParser(String sql) {
		this.sql = sql;
		this.sqlLength = sql.length();
		this.currentPos = -1;
		this.parsedSQL = new ParsedSQL();
	}
	
	public String getSql() {
		return sql;
	}
	
	/**
	 * Main entry point: will parse SQL given at contructor
	 *
	 * @return the resulting parse (not null)
	 */
	public ParsedSQL parse() {
		this.sqlSnippet = new StringBuilder(50);
		while (currentPos < sqlLength) {
			doUntil(SPECIAL_SYMBOLS, new ParsingListener() {
				@Override
				public void onRead() {
					sqlSnippet.append(currentChar);
				}
				
				@Override
				public void onConsumptionEnd() {
					// sql snippet end detected => good for "parsed sql"
					parsedSQL.addSqlSnippet(sqlSnippet.toString());
					// according to detected end, we'll pursue with adhoc consumption
					switch (currentChar) {
						case SIMPLE_QUOTE:
						case DOUBLE_QUOTE:
							readQuotes();
							break;
						case SEMI_COLON:
							readParam();
							break;
						default:
							// suspicious case: symbol consumption is not implemented (developper forgetting) 
							throw new RuntimeException("Symbol '" + currentChar + "' was set as blocker but its consumption is not implemented");
					}
					// closing sql snippet so next consumers can append chars to it without collision
					sqlSnippet.setLength(0);
				}
			});
			// onConsumptionEnd() is not called on String end: we consumer remaining chars
			if (currentPos == sqlLength && sqlSnippet.length() != 0) {
				parsedSQL.addSqlSnippet(sqlSnippet.toString());
			}
		}
		return parsedSQL;
	}
	
	/**
	 * Consumes a named parameter
	 */
	private void readParam() {
		StringBuilder paramName = new StringBuilder();
		doWhile(PARAMETER_NAME_ALLOWED_CHARACTERS, new ParsingListener() {
			@Override
			public void onRead() {
				paramName.append(currentChar);
			}
			
			@Override
			public void onConsumptionEnd() {
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
		// onConsumptionEnd() is not called on String end: we must end what we began
		if (currentPos == sqlLength) {
			if (paramName.length() == 0) {
				throw new IllegalArgumentException("Parameter name can't be empty at position " + currentPos);
			} else {
				parsedSQL.addParam(paramName.toString());
			}
		}
	}
	
	/**
	 * Consumes quotes
	 */
	private void readQuotes() {
		StringBuilder quotes = new StringBuilder();
		char prefix = currentChar;
		doUntil(TEXT_MARKS, new ParsingListener() {
			@Override
			public void onRead() {
				quotes.append(currentChar);
			}
			
			@Override
			public void onConsumptionEnd() {
				// nothing to do
			}
		});
		parsedSQL.addSqlSnippet(prefix + quotes.toString() + prefix);
	}
	
	/**
	 * Goes foward until read character is in given characters set (stoppers). Calls {@link ParsingListener} methods during consumption.
	 *
	 * @param stoppingChars characters that stops consumption
	 * @param parsingListener listener called during characters consumption
	 */
	private void doUntil(Set<Character> stoppingChars, ParsingListener parsingListener) {
		boolean charFound = true;
		while (charFound && read()) {
			charFound = !stoppingChars.contains(currentChar);
			if (charFound) {
				parsingListener.onRead();
			}
		}
		if (!charFound) {
			parsingListener.onConsumptionEnd();
		}
	}
	
	/**
	 * Goes foward while read character is in given characters set (continuers). Calls {@link ParsingListener} methods during consumption.
	 *
	 * @param continuingChars characters that must be consumed
	 * @param parsingListener listener called during characters consumption
	 */
	private void doWhile(Set<Character> continuingChars, ParsingListener parsingListener) {
		boolean charFound = true;
		while (charFound && read()) {
			charFound = continuingChars.contains(currentChar);
			if (charFound) {
				parsingListener.onRead();
			}
		}
		if (!charFound) {
			parsingListener.onConsumptionEnd();
		}
	}
	
	/**
	 * Consume next Character from sql. Put it in currentChar field.
	 *
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
	 */
	private void unread() {
		if (--currentPos >= 0) {
			currentChar = sql.charAt(currentPos);
		}
	}
	
	/**
	 * Allows actions to be done during parsing phases.
	 *
	 * @see #doUntil(Set, ParsingListener)
	 * @see #doWhile(Set, ParsingListener)
	 */
	private interface ParsingListener {
		void onRead();
		
		void onConsumptionEnd();
	}
	
	/**
	 * Returned object by the {@link SQLParameterParser#parse()} method. Represents a list of SQL code and named
	 * parameters.
	 */
	public static class ParsedSQL {
		
		/** SQL elements: sql code (String) and named parameters (Parameter) */
		private List<Object /* String or Parameter */> sqlSnippets = new ArrayList<>(10);
		
		/** Parameters mapped on their names */
		private Map<String, Parameter> parametersMap = new HashMap<>();
		
		public ParsedSQL() {
		}
		
		public ParsedSQL(List<Object /* sql or Parameter */> sqlSnippets, Map<String, Parameter> parametersMap) {
			this.sqlSnippets = sqlSnippets;
			this.parametersMap = parametersMap;
		}
		
		/**
		 * Gives found SQL elements: mix of Strings and Parameters. Expected to be in found order.
		 *
		 * @return a mix of Strings and Parameters, in order of finding
		 */
		public List<Object> getSqlSnippets() {
			return sqlSnippets;
		}
		
		public Map<String, Parameter> getParametersMap() {
			return parametersMap;
		}
		
		public void addParam(String paramName) {
			Parameter parameter = this.parametersMap.computeIfAbsent(paramName, Parameter::new);
			// we must put the same instance into the Map as into the List for the expansion algorithm
			this.sqlSnippets.add(parameter);
		}
		
		public void addCollectionParam(String paramName) {
			Parameter parameter = this.parametersMap.computeIfAbsent(paramName, Parameter::new);
			if (!(parameter instanceof CollectionParameter)) {
				parameter = new CollectionParameter(paramName);
				this.parametersMap.put(paramName, parameter);
			}
			// we must put the same instance into the Map as into the List the for expansion algorithm
			this.sqlSnippets.add(parameter);
		}
		
		public void addSqlSnippet(String sqlSnippet) {
			int size = sqlSnippets.size();
			// String is concatenated to last String, else it is simply added to the end
			if (size > 0 && sqlSnippets.get(size - 1) instanceof String) {
				this.sqlSnippets.set(size - 1, sqlSnippets.get(size - 1) + sqlSnippet);
			} else {
				this.sqlSnippets.add(sqlSnippet);
			}
		}
		
		/**
		 * Implementation for simple tracing
		 * @return ":" + parameter name
		 */
		@Override
		public String toString() {
			return new StringAppender() {
				@Override
				public StringAppender cat(Object s) {
					if (s instanceof Parameter) {
						// NB: don't merge nexts statements to super.cat(...) because it will call us back
						super.cat(":");
						super.cat(((Parameter) s).getName());
					} else {
						super.cat(s);
					}
					return this;
				}
			}.cat(getSqlSnippets()).toString();
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
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			Parameter parameter = (Parameter) o;
			return Objects.equals(name, parameter.name);
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
