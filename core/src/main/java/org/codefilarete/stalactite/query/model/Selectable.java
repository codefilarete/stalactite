package org.codefilarete.stalactite.query.model;

/**
 * Contract for elements to be put in a Select clause, in order to be transformed to SQL
 * 
 * @see SelectableString
 * @author Guillaume Mary
 */
public interface Selectable {
	
	/**
	 * Gives the SQL string that must be put in the select clause.
	 * Expected to be a valid and security-proof expression : don't consider any validation will be done.
	 * 
	 * @return any non-null SQL statement for a select clause
	 */
	String getExpression();
	
	/**
	 * Implementation for String to be put in Select clause
	 */
	class SelectableString implements Selectable {
		
		private final String expression;
		
		public SelectableString(String expression) {
			this.expression = expression;
		}
		
		@Override
		public String getExpression() {
			return expression;
		}
	}
}
