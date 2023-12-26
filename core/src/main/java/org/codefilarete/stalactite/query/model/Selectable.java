package org.codefilarete.stalactite.query.model;

/**
 * Contract for elements to be put in a Select clause, in order to be transformed to SQL
 * 
 * @see SelectableString
 * @author Guillaume Mary
 */
public interface Selectable<C> {
	
	/**
	 * Gives the SQL string that must be put in the select clause.
	 * Expected to be a valid and security-proof expression : don't consider any validation will be done.
	 * 
	 * @return any non-null SQL statement for a select clause
	 */
	String getExpression();
	
	Class<C> getJavaType();
	
	/**
	 * Implementation for String to be put in Select clause
	 */
	class SelectableString<C> implements Selectable<C> {
		
		private final String expression;
		
		private final Class<C> javaType;
		
		public SelectableString(String expression, Class<C> javaType) {
			this.expression = expression;
			this.javaType = javaType;
		}
		
		@Override
		public String getExpression() {
			return expression;
		}
		
		@Override
		public Class<C> getJavaType() {
			return javaType;
		}
	}
}
