package org.codefilarete.stalactite.query.builder;

import java.util.function.Consumer;

import org.codefilarete.stalactite.query.api.Selectable;
import org.codefilarete.stalactite.query.model.ValuedVariable;
import org.codefilarete.stalactite.query.api.Variable;
import org.codefilarete.stalactite.query.model.operator.Cast;
import org.codefilarete.stalactite.query.model.operator.Count;
import org.codefilarete.stalactite.query.model.operator.SQLFunction;
import org.codefilarete.stalactite.sql.ddl.JavaTypeToSqlTypeMapping;

/**
 * Factory for {@link FunctionSQLBuilder}.
 * 
 * @author Guillaume Mary
 * @see #functionSQLBuilder(DMLNameProvider) 
 */
public class FunctionSQLBuilderFactory {
	
	private final JavaTypeToSqlTypeMapping javaTypeToSqlTypeMapping;
	
	/**
	 * @param javaTypeToSqlTypeMapping necessary to cast(..) function to write cast type
	 */
	public FunctionSQLBuilderFactory(JavaTypeToSqlTypeMapping javaTypeToSqlTypeMapping) {
		this.javaTypeToSqlTypeMapping = javaTypeToSqlTypeMapping;
	}
	
	public FunctionSQLBuilder functionSQLBuilder(DMLNameProvider dmlNameProvider) {
		return new FunctionSQLBuilder(dmlNameProvider, this.javaTypeToSqlTypeMapping);
	}
	
	/**
	 * A class made to print a {@link SQLFunction}
	 * Used by {@link org.codefilarete.stalactite.query.builder.WhereSQLBuilderFactory.WhereSQLBuilder}
	 * and {@link org.codefilarete.stalactite.query.builder.SelectSQLBuilderFactory.SelectSQLBuilder}
	 * 
	 * @author Guillaume Mary
	 */
	public static class FunctionSQLBuilder {
		
		private final DMLNameProvider dmlNameProvider;
		
		private final JavaTypeToSqlTypeMapping javaTypeToSqlTypeMapping;
		
		public FunctionSQLBuilder(DMLNameProvider dmlNameProvider, JavaTypeToSqlTypeMapping javaTypeToSqlTypeMapping) {
			this.dmlNameProvider = dmlNameProvider;
			this.javaTypeToSqlTypeMapping = javaTypeToSqlTypeMapping;
		}
		
		/**
		 * Main entry point
		 */
		public <N> void cat(SQLFunction<N, ?> operator, SQLAppender sql) {
			if (operator instanceof Cast) {
				catCast((Cast) operator, sql);
			} else {
				// by default the SQLFunction is rendered only by its name
				sql.cat(operator.getExpression(), "(");
				if (operator instanceof Count && ((Count) operator).isDistinct()) {
					sql.cat("distinct ");
				}
				Variable<N> functionValue = operator.getValue();
				if (functionValue instanceof ValuedVariable) {
					N functionArguments = ((ValuedVariable<N>) functionValue).getValue();
					if (functionArguments instanceof Iterable) {
						for (Object argument : (Iterable) functionArguments) {
							catArgument(sql, argument, sql::catValue);
							sql.cat(", ");
						}
						sql.removeLastChars(2);
					} else {
						catArgument(sql, functionArguments, sql::catValue);
					}
					sql.cat(")");
				}
			}
		}
		
		private void catArgument(SQLAppender sql, Object argument, Consumer<Object> fallbackHandler) {
			if (argument instanceof ValuedVariable) {
				argument = ((ValuedVariable) argument).getValue();
			}
			if (argument instanceof SQLFunction) {
				cat((SQLFunction) argument, sql);
			} else if (argument instanceof Selectable) {
				sql.cat(dmlNameProvider.getName((Selectable) argument));
			} else {
				fallbackHandler.accept(argument);
			}
		}
		
		public void catCast(Cast<?, ?> cast, SQLAppender sqlAppender) {
			sqlAppender.cat(cast.getExpression(), "(");
			// we think that Cast arguments can hardly be a value, but more an expression, therefore when a String is given to it, we don't want it
			// to be surrounded by "'", which does catValue(..), hence, as a fallback, we simply append it
			catArgument(sqlAppender, cast.getValue(), value -> sqlAppender.cat(String.valueOf(value)));
			sqlAppender.cat(" as ", javaTypeToSqlTypeMapping.getTypeName(cast.getJavaType(), cast.getTypeSize()), ")");
		}
	}
}
