package org.codefilarete.stalactite.query.builder;

import org.codefilarete.stalactite.query.model.Selectable;
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
				N functionArguments = operator.getValue();
				if (functionArguments instanceof Iterable) {
					for (Object argument : (Iterable) functionArguments) {
						catArgument(sql, argument);
						sql.cat(", ");
					}
					sql.removeLastChars(2);
				} else {
					catArgument(sql, functionArguments);
				}
				sql.cat(")");
			}
		}
		
		private void catArgument(SQLAppender sql, Object argument) {
			if (argument instanceof SQLFunction) {
				cat((SQLFunction) argument, sql);
			} else if (argument instanceof Selectable) {
				sql.cat(dmlNameProvider.getName((Selectable) argument));
			} else {
				sql.catValue(argument);
			}
		}
		
		public void catCast(Cast<?, ?> cast, SQLAppender sqlAppender) {
			sqlAppender.cat(cast.getExpression(), "(");
			catArgument(sqlAppender, cast.getValue());
			sqlAppender.cat(" as ", javaTypeToSqlTypeMapping.getTypeName(cast.getJavaType(), cast.getTypeSize()), ")");
		}
	}
}
