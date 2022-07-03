package org.codefilarete.stalactite.query.builder;

import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.query.model.operator.Cast;
import org.codefilarete.stalactite.query.model.operator.SQLFunction;
import org.codefilarete.stalactite.sql.ddl.DefaultTypeMapping;
import org.codefilarete.stalactite.sql.ddl.JavaTypeToSqlTypeMapping;
import org.codefilarete.tool.VisibleForTesting;

import java.util.Collections;

/**
 * A class made to print a {@link SQLFunction}
 * 
 * @author Guillaume Mary
 */
public class FunctionSQLBuilder {
	
	private final DMLNameProvider dmlNameProvider;
	private final JavaTypeToSqlTypeMapping javaTypeToSqlTypeMapping;
	
	@VisibleForTesting
	FunctionSQLBuilder() {
		this(new DMLNameProvider(Collections.emptyMap()), new DefaultTypeMapping());
	}
	
	/**
	 * @param dmlNameProvider provider of column names to be printed (to provide aliases for instance)
	 * @param javaTypeToSqlTypeMapping necessary to cast(..) function to write cast type
	 */
	public FunctionSQLBuilder(DMLNameProvider dmlNameProvider, JavaTypeToSqlTypeMapping javaTypeToSqlTypeMapping) {
		this.dmlNameProvider = dmlNameProvider;
		this.javaTypeToSqlTypeMapping = javaTypeToSqlTypeMapping;
	}
	
	/**
	 * Main entry point
	 */
	public <N> void cat(SQLFunction<N> operator, SQLAppender sql) {
		if (operator instanceof Cast) {
			catCast((Cast) operator, sql);
		} else {
			sql.cat(operator.getExpression(), "(");
			for (Object argument : operator.getArguments()) {
				if (argument instanceof Selectable) {
					sql.cat(dmlNameProvider.getName((Selectable<?>) argument));
				} else if (argument instanceof CharSequence) {
					sql.cat(argument.toString());
				}
			}
			sql.cat(")");
		}
	}
	
	public void catCast(Cast<?> cast, SQLAppender sqlAppender) {
		sqlAppender.cat(cast.getExpression(), "(");
		if (cast.getCastTarget() instanceof SQLFunction) {
			this.cat((SQLFunction) cast.getCastTarget(), sqlAppender);
		} else {
			sqlAppender.catValue(null, cast.getCastTarget());
		}
		sqlAppender.cat(" as " , javaTypeToSqlTypeMapping.getTypeName(cast.getJavaType(), cast.getTypeSize()), ")");
	}
}
