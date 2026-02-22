package org.codefilarete.stalactite.query.builder;

import org.codefilarete.stalactite.query.builder.FunctionSQLBuilderFactory.FunctionSQLBuilder;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.Select;
import org.codefilarete.stalactite.query.api.Selectable;
import org.codefilarete.stalactite.query.model.operator.SQLFunction;
import org.codefilarete.stalactite.sql.ddl.JavaTypeToSqlTypeMapping;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.Strings;

/**
 * Factory for {@link SelectSQLBuilder}. It's overridable through {@link org.codefilarete.stalactite.sql.Dialect}
 * which then let one gives its own implementation of {@link SelectSQLBuilderFactory}.
 *
 * @author Guillaume Mary
 * @see #queryBuilder(Select, DMLNameProvider)
 */
public class SelectSQLBuilderFactory {
	
	private final FunctionSQLBuilderFactory functionSQLBuilderFactory;
	
	/**
	 * A default constructor that uses default factories, use mainly for test or default behavior (not related to a database vendor)
	 *
	 * @param javaTypeToSqlTypeMapping a {@link Query}
	 */
	public SelectSQLBuilderFactory(JavaTypeToSqlTypeMapping javaTypeToSqlTypeMapping) {
		this(new FunctionSQLBuilderFactory(javaTypeToSqlTypeMapping));
	}
	
	public SelectSQLBuilderFactory(FunctionSQLBuilderFactory functionSQLBuilderFactory1) {
		this.functionSQLBuilderFactory = functionSQLBuilderFactory1;
	}
	
	public SelectSQLBuilder queryBuilder(Select select, DMLNameProvider dmlNameProvider) {
		return new SelectSQLBuilder(select, dmlNameProvider, this.functionSQLBuilderFactory.functionSQLBuilder(dmlNameProvider));
	}
	
	/**
	 * Formats {@link Select} object to an SQL {@link String} through {@link #toSQL()}.
	 * 
	 * @author Guillaume Mary
	 * @see #toSQL()
	 */
	public static class SelectSQLBuilder implements SQLBuilder {
		
		private final Select select;
		private final DMLNameProvider dmlNameProvider;
		private final FunctionSQLBuilder functionSQLBuilder;
		
		public SelectSQLBuilder(Select select, DMLNameProvider dmlNameProvider, FunctionSQLBuilder functionSQLBuilder) {
			this.select = select;
			this.dmlNameProvider = dmlNameProvider;
			this.functionSQLBuilder = functionSQLBuilder;
		}
		
		@Override
		public String toSQL() {
			StringSQLAppender result = new StringSQLAppender(dmlNameProvider);
			result.catIf(this.select.isDistinct(), "distinct ");
			cat(this.select, result);
			return result.getSQL();
		}
		
		private void cat(Iterable<? extends Selectable<?> /* String, Column or AliasedColumn */> select, StringSQLAppender sql) {
			for (Object o : select) {
				if (o instanceof SQLFunction) {
					cat((SQLFunction) o, sql);
				} else if (o instanceof Selectable) {	// must be after previous ifs because they deal with dedicated Selectable cases
					cat((Selectable) o, sql);
				} else if (o instanceof Iterable) {
					cat((Iterable<? extends Selectable<?> /* String, Column or AliasedColumn */>) o, sql);
				} else {
					throw new UnsupportedOperationException("Operator " + Reflections.toString(o.getClass()) + " is not implemented");
				}
				sql.cat(", ");
			}
			if (Strings.tail(sql.getSQL(), 2).equals(", ")) {	// if not, means select was empty
				// cut the trailing comma
				sql.removeLastChars(2);
			}
		}
		
		protected void cat(Selectable<?> column, StringSQLAppender sql) {
			String alias = select.getAliases().get(column);
			sql.catColumn(column).catIf(!Strings.isEmpty(alias), " as " + alias);
		}
		
		private void cat(SQLFunction<?, ?> operator, StringSQLAppender appenderWrapper) {
			String alias = select.getAliases().get(operator);	// can be UnitaryOperator which is Selectable
			functionSQLBuilder.cat(operator, appenderWrapper);
			appenderWrapper.catIf(!Strings.isEmpty(alias), " as " + alias);
		}
	}
}
