package org.codefilarete.stalactite.query.builder;

import org.codefilarete.stalactite.query.builder.FunctionSQLBuilderFactory.FunctionSQLBuilder;
import org.codefilarete.stalactite.query.builder.OperatorSQLBuilderFactory.OperatorSQLBuilder;
import org.codefilarete.stalactite.query.model.AbstractCriterion;
import org.codefilarete.stalactite.query.model.AbstractCriterion.LogicalOperator;
import org.codefilarete.stalactite.query.model.ColumnCriterion;
import org.codefilarete.stalactite.query.model.ConditionalOperator;
import org.codefilarete.stalactite.query.model.CriteriaChain;
import org.codefilarete.stalactite.query.model.RawCriterion;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.query.model.operator.BiOperandOperator;
import org.codefilarete.stalactite.query.model.operator.SQLFunction;
import org.codefilarete.stalactite.query.model.operator.TupleIn;
import org.codefilarete.stalactite.sql.ddl.JavaTypeToSqlTypeMapping;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.SQLStatement.BindingException;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.StringAppender;

/**
 * Factory for {@link WhereSQLBuilder}. It's overridable through {@link org.codefilarete.stalactite.sql.Dialect#setQuerySQLBuilderFactory(QuerySQLBuilderFactory)}
 * which then let one gives its own implementation of {@link WhereSQLBuilderFactory}.
 *
 * @author Guillaume Mary
 * @see #whereBuilder(CriteriaChain, DMLNameProvider)
 */
public class WhereSQLBuilderFactory {

	public static final String AND = "and";
	public static final String OR = "or";
	
	private final ColumnBinderRegistry parameterBinderRegistry;
	private final OperatorSQLBuilderFactory operatorSqlBuilderFactory;
	private final FunctionSQLBuilderFactory functionSQLBuilderFactory;
	
	public WhereSQLBuilderFactory(JavaTypeToSqlTypeMapping javaTypeToSqlTypeMapping, ColumnBinderRegistry parameterBinderRegistry) {
		this(parameterBinderRegistry,
				new OperatorSQLBuilderFactory(),
				new FunctionSQLBuilderFactory(javaTypeToSqlTypeMapping));
	}
	
	public WhereSQLBuilderFactory(ColumnBinderRegistry parameterBinderRegistry,
								  OperatorSQLBuilderFactory operatorSqlBuilderFactory,
								  FunctionSQLBuilderFactory functionSQLBuilderFactory) {
		this.parameterBinderRegistry = parameterBinderRegistry;
		this.operatorSqlBuilderFactory = operatorSqlBuilderFactory;
		this.functionSQLBuilderFactory = functionSQLBuilderFactory;
	}
	
	public WhereSQLBuilder whereBuilder(CriteriaChain where, DMLNameProvider dmlNameProvider) {
		FunctionSQLBuilder functionSQLBuilder = functionSQLBuilderFactory.functionSQLBuilder(dmlNameProvider);
		return new WhereSQLBuilder(
				where,
				dmlNameProvider,
				parameterBinderRegistry,
				operatorSqlBuilderFactory.operatorSQLBuilder(functionSQLBuilder),
				functionSQLBuilder);
	}
	
	/**
	 * Formats {@link CriteriaChain} object to an SQL {@link String} through {@link #toSQL()}.
	 *
	 * @author Guillaume Mary
	 * @see #toSQL()
	 */
	public static class WhereSQLBuilder implements SQLBuilder, PreparedSQLBuilder {
		
		private final CriteriaChain where;
		
		private final DMLNameProvider dmlNameProvider;
		
		private final ColumnBinderRegistry parameterBinderRegistry;
		
		private final OperatorSQLBuilder operatorSqlBuilder;
		
		private final FunctionSQLBuilder functionSQLBuilder;
		
		public WhereSQLBuilder(CriteriaChain where,
							   DMLNameProvider dmlNameProvider,
							   ColumnBinderRegistry parameterBinderRegistry,
							   OperatorSQLBuilder operatorSqlBuilder,
							   FunctionSQLBuilder functionSQLBuilder) {
			this.where = where;
			this.dmlNameProvider = dmlNameProvider;
			this.parameterBinderRegistry = parameterBinderRegistry;
			this.operatorSqlBuilder = operatorSqlBuilder;
			this.functionSQLBuilder = functionSQLBuilder;
		}
		
		@Override
		public String toSQL() {
			return appendSQL(new StringAppender());
		}
		
		public String appendSQL(StringAppender sql) {
			return appendSQL(new StringAppenderWrapper(sql, dmlNameProvider));
		}
		
		public String appendSQL(SQLAppender sql) {
			WhereAppender whereAppender = new WhereAppender(sql, dmlNameProvider, operatorSqlBuilder, functionSQLBuilder);
			whereAppender.cat(where);
			return sql.getSQL();
		}
		
		@Override
		public PreparedSQL toPreparedSQL() {
			return toPreparedSQL(new StringAppender(), parameterBinderRegistry);
		}
		
		public PreparedSQL toPreparedSQL(StringAppender sql, ColumnBinderRegistry parameterBinderRegistry) {
			PreparedSQLWrapper preparedSQLWrapper = new PreparedSQLWrapper(new StringAppenderWrapper(sql, dmlNameProvider), parameterBinderRegistry, dmlNameProvider);
			return toPreparedSQL(preparedSQLWrapper);
		}
		
		public PreparedSQL toPreparedSQL(PreparedSQLWrapper preparedSQLWrapper) {
			WhereAppender whereAppender = new WhereAppender(preparedSQLWrapper, dmlNameProvider, operatorSqlBuilder, functionSQLBuilder);
			whereAppender.cat(where);
			PreparedSQL result = new PreparedSQL(preparedSQLWrapper.getSQL(), preparedSQLWrapper.getParameterBinders());
			result.setValues(preparedSQLWrapper.getValues());
			return result;
		}
		
		public class WhereAppender {
			
			private final SQLAppender sql;
			
			private final OperatorSQLBuilder operatorSqlBuilder;
			
			private final FunctionSQLBuilder functionSQLBuilder;
			
			private final DMLNameProvider dmlNameProvider;
			
			public WhereAppender(SQLAppender sql,
								 DMLNameProvider dmlNameProvider,
								 OperatorSQLBuilder operatorSqlBuilder,
								 FunctionSQLBuilder functionSQLBuilder) {
				this.sql = sql;
				this.operatorSqlBuilder = operatorSqlBuilder;
				this.functionSQLBuilder = functionSQLBuilder;
				this.dmlNameProvider = dmlNameProvider;
			}
			
			/**
			 * Main entry point
			 * @param criteria the conditions that must be appended
			 */
			public void cat(CriteriaChain criteria) {
				boolean isNotFirst = false;
				for (Object criterion : criteria) {
					if (isNotFirst) {
						cat(((AbstractCriterion) criterion).getOperator());
					} else {
						isNotFirst = true;
					}
					cat(criterion);
				}
			}
			
			private void cat(Object o) {
				if (o instanceof CharSequence) {
					sql.cat(o.toString());
				} else if (o instanceof CriteriaChain) {
					sql.cat("(");
					cat((CriteriaChain) o);
					sql.cat(")");
				} else if (o instanceof RawCriterion) {
					for (Object conditionItem : ((RawCriterion) o).getCondition()) {
						catConditionItem(conditionItem);
					}
				} else if (o instanceof ColumnCriterion) {
					cat((ColumnCriterion) o);
				}
			}
			
			private void catConditionItem(Object conditionItem) {
				if (conditionItem instanceof ColumnCriterion) {
					cat((ColumnCriterion) conditionItem);
				} else if (conditionItem instanceof CharSequence) {
					sql.cat(conditionItem.toString());
				} else if (conditionItem instanceof CriteriaChain) {
					sql.cat("(");
					cat((CriteriaChain) conditionItem);
					sql.cat(")");
				} else if (conditionItem instanceof SQLFunction) {    // made for having(sum(col), eq(..))
					cat((SQLFunction) conditionItem);
				} else if (conditionItem instanceof Selectable) {
					cat((Selectable) conditionItem);
				} else if (conditionItem instanceof TupleIn) {    // "if" to be done before "if" about ConditionalOperator to take inheritance into account  
					catTupledIn((TupleIn) conditionItem);
				} else if (conditionItem instanceof ConditionalOperator) {
					cat((ConditionalOperator) conditionItem);
				} else {
					throw new UnsupportedOperationException("Unknown criterion type " + Reflections.toString(conditionItem.getClass()));
				}
			}
			
			public void cat(ColumnCriterion criterion) {
				Object condition = criterion.getCondition();
				if (condition instanceof BiOperandOperator) {    // "if" to be done before "if" about ConditionalOperator to take inheritance into account  
					cat(criterion.getColumn(), (BiOperandOperator) condition);
				} else {
					cat(criterion.getColumn());
					sql.cat(" ");
					if (condition instanceof CharSequence) {
						sql.cat(condition.toString());
					} else if (condition instanceof ConditionalOperator) {
						cat(criterion.getColumn(), (ConditionalOperator) condition);
					} else {
						try {
							parameterBinderRegistry.getBinder(condition.getClass());
						} catch (BindingException e) {
							throw new IllegalArgumentException("Unknown criterion type " + Reflections.toString(condition.getClass()));
						}
						sql.catValue(condition);
					}
				}
			}
			
			public void cat(Selectable column) {
				// delegated to dmlNameProvider
				sql.cat(this.dmlNameProvider.getName(column));
			}
			
			public void cat(LogicalOperator operator) {
				if (operator != null) {
					sql.cat(" ", getName(operator), " ");
				}
			}
			
			public void cat(Selectable c, BiOperandOperator operator) {
				for (Object o : operator.asRawCriterion(c)) {
					catConditionItem(o);
					sql.cat(" ");
				}
				sql.removeLastChars(1);
			}
			
			public void cat(ConditionalOperator operator) {
				operatorSqlBuilder.cat(operator, sql);
			}
			
			public void catTupledIn(TupleIn operator) {
				operatorSqlBuilder.catTupledIn(operator, sql);
			}
			
			public void cat(SQLFunction sqlFunction) {
				functionSQLBuilder.cat(sqlFunction, sql);
			}
			
			public void cat(Selectable column, ConditionalOperator operator) {
				operatorSqlBuilder.cat(column, operator, sql);
			}
			
			public String getName(LogicalOperator operator) {
				switch (operator) {
					case AND:
						return AND;
					case OR:
						return OR;
					default:
						throw new IllegalArgumentException("Operator " + operator + " is unknown");
				}
			}
		}
	}
}
