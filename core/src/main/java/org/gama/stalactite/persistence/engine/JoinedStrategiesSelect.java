package org.gama.stalactite.persistence.engine;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.function.Function;

import org.gama.sql.binder.ParameterBinder;
import org.gama.stalactite.persistence.engine.JoinedStrategiesSelect.StrategyJoins.Join;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;
import org.gama.stalactite.query.model.From;
import org.gama.stalactite.query.model.SelectQuery;

/**
 * Class that eases the creation of a SQL selection with multiple joined {@link ClassMappingStrategy}.
 * The representation of a link between strategies is done throught {@link StrategyJoins}
 *
 * @author Guillaume Mary
 * @see #buildSelectQuery()
 */
public class JoinedStrategiesSelect<T, I> {
	
	public static final String FIRST_STRATEGY_NAME = "ROOT";
	
	/** Mappig between column name in select and their {@link ParameterBinder} for reading */
	private final Map<String, ParameterBinder> selectParameterBinders = new HashMap<>();
	/** Will give the {@link ParameterBinder} for the reading of the final select clause */
	private final ParameterBinderProvider parameterBinderProvider;
	/** The very first {@link ClassMappingStrategy} on which other strategies will be joined */
	private final StrategyJoins<T> root;
	/**
	 * A mapping between a name and join to help finding them when we want to join them with a new one
	 * @see #add(String, ClassMappingStrategy, Column, Column, Function)
	 */
	private final Map<String, StrategyJoins> strategyIndex = new HashMap<>();
	/** The objet that will help to give names of strategies into the index */
	private final StrategyIndexNamer indexNamer = new StrategyIndexNamer();
	
	/**
	 * Default constructor
	 *
	 * @param classMappingStrategy the root strategy, added strategy will be joined wih it
	 * @param parameterBinderProvider the objet that will give {@link ParameterBinder} to read the selected columns
	 */
	JoinedStrategiesSelect(ClassMappingStrategy<T, I> classMappingStrategy, ParameterBinderProvider parameterBinderProvider) {
		this(classMappingStrategy, parameterBinderProvider, FIRST_STRATEGY_NAME);
	}
	
	/**
	 * Default constructor
	 *
	 * @param classMappingStrategy the root strategy, added strategy will be joined wih it
	 * @param parameterBinderProvider the objet that will give {@link ParameterBinder} to read the selected columns
	 */
	JoinedStrategiesSelect(ClassMappingStrategy<T, I> classMappingStrategy, ParameterBinderProvider parameterBinderProvider, String strategyName) {
		this.parameterBinderProvider = parameterBinderProvider;
		this.root = new StrategyJoins<>(classMappingStrategy);
		this.strategyIndex.put(strategyName, this.root);
	}
	
	@SuppressWarnings("unchecked")
	public ClassMappingStrategy<T, I> getRoot() {
		return (ClassMappingStrategy<T, I>) root.getStrategy();
	}
	
	public Map<String, ParameterBinder> getSelectParameterBinders() {
		return selectParameterBinders;
	}
	
	public SelectQuery buildSelectQuery() {
		SelectQuery selectQuery = new SelectQuery();
		
		// initialization of the from clause with the very first table
		From from = selectQuery.getFrom().add(root.getTable());
		addColumnsToSelect(root.getStrategy().getSelectableColumns(), selectQuery, selectParameterBinders);
		
		Queue<Join> stack = new ArrayDeque<>();
		stack.addAll(root.getJoins());
		while (!stack.isEmpty()) {
			Join join = stack.poll();
			addColumnsToSelect(join.getStrategy().getStrategy().getSelectableColumns(), selectQuery, selectParameterBinders);
			Column leftJoinColumn = join.getLeftJoinColumn();
			Column rightJoinColumn = join.getRightJoinColumn();
			from.add(from.new ColumnJoin(leftJoinColumn, rightJoinColumn, join.isOuter() ? false : null));
			
			stack.addAll(join.getStrategy().getJoins());
		}
		
		return selectQuery;
	}
	
	public <U> String add(String leftStrategyName, ClassMappingStrategy<U, ?> strategy, Column leftJoinColumn, Column rightJoinColumn,
						  Function<T, Iterable<U>> setter) {
		// we outer join nullable columns, except primary keys because it is nonsense
		boolean isOuterJoin = !leftJoinColumn.isNullable() && !leftJoinColumn.isPrimaryKey();
		return add(leftStrategyName, strategy, leftJoinColumn, rightJoinColumn, isOuterJoin, setter);
	}
	
	public <U> String add(String leftStrategyName, ClassMappingStrategy<U, ?> strategy, Column leftJoinColumn, Column rightJoinColumn,
						   boolean isOuterJoin, Function<T, Iterable<U>> setter) {
		StrategyJoins hangingJoins = this.strategyIndex.get(leftStrategyName);
		if (hangingJoins == null) {
			throw new IllegalStateException("No strategy with name " + leftStrategyName + " exists to add a new strategy");
		}
		hangingJoins.add(strategy, leftJoinColumn, rightJoinColumn, isOuterJoin, Function.identity());
		String indexKey = indexNamer.generateName(strategy);
		strategyIndex.put(indexKey, hangingJoins);
		return indexKey;
	}
	
	public Collection<StrategyJoins> getStrategies() {
		return strategyIndex.values();
	}
	
	private void addColumnsToSelect(Iterable<Column> selectableColumns, SelectQuery selectQuery, Map<String, ParameterBinder> 
			selectParameterBinders) {
		for (Column selectableColumn : selectableColumns) {
			selectQuery.select(selectableColumn, selectableColumn.getAlias());
			// we link the column alias to the binder so it will be easy to read the ResultSet
			// TODO: voir s'il ne faut pas "contextualiser" l'alias en fonction du Dialect (problème de case notamment), ou mettre le ResultSet dans
			// un wrapper comme row insensible à la case
			selectParameterBinders.put(selectableColumn.getAlias(), parameterBinderProvider.getBinder(selectableColumn));
		}
	}
	
	/**
	 * Joins between strategies: owns the left part of the join, and "right parts" are represented by a collection of {@link Join}.
	 *
	 * @param <I> the type of the entity mapped by the {@link ClassMappingStrategy}
	 */
	static class StrategyJoins<I> {
		/** The left part of the join */
		private final ClassMappingStrategy<I, ?> strategy;
		/** Joins */
		private final List<Join> joins = new ArrayList<>();
		
		private StrategyJoins(ClassMappingStrategy<I, ?> strategy) {
			this.strategy = strategy;
		}
		
		public List<Join> getJoins() {
			return joins;
		}
		
		/**
		 * Add a join between the owned strategy and the given one
		 *
		 * @param strategy the new strategy on which to join
		 * @param leftJoinColumn the column of the owned strategy table (no check done) on which the join will be made
		 * @param rightJoinColumn the column of the new strategy table (no check done) on whoch the join will be made
		 * @param isOuterJoin indicates if the join is an outer (left) one or not
		 * @param setter the function to use for applyling instance of the new strategy on the owned one
		 * @param <U> the new strategy mapped type
		 * @return the created join
		 */
		private <U> Join<I, U> add(ClassMappingStrategy<U, ?> strategy, Column leftJoinColumn, Column rightJoinColumn, boolean isOuterJoin,
								   Function<I, U> setter) {
			Join<I, U> join = new Join<>(strategy, leftJoinColumn, rightJoinColumn, isOuterJoin, setter);
			this.joins.add(join);
			return join;
		}
		
		public ClassMappingStrategy<I, ?> getStrategy() {
			return strategy;
		}
		
		public Table getTable() {
			return strategy.getTargetTable();
		}
		
		/** The "right part" of a join between between 2 {@link ClassMappingStrategy} */
		public static class Join<I, O> {
			/** The right part of the join */
			private final StrategyJoins<O> strategy;
			/** Join column with previous strategy table */
			private final Column leftJoinColumn;
			/** Join column with next strategy table */
			private final Column rightJoinColumn;
			/** Indicates if the join must be an outer join */
			private final boolean outer;
			/** Setter for instances of previous strategy entities on this strategy */
			private final Function<I, O> setter;
			
			private Join(ClassMappingStrategy<O, ?> strategy, Column leftJoinColumn, Column rightJoinColumn, boolean outer, Function<I, O> setter) {
				this.strategy = new StrategyJoins<>(strategy);
				this.leftJoinColumn = leftJoinColumn;
				this.rightJoinColumn = rightJoinColumn;
				this.outer = outer;
				this.setter = setter;
			}
			
			private StrategyJoins<O> getStrategy() {
				return strategy;
			}
			
			private Column getLeftJoinColumn() {
				return leftJoinColumn;
			}
			
			private Column getRightJoinColumn() {
				return rightJoinColumn;
			}
			
			public boolean isOuter() {
				return outer;
			}
			
			public Function<I, O> getSetter() {
				return setter;
			}
		}
	}
	
	private static class StrategyIndexNamer {
		
		private int aliasCount = 0;
		
		private String generateName(ClassMappingStrategy classMappingStrategy) {
			return classMappingStrategy.getTargetTable().getAbsoluteName() + aliasCount++;
		}
	}
	
	@FunctionalInterface
	public interface ParameterBinderProvider {
		ParameterBinder getBinder(Column column);
	}
}
