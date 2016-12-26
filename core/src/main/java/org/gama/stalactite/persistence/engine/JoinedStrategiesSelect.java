package org.gama.stalactite.persistence.engine;

import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.function.BiConsumer;

import org.gama.spy.MethodReferenceCapturer;
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
	 * @see #add(String, ClassMappingStrategy, Column, Column, BiConsumer)
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
	
	/** @deprecated use {@link #add(String, ClassMappingStrategy, Column, Column, boolean, BiConsumer, Class)}  */
	@Deprecated
	public <U> String add(String leftStrategyName, ClassMappingStrategy<U, ?> strategy, Column leftJoinColumn, Column rightJoinColumn,
						  BiConsumer<T, Iterable<U>> setter) {
		StrategyJoins hangingJoins = getStrategyJoins(leftStrategyName);
		if (hangingJoins == null) {
			throw new IllegalStateException("No strategy with name " + leftStrategyName + " exists to add a new strategy");
		}
		MethodReferenceCapturer methodReferenceCapturer = new MethodReferenceCapturer(hangingJoins.getStrategy().getClassToPersist());
		Method capture = methodReferenceCapturer.capture(setter);
		Class<? extends Collection> oneToManyType = null;
		if (capture != null && Collection.class.isAssignableFrom(capture.getReturnType())) {
			oneToManyType = (Class<? extends Collection>) capture.getReturnType();
		}
		// we outer join nullable columns
		boolean isOuterJoin = rightJoinColumn.isNullable();
		return add(leftStrategyName, strategy, leftJoinColumn, rightJoinColumn, isOuterJoin, setter, oneToManyType);
	}
	
	public <U> String add(String leftStrategyName, ClassMappingStrategy<U, ?> strategy, Column leftJoinColumn, Column rightJoinColumn,
						  boolean isOuterJoin, BiConsumer<T, Iterable<U>> setter, Class<? extends Collection> oneToManyType) {
		StrategyJoins hangingJoins = getStrategyJoins(leftStrategyName);
		if (hangingJoins == null) {
			throw new IllegalStateException("No strategy with name " + leftStrategyName + " exists to add a new strategy");
		}
		return add(hangingJoins, strategy, leftJoinColumn, rightJoinColumn, isOuterJoin, setter, oneToManyType);
	}
	
	private <U> String add(StrategyJoins owner, ClassMappingStrategy<U, ?> strategy,
						   Column leftJoinColumn, Column rightJoinColumn, boolean isOuterJoin,
						   BiConsumer<T, Iterable<U>> setter, Class<? extends Collection> oneToManyType) {
		Join join = owner.add(strategy, leftJoinColumn, rightJoinColumn, isOuterJoin, setter, oneToManyType);
		String indexKey = indexNamer.generateName(strategy);
		strategyIndex.put(indexKey, join.getStrategy());
		return indexKey;
	}
	
	StrategyJoins getStrategyJoins(String leftStrategyName) {
		return this.strategyIndex.get(leftStrategyName);
	}
	
	public Collection<StrategyJoins> getStrategies() {
		return strategyIndex.values();
	}
	
	private void addColumnsToSelect(Iterable<Column> selectableColumns, SelectQuery selectQuery,
									Map<String, ParameterBinder> selectParameterBinders) {
		for (Column selectableColumn : selectableColumns) {
			String alias = selectableColumn.getAlias();
			selectQuery.select(selectableColumn, alias);
			// we link the column alias to the binder so it will be easy to read the ResultSet
			// TODO: voir s'il ne faut pas "contextualiser" l'alias en fonction du Dialect (problème de case notamment), ou mettre le ResultSet dans
			// un wrapper comme row insensible à la case
			selectParameterBinders.put(alias, parameterBinderProvider.getBinder(selectableColumn));
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
		 * @param <U> the new strategy mapped type
		 * @param strategy the new strategy on which to join
		 * @param leftJoinColumn the column of the owned strategy table (no check done) on which the join will be made
		 * @param rightJoinColumn the column of the new strategy table (no check done) on whoch the join will be made
		 * @param isOuterJoin indicates if the join is an outer (left) one or not
		 * @param setter the function to use for applyling instance of the new strategy on the owned one
		 * @return the created join
		 */
		private <U> Join<I, U> add(ClassMappingStrategy strategy, Column leftJoinColumn, Column rightJoinColumn, boolean isOuterJoin,
								   BiConsumer<I, Iterable<U>> setter, Class<? extends Collection> oneToManyType) {
			Join<I, U> join = new Join<>(strategy, leftJoinColumn, rightJoinColumn, isOuterJoin, setter, oneToManyType);
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
			/** Indicates if the join must be an inner or (left) outer join */
			private final boolean outer;
			/** Setter for instances of previous strategy entities on this strategy */
			private final BiConsumer<I, Iterable<O>> setter;
			/** Type of the Collection in case of a OneToMany case, expected to be null when join is not a -many relation */
			private final Class<? extends Collection> oneToManyType;
			
			private Join(ClassMappingStrategy<O, ?> strategy, Column leftJoinColumn, Column rightJoinColumn, boolean outer,
						 BiConsumer<I, Iterable<O>> setter, Class<? extends Collection> oneToManyType) {
				this.strategy = new StrategyJoins<>(strategy);
				this.leftJoinColumn = leftJoinColumn;
				this.rightJoinColumn = rightJoinColumn;
				this.outer = outer;
				this.setter = setter;
				this.oneToManyType = oneToManyType;
			}
			
			StrategyJoins<O> getStrategy() {
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
			
			public BiConsumer<I, Iterable<O>> getSetter() {
				return setter;
			}
			
			public Class<? extends Collection> getOneToManyType() {
				return oneToManyType;
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
