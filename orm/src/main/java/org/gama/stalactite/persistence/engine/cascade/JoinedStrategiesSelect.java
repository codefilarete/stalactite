package org.gama.stalactite.persistence.engine.cascade;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import org.gama.lang.Strings;
import org.gama.stalactite.persistence.engine.BeanRelationFixer;
import org.gama.stalactite.persistence.engine.cascade.AbstractJoin.JoinType;
import org.gama.stalactite.persistence.engine.cascade.StrategyJoinsRowTransformer.EntityInflater;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.IEntityMappingStrategy;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.model.From;
import org.gama.stalactite.query.model.Query;
import org.gama.stalactite.sql.binder.ParameterBinder;
import org.gama.stalactite.sql.binder.ParameterBinderProvider;
import org.gama.stalactite.sql.result.Row;

import static org.gama.stalactite.persistence.engine.cascade.AbstractJoin.JoinType.OUTER;
import static org.gama.stalactite.query.model.From.AbstractJoin.JoinDirection.INNER_JOIN;
import static org.gama.stalactite.query.model.From.AbstractJoin.JoinDirection.LEFT_OUTER_JOIN;

/**
 * Class that eases the creation of a SQL selection with multiple joined {@link ClassMappingStrategy}.
 * The representation of a link between strategies is done through {@link StrategyJoins}.
 * 
 * Joins have a name (very first one is {@value #ROOT_STRATEGY_NAME}, see {@link #ROOT_STRATEGY_NAME}) so one can reference it in further
 * joins for joining a table multiple times.
 *
 * @author Guillaume Mary
 * @see #buildSelectQuery()
 */
public class JoinedStrategiesSelect<C, I, T extends Table> {
	
	/** Key of the very first {@link StrategyJoins} added to the join structure (the one generated by constructor), see {@link #getJoinsRoot()} */
	public static final String ROOT_STRATEGY_NAME = "ROOT";
	
	/** Mappig between column name in select and their {@link ParameterBinder} for reading */
	private final Map<String, ParameterBinder> selectParameterBinders = new HashMap<>();
	/** Aliases of columns. Values are keys of {@link #selectParameterBinders} */
	private final Map<Column, String> aliases = new HashMap<>();
	/** Will give the {@link ParameterBinder} for the reading of the final select clause */
	private final ParameterBinderProvider<Column> parameterBinderProvider;
	/** The very first {@link ClassMappingStrategy} on which other strategies will be joined */
	private final StrategyJoins<C, T> root;
	/**
	 * A mapping between a name and join to help finding them when we want to join them with a new one
	 * @see #addRelationJoin(String, IEntityMappingStrategy, Column, Column, JoinType, BeanRelationFixer) 
	 */
	private final Map<String, AbstractJoin> strategyIndex = new HashMap<>();
	/** The objet that will help to give names of strategies into the index (no impact on the generated SQL) */
	private final StrategyIndexNamer indexNamer = new StrategyIndexNamer();
	
	private ColumnAliasBuilder columnAliasBuilder = new ColumnAliasBuilder();
	
	/**
	 * Main constructor to create a root for further joins
	 *
	 * @param classMappingStrategy the root strategy, added strategy will be joined wih it
	 * @param parameterBinderProvider the objet that will give {@link ParameterBinder} to read the selected columns
	 */
	JoinedStrategiesSelect(IEntityMappingStrategy<C, I, T> classMappingStrategy, ParameterBinderProvider<Column> parameterBinderProvider) {
		this.parameterBinderProvider = parameterBinderProvider;
		this.root = new StrategyJoins<>(classMappingStrategy);
	}
	
	public Map<String, ParameterBinder> getSelectParameterBinders() {
		return selectParameterBinders;
	}
	
	/**
	 * @return the generated aliases by {@link Column} during the {@link #addColumnsToSelect(String, Iterable, Query)} phase,
	 * 	unmodifiable because is not expected to be altered outside of this class
	 */
	public Map<Column, String> getAliases() {
		return Collections.unmodifiableMap(aliases);
	}
	
	public String getAlias(Column column) {
		return aliases.get(column);
	}
	
	/**
	 * Give the root of all joins needed by the strategy to build its entity graph at load time and persist it.  
	 * @return a {@link StrategyJoins} which is the root of the graph, which {@link ClassMappingStrategy} is the one given at construction time of this instance
	 */
	public StrategyJoins<C, T> getJoinsRoot() {
		return root;
	}
	
	
	/**
	 * Gives a particular node of the joins graph by its name. Joins graph name are given in return of
	 * {@link #addRelationJoin(String, IEntityMappingStrategy, Column, Column, JoinType, BeanRelationFixer)}.
	 * Prefer usage of {@link #getJoinsRoot()} when retrieval of root is requested (to prevent exposing {@link #ROOT_STRATEGY_NAME})
	 * 
	 * @param leftStrategyName join node name to be given
	 * @return null if the node doesn't exist
	 * @see #getJoinsRoot()
	 */
	@Nullable
	public AbstractJoin getJoin(String leftStrategyName) {
		return this.strategyIndex.get(leftStrategyName);
	}
	
	/**
	 * Gives a particular node of the joins graph by its name. Joins graph name are given in return of
	 * {@link #addRelationJoin(String, IEntityMappingStrategy, Column, Column, JoinType, BeanRelationFixer)}.
	 * Prefer usage of {@link #getJoinsRoot()} when retrieval of root is requested (to prevent exposing {@link #ROOT_STRATEGY_NAME})
	 * 
	 * @param leftStrategyName join node name to be given
	 * @return null if the node doesn't exist
	 * @see #getJoinsRoot()
	 */
	@Nullable
	public StrategyJoins getStrategyJoins(String leftStrategyName) {
		if (ROOT_STRATEGY_NAME.equals(leftStrategyName)) {
			return getJoinsRoot();
		} else {
			return org.gama.lang.Nullable.nullable(getJoin(leftStrategyName)).map(AbstractJoin::getStrategy).get();
		}
	}
	
	public Query buildSelectQuery() {
		Query query = new Query();
		
		// initialization of the from clause with the very first table
		From from = query.getFromSurrogate().add(root.getTable());
		addColumnsToSelect(root.getTableAlias(), root.getColumnsToSelect(), query);
		
		Queue<AbstractJoin> stack = new ArrayDeque<>(root.getJoins());
		while (!stack.isEmpty()) {
			AbstractJoin<?> join = stack.poll();
			addColumnsToSelect(join.getStrategy().getTableAlias(), join.getStrategy().getColumnsToSelect(), query);
			Column leftJoinColumn = join.getLeftJoinColumn();
			Column rightJoinColumn = join.getRightJoinColumn();
			from.add(from.new ColumnJoin(leftJoinColumn, rightJoinColumn, join.getJoinType() == OUTER ? LEFT_OUTER_JOIN : INNER_JOIN));
			
			stack.addAll(join.getStrategy().getJoins());
		}
		
		return query;
	}
	
	/**
	 * Provides tables implied in this join.
	 * 
	 * @return all tables used by this join
	 */
	public Set<Table> giveTables() {
		Set<Table> result = new HashSet<>();
		
		// initialization of the from clause with the very first table
		result.add(root.getTable());
		Queue<AbstractJoin> stack = new ArrayDeque<>(root.getJoins());
		while (!stack.isEmpty()) {
			AbstractJoin<?> join = stack.poll();
			result.add(join.getStrategy().getTable());
			stack.addAll(join.getStrategy().getJoins());
		}
		return result;
	}
	
	/**
	 * Adds a join to this select.
	 * Use for one-to-one or one-to-many cases when join is used to describe a related bean. 
	 * 
	 * @param leftStrategyName the name of a (previously) registered join. {@code leftJoinColumn} must be a {@link Column} of its left {@link Table}
	 * @param strategy the strategy of the mapped bean. Used to give {@link Column}s and {@link org.gama.stalactite.persistence.mapping.IRowTransformer}
	 * @param leftJoinColumn the {@link Column} (of previous strategy left table) to be joined with {@code rightJoinColumn}
	 * @param rightJoinColumn the {@link Column} (of the strategy table) to be joined with {@code leftJoinColumn}
	 * @param joinType says wether or not the join must be open
	 * @param beanRelationFixer a function to fullfill relation between 2 strategies beans
	 * @param <U> type of bean mapped by the given strategy
	 * @param <T1> joined left table
	 * @param <T2> joined right table
	 * @param <ID> type of joined values
	 * @return the name of the created join, to be used as a key for other joins (through this method {@code leftStrategyName} argument)
	 */
	public <U, T1 extends Table<T1>, T2 extends Table<T2>, ID> String addRelationJoin(String leftStrategyName,
																					  IEntityMappingStrategy<U, ID, T2> strategy,
																					  Column<T1, ID> leftJoinColumn,
																					  Column<T2, ID> rightJoinColumn,
																					  JoinType joinType,
																					  BeanRelationFixer<C, U> beanRelationFixer) {
		return addJoin(leftStrategyName, strategy,
				owningNode -> owningNode.add(strategy, leftJoinColumn, rightJoinColumn, joinType, beanRelationFixer));
	}
	
	/**
	 * Adds a join to this select.
	 * Use for inheritance cases when joined data are used to complete an existing bean. 
	 *
	 * @param leftStrategyName the name of a (previously) registered join. {@code leftJoinColumn} must be a {@link Column} of its left {@link Table}.
	 * 						Right table data will be merged with this "root".
	 * @param strategy the strategy of the mapped bean. Used to give {@link Column}s and {@link org.gama.stalactite.persistence.mapping.IRowTransformer}
	 * @param leftJoinColumn the {@link Column} (of previous strategy left table) to be joined with {@code rightJoinColumn}
	 * @param rightJoinColumn the {@link Column} (of the strategy table) to be joined with {@code leftJoinColumn}
	 * @param <U> type of bean mapped by the given strategy
	 * @param <T1> left table type
	 * @param <T2> right table type
	 * @param <ID> type of joined values
	 * @return the name of the created join, to be used as a key for other joins (through this method {@code leftStrategyName} argument)
	 */
	public <U, T1 extends Table<T1>, T2 extends Table<T2>, ID> String addMergeJoin(String leftStrategyName,
																				   IEntityMappingStrategy<U, ID, T2> strategy,
																				   Column<T1, ID> leftJoinColumn,
																				   Column<T2, ID> rightJoinColumn) {
		return addJoin(leftStrategyName, strategy,
				owningNode -> owningNode.addMergeJoin(strategy, leftJoinColumn, rightJoinColumn));
	}
	
	/**
	 * Adds a merge join to this select : no bean will be created by given {@link EntityInflater}, only its
	 * {@link org.gama.stalactite.persistence.mapping.AbstractTransformer#applyRowToBean(Row, Object)} will be used during bean graph loading process.
	 *
	 * @param leftStrategyName join name on which join must be created
	 * @param strategy strategy to be used to load bean
	 * @param columnsToSelect columns that must be added to final select
	 * @param leftJoinColumn left join column, expected to be one of left strategy table
	 * @param rightJoinColumn right join column
	 * @param joinType type of join to create
	 * @param <T1> left table type
	 * @param <T2> right table type
	 * @param <ID> type of joined values
	 * @return the name of the created join, to be used as a key for other joins (through this method {@code leftStrategyName} argument)
	 */
	public <U, T1 extends Table<T1>, T2 extends Table<T2>, ID> String addMergeJoin(String leftStrategyName,
																				   EntityInflater<U, ID> strategy,
																				   Set<Column<Table, Object>> columnsToSelect,
																				   Column<T1, ID> leftJoinColumn,
																				   Column<T2, ID> rightJoinColumn,
																				   JoinType joinType) {
		return addJoin(leftStrategyName, strategy,
				owningNode -> owningNode.addMergeJoin(strategy, columnsToSelect, leftJoinColumn, rightJoinColumn, joinType));
	}
	
	/**
	 * Adds a passive join to this select : this kind if join doesn't take part to bean construction, it aims only at adding an SQL join to
	 * bean graph loading.
	 * 
	 * @param leftStrategyName join name on which join must be created
	 * @param leftJoinColumn left join column, expected to be one of left strategy table
	 * @param rightJoinColumn right join column
	 * @param joinType type of join to create
	 * @param columnsToSelect columns that must be added to final select
	 * @param <T1> left table type
	 * @param <T2> right table type
	 * @param <ID> type of joined values
	 * @return the name of the created join, to be used as a key for other joins (through this method {@code leftStrategyName} argument)
	 */
	public <T1 extends Table<T1>, T2 extends Table<T2>, ID> String addPassiveJoin(String leftStrategyName,
																				     Column<T1, ID> leftJoinColumn,
																				     Column<T2, ID> rightJoinColumn,
																					 JoinType joinType,
																					 Set<Column<Table, Object>> columnsToSelect) {
		return addJoin(leftStrategyName, () -> "passive_" + leftJoinColumn + "_ " + rightJoinColumn,
				owningNode -> owningNode.addPassiveJoin(leftJoinColumn, rightJoinColumn, joinType, columnsToSelect));
	}
	
	/**
	 * Adds a join to this select.
	 * Not aimed at being public.
	 *
	 * @param leftStrategyName the name of a (previously) registered join. {@code leftJoinColumn} must be a {@link Column} of its left {@link Table}.
	 * @param strategy the strategy of the mapped bean. Used to give {@link Column}s and
	 * {@link org.gama.stalactite.persistence.mapping.IRowTransformer}
	 * @param joinFactory builder for adhoc join
	 * @param <U> type of bean mapped by the given strategy
	 * @param <T1> joined left table
	 * @param <T2> joined right table
	 * @param <ID> type of joined values
	 */
	private <U, T1 extends Table<T1>, T2 extends Table<T2>, ID> String addJoin(String leftStrategyName,
																			   IEntityMappingStrategy<U, ID, T2> strategy,
																			   Function<StrategyJoins, AbstractJoin> joinFactory) {
		return addJoin(leftStrategyName, () -> indexNamer.generateName(strategy), joinFactory);
	}
	
	private <U, T1 extends Table<T1>, T2 extends Table<T2>, ID> String addJoin(String leftStrategyName,
																			   EntityInflater<U, ID> strategy,
																			   Function<StrategyJoins, AbstractJoin> joinFactory) {
		return addJoin(leftStrategyName, () -> indexNamer.generateName(strategy), joinFactory);
	}
	
	private String addJoin(String leftStrategyName, Supplier<String> joinNameProvider, Function<StrategyJoins, AbstractJoin> joinFactory) {
		StrategyJoins owningNode = getStrategyJoins(leftStrategyName);
		if (owningNode == null) {
			throw new IllegalArgumentException("No strategy with name " + leftStrategyName + " exists to add a new strategy on");
		}
		AbstractJoin join = joinFactory.apply(owningNode);
		String joinName = joinNameProvider.get();
		this.strategyIndex.put(joinName, join);
		return joinName;
	}
	
	private <T1 extends Table<T1>> void addColumnsToSelect(String tableAliasOverride, Iterable<Column<T1, Object>> selectableColumns, Query query) {
		for (Column selectableColumn : selectableColumns) {
			String tableAlias = columnAliasBuilder.buildAlias(selectableColumn.getTable(), tableAliasOverride);
			String alias = columnAliasBuilder.buildAlias(tableAlias, selectableColumn);
			query.select(selectableColumn, alias);
			// we link the column alias to the binder so it will be easy to read the ResultSet
			selectParameterBinders.put(alias, parameterBinderProvider.getBinder(selectableColumn));
			aliases.put(selectableColumn, alias);
		}
	}
	
	private static class StrategyIndexNamer {
		
		private int aliasCount = 0;
		
		private String generateName(IEntityMappingStrategy classMappingStrategy) {
			return classMappingStrategy.getTargetTable().getAbsoluteName() + aliasCount++;
		}
		
		private String generateName(EntityInflater classMappingStrategy) {
			return classMappingStrategy.getEntityType().getSimpleName() + aliasCount++;
		}
	}
	
	private static class ColumnAliasBuilder {
		
		/**
		 * Gives the alias of a table
		 * @param table the {@link Table} for which an alias is requested
		 * @param aliasOverride an optional given alias
		 * @return the given alias in priority or the name of the table
		 */
		public String buildAlias(Table table, String aliasOverride) {
			return (String) Strings.preventEmpty(aliasOverride, table.getName());
		}
		
		/**
		 * Gives the alias of a Column 
		 * @param tableAlias a non-null table alias
		 * @param selectableColumn the {@link Column} for which an alias is requested
		 * @return tableAlias + "_" + column.getName()
		 */
		public String buildAlias(@Nonnull String tableAlias, Column selectableColumn) {
			return tableAlias + "_" + selectableColumn.getName();
		}
	}
}
