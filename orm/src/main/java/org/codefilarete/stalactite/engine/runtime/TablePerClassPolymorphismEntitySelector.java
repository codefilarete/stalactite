package org.codefilarete.stalactite.engine.runtime;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.engine.runtime.load.EntityInflater;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder.EntityTreeQuery;
import org.codefilarete.stalactite.engine.runtime.load.JoinRoot;
import org.codefilarete.stalactite.mapping.AbstractTransformer;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.mapping.RowTransformer;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.query.ConfiguredEntityCriteria;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory.QuerySQLBuilder;
import org.codefilarete.stalactite.query.model.CriteriaChain;
import org.codefilarete.stalactite.query.model.LimitAware;
import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.query.model.OrderByChain;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.QueryEase;
import org.codefilarete.stalactite.query.model.Select;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.query.model.Selectable.SelectableString;
import org.codefilarete.stalactite.query.model.Union;
import org.codefilarete.stalactite.query.model.QueryStatement.PseudoTable;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.codefilarete.stalactite.sql.result.Row;
import org.codefilarete.stalactite.sql.result.RowIterator;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.ReadOperation;
import org.codefilarete.stalactite.sql.statement.SQLExecutionException;
import org.codefilarete.stalactite.sql.statement.binder.ResultSetReader;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.bean.Objects;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Collections;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.collection.KeepOrderSet;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_STRATEGY_NAME;

/**
 * @author Guillaume Mary
 */
public class TablePerClassPolymorphismEntitySelector<C, I, T extends Table<T>> extends AbstractPolymorphicEntitySelector<C, I, T> {
	
	@VisibleForTesting
	static final String DISCRIMINATOR_ALIAS = "DISCRIMINATOR";
	static final SelectableString<String> DISCRIMINATOR_COLUMN = new SelectableString<>(DISCRIMINATOR_ALIAS, String.class);
	
	private final ConfiguredRelationalPersister<C, I> mainPersister;
	private final IdentifierAssembler<I, T> identifierAssembler;
	private final T mainTable;
	private final Map<String, Class> discriminatorValues;
	private final SingleLoadEntityJoinTree<C, I> singleLoadEntityJoinTree;
	private final PhasedEntityJoinTree<C, I> phasedLoadEntityJoinTree;
	
	public TablePerClassPolymorphismEntitySelector(
			ConfiguredRelationalPersister<C, I> mainPersister,
			Map<? extends Class<C>, ? extends ConfiguredRelationalPersister<C, I>> persisterPerSubclass,
			ConnectionProvider connectionProvider,
			Dialect dialect
	) {
		super(mainPersister.getEntityJoinTree(), persisterPerSubclass, connectionProvider, dialect);
		this.mainPersister = mainPersister;
		this.identifierAssembler = mainPersister.getMapping().getIdMapping().getIdentifierAssembler();
		this.mainTable = (T) mainPersister.getMainTable();
		// building readers and aliases for union-all query
		this.discriminatorValues = new HashMap<>();
		persisterPerSubclass.forEach((subEntityType, subEntityTable) -> {
			discriminatorValues.put(subEntityType.getSimpleName(), subEntityType);
		});
		singleLoadEntityJoinTree = buildSingleLoadEntityJoinTree();
		phasedLoadEntityJoinTree = build2PhasesLoadEntityJoinTree();
	}
	
	/**
	 * Creates an {@link EntityJoinTree} which main Table is actually a Union clause made of sub-entities tables
	 * @return an appropriate {@link EntityJoinTree}
	 */
	private SingleLoadEntityJoinTree<C, I> buildSingleLoadEntityJoinTree() {
		Union union = new Union();
		Set<Selectable<?>> allColumnsInHierarchy = Collections.cat(Arrays.asList(mainPersister), persisterPerSubclass.values())
				.stream().flatMap(persister -> ((Table<?>) persister.getMainTable()).getColumns().stream())
				.map(column -> new SelectableString<>(column.getExpression(), column.getJavaType()))
				.collect(Collectors.toCollection(KeepOrderSet::new));
		
		Map<String, ConfiguredRelationalPersister<C, I>> discriminatorPerSubPersister = new HashMap<>();
		persisterPerSubclass.forEach((subEntityType, subEntityPersister) -> {
			String discriminatorValue = subEntityType.getSimpleName();
			
			subEntityPersister.getMainTable().getColumns();
			Map<Selectable<?>, String> subQueryColumns = new KeepOrderMap<>();
			allColumnsInHierarchy.forEach(pseudoColumn -> {
				Column column = subEntityPersister.getMainTable().findColumn(pseudoColumn.getExpression());
				if (column != null) {
					subQueryColumns.put(column, column.getName());
				} else {
					subQueryColumns.put(Operators.cast((String) null, pseudoColumn.getJavaType()), pseudoColumn.getExpression());
				}
			});
			Query query = QueryEase.
					select(subQueryColumns)
					.add("'" + discriminatorValue + "'", String.class).as(DISCRIMINATOR_ALIAS)
					.from(subEntityPersister.getMainTable())
					.getQuery();
			union.getQueries().add(query);
			query.getColumns()
					.forEach(column -> union.registerColumn(column.getExpression(), column.getJavaType(), subQueryColumns.get(column)));
			discriminatorPerSubPersister.put(discriminatorValue, subEntityPersister);
		});
		union.registerColumn(DISCRIMINATOR_COLUMN.getExpression(), String.class, DISCRIMINATOR_ALIAS);
		
		// Note that it's very important to use main table name to mimic virtual main table else joins (below) won't work
		PseudoTable pseudoTable = union.asPseudoTable(mainTable.getName());
		// we add joins to the union clause
		SingleLoadEntityJoinTree<C, I> result = new SingleLoadEntityJoinTree<>(mainPersister, discriminatorPerSubPersister, pseudoTable, allColumnsInHierarchy);
		mainEntityJoinTree.projectTo(result, ROOT_STRATEGY_NAME);
		return result;
	}
	
	/**
	 * Creates an {@link EntityJoinTree} which main Table is actually a Union clause made of sub-entities tables
	 * @return an appropriate {@link EntityJoinTree}
	 */
	private PhasedEntityJoinTree<C, I> build2PhasesLoadEntityJoinTree() {
		Map<String, String> aliasPerColumnName = Iterables.map(mainTable.getColumns(), Column::getName, Column::getName);
		Union union = new Union();
		persisterPerSubclass.forEach((subEntityType, subEntityPersister) -> {
			String discriminatorValue = subEntityType.getSimpleName();
			Query query = buildSubConfigurationQuery(discriminatorValue, mainTable, subEntityPersister.getMainTable(), aliasPerColumnName);
			union.getQueries().add(query);
			query.getColumns()
					.stream().filter(column -> !column.getExpression().equals(discriminatorValue))
					.forEach(column -> union.registerColumn(column.getExpression(), column.getJavaType()));
		});
		
		// Note that it's very important to use main table name to mimic virtual main table else joins (below) won't work
		PseudoTable pseudoTable = union.asPseudoTable(mainTable.getName());
		// we add joins to the union clause
		PhasedEntityJoinTree<C, I> result = new PhasedEntityJoinTree<>(pseudoTable, mainTable);
		mainEntityJoinTree.projectTo(result, ROOT_STRATEGY_NAME);
		return result;
	}
	
	private <SUBTABLE extends Table<SUBTABLE>> Query buildSubConfigurationQuery(String discriminatorValue,
																				T mainTable,
																				SUBTABLE subEntityTable,
																				Map<String, String> aliasPerColumnName) {
		Map<Column<SUBTABLE, ?>, String> aliasPerColumn = Iterables.map(mainTable.getColumns(),
				// we keep only common columns
				column -> subEntityTable.getColumn(column.getName()),
				c -> aliasPerColumnName.get(c.getName()),
				KeepOrderMap::new);
		return QueryEase.
				select(aliasPerColumn)
				.add("'" + discriminatorValue + "'", String.class).as(DISCRIMINATOR_ALIAS)
				.from(subEntityTable)
				.getQuery();
	}
	
	@Override
	Set<C> selectWithSingleQuery(ConfiguredEntityCriteria where, Consumer<OrderByChain<?>> orderByClauseConsumer, Consumer<LimitAware<?>> limitAwareConsumer) {
		// Condition doesn't have criteria on a collection property (*-to-many) : the load can be done with one query because the SQL criteria
		// doesn't make a subset of the entity graph
		EntityTreeQuery<C> entityTreeQuery = new EntityTreeQueryBuilder<>(singleLoadEntityJoinTree, dialect.getColumnBinderRegistry()).buildSelectQuery();
		Query query = entityTreeQuery.getQuery();
		
		// we add union columns
		Map<Selectable<?>, Selectable<?>> columnClones = entityTreeQuery.getColumnClones();
		Map<Selectable<?>, Selectable<?>> originalColumnsToClones = new IdentityHashMap<>(columnClones.size());
		originalColumnsToClones.putAll(columnClones);
		originalColumnsToClones.putAll(singleLoadEntityJoinTree.getMainColumnToPseudoColumn());
		originalColumnsToClones.put(DISCRIMINATOR_COLUMN, DISCRIMINATOR_COLUMN);
		
		orderByClauseConsumer.accept(new ColumnCloneAwareOrderBy(query.orderBy(), originalColumnsToClones));
		limitAwareConsumer.accept(query.orderBy());
		// since criteria is passed to union subqueries, we don't need it into the entire query
		QuerySQLBuilder sqlQueryBuilder = dialect.getQuerySQLBuilderFactory().queryBuilder(query, where.getCriteria(), originalColumnsToClones);
		EntityTreeInflater<C> inflater = entityTreeQuery.getInflater();
		PreparedSQL preparedSQL = sqlQueryBuilder.toPreparedSQL();
		try (ReadOperation<Integer> readOperation = new ReadOperation<>(preparedSQL, connectionProvider)) {
			ResultSet resultSet = readOperation.execute();
			// NB: we give the same ParametersBinders of those given at ColumnParameterizedSelect since the row iterator is expected to read column from it
			RowIterator rowIterator = new RowIterator(resultSet, entityTreeQuery.getSelectParameterBinders());
			return inflater.transform(() -> rowIterator, 50);
		} catch (RuntimeException e) {
			throw new SQLExecutionException(preparedSQL.getSQL(), e);
		}
	}
	
	@Override
	Set<C> selectIn2Phases(ConfiguredEntityCriteria where, Consumer<OrderByChain<?>> orderByClauseConsumer, Consumer<LimitAware<?>> limitAwareConsumer) {
		EntityTreeQuery<C> entityTreeQuery = new EntityTreeQueryBuilder<>(phasedLoadEntityJoinTree, dialect.getColumnBinderRegistry()).buildSelectQuery();
		Query query = entityTreeQuery.getQuery();
		
		Map<Selectable<?>, Selectable<?>> columnClones = entityTreeQuery.getColumnClones();
		Map<Selectable<?>, Selectable<?>> originalColumnsToClones = new IdentityHashMap<>(columnClones.size());
		originalColumnsToClones.putAll(columnClones);
		originalColumnsToClones.putAll(phasedLoadEntityJoinTree.getMainColumnToPseudoColumn());
		// since criteria is passed to union subqueries, we don't need it into the entire query
		QuerySQLBuilder sqlQueryBuilder = dialect.getQuerySQLBuilderFactory().queryBuilder(query, where.getCriteria(), originalColumnsToClones);
		
		// First phase : selecting ids (made by clearing selected elements for performance issue)
		query.getSelectSurrogate().clear();
		mainTable.getPrimaryKey().getColumns().forEach(pkColumn -> {
			Selectable<?> column = mainEntityJoinTree.getRoot().getTable().findColumn(pkColumn.getName());
			query.select(column, pkColumn.getName());
		});
		query.select(DISCRIMINATOR_COLUMN);
		
		// selecting ids and their entity type
		Map<String, ResultSetReader> columnReaders = new HashMap<>();
		Map<Selectable<?>, String> aliases = query.getAliases();
		aliases.forEach((selectable, alias) -> {
			ResultSetReader<?> reader;
			if (selectable instanceof Column) {
				reader = dialect.getColumnBinderRegistry().getReader((Column) selectable);
			} else {
				reader = dialect.getColumnBinderRegistry().getReader(selectable.getJavaType());
			}
			// by default, in SQL, columns can be found through their name if they have no alias
			// Note that this should only happen here for discriminator column because we didn't give it an alias (unnecessary)
			alias = Objects.preventNull(alias, selectable.getExpression());
			columnReaders.put(alias, reader);
		});
		// Faking that we put the main table column into the query to let external user look for main table Columns,
		// else it's very difficult for them to look through Column object since query contains pseudo column in select due to union usage.
		mainTable.getColumns().forEach(column -> {
			aliases.put(column, column.getName());
		});
		ColumnedRow columnedRow = new ColumnedRow(aliases::get);
		orderByClauseConsumer.accept(new ColumnCloneAwareOrderBy(query.orderBy(), originalColumnsToClones));
		limitAwareConsumer.accept(query.orderBy());
		
		Map<Class, Set<I>> idsPerSubclass = readIds(sqlQueryBuilder.toPreparedSQL(), columnReaders, columnedRow);
		
		// Second phase : selecting entities by delegating it to each subclass loader
		// It will generate 1 query per found subclass, made as this :
		// - to avoid superfluous join and complex query in case of relation
		// - make it simpler to implement
		Set<C> result = new HashSet<>();
		idsPerSubclass.forEach((subClass, subEntityIds) -> result.addAll(persisterPerSubclass.get(subClass).select(subEntityIds)));
		return result;
	}
	
	private Map<Class, Set<I>> readIds(PreparedSQL preparedSQL, Map<String, ResultSetReader> columnReaders, ColumnedRow columnedRow) {
		try (ReadOperation<Integer> closeableOperation = new ReadOperation<>(preparedSQL, connectionProvider)) {
			ResultSet resultSet = closeableOperation.execute();
			RowIterator rowIterator = new RowIterator(resultSet, columnReaders);
			// Below we keep order of given entities mainly to get steady unit tests. Meanwhile, this may have performance
			// impacts but very difficult to measure
			Map<Class, Set<I>> idsPerSubclass = new KeepOrderMap<>();
			rowIterator.forEachRemaining(row -> {
				// looking for entity type on row : we read each subclass PK and check for nullity. The non-null one is the good one
				String discriminatorValue = (String) row.get(DISCRIMINATOR_ALIAS);
				// NB: we trim because some database (as HSQLDB) adds some padding in order that all values get same length
				Class<? extends C> entitySubclass = discriminatorValues.get(discriminatorValue.trim());
				
				// adding identifier to subclass' ids
				idsPerSubclass.computeIfAbsent(entitySubclass, k -> new HashSet<>())
						.add(identifierAssembler.assemble(row, columnedRow));
			});
			return idsPerSubclass;
		} catch (RuntimeException e) {
			throw new SQLExecutionException(preparedSQL.getSQL(), e);
		}
	}
	
	@Override
	public <R, O> R selectProjection(Consumer<Select> selectAdapter, Accumulator<? super Function<Selectable<O>, O>, Object, R> accumulator, CriteriaChain where,
									 boolean distinct, Consumer<OrderByChain<?>> orderByClauseConsumer, Consumer<LimitAware<?>> limitAwareConsumer) {
		EntityTreeQuery<C> entityTreeQuery = new EntityTreeQueryBuilder<>(singleLoadEntityJoinTree, dialect.getColumnBinderRegistry()).buildSelectQuery();
		Query query = entityTreeQuery.getQuery();
		query.getSelectSurrogate().setDistinct(distinct);
		
		Map<Selectable<?>, Selectable<?>> columnClones = entityTreeQuery.getColumnClones();
		Map<Selectable<?>, Selectable<?>> originalColumnsToClones = new IdentityHashMap<>(columnClones.size());
		originalColumnsToClones.putAll(columnClones);
		originalColumnsToClones.putAll(singleLoadEntityJoinTree.getMainColumnToPseudoColumn());
		// since criteria is passed to union subqueries, we don't need it into the entire query
		QuerySQLBuilder sqlQueryBuilder = dialect.getQuerySQLBuilderFactory().queryBuilder(query, where, originalColumnsToClones);
		
		selectAdapter.accept(query.getSelectSurrogate());
		Map<Selectable<?>, String> aliases = query.getAliases();
		ColumnedRow columnedRow = new ColumnedRow(aliases::get);
		
		Map<String, ResultSetReader<?>> columnReaders = Iterables.map(query.getColumns(), new AliasAsserter<>(aliases::get), selectable -> dialect.getColumnBinderRegistry().getBinder(selectable.getJavaType()));
		
		PreparedSQL preparedSQL = sqlQueryBuilder.toPreparedSQL();
		return readProjection(preparedSQL, columnReaders, columnedRow, accumulator);
	}
	
	private static class PhasedEntityJoinTree<C, I> extends EntityJoinTree<C, I> {
		
		private final IdentityHashMap<Column<?, ?>, Selectable<?>> mainColumnToPseudoColumn = new IdentityHashMap<>();
		
		private PhasedEntityJoinTree(PseudoTable pseudoTable, Table mainTable) {
			super(
					// There's no need to have a working EntityInflater here because the PhasedEntityJoinTree
					// is not used to inflate the entities (it's done by dedicated select from sub-entities)
					new EntityInflater<C, I>() {
						@Override
						public Class<C> getEntityType() {
							return null;
						}
						
						@Override
						public I giveIdentifier(Row row, ColumnedRow columnedRow) {
							return null;
						}
						
						@Override
						public RowTransformer<C> copyTransformerWithAliases(ColumnedRow columnedRow) {
							// we can afford to return null because the copy is not used
							return null;
						}
						
						@Override
						public Set<Selectable<?>> getSelectableColumns() {
							return (Set) pseudoTable.getColumns();
						}
					}, pseudoTable);
			// Building a mapping between main persister columns and those of union
			// this will allow us to lookup for main persister columns values in final ResultSet
			pseudoTable.getColumns().forEach(pseudoColumn -> {
				Column column = mainTable.findColumn(pseudoColumn.getExpression());
				if (column != null) {
					mainColumnToPseudoColumn.put(column, pseudoColumn);
				}
			});
		}
		
		@Override
		public JoinRoot<C, I, PseudoTable> getRoot() {
			return (JoinRoot<C, I, PseudoTable>) super.getRoot();
		}
		
		public IdentityHashMap<Column<?, ?>, Selectable<?>> getMainColumnToPseudoColumn() {
			return mainColumnToPseudoColumn;
		}
	}
	
	private static class SingleLoadEntityJoinTree<C, I> extends EntityJoinTree<C, I> {
		
		private final IdentityHashMap<Column<?, ?>, Selectable<?>> mainColumnToPseudoColumn = new IdentityHashMap<>();
		
		public <T extends Table<T>> SingleLoadEntityJoinTree(ConfiguredRelationalPersister<C, I> mainPersister,
															 Map<String, ConfiguredRelationalPersister<C, I>> discriminatorPerSubPersister,
															 PseudoTable pseudoTable,
															 Set<Selectable<?>> allColumnInHierarchy) {
			super(new EntityInflater<C, I>() {
						@Override
						public Class<C> getEntityType() {
							return mainPersister.getClassToPersist();
						}
						
						@Override
						public I giveIdentifier(Row row, ColumnedRow columnedRow) {
							return mainPersister.getMapping().getIdMapping().getIdentifierAssembler().assemble(row, columnedRow);
						}
						
						@Override
						public RowTransformer<C> copyTransformerWithAliases(ColumnedRow columnedRow) {
							Map<String, RowTransformer<C>> rowTransformerPerDiscriminator =
									Iterables.map(discriminatorPerSubPersister.entrySet(), Entry::getKey, entry -> entry.getValue().getMapping().copyTransformerWithAliases(columnedRow));
							return new RowTransformer<C>() {
								@Override
								public C transform(Row row) {
									String value = columnedRow.getValue(DISCRIMINATOR_COLUMN, row);
									// NB: we trim because some database (as HSQLDB) adds some padding in order that all values get same length
									return rowTransformerPerDiscriminator.get(value.trim()).transform(row);
								}
								
								@Override
								public void applyRowToBean(Row row, C bean) {
									// Nothing to do because we override transform(..) which does all we need (forward to sub transformer)
								}
								
								@Override
								public AbstractTransformer<C> copyWithAliases(ColumnedRow columnedRow) {
									// safeguard against unexpected behavior
									throw new IllegalStateException("copyWithAliases(..) is not expected to be called twice");
								}
								
								@Override
								public void addTransformerListener(TransformerListener<? extends C> listener) {
									
								}
							};
						}
						
						@Override
						public Set<Selectable<?>> getSelectableColumns() {
							// since this inflater will be used in the case of the union, we provide all the columns available in the hierarchy,
							// added with the discriminator column
							KeepOrderSet<Selectable<?>> result = new KeepOrderSet<>(allColumnInHierarchy);
							result.add(DISCRIMINATOR_COLUMN);
							return result;
						}
					},
					pseudoTable);
			// Building a mapping between main persister columns and those of union
			// this will allow us to lookup for main persister columns values in final ResultSet
			pseudoTable.getColumns().forEach(pseudoColumn -> {
				Column column = mainPersister.getMainTable().findColumn(pseudoColumn.getExpression());
				if (column != null) {
					mainColumnToPseudoColumn.put(column, pseudoColumn);
				}
			});
		}
		
		@Override
		public JoinRoot<C, I, PseudoTable> getRoot() {
			return (JoinRoot<C, I, PseudoTable>) super.getRoot();
		}
		
		public IdentityHashMap<Column<?, ?>, Selectable<?>> getMainColumnToPseudoColumn() {
			return mainColumnToPseudoColumn;
		}
	}
}
