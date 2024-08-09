package org.codefilarete.stalactite.engine.runtime;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.engine.runtime.load.EntityInflater;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder.EntityTreeQuery;
import org.codefilarete.stalactite.engine.runtime.load.JoinRoot;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.mapping.RowTransformer;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.query.EntitySelector;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory.QuerySQLBuilder;
import org.codefilarete.stalactite.query.model.CriteriaChain;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.QueryEase;
import org.codefilarete.stalactite.query.model.Select;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.query.model.Union;
import org.codefilarete.stalactite.query.model.Union.UnionInFrom;
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
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.collection.KeepOrderSet;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_STRATEGY_NAME;

/**
 * @author Guillaume Mary
 */
public class TablePerClassPolymorphismEntitySelector<C, I, T extends Table<T>> implements EntitySelector<C, I> {
	
	@VisibleForTesting
	static final String DISCRIMINATOR_ALIAS = "DISCRIMINATOR";
	
	private final IdentifierAssembler<I, T> identifierAssembler;
	private final Map<Class<C>, ConfiguredRelationalPersister<C, I>> persisterPerSubclass;
	private final EntityJoinTree<C, I> mainEntityJoinTree;
	private final ConnectionProvider connectionProvider;
	private final Dialect dialect;
	private final T mainTable;
	private final Map<String, Class> discriminatorValues;
	private final TablePerClassEntityJoinTree<C, I> entityJoinTree;
	
	public TablePerClassPolymorphismEntitySelector(
			IdentifierAssembler<I, T> identifierAssembler,
			Map<? extends Class<C>, ? extends ConfiguredRelationalPersister<C, I>> persisterPerSubclass,
			EntityJoinTree<C, I> mainEntityJoinTree,
			ConnectionProvider connectionProvider,
			Dialect dialect
	) {
		this.identifierAssembler = identifierAssembler;
		this.persisterPerSubclass = (Map<Class<C>, ConfiguredRelationalPersister<C, I>>) persisterPerSubclass;
		this.mainEntityJoinTree = mainEntityJoinTree;
		this.connectionProvider = connectionProvider;
		this.dialect = dialect;
		this.mainTable = (T) mainEntityJoinTree.getRoot().getTable();
		// building readers and aliases for union-all query
		this.discriminatorValues = new HashMap<>();
		persisterPerSubclass.forEach((subEntityType, subEntityTable) -> {
			discriminatorValues.put(subEntityType.getSimpleName(), subEntityType);
		});
		entityJoinTree = buildEntityJoinTree();
	}
	
	/**
	 * Creates an {@link EntityJoinTree} which main Table is actually a Union clause made of sub-entities tables
	 * @return an appropriate {@link EntityJoinTree}
	 */
	TablePerClassEntityJoinTree<C, I> buildEntityJoinTree() {
		Map<String, String> aliasPerColumnName = Iterables.map(mainTable.getColumns(), Column::getName, Column::getName);
		Set<Query> queries = new KeepOrderSet<>();
		persisterPerSubclass.forEach((subEntityType, subEntityPersister) -> {
			Query query = buildSubConfigurationQuery("'" + subEntityType.getSimpleName() + "'", mainTable, (Table) subEntityPersister.getMainTable(), aliasPerColumnName);
			queries.add(query);
		});
		
		Union union = new Union(queries);
		queries.forEach(query -> {
			query.getColumns().forEach(column -> union.registerColumn(column.getExpression(), column.getJavaType()));
		});
		// Note that it's very important to use main table name to mimic virtual main table else joins (below) won't work
		UnionInFrom pseudoTable = union.asPseudoTable(mainTable.getName());
		// we add joins to the union clause
		TablePerClassEntityJoinTree<C, I> result = new TablePerClassEntityJoinTree<>(pseudoTable, mainTable);
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
				.add(discriminatorValue, String.class).as(DISCRIMINATOR_ALIAS)
				.from(subEntityTable)
				.getQuery();
	}
	
	@Override
	public Set<C> select(CriteriaChain where) {
		EntityTreeQuery<C> entityTreeQuery = new EntityTreeQueryBuilder<>(entityJoinTree, dialect.getColumnBinderRegistry()).buildSelectQuery();
		Query query = entityTreeQuery.getQuery();
		
		IdentityHashMap<Selectable<?>, Selectable<?>> columnClones = entityTreeQuery.getColumnClones();
		IdentityHashMap<Selectable<?>, Selectable<?>> originalColumnsToClones = new IdentityHashMap<>(columnClones.size());
		originalColumnsToClones.putAll(columnClones);
		originalColumnsToClones.putAll(entityJoinTree.getMainColumnToPseudoColumn());
		// since criteria is passed to union subqueries, we don't need it into the entire query
		QuerySQLBuilder sqlQueryBuilder = dialect.getQuerySQLBuilderFactory().queryBuilder(query, where, originalColumnsToClones);
		
		// First phase : selecting ids (made by clearing selected elements for performance issue)
		query.getSelectSurrogate().clear();
		mainTable.getPrimaryKey().getColumns().forEach(pkColumn -> {
			Selectable<?> column = mainEntityJoinTree.getRoot().getTable().findColumn(pkColumn.getName());
			query.select(column, pkColumn.getName());
		});
		query.select(DISCRIMINATOR_ALIAS, String.class);
		
		// selecting ids and their entity type
		Map<String, ResultSetReader> columnReaders = new HashMap<>();
		Map<Selectable<?>, String> aliases = query.getAliases();
		aliases.forEach((selectable, s) -> {
			ResultSetReader<?> reader;
			if (selectable instanceof Column) {
				reader = dialect.getColumnBinderRegistry().getReader((Column) selectable);
			} else {
				reader = dialect.getColumnBinderRegistry().getReader(selectable.getJavaType());
			}
			columnReaders.put(s, reader);
		});
		// Faking that we put the main table column into the query to let external user look for main table Columns,
		// else it's very difficult for them to look through Column object since query contains pseudo column in select due to union usage.
		mainTable.getColumns().forEach(column -> {
			aliases.put(column, column.getName());
		});
		columnReaders.put(DISCRIMINATOR_ALIAS, dialect.getColumnBinderRegistry().getBinder(String.class));
		ColumnedRow columnedRow = new ColumnedRow(aliases::get);
		
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
	public <R, O> R selectProjection(Consumer<Select> selectAdapter, Accumulator<? super Function<Selectable<O>, O>, Object, R> accumulator, CriteriaChain where, boolean distinct) {
		EntityTreeQuery<C> entityTreeQuery = new EntityTreeQueryBuilder<>(entityJoinTree, dialect.getColumnBinderRegistry()).buildSelectQuery();
		Query query = entityTreeQuery.getQuery();
		query.getSelectSurrogate().setDistinct(distinct);
		
		IdentityHashMap<Selectable<?>, Selectable<?>> columnClones = entityTreeQuery.getColumnClones();
		IdentityHashMap<Selectable<?>, Selectable<?>> originalColumnsToClones = new IdentityHashMap<>(columnClones.size());
		originalColumnsToClones.putAll(columnClones);
		originalColumnsToClones.putAll(entityJoinTree.getMainColumnToPseudoColumn());
		// since criteria is passed to union subqueries, we don't need it into the entire query
		QuerySQLBuilder sqlQueryBuilder = dialect.getQuerySQLBuilderFactory().queryBuilder(query, where, originalColumnsToClones);
		
		selectAdapter.accept(query.getSelectSurrogate());
		Map<Selectable<?>, String> aliases = query.getAliases();
		ColumnedRow columnedRow = new ColumnedRow(aliases::get);
		
		Map<String, ResultSetReader<?>> columnReaders = Iterables.map(query.getColumns(), new AliasAsserter<>(aliases::get), selectable -> dialect.getColumnBinderRegistry().getBinder(selectable.getJavaType()));
		
		PreparedSQL preparedSQL = sqlQueryBuilder.toPreparedSQL();
		return readProjection(preparedSQL, columnReaders, columnedRow, accumulator);
	}
	
	private <R, O> R readProjection(PreparedSQL preparedSQL, Map<String, ResultSetReader<?>> columnReaders, ColumnedRow columnedRow, Accumulator<? super Function<Selectable<O>, O>, Object, R> accumulator) {
		try (ReadOperation<Integer> closeableOperation = new ReadOperation<>(preparedSQL, connectionProvider)) {
			ResultSet resultSet = closeableOperation.execute();
			RowIterator rowIterator = new RowIterator(resultSet, columnReaders);
			return accumulator.collect(Iterables.stream(rowIterator).map(row -> (Function<Selectable<O>, O>) selectable -> columnedRow.getValue(selectable, row)).collect(Collectors.toList()));
		} catch (RuntimeException e) {
			throw new SQLExecutionException(preparedSQL.getSQL(), e);
		}
	}
	
	/**
	 * Small class that will be used to ensure that a {@link Selectable} as an alias in the query
	 * @param <S>
	 * @author Guillaume Mary
	 */
	private static class AliasAsserter<S extends Selectable> implements Function<S, String> {
		
		private final Function<S, String> delegate;
		
		private AliasAsserter(Function<S, String> delegate) {
			this.delegate = delegate;
		}
		
		@Override
		public String apply(S selectable) {
			String alias = delegate.apply(selectable);
			if (alias == null) {
				throw new IllegalArgumentException("Item '" + selectable.getExpression() + "' must have an alias");
			}
			return alias;
		}
	}
	
	private static class TablePerClassEntityJoinTree<C, I> extends EntityJoinTree<C, I> {
		
		private final IdentityHashMap<Column<?, ?>, Selectable<?>> mainColumnToPseudoColumn = new IdentityHashMap<>();
		
		public TablePerClassEntityJoinTree(UnionInFrom pseudoTable, Table mainTable) {
			super(
					// There's no need to have a working EntityInflater here because the TablePerClassEntityJoinTree
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
		public JoinRoot<C, I, UnionInFrom> getRoot() {
			return (JoinRoot<C, I, UnionInFrom>) super.getRoot();
		}
		
		public IdentityHashMap<Column<?, ?>, Selectable<?>> getMainColumnToPseudoColumn() {
			return mainColumnToPseudoColumn;
		}
	}
}
