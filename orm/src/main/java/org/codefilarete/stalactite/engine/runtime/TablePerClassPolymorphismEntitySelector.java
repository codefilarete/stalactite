package org.codefilarete.stalactite.engine.runtime;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.query.EntitySelector;
import org.codefilarete.stalactite.query.model.AbstractCriterion;
import org.codefilarete.stalactite.query.model.ColumnCriterion;
import org.codefilarete.stalactite.query.model.CriteriaChain;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.QueryEase;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.query.model.Union;
import org.codefilarete.stalactite.query.model.Where;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.RowIterator;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.ReadOperation;
import org.codefilarete.stalactite.sql.statement.SQLExecutionException;
import org.codefilarete.stalactite.sql.statement.binder.ResultSetReader;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.collection.KeepOrderSet;

/**
 * @author Guillaume Mary
 */
public class TablePerClassPolymorphismEntitySelector<C, I, T extends Table<T>> implements EntitySelector<C, I> {
	
	@VisibleForTesting
	static final String DISCRIMINATOR_ALIAS = "DISCRIMINATOR";
	
	private final IdentifierAssembler<I, T> identifierAssembler;
	private final Map<Class<? extends C>, Table> tablePerSubConfiguration;
	private final Map<Class<? extends C>, SimpleRelationalEntityPersister<? extends C, I, ?>> persisterPerSubclass;
	private final ConnectionProvider connectionProvider;
	private final Dialect dialect;
	private final T mainTable;
	private final Map<String, Class> discriminatorValues;
	
	public TablePerClassPolymorphismEntitySelector(
			IdentifierAssembler<I, T> identifierAssembler,
			Map<Class<? extends C>, ? extends Table> tablePerSubConfiguration,
			Map<Class<? extends C>, SimpleRelationalEntityPersister<? extends C, I, ?>> persisterPerSubclass,
			T mainTable,
			ConnectionProvider connectionProvider,
			Dialect dialect
	) {
		this.identifierAssembler = identifierAssembler;
		this.tablePerSubConfiguration = (Map<Class<? extends C>, Table>) tablePerSubConfiguration;
		this.persisterPerSubclass = persisterPerSubclass;
		this.connectionProvider = connectionProvider;
		this.dialect = dialect;
		this.mainTable = mainTable;
		// building readers and aliases for union-all query
		this.discriminatorValues = new HashMap<>();
		tablePerSubConfiguration.forEach((subEntityType, subEntityTable) -> {
			discriminatorValues.put(subEntityType.getSimpleName(), subEntityType);
		});
	}
	
	@Override
	public Set<C> select(CriteriaChain where) {
		// Selecting ids and their discriminator by creating a union of select-per-table
		// Columns of the union are the primary key ones, plus the discriminator
		PrimaryKey<T, Object> pk = mainTable.getPrimaryKey();
		Map<String, String> aliasPerPKColumnName = Iterables.map(pk.getColumns(), Column::getName, Column::getAlias);
		
		// building readers and aliases for union-all query
		Map<String, ResultSetReader> readers = new HashMap<>();
		readers.put(DISCRIMINATOR_ALIAS, dialect.getColumnBinderRegistry().getBinder(String.class));
		Map<Column<T, Object>, String> aliases = new IdentityHashMap<>();
		pk.getColumns().forEach(pkColumn -> {
			readers.put(aliasPerPKColumnName.get(pkColumn.getName()), dialect.getColumnBinderRegistry().getBinder(pkColumn));
			aliases.put(pkColumn, pkColumn.getAlias());
		});
		
		// Below we keep order of given entities mainly to get steady unit tests. Meanwhile, this may have performance
		// impacts but very difficult to measure
		Set<Query> queries = new KeepOrderSet<>();
		tablePerSubConfiguration.forEach((subEntityType, subEntityTable) -> {
			Query query = buildSubConfigurationQuery("'" + subEntityType.getSimpleName() + "'", subEntityTable, aliasPerPKColumnName);
			
			Where projectedWhere = new Where();
			for (AbstractCriterion c : ((CriteriaChain<?>) where)) {
				// TODO: take other types into account
				if (c instanceof ColumnCriterion) {
					ColumnCriterion columnCriterion = (ColumnCriterion) c;
					Column projectedColumn = subEntityTable.getColumn(columnCriterion.getColumn().getName());
					projectedWhere.add(columnCriterion.copyFor(projectedColumn));
				}
			}
			if (projectedWhere.iterator().hasNext()) {    // prevents from empty where causing malformed SQL
				query.getWhere().and(projectedWhere);
			}
			queries.add(query);
		});
		
		Union union = new Union(queries);
		Query queryWrappingUnion = new Query();
		Set<Selectable<Object>> unionColumns = pk.getColumns().stream().map(pkColumn -> new Selectable.SelectableString<>(pkColumn.getAlias(), pkColumn.getJavaType())).collect(Collectors.toSet());
		queryWrappingUnion.select(unionColumns);
		queryWrappingUnion.select(DISCRIMINATOR_ALIAS, String.class);
		queryWrappingUnion.from(union.asPseudoTable(mainTable.getName() + "_union"));
		
		PreparedSQL preparedSql = dialect.getQuerySQLBuilderFactory().queryBuilder(queryWrappingUnion).toPreparedSQL();
		Map<Class, Set<I>> idsPerSubclass = readIds(preparedSql, readers, new ColumnedRow(aliases::get));
		Set<C> result = new KeepOrderSet<>();
		idsPerSubclass.forEach((subclass, subclassIds) -> result.addAll(persisterPerSubclass.get(subclass).select(subclassIds)));
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
	
	/**
	 * Build sub-query for given configuration.
	 * Resulting query will take place into main union query run afterward, hence selected column aliases must be the
	 * same for every sub-query to fit "union all" selection principle.
	 *
	 * @param <SUBTABLE> sub-entity table type
	 * @param discriminatorValue sub-entity discriminator value
	 * @param subEntityTable sub-query table source
	 * @param aliasPerPKColumnName alias per column to be used, column name is entry key because nothing else can be used
	 * @return a query that will be used in a "union all" one
	 */
	private <SUBTABLE extends Table<SUBTABLE>> Query buildSubConfigurationQuery(String discriminatorValue,
																				SUBTABLE subEntityTable,
																				Map<String, String> aliasPerPKColumnName) {
		Map<Column<SUBTABLE, Object>, String> aliasPerPKColumn = Iterables.map(subEntityTable.getPrimaryKey().getColumns(),
				Function.identity(),
				c -> aliasPerPKColumnName.get(c.getName()));
		return QueryEase.
				select(aliasPerPKColumn)
				.add(discriminatorValue, String.class).as(DISCRIMINATOR_ALIAS)
				.from(subEntityTable)
				.getQuery();
	}
}
