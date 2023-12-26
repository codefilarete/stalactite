package org.codefilarete.stalactite.engine.runtime;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.query.EntitySelectExecutor;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory.QuerySQLBuilder;
import org.codefilarete.stalactite.query.model.AbstractCriterion;
import org.codefilarete.stalactite.query.model.ColumnCriterion;
import org.codefilarete.stalactite.query.model.CriteriaChain;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.QueryEase;
import org.codefilarete.stalactite.query.model.Where;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.RowIterator;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.ReadOperation;
import org.codefilarete.stalactite.sql.statement.binder.PreparedStatementWriter;
import org.codefilarete.stalactite.sql.statement.binder.ResultSetReader;
import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.trace.ModifiableInt;

/**
 * @author Guillaume Mary
 */
public class TablePerClassPolymorphismEntitySelectExecutor<C, I, T extends Table<T>> implements EntitySelectExecutor<C> {
	
	private static final String UNION_ALL_SEPARATOR = ") union all (";
	private static final String DISCRIMINATOR_ALIAS = "DISCRIMINATOR";
	
	private final IdentifierAssembler<I, T> identifierAssembler;
	private final Map<Class<? extends C>, Table> tablePerSubConfiguration;
	private final Map<Class<? extends C>, SimpleRelationalEntityPersister<? extends C, I, ?>> persisterPerSubclass;
	private final ConnectionProvider connectionProvider;
	private final Dialect dialect;
	private final T mainTable;
	
	public TablePerClassPolymorphismEntitySelectExecutor(
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
	}
	
	@Override
	public Set<C> loadGraph(CriteriaChain where) {
		Set<PreparedSQL> queries = new HashSet<>();
		Map<String, Class> discriminatorValues = new HashMap<>();
		
		
		// selecting ids and their discriminator
		PrimaryKey<T, I> pk = mainTable.getPrimaryKey();
		Map<String, String> aliasPerPKColumnName = Iterables.map(pk.getColumns(), Column::getName, Column::getAlias);
		
		// building readers and aliases for union-all query
		Map<String, ResultSetReader> readers = new HashMap<>();
		Map<Column<T, Object>, String> aliases = new IdentityHashMap<>();
		readers.put(DISCRIMINATOR_ALIAS, dialect.getColumnBinderRegistry().getBinder(String.class));
		pk.getColumns().forEach(pkColumn -> {
			readers.put(aliasPerPKColumnName.get(pkColumn.getName()), dialect.getColumnBinderRegistry().getBinder(pkColumn));
			aliases.put(pkColumn, pkColumn.getAlias());
		});
		
		tablePerSubConfiguration.forEach((subEntityType, subEntityTable) -> {
			discriminatorValues.put(subEntityType.getSimpleName(), subEntityType);
			Query query = buildSubConfigurationQuery("'" + subEntityType.getSimpleName() + "'", subEntityTable, aliasPerPKColumnName);
			
			Where projectedWhere = new Where();
			for(AbstractCriterion c : ((CriteriaChain<?>) where)) {
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
			
			QuerySQLBuilder sqlQueryBuilder = dialect.getQuerySQLBuilderFactory().queryBuilder(query);
			PreparedSQL preparedSQL = sqlQueryBuilder.toPreparedSQL();
			queries.add(preparedSQL);
		});
		
		Map<Integer, PreparedStatementWriter> parameterBinders = new HashMap<>();
		Map<Integer, Object> values = new HashMap<>();
		StringAppender unionSql = new StringAppender();
		ModifiableInt parameterIndex = new ModifiableInt(1);
		queries.forEach(preparedSQL -> {
			unionSql.cat(preparedSQL.getSQL(), UNION_ALL_SEPARATOR);
			preparedSQL.getValues().values().forEach(value -> {
				// since ids are all
				values.put(parameterIndex.getValue(), value);
				// NB: parameter binder is expected to be always the same since we always put ids
				parameterBinders.put(parameterIndex.getValue(),
						preparedSQL.getParameterBinder(1 + parameterIndex.getValue() % preparedSQL.getValues().size()));
				parameterIndex.increment();
			});
		});
		unionSql.cutTail(UNION_ALL_SEPARATOR.length())
				.wrap("(", ")");
		
		PreparedSQL preparedSQL = new PreparedSQL(unionSql.toString(), parameterBinders);
		preparedSQL.setValues(values);
		
		ColumnedRow rowAliaser = new ColumnedRow(aliases::get);
		Map<Class, Set<I>> idsPerSubclass = new HashMap<>();
		try (ReadOperation readOperation = new ReadOperation<>(preparedSQL, connectionProvider)) {
			ResultSet resultSet = readOperation.execute();
			RowIterator resultSetIterator = new RowIterator(resultSet, readers);
			resultSetIterator.forEachRemaining(row -> {
				
				// looking for entity type on row : we read each subclass PK and check for nullity. The non-null one is the good one
				String discriminatorValue = (String) row.get(DISCRIMINATOR_ALIAS);
				// NB: we trim because some database (as HSQLDB) adds some padding in order that all values get same length
				Class<? extends C> entitySubclass = discriminatorValues.get(discriminatorValue.trim());
				
				// adding identifier to subclass' ids
				idsPerSubclass.computeIfAbsent(entitySubclass, k -> new HashSet<>())
						.add(identifierAssembler.assemble(row, rowAliaser));
			});
		}
		
		Set<C> result = new HashSet<>();
		idsPerSubclass.forEach((subclass, subclassIds) -> result.addAll(persisterPerSubclass.get(subclass).select(subclassIds)));
		
		return result;
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
