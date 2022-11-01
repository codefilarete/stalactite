package org.codefilarete.stalactite.engine.runtime;

import java.sql.ResultSet;
import java.util.*;

import org.codefilarete.stalactite.query.EntitySelectExecutor;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilder;
import org.codefilarete.stalactite.query.model.*;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.RowIterator;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.ReadOperation;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.PreparedStatementWriter;
import org.codefilarete.stalactite.sql.statement.binder.ResultSetReader;
import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.trace.ModifiableInt;

/**
 * @author Guillaume Mary
 */
public class TablePerClassPolymorphicEntitySelectExecutor<C, I, T extends Table> implements EntitySelectExecutor<C> {
	
	private static final String UNION_ALL_SEPARATOR = ") union all (";
	
	private final Map<Class, Table> tablePerSubConfiguration;
	private final Map<Class<? extends C>, SimpleRelationalEntityPersister<C, I, T>> persisterPerSubclass;
	private final ConnectionProvider connectionProvider;
	private final Dialect dialect;
	private final Table mainTable;
	
	public TablePerClassPolymorphicEntitySelectExecutor(
			Map<Class, Table> tablePerSubConfiguration,
			Map<Class<? extends C>, SimpleRelationalEntityPersister<C, I, T>> persisterPerSubclass,
			T mainTable,
			ConnectionProvider connectionProvider,
			Dialect dialect
	) {
		this.tablePerSubConfiguration = tablePerSubConfiguration;
		this.persisterPerSubclass = persisterPerSubclass;
		this.connectionProvider = connectionProvider;
		this.dialect = dialect;
		this.mainTable = mainTable;
	}
	
	@Override
	public List<C> loadGraph(CriteriaChain where) {
		Set<PreparedSQL> queries = new HashSet<>();
		Map<String, Class> discriminatorValues = new HashMap<>();
		String discriminatorAlias = "Y";
		String pkAlias = "PK";
		Map<String, ResultSetReader> readers = new HashMap<>();
		readers.put(discriminatorAlias, dialect.getColumnBinderRegistry().getBinder(String.class));
		ParameterBinder pkBinder = dialect.getColumnBinderRegistry().getBinder((Column) Iterables.first(mainTable.getPrimaryKey().getColumns()));
		readers.put(pkAlias, pkBinder);
		tablePerSubConfiguration.forEach((subEntityType, subEntityTable) -> {
			Column<T, I> primaryKey = (Column<T, I>) Iterables.first(subEntityTable.getPrimaryKey().getColumns());
			String discriminatorValue = subEntityType.getSimpleName();
			discriminatorValues.put(discriminatorValue, subEntityType);
			Query query = QueryEase.
					select(primaryKey, pkAlias)
					.add("'"+ discriminatorValue +"'", String.class).as(discriminatorAlias)
					.from(subEntityTable)
					.getQuery();
			
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
			
			QuerySQLBuilder sqlQueryBuilder = new QuerySQLBuilder(query, dialect);
			PreparedSQL preparedSQL = sqlQueryBuilder.toPreparedSQL(dialect.getColumnBinderRegistry());
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
		
		
		Map<Class, Set<I>> idsPerSubclass = new HashMap<>();
		try (ReadOperation readOperation = new ReadOperation<>(preparedSQL, connectionProvider)) {
			ResultSet resultSet = readOperation.execute();
			RowIterator resultSetIterator = new RowIterator(resultSet, readers);
			resultSetIterator.forEachRemaining(row -> {
				
				// looking for entity type on row : we read each subclass PK and check for nullity. The non-null one is the good one
				String discriminatorValue = (String) row.get(discriminatorAlias);
				// NB: we trim because some database (as HSQLDB) adds some padding in order that all values get same length
				Class<? extends C> entitySubclass = discriminatorValues.get(discriminatorValue.trim());
				
				// adding identifier to subclass' ids
				idsPerSubclass.computeIfAbsent(entitySubclass, k -> new HashSet<>())
						.add((I) row.get(pkAlias));
			});
		}
		
		List<C> result = new ArrayList<>();
		idsPerSubclass.forEach((subclass, subclassIds) -> result.addAll(persisterPerSubclass.get(subclass).select(subclassIds)));
		
		return result;
	}
}
