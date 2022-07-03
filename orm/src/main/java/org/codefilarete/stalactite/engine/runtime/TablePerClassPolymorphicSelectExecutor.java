package org.codefilarete.stalactite.engine.runtime;

import java.sql.ResultSet;
import java.util.*;

import org.codefilarete.stalactite.engine.SelectExecutor;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilder;
import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.QueryEase;
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
public class TablePerClassPolymorphicSelectExecutor<C, I, T extends Table> implements SelectExecutor<C, I> {
	
	private final Map<Class, Table> tablePerSubEntity;
	private final Map<Class<? extends C>, SelectExecutor<C, I>> subEntitiesSelectors;
	private final ConnectionProvider connectionProvider;
	private final Dialect dialect;
	private final Table mainTable;
	
	public TablePerClassPolymorphicSelectExecutor(
			Map<Class, Table> tablePerSubEntity,
			Map<Class<? extends C>, SelectExecutor<C, I>> subEntitiesSelectors,
			T mainTable,
			ConnectionProvider connectionProvider,
			Dialect dialect
	) {
		this.tablePerSubEntity = tablePerSubEntity;
		this.subEntitiesSelectors = subEntitiesSelectors;
		this.connectionProvider = connectionProvider;
		this.dialect = dialect;
		this.mainTable = mainTable;
	}
	
	@Override
	public List<C> select(Iterable<I> ids) {
		// Doing this in 2 phases
		// - make a select with id + discriminator in select clause and ids in where to determine ids per subclass type
		// - call the right subclass joinExecutor with dedicated ids
		// TODO : (with which listener ?)
		
		Set<PreparedSQL> queries = new HashSet<>();
		Map<String, Class> discriminatorValues = new HashMap<>();
		String discriminatorAlias = "Y";
		String pkAlias = "PK";
		Map<String, ResultSetReader> readers = new HashMap<>();
		readers.put(discriminatorAlias, dialect.getColumnBinderRegistry().getBinder(String.class));
		ParameterBinder pkBinder = dialect.getColumnBinderRegistry().getBinder(
				(Column) Iterables.first(mainTable.getPrimaryKey().getColumns()));
		readers.put(pkAlias, pkBinder);
		tablePerSubEntity.forEach((subEntityType, subEntityTable) -> {
			Column<T, I> primaryKey = (Column<T, I>) Iterables.first(subEntityTable.getPrimaryKey().getColumns());
			String discriminatorValue = subEntityType.getSimpleName();
			discriminatorValues.put(discriminatorValue, subEntityType);
			Query.FluentSelectClause select = QueryEase.
					select(primaryKey, pkAlias);
			select.add("'" + discriminatorValue + "'", String.class);
			Query.FluentSelectClauseAliasableExpression add = select
					.add("'" + discriminatorValue + "'", String.class);
			Query query = add.as(discriminatorAlias)
					.from(subEntityTable)
					.where(primaryKey, Operators.in(ids)).getQuery();
			QuerySQLBuilder sqlQueryBuilder = new QuerySQLBuilder(query, dialect);
			PreparedSQL preparedSQL = sqlQueryBuilder.toPreparedSQL(dialect.getColumnBinderRegistry());
			queries.add(preparedSQL);
		});
		
		Map<Integer, PreparedStatementWriter> parameterBinders = new HashMap<>();
		Map<Integer, Object> values = new HashMap<>();
		StringAppender unionSql = new StringAppender();
		ModifiableInt parameterIndex = new ModifiableInt(1);
		queries.forEach(preparedSQL -> {
			unionSql.cat(preparedSQL.getSQL(), ") union all (");
			preparedSQL.getValues().values().forEach(value -> {
				// since ids are all
				values.put(parameterIndex.getValue(), value);
				// NB: parameter binder is expected to be always the same since we always put ids
				parameterBinders.put(parameterIndex.getValue(), pkBinder);
				parameterIndex.increment();
			});
		});
		unionSql.cutTail(") union all (".length());
		unionSql.wrap("(", ")");
		
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
		idsPerSubclass.forEach((subclass, subclassIds) -> result.addAll(subEntitiesSelectors.get(subclass).select(subclassIds)));
		
		return result;
	}
}
