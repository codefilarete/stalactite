package org.gama.stalactite.persistence.engine.runtime;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.gama.lang.StringAppender;
import org.gama.lang.collection.Iterables;
import org.gama.lang.function.Functions;
import org.gama.lang.trace.ModifiableInt;
import org.gama.stalactite.persistence.engine.SelectExecutor;
import org.gama.stalactite.persistence.engine.SubEntityMappingConfiguration;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.builder.SQLQueryBuilder;
import org.gama.stalactite.query.model.Operators;
import org.gama.stalactite.query.model.Query;
import org.gama.stalactite.query.model.QueryEase;
import org.gama.stalactite.sql.ConnectionProvider;
import org.gama.stalactite.sql.binder.ParameterBinder;
import org.gama.stalactite.sql.binder.PreparedStatementWriter;
import org.gama.stalactite.sql.binder.ResultSetReader;
import org.gama.stalactite.sql.dml.PreparedSQL;
import org.gama.stalactite.sql.dml.ReadOperation;
import org.gama.stalactite.sql.result.RowIterator;

/**
 * @author Guillaume Mary
 */
public class TablePerClassPolymorphicSelectExecutor<C, I, T extends Table> implements SelectExecutor<C, I> {
	
	private final Map<Class, Table> tablePerSubEntity;
	private final Map<Class<? extends C>, SelectExecutor<C, I>> subEntitiesSelectors;
	private final ConnectionProvider connectionProvider;
	private final ColumnBinderRegistry columnBinderRegistry;
	private final Table mainTable;
	
	public TablePerClassPolymorphicSelectExecutor(
			Map<SubEntityMappingConfiguration, Table> tablePerSubConfiguration,
			Map<Class<? extends C>, SimpleRelationalEntityPersister<C, I, T>> persisterPerSubclass,
			T mainTable,
			ConnectionProvider connectionProvider,
			ColumnBinderRegistry columnBinderRegistry,
			boolean safeGuard
	) {
		this.tablePerSubEntity = Iterables.map(tablePerSubConfiguration.entrySet(),
				Functions.chain(Entry::getKey, SubEntityMappingConfiguration::getEntityType),
				Entry::getValue);
		this.subEntitiesSelectors = Iterables.map(persisterPerSubclass.entrySet(),
				Entry::getKey,
				Functions.chain(Entry::getValue, SimpleRelationalEntityPersister::getSelectExecutor));
		this.connectionProvider = connectionProvider;
		this.columnBinderRegistry = columnBinderRegistry;
		this.mainTable = mainTable;
	}
	
	public TablePerClassPolymorphicSelectExecutor(
			Map<Class, Table> tablePerSubEntity,
			Map<Class<? extends C>, SelectExecutor<C, I>> subEntitiesSelectors,
			T mainTable,
			ConnectionProvider connectionProvider,
			ColumnBinderRegistry columnBinderRegistry
	) {
		this.tablePerSubEntity = tablePerSubEntity;
		this.subEntitiesSelectors = subEntitiesSelectors;
		this.connectionProvider = connectionProvider;
		this.columnBinderRegistry = columnBinderRegistry;
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
		readers.put(discriminatorAlias, columnBinderRegistry.getBinder(String.class));
		ParameterBinder pkBinder = columnBinderRegistry.getBinder(
				(Column) Iterables.first(mainTable.getPrimaryKey().getColumns()));
		readers.put(pkAlias, pkBinder);
		tablePerSubEntity.forEach((subEntityType, subEntityTable) -> {
			Column<T, I> primaryKey = (Column<T, I>) Iterables.first(subEntityTable.getPrimaryKey().getColumns());
			String discriminatorValue = subEntityType.getSimpleName();
			discriminatorValues.put(discriminatorValue, subEntityType);
			Query query = QueryEase.
					select(primaryKey, pkAlias)
					.add("'"+ discriminatorValue +"' as " + discriminatorAlias)
					.from(subEntityTable)
					.where(primaryKey, Operators.in(ids)).getQuery();
			SQLQueryBuilder sqlQueryBuilder = new SQLQueryBuilder(query);
			PreparedSQL preparedSQL = sqlQueryBuilder.toPreparedSQL(columnBinderRegistry);
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
