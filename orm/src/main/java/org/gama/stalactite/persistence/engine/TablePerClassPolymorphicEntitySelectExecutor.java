package org.gama.stalactite.persistence.engine;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.gama.lang.StringAppender;
import org.gama.lang.collection.Iterables;
import org.gama.lang.trace.ModifiableInt;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.query.IEntitySelectExecutor;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.builder.SQLQueryBuilder;
import org.gama.stalactite.query.model.AbstractCriterion;
import org.gama.stalactite.query.model.ColumnCriterion;
import org.gama.stalactite.query.model.CriteriaChain;
import org.gama.stalactite.query.model.Query;
import org.gama.stalactite.query.model.QueryEase;
import org.gama.stalactite.query.model.Where;
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
public class TablePerClassPolymorphicEntitySelectExecutor<C, I, T extends Table> implements IEntitySelectExecutor<C> {
	
	private final Map<SubEntityMappingConfiguration, Table> tablePerSubConfiguration;
	private final Map<Class<? extends C>, JoinedTablesPersister<C, I, T>> persisterPerSubclass;
	private final ConnectionProvider connectionProvider;
	private final ColumnBinderRegistry columnBinderRegistry;
	private final Table mainTable;
	
	public TablePerClassPolymorphicEntitySelectExecutor(
			Map<SubEntityMappingConfiguration, Table> tablePerSubConfiguration,
			Map<Class<? extends C>, JoinedTablesPersister<C, I, T>> persisterPerSubclass,
			T mainTable,
			ConnectionProvider connectionProvider,
			ColumnBinderRegistry columnBinderRegistry
	) {
		this.tablePerSubConfiguration = tablePerSubConfiguration;
		this.persisterPerSubclass = persisterPerSubclass;
		this.connectionProvider = connectionProvider;
		this.columnBinderRegistry = columnBinderRegistry;
		this.mainTable = mainTable;
	}
	
	@Override
	public List<C> loadSelection(CriteriaChain where) {
		return null;
	}
	
	@Override
	public List<C> loadGraph(CriteriaChain where) {
		Set<PreparedSQL> queries = new HashSet<>();
		Map<String, Class> discriminatorValues = new HashMap<>();
		String discriminatorAlias = "Y";
		String pkAlias = "PK";
		Map<String, ResultSetReader> aliases = new HashMap<>();
		aliases.put(discriminatorAlias, columnBinderRegistry.getBinder(String.class));
		ParameterBinder pkBinder = columnBinderRegistry.getBinder((Column) Iterables.first(mainTable.getPrimaryKey().getColumns()));
		aliases.put(pkAlias, pkBinder);
		tablePerSubConfiguration.forEach((subEntityConfiguration, subEntityTable) -> {
			Column<T, I> primaryKey = (Column<T, I>) Iterables.first(subEntityTable.getPrimaryKey().getColumns());
			String discriminatorValue = subEntityConfiguration.getEntityType().getSimpleName();
			discriminatorValues.put(discriminatorValue, subEntityConfiguration.getEntityType());
			Query query = QueryEase.
					select(primaryKey, pkAlias)
					.add("'"+ discriminatorValue +"' as " + discriminatorAlias)
					.from(subEntityTable)
					.getSelectQuery();
			
			Where projectedWhere = new Where();
			for(AbstractCriterion c : ((CriteriaChain<?>) where)) {
				// TODO: take other types into acount
				if (c instanceof ColumnCriterion) {
					ColumnCriterion columnCriterion = (ColumnCriterion) c;
					Column projectedColumn = subEntityTable.getColumn(columnCriterion.getColumn().getName());
					projectedWhere.add(columnCriterion.copyFor(projectedColumn));
				}
			}
			if (projectedWhere.iterator().hasNext()) {    // prevents from empty where causing malformed SQL
				query.getWhere().and(projectedWhere);
			}
			
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
				parameterBinders.put(parameterIndex.getValue(),
						preparedSQL.getParameterBinder(1 + parameterIndex.getValue() % preparedSQL.getValues().size()));
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
			RowIterator resultSetIterator = new RowIterator(resultSet, aliases);
			resultSetIterator.forEachRemaining(row -> {
				
				// looking for entity type on row : we read each subclass PK and check for nullity. The non-null one is the 
				// right one
				String discriminatorValue = (String) row.get(discriminatorAlias);
				// NB: we trim bacause some database (as HSQLDB) adds some padding in order that all values get same length
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
