package org.codefilarete.stalactite.engine.runtime;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.codefilarete.stalactite.engine.SelectExecutor;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory.QuerySQLBuilder;
import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.QueryEase;
import org.codefilarete.stalactite.query.model.operator.TupleIn;
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
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.trace.ModifiableInt;

/**
 * @author Guillaume Mary
 */
public class TablePerClassPolymorphicSelectExecutor<C, I, T extends Table<T>> implements SelectExecutor<C, I> {
	
	private static final String UNION_ALL_SEPARATOR = ") union all (";
	@VisibleForTesting
	protected static final String DISCRIMINATOR_ALIAS = "Y";
	
	private final ConfiguredRelationalPersister<C, I> mainPersister;
	private final Map<Class<? extends C>, Table> tablePerSubEntity;
	private final Map<Class<? extends C>, SimpleRelationalEntityPersister<? extends C, I ,?>> subEntitiesPersisters;
	private final ConnectionProvider connectionProvider;
	private final Dialect dialect;
	private final String discriminatorAlias;
	private final Map<Class, String> discriminatorValuePerSubType;
	private final Map<String, Class> subTypePerDiscriminatorValue;
	
	public TablePerClassPolymorphicSelectExecutor(
			ConfiguredRelationalPersister<C, I> mainPersister,
			Map<Class<? extends C>, ? extends Table> tablePerSubEntity,
			Map<Class<? extends C>, SimpleRelationalEntityPersister<? extends C, I, ?>> subEntitiesPersisters,
			ConnectionProvider connectionProvider,
			Dialect dialect
	) {
		this.mainPersister = mainPersister;
		this.tablePerSubEntity = (Map<Class<? extends C>, Table>) tablePerSubEntity;
		this.subEntitiesPersisters = subEntitiesPersisters;
		this.connectionProvider = connectionProvider;
		this.dialect = dialect;
		this.discriminatorAlias = DISCRIMINATOR_ALIAS;
		
		this.discriminatorValuePerSubType = Iterables.map(this.tablePerSubEntity.entrySet(), Entry::getKey, entry -> entry.getKey().getSimpleName());
		
		this.subTypePerDiscriminatorValue = Iterables.map(this.tablePerSubEntity.entrySet(), entry -> entry.getKey().getSimpleName(), Entry::getKey);
	}
	
	@Override
	public Set<C> select(Iterable<I> ids) {
		// Doing this in 2 phases
		// - make a select with id + discriminator in select clause and ids in where to determine ids per subclass type
		// - call the right subclass joinExecutor with dedicated ids
		// TODO : (with which listener ?)
		
		PrimaryKey<T, I> primaryKey = mainPersister.getMainTable().getPrimaryKey();
		if (primaryKey.isComposed() && !this.dialect.supportsTupleCondition()) {
			throw new UnsupportedOperationException("Database doesn't support tuple-in so selection can't be done trivially, not yet supported");
		}
		
		// Below we keep order of given entities mainly to get steady unit tests. Meanwhile, this may have performance
		// impacts but very difficult to measure
		Set<PreparedSQL> queries = new KeepOrderSet<>();
		Map<String, ResultSetReader> readers = new HashMap<>();
		readers.put(discriminatorAlias, dialect.getColumnBinderRegistry().getBinder(String.class));
		((T) mainPersister.getMainTable()).getPrimaryKey().getColumns().forEach(column -> {
			readers.put(column.getName(), dialect.getColumnBinderRegistry().getBinder(column));
		});
		ColumnedRow columnedRow = new ColumnedRow(selectable -> ((Column) selectable).getName());
		tablePerSubEntity.forEach((subEntityType, subEntityTable) -> {
			PrimaryKey<T, I> subClassPrimaryKey = (PrimaryKey<T, I>) subEntityTable.getPrimaryKey();
			String discriminatorValue = discriminatorValuePerSubType.get(subEntityType);
			// columns must be in same orders in each select clause of the union else a database type mismatch may occur, so we sort them
			SortedMap<Column<T, Object>, String> aliases = new TreeMap<>(Comparator.comparing(Column::getName));
			subClassPrimaryKey.getColumns().forEach(column -> aliases.put(column, columnedRow.getAlias(column)));
			Query.FluentFromClause from = QueryEase
					.select(aliases)
					.add("'" + discriminatorValue + "'", String.class).as(discriminatorAlias)
					.from(subEntityTable);
			
			if (!primaryKey.isComposed()) {
				// Note that casting first element as Column<T, I> is required to match method that generates right SQL,
				// else it goes in Object method which is more vague and generate wrong SQL
				from.where((Column<T, I>) Iterables.first(subClassPrimaryKey.getColumns()), Operators.in(ids));
			} else {
				List<I> idsAsList = org.codefilarete.tool.collection.Collections.asList(ids);
				Map<Column<T, Object>, Object> columnValues = subEntitiesPersisters.get(subEntityType).getMapping().getIdMapping().<T>getIdentifierAssembler().getColumnValues(idsAsList);
				from.where(transformCompositeIdentifierColumnValuesToTupleInValues(idsAsList.size(), columnValues));
			}
			
			Query query = from.getQuery();
			QuerySQLBuilder sqlQueryBuilder = dialect.getQuerySQLBuilderFactory().queryBuilder(query);
			PreparedSQL preparedSQL = sqlQueryBuilder.toPreparedSQL();
			queries.add(preparedSQL);
		});
		
		Map<Integer, PreparedStatementWriter> parameterBinders = new HashMap<>();
		Map<Integer, Object> values = new HashMap<>();
		StringAppender unionSql = new StringAppender();
		ModifiableInt parameterIndex = new ModifiableInt(1);
		ModifiableInt queriesCount = new ModifiableInt(1);
		queries.forEach(preparedSQL -> {
			unionSql.cat(preparedSQL.getSQL(), UNION_ALL_SEPARATOR);
			preparedSQL.getValues().values().forEach(value -> {
				values.put(parameterIndex.getValue(), value);
				PreparedStatementWriter<Object> parameterBinder;
				if (!primaryKey.isComposed()) {
					parameterBinder = dialect.getColumnBinderRegistry().getBinder(Iterables.first(((T) mainPersister.getMainTable()).getPrimaryKey().getColumns()));
				} else {
					parameterBinder = preparedSQL.getParameterBinder(parameterIndex.getValue() - ((queriesCount.getValue() - 1) * primaryKey.getColumns().size()));
				}
				parameterBinders.put(parameterIndex.getValue(), parameterBinder);
				parameterIndex.increment();
			});
			queriesCount.increment();
		});
		unionSql.cutTail(UNION_ALL_SEPARATOR.length());
		unionSql.wrap("(", ")");
		
		PreparedSQL preparedSQL = new PreparedSQL(unionSql.toString(), parameterBinders);
		preparedSQL.setValues(values);
		
		
		// Below we keep order of given entities mainly to get steady unit tests. Meanwhile, this may have performance
		// impacts but very difficult to measure
		Map<Class, Set<I>> idsPerSubclass = new KeepOrderMap<>();
		try (ReadOperation readOperation = new ReadOperation<>(preparedSQL, connectionProvider)) {
			ResultSet resultSet = readOperation.execute();
			RowIterator resultSetIterator = new RowIterator(resultSet, readers);
			resultSetIterator.forEachRemaining(row -> {
				
				// looking for entity type on row : we read each subclass PK and check for nullity. The non-null one is the good one
				String discriminatorValue = (String) row.get(discriminatorAlias);
				// NB: we trim because some database (as HSQLDB) adds some padding in order that all values get same length
				Class<? extends C> entitySubclass = subTypePerDiscriminatorValue.get(discriminatorValue.trim());
				
				// adding identifier to subclass' ids
				idsPerSubclass.computeIfAbsent(entitySubclass, k -> new HashSet<>())
						.add(subEntitiesPersisters.get(entitySubclass).getMapping().getIdMapping().<T>getIdentifierAssembler().assemble(row, columnedRow));
			});
		}
		
		Set<C> result = new HashSet<>();
		idsPerSubclass.forEach((subclass, subclassIds) -> result.addAll(subEntitiesPersisters.get(subclass).select(subclassIds)));
		
		return result;
	}
	
	@VisibleForTesting
	TupleIn transformCompositeIdentifierColumnValuesToTupleInValues(int idsCount, Map<? extends Column<T, Object>, Object> values) {
		List<Object[]> resultValues = new ArrayList<>(idsCount);
		
		Column<?, ?>[] columns = new ArrayList<>(values.keySet()).toArray(new Column[0]);
		for (int i = 0; i < idsCount; i++) {
			List<Object> beanValues = new ArrayList<>(columns.length);
			for (Column<?, ?> column: columns) {
				Object value = values.get(column);
				// we respect initial will as well as ExpandableStatement.doApplyValue(..) algorithm
				if (value instanceof List) {
					beanValues.add(((List) value).get(i));
				} else {
					beanValues.add(value);
				}
			}
			resultValues.add(beanValues.toArray());
		}
		
		return new TupleIn(columns, resultValues);
	}
}
