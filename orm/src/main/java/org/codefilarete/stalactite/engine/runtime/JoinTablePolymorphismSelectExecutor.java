package org.codefilarete.stalactite.engine.runtime;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.codefilarete.stalactite.engine.SelectExecutor;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory.QuerySQLBuilder;
import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.QueryEase;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.query.model.operator.TupleIn;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.RowIterator;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.ReadOperation;
import org.codefilarete.stalactite.sql.statement.binder.ResultSetReader;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.Collections;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderMap;

/**
 * @author Guillaume Mary
 */
public class JoinTablePolymorphismSelectExecutor<C, I, T extends Table<T>> implements SelectExecutor<C, I> {
	
	private final ConfiguredRelationalPersister<C, I> mainPersister;
	private final Map<Class<? extends C>, Table> tablePerSubEntity;
	private final Map<Class<? extends C>, ConfiguredRelationalPersister<C, I>> subEntitiesPersisters;
	private final ConnectionProvider connectionProvider;
	private final Dialect dialect;
	
	public JoinTablePolymorphismSelectExecutor(
			ConfiguredRelationalPersister<C, I> mainPersister,
			Map<Class<? extends C>, Table> tablePerSubEntity,
			Map<Class<? extends C>, ConfiguredRelationalPersister<? extends C, I>> subEntitiesPersisters,
			ConnectionProvider connectionProvider,
			Dialect dialect
	) {
		this.mainPersister = mainPersister;
		this.tablePerSubEntity = tablePerSubEntity;
		this.subEntitiesPersisters = (Map) subEntitiesPersisters;
		this.connectionProvider = connectionProvider;
		this.dialect = dialect;
	}
	
	@Override
	public Set<C> select(Iterable<I> ids) {
		// 2 possibilities :
		// - execute a request that join all tables and all relations, then give result to transformer
		//   Pros : one request, simple approach
		//   Cons : one eventually big/complex request, has some drawback on how to create this request (impacts on parent
		//          Persister behavior) and how to build the transformer. In conclusion quite complex
		// - do it in 2+ phases : one request to determine which id matches which type, then ask each sub classes to load
		//   their own type
		//   Pros : suclasses must know common properties/trunk which will be necessary for updates too (to compute 
		//   differences)
		//   Cons : first request not so easy to write. Performance may be lower because of 1+N (one per subclass) database 
		//   requests
		// => option 2 chosen. May be reviewed later, or make this policy configurable.
		
		// Doing this in 2 phases
		// - make a select with id + discriminator in select clause and ids in where to determine ids per subclass type
		// - call the right subclass joinExecutor with dedicated ids
		
		PrimaryKey<T, I> primaryKey = mainPersister.getMainTable().getPrimaryKey();
		if (primaryKey.isComposed() && !this.dialect.supportsTupleCondition()) {
			throw new UnsupportedOperationException("Database doesn't support tuple-in so selection can't be done trivially, not yet supported");
		}
		
		Query.FluentFromClause from = QueryEase.
				select(Iterables.map(primaryKey.getColumns(), Function.identity(), Column::getAlias))
				.from(mainPersister.getMainTable());
		
		if (!primaryKey.isComposed()) {
			// Note that casting first element as Column<T, I> is required to match method that generates right SQL,
			// else it goes in Object method which is more vague and generate wrong SQL
			from.where((Column<T, I>) Iterables.first(primaryKey.getColumns()), Operators.in(ids));
		} else {
			List<I> idsAsList = Collections.asList(ids);
			Map<Column<T, ?>, Object> columnValues = mainPersister.getMapping().getIdMapping().<T>getIdentifierAssembler().getColumnValues(idsAsList);
			from.where(transformCompositeIdentifierColumnValuesToTupleInValues(idsAsList.size(), columnValues));
		}
		Query query = from.getQuery();
		tablePerSubEntity.values().forEach(subTable -> {
			PrimaryKey<?, ?> subclassPrimaryKey = subTable.getPrimaryKey();
			query.select(Iterables.map(subclassPrimaryKey.getColumns(), Function.identity(), Column::getAlias));
			query.getFrom().leftOuterJoin(primaryKey, subclassPrimaryKey);
		});
		QuerySQLBuilder sqlQueryBuilder = dialect.getQuerySQLBuilderFactory().queryBuilder(query);
		Map<Selectable<?>, String> aliases = query.getSelectSurrogate().getAliases();
		PreparedSQL preparedSQL = sqlQueryBuilder.toPreparableSQL().toPreparedSQL(new HashMap<>());
		// Below we keep order of given entities mainly to get steady unit tests. Meanwhile, this may have performance
		// impacts but very difficult to measure
		Map<Class, Set<I>> idsPerSubclass = new KeepOrderMap<>();
		try (ReadOperation readOperation = new ReadOperation<>(preparedSQL, connectionProvider)) {
			ResultSet resultSet = readOperation.execute();
			Map<String, ResultSetReader> readers = new HashMap<>();
			aliases.forEach((c, as) -> readers.put(as, dialect.getColumnBinderRegistry().getBinder((Column) c)));
			
			RowIterator resultSetIterator = new RowIterator(resultSet, readers);
			ColumnedRow columnedRow = new ColumnedRow(aliases::get);
			resultSetIterator.forEachRemaining(row -> {
				// looking for entity type on row : we read each subclass PK and check for nullity. The non-null one is the good one
				subEntitiesPersisters.values().forEach(subPersister -> {
					I id = subPersister.getMapping().getIdMapping().getIdentifierAssembler().assemble(row, columnedRow);
					if (id != null) {
						idsPerSubclass.computeIfAbsent(subPersister.getClassToPersist(), k -> new HashSet<>())
								.add(id);
					}
				});
			});
		}
		
		Set<C> result = new HashSet<>();
		idsPerSubclass.forEach((subclass, subclassIds) -> result.addAll(subEntitiesPersisters.get(subclass).select(subclassIds)));
		
		return result;
	}
	
	@VisibleForTesting
	TupleIn transformCompositeIdentifierColumnValuesToTupleInValues(int idsCount, Map<? extends Column<T, ?>, Object> values) {
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
