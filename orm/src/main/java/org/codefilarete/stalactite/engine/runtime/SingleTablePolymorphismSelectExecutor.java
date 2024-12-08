package org.codefilarete.stalactite.engine.runtime;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.codefilarete.stalactite.engine.JoinableSelectExecutor;
import org.codefilarete.stalactite.engine.PolymorphismPolicy.SingleTablePolymorphism;
import org.codefilarete.stalactite.engine.SelectExecutor;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.engine.runtime.load.EntityMerger.EntityMergerAdapter;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory.QuerySQLBuilder;
import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.QueryEase;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.query.model.operator.TupleIn;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.stalactite.sql.result.RowIterator;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.ReadOperation;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.ResultSetReader;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderMap;

/**
 * @author Guillaume Mary
 */
public class SingleTablePolymorphismSelectExecutor<C, I, T extends Table<T>, DTYPE>
		implements SelectExecutor<C, I>, JoinableSelectExecutor {
	
	private final ConfiguredRelationalPersister<C, I> mainPersister;
	private final Map<Class<C>, ConfiguredRelationalPersister<C, I>> subEntitiesPersisters;
	private final Column discriminatorColumn;
	private final SingleTablePolymorphism polymorphismPolicy;
	private final ConnectionProvider connectionProvider;
	private final Dialect dialect;
	
	public SingleTablePolymorphismSelectExecutor(ConfiguredRelationalPersister<C, I> mainPersister,
	                                             Map<? extends Class<C>, ? extends ConfiguredRelationalPersister<C, I>> subEntitiesPersisters,
												 Column<T, DTYPE> discriminatorColumn,
												 SingleTablePolymorphism polymorphismPolicy,
												 ConnectionProvider connectionProvider,
												 Dialect dialect) {
		this.mainPersister = mainPersister;
		this.polymorphismPolicy = polymorphismPolicy;
		this.connectionProvider = connectionProvider;
		this.dialect = dialect;
		this.discriminatorColumn = discriminatorColumn;
		this.subEntitiesPersisters = (Map<Class<C>, ConfiguredRelationalPersister<C, I>>) subEntitiesPersisters;
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
		
		Query.FluentFromClause from = QueryEase.
				select(Iterables.map(primaryKey.getColumns(), Function.identity(), Column::getAlias))
				.add(discriminatorColumn, discriminatorColumn.getAlias())
				.from(mainPersister.getMainTable());
		
		if (!primaryKey.isComposed()) {
			// Note that casting first element as Column<T, I> is required to match method that generates right SQL,
			// else it goes in Object method which is more vague and generate wrong SQL
			from.where((Column<T, I>) Iterables.first(primaryKey.getColumns()), Operators.in(ids));
		} else {
			List<I> idsAsList = org.codefilarete.tool.collection.Collections.asList(ids);
			Map<Column<T, Object>, Object> columnValues = mainPersister.getMapping().getIdMapping().<T>getIdentifierAssembler().getColumnValues(idsAsList);
			from.where(transformCompositeIdentifierColumnValuesToTupleInValues(idsAsList.size(), columnValues));
		}
		Query query = from.getQuery();
		
		QuerySQLBuilder sqlQueryBuilder = dialect.getQuerySQLBuilderFactory().queryBuilder(query);
		ColumnBinderRegistry columnBinderRegistry = dialect.getColumnBinderRegistry();
		PreparedSQL preparedSQL = sqlQueryBuilder.toPreparedSQL().toPreparedSQL(new HashMap<>());
		Map<Selectable<?>, String> aliases = query.getSelectSurrogate().getAliases();
		// using keep order Map for steady test, shouldn't have impact on performances
		Map<Class, Set<I>> idsPerSubclass = new KeepOrderMap<>(polymorphismPolicy.getSubClasses().size());
		try(ReadOperation readOperation = new ReadOperation<>(preparedSQL, connectionProvider)) {
			ResultSet resultSet = readOperation.execute();
			Map<String, ResultSetReader> readers = new HashMap<>();
			aliases.forEach((c, as) -> {
				// TODO: reader computation should be done (and is done) in columnBinderRegistry but it doesn't handle Selectables, only Columns,
				// and making it to do so is not simple. So for now we do this little trick.
				ResultSetReader reader;
				if (c instanceof Column) {
					reader = columnBinderRegistry.getBinder((Column) c);
				} else {
					reader = columnBinderRegistry.getBinder(c.getJavaType());
				}
				readers.put(as, reader);
			});
			
			RowIterator resultSetIterator = new RowIterator(resultSet, readers);
			ColumnedRow columnedRow = new ColumnedRow(aliases::get);
			resultSetIterator.forEachRemaining(row -> {
				DTYPE dtype = (DTYPE) columnedRow.getValue(discriminatorColumn, row);
				Class<? extends C> entitySubclass = polymorphismPolicy.getClass(dtype);
				// adding identifier to subclass' ids
				I id = subEntitiesPersisters.get(entitySubclass).getMapping().getIdMapping().getIdentifierAssembler().assemble(row, columnedRow);
				if (id != null) {
					idsPerSubclass.computeIfAbsent(entitySubclass, k -> new HashSet<>())
							.add(id);
				}
			});
		}
		
		Set<C> result = new HashSet<>();
		idsPerSubclass.forEach((subclass, subclassIds) -> result.addAll(subEntitiesPersisters.get(subclass).select(subclassIds)));
		
		return result;
	}
	
	@Override
	public <U, T1 extends Table<T1>, T2 extends Table<T2>, ID> String addRelation(String leftStrategyName,
																				  EntityMapping<U, ID, T2> strategy,
																				  BeanRelationFixer beanRelationFixer,
																				  Key<T1, ID> leftJoinColumn,
																				  Key<T2, ID> rightJoinColumn,
																				  boolean isOuterJoin) {
		Set<String> joinNames = new HashSet<>();
		subEntitiesPersisters.forEach((entityClass, persister) -> {
			String joinName = persister.getEntityJoinTree().addRelationJoin(leftStrategyName, new EntityMappingAdapter<>(strategy),
					leftJoinColumn, rightJoinColumn, null, isOuterJoin ? JoinType.OUTER : JoinType.INNER, beanRelationFixer, Collections.emptySet());
			joinNames.add(joinName);
		});
		if (joinNames.size() == 1) {
			return Iterables.first(joinNames);
		} else {
			// Grave : this class is a kind of proxy for subEntities mapping. The addRelation(..) can only be done for parent-common properties
			// of entities, hence it is not expected that join names would be different, else caller isn't able to find created join.
			// This code is made as a safeguard
			throw new IllegalStateException("Different names for same join is not expected");
		}
	}
	
	@Override
	public <U, T1 extends Table<T1>, T2 extends Table<T2>, ID> String addMergeJoin(String leftStrategyName,
																				   EntityMapping<U, ID, T2> strategy,
																				   Key<T1, ID> leftJoinColumn,
																				   Key<T2, ID> rightJoinColumn) {
		Set<String> joinNames = new HashSet<>();
		subEntitiesPersisters.forEach((entityClass, persister) -> {
			String joinName = persister.getEntityJoinTree().addMergeJoin(leftStrategyName, new EntityMergerAdapter<>(strategy),
					leftJoinColumn, rightJoinColumn);
			joinNames.add(joinName);
		});
		if (joinNames.size() == 1) {
			return Iterables.first(joinNames);
		} else {
			// Grave : this class is a kind of proxy for subEntities mapping. The addRelation(..) can only be done for parent-common properties
			// of entities, hence it is not expected that join names would be different, else caller isn't able to find created join.
			// This code is made as a safeguard
			throw new IllegalStateException("Different names for same join is not expected");
		}
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
