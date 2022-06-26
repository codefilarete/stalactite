package org.codefilarete.stalactite.engine.runtime;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codefilarete.stalactite.engine.JoinableSelectExecutor;
import org.codefilarete.stalactite.engine.PolymorphismPolicy.SingleTablePolymorphism;
import org.codefilarete.stalactite.engine.SelectExecutor;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityMerger.EntityMergerAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilder;
import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.QueryEase;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.stalactite.sql.result.RowIterator;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.ReadOperation;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.ResultSetReader;
import org.codefilarete.tool.collection.Iterables;

/**
 * @author Guillaume Mary
 */
public class SingleTablePolymorphismSelectExecutor<C, I, T extends Table, DTYPE>
		implements SelectExecutor<C, I>, JoinableSelectExecutor {
	
	private final Map<Class<? extends C>, EntityConfiguredJoinedTablesPersister<C, I>> subEntitiesPersisters;
	private final Column discriminatorColumn;
	private final SingleTablePolymorphism polymorphismPolicy;
	private final T table;
	private final ConnectionProvider connectionProvider;
	private final Dialect dialect;
	
	public SingleTablePolymorphismSelectExecutor(Map<Class<? extends C>, EntityConfiguredJoinedTablesPersister<? extends C, I>> subEntitiesPersisters,
													   Column<T, DTYPE> discriminatorColumn,
													   SingleTablePolymorphism polymorphismPolicy,
													   T table,
													   ConnectionProvider connectionProvider,
													   Dialect dialect) {
		this.polymorphismPolicy = polymorphismPolicy;
		this.table = table;
		this.connectionProvider = connectionProvider;
		this.dialect = dialect;
		this.discriminatorColumn = discriminatorColumn;
		this.subEntitiesPersisters = (Map) subEntitiesPersisters;
	}
	
	public Map<Class<? extends C>, EntityConfiguredJoinedTablesPersister<C, I>> getSubEntitiesPersisters() {
		return subEntitiesPersisters;
	}
	
	@Override
	public List<C> select(Iterable<I> ids) {
		// Doing this in 2 phases
		// - make a select with id + discriminator in select clause and ids in where to determine ids per subclass type
		// - call the right subclass joinExecutor with dedicated ids
		// TODO : (with which listener ?)
		
		Column<T, I> primaryKey = (Column<T, I>) Iterables.first(table.getPrimaryKey().getColumns());
		Query query = QueryEase.
				select(primaryKey, primaryKey.getAlias())
				.add(discriminatorColumn, discriminatorColumn.getAlias())
				.from(table)
				.where(primaryKey, Operators.in(ids)).getQuery();
		QuerySQLBuilder sqlQueryBuilder = new QuerySQLBuilder(query);
		ColumnBinderRegistry columnBinderRegistry = dialect.getColumnBinderRegistry();
		PreparedSQL preparedSQL = sqlQueryBuilder.toPreparedSQL(columnBinderRegistry);
		Map<Selectable<?>, String> aliases = query.getSelectSurrogate().getAliases();
		Map<Class, Set<I>> idsPerSubclass = new HashMap<>();
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
				idsPerSubclass.computeIfAbsent(entitySubclass, k -> new HashSet<>())
						.add((I) columnedRow.getValue(primaryKey, row));
			});
		}
		
		List<C> result = new ArrayList<>();
		idsPerSubclass.forEach((subclass, subclassIds) -> result.addAll(subEntitiesPersisters.get(subclass).select(subclassIds)));
		
		return result;
	}
	
	@Override
	public <U, T1 extends Table<T1>, T2 extends Table<T2>, ID> String addRelation(String leftStrategyName,
																				  EntityMapping<U, ID, T2> strategy,
																				  BeanRelationFixer beanRelationFixer,
																				  Column<T1, ID> leftJoinColumn,
																				  Column<T2, ID> rightJoinColumn,
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
																				   Column<T1, ID> leftJoinColumn,
																				   Column<T2, ID> rightJoinColumn) {
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
}
