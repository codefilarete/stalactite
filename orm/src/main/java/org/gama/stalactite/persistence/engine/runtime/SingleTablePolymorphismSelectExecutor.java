package org.gama.stalactite.persistence.engine.runtime;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.gama.lang.collection.Iterables;
import org.gama.stalactite.persistence.engine.SelectExecutor;
import org.gama.stalactite.persistence.engine.JoinableSelectExecutor;
import org.gama.stalactite.persistence.engine.PolymorphismPolicy.SingleTablePolymorphism;
import org.gama.stalactite.persistence.engine.runtime.load.EntityJoinTree.EntityInflater.EntityMappingStrategyAdapter;
import org.gama.stalactite.persistence.engine.runtime.load.EntityJoinTree.EntityMerger.EntityMergerAdapter;
import org.gama.stalactite.persistence.engine.runtime.load.EntityJoinTree.JoinType;
import org.gama.stalactite.persistence.mapping.ColumnedRow;
import org.gama.stalactite.persistence.mapping.EntityMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.builder.SQLQueryBuilder;
import org.gama.stalactite.query.model.Operators;
import org.gama.stalactite.query.model.Query;
import org.gama.stalactite.query.model.QueryEase;
import org.gama.stalactite.sql.ConnectionProvider;
import org.gama.stalactite.sql.binder.ResultSetReader;
import org.gama.stalactite.sql.dml.PreparedSQL;
import org.gama.stalactite.sql.dml.ReadOperation;
import org.gama.stalactite.sql.result.BeanRelationFixer;
import org.gama.stalactite.sql.result.RowIterator;

/**
 * @author Guillaume Mary
 */
public class SingleTablePolymorphismSelectExecutor<C, I, T extends Table, D>
		implements SelectExecutor<C, I>, JoinableSelectExecutor {
	
	private final Map<Class<? extends C>, EntityConfiguredJoinedTablesPersister<C, I>> subEntitiesPersisters;
	private final Column discriminatorColumn;
	private final SingleTablePolymorphism polymorphismPolicy;
	private final T table;
	private final ConnectionProvider connectionProvider;
	private final Dialect dialect;
	
	public SingleTablePolymorphismSelectExecutor(Map<Class<? extends C>, EntityConfiguredJoinedTablesPersister<C, I>> subEntitiesPersisters,
													   Column<T, D> discriminatorColumn,
													   SingleTablePolymorphism polymorphismPolicy,
													   T table,
													   ConnectionProvider connectionProvider,
													   Dialect dialect) {
		this.polymorphismPolicy = polymorphismPolicy;
		this.table = table;
		this.connectionProvider = connectionProvider;
		this.dialect = dialect;
		this.discriminatorColumn = discriminatorColumn;
		this.subEntitiesPersisters = subEntitiesPersisters;
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
		SQLQueryBuilder sqlQueryBuilder = new SQLQueryBuilder(query);
		PreparedSQL preparedSQL = sqlQueryBuilder.toPreparedSQL(dialect.getColumnBinderRegistry());
		Map<Column, String> aliases = query.getSelectSurrogate().giveColumnAliases();
		Map<Class, Set<I>> idsPerSubclass = new HashMap<>();
		try(ReadOperation readOperation = new ReadOperation<>(preparedSQL, connectionProvider)) {
			ResultSet resultSet = readOperation.execute();
			Map<String, ResultSetReader> readers = new HashMap<>();
			aliases.forEach((c, as) -> readers.put(as, dialect.getColumnBinderRegistry().getBinder(c)));
			
			RowIterator resultSetIterator = new RowIterator(resultSet, readers);
			ColumnedRow columnedRow = new ColumnedRow(aliases::get);
			resultSetIterator.forEachRemaining(row -> {
				D dtype = (D) columnedRow.getValue(discriminatorColumn, row);
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
																				  EntityMappingStrategy<U, ID, T2> strategy,
																				  BeanRelationFixer beanRelationFixer,
																				  Column<T1, ID> leftJoinColumn,
																				  Column<T2, ID> rightJoinColumn,
																				  boolean isOuterJoin) {
		Set<String> joinNames = new HashSet<>();
		subEntitiesPersisters.forEach((entityClass, persister) -> {
			String joinName = persister.getEntityJoinTree().addRelationJoin(leftStrategyName, new EntityMappingStrategyAdapter<>(strategy),
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
	public <U, T1 extends Table<T1>, T2 extends Table<T2>, ID> String addComplementaryJoin(String leftStrategyName,
																						   EntityMappingStrategy<U, ID, T2> strategy,
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
