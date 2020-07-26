package org.gama.stalactite.persistence.engine.runtime;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.gama.lang.Duo;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Maps;
import org.gama.stalactite.persistence.engine.PolymorphismPolicy.SingleTablePolymorphism;
import org.gama.stalactite.persistence.query.IEntitySelectExecutor;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.builder.SQLQueryBuilder;
import org.gama.stalactite.query.model.CriteriaChain;
import org.gama.stalactite.query.model.Query;
import org.gama.stalactite.sql.ConnectionProvider;
import org.gama.stalactite.sql.dml.PreparedSQL;
import org.gama.stalactite.sql.dml.ReadOperation;
import org.gama.stalactite.sql.dml.SQLExecutionException;
import org.gama.stalactite.sql.result.RowIterator;

/**
 * @author Guillaume Mary
 */
public class SingleTablePolymorphismEntitySelectExecutor<C, I, T extends Table, D> implements IEntitySelectExecutor<C> {
	
	private static final String DISCRIMINATOR_ALIAS = "DISCRIMINATOR";
	private static final String PRIMARY_KEY_ALIAS = "PK";
	
	private final Map<Class<? extends C>, JoinedTablesPersister<C, I, T>> persisterPerSubclass;
	private final Column discriminatorColumn;
	private final SingleTablePolymorphism polymorphismPolicy;
	private final EntityMappingStrategyTreeSelectBuilder<C, I, T> mainEntityMappingStrategyTreeSelectBuilder;
	private final ConnectionProvider connectionProvider;
	private final Dialect dialect;
	
	public SingleTablePolymorphismEntitySelectExecutor(Map<Class<? extends C>, JoinedTablesPersister<C, I, T>> persisterPerSubclass,
												Column<T, D> discriminatorColumn,
												SingleTablePolymorphism polymorphismPolicy,
												EntityMappingStrategyTreeSelectBuilder<C, I, T> mainEntityMappingStrategyTreeSelectBuilder,
												ConnectionProvider connectionProvider,
												Dialect dialect) {
		this.persisterPerSubclass = persisterPerSubclass;
		this.discriminatorColumn = discriminatorColumn;
		this.polymorphismPolicy = polymorphismPolicy;
		this.mainEntityMappingStrategyTreeSelectBuilder = mainEntityMappingStrategyTreeSelectBuilder;
		this.connectionProvider = connectionProvider;
		this.dialect = dialect;
	}
	
	@Override
	public List<C> loadSelection(CriteriaChain where) {
		return null;
	}
	
	@Override
	public List<C> loadGraph(CriteriaChain where) {
		Query query = mainEntityMappingStrategyTreeSelectBuilder.buildSelectQuery();
		
		SQLQueryBuilder sqlQueryBuilder = IEntitySelectExecutor.createQueryBuilder(where, query);
		
		// selecting ids and their discriminator
		Column<T, I> pk = (Column<T, I>) Iterables.first(mainEntityMappingStrategyTreeSelectBuilder.getRoot().getTable().getPrimaryKey().getColumns());
		query.select(pk, PRIMARY_KEY_ALIAS);
		query.select(discriminatorColumn, DISCRIMINATOR_ALIAS);
		List<Duo<I, D>> ids = readIds(sqlQueryBuilder, pk);
		
		Map<Class, Set<I>> xx = new HashMap<>();
		ids.forEach(id -> xx.computeIfAbsent(polymorphismPolicy.getClass(id.getRight()), k -> new HashSet<>()).add(id.getLeft()));
		
		List<C> result = new ArrayList<>();
		
		xx.forEach((k, v) -> result.addAll(persisterPerSubclass.get(k).select(v)));
		return result;
	}
	
	private List<Duo<I, D>> readIds(SQLQueryBuilder sqlQueryBuilder, Column<T, I> pk) {
		PreparedSQL preparedSQL = sqlQueryBuilder.toPreparedSQL(dialect.getColumnBinderRegistry());
		try (ReadOperation<Integer> closeableOperation = new ReadOperation<>(preparedSQL, connectionProvider)) {
			ResultSet resultSet = closeableOperation.execute();
			RowIterator rowIterator = new RowIterator(resultSet,
					Maps.asMap(PRIMARY_KEY_ALIAS, dialect.getColumnBinderRegistry().getBinder(pk))
							.add(DISCRIMINATOR_ALIAS, dialect.getColumnBinderRegistry().getBinder(discriminatorColumn)));
			return Iterables.collectToList(() -> rowIterator, row -> new Duo<>((I) row.get(PRIMARY_KEY_ALIAS), (D) row.get(DISCRIMINATOR_ALIAS)));
		} catch (RuntimeException e) {
			throw new SQLExecutionException(preparedSQL.getSQL(), e);
		}
	}
}
