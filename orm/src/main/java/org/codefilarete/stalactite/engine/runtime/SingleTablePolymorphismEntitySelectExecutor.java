package org.codefilarete.stalactite.engine.runtime;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codefilarete.stalactite.engine.PolymorphismPolicy.SingleTablePolymorphism;
import org.codefilarete.stalactite.query.EntitySelectExecutor;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilder;
import org.codefilarete.stalactite.query.model.CriteriaChain;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.ReadOperation;
import org.codefilarete.stalactite.sql.statement.SQLExecutionException;
import org.codefilarete.stalactite.sql.result.RowIterator;

/**
 * @author Guillaume Mary
 */
public class SingleTablePolymorphismEntitySelectExecutor<C, I, T extends Table, DTYPE> implements EntitySelectExecutor<C> {
	
	private static final String DISCRIMINATOR_ALIAS = "DISCRIMINATOR";
	private static final String PRIMARY_KEY_ALIAS = "PK";
	
	private final Map<Class<C>, ConfiguredRelationalPersister<C, I>> persisterPerSubclass;
	private final Column discriminatorColumn;
	private final SingleTablePolymorphism polymorphismPolicy;
	private final EntityJoinTree<C, I> entityJoinTree;
	private final ConnectionProvider connectionProvider;
	private final Dialect dialect;
	
	public SingleTablePolymorphismEntitySelectExecutor(Map<? extends Class<C>, ? extends ConfiguredRelationalPersister<C, I>> persisterPerSubclass,
												Column<T, DTYPE> discriminatorColumn,
												SingleTablePolymorphism polymorphismPolicy,
												EntityJoinTree<C, I> mainEntityJoinTree,
												ConnectionProvider connectionProvider,
												Dialect dialect) {
		this.persisterPerSubclass = (Map<Class<C>, ConfiguredRelationalPersister<C, I>>) persisterPerSubclass;
		this.discriminatorColumn = discriminatorColumn;
		this.polymorphismPolicy = polymorphismPolicy;
		this.entityJoinTree = mainEntityJoinTree;
		this.connectionProvider = connectionProvider;
		this.dialect = dialect;
	}
	
	@Override
	public Set<C> loadGraph(CriteriaChain where) {
		Query query = new EntityTreeQueryBuilder<>(entityJoinTree, dialect.getColumnBinderRegistry()).buildSelectQuery().getQuery();
		
		QuerySQLBuilder sqlQueryBuilder = new QuerySQLBuilder(query, dialect, where);
		
		// selecting ids and their discriminator
		Column<T, I> pk = (Column<T, I>) Iterables.first(entityJoinTree.getRoot().getTable().getPrimaryKey().getColumns());
		query.select(pk, PRIMARY_KEY_ALIAS);
		query.select(discriminatorColumn, DISCRIMINATOR_ALIAS);
		List<Duo<I, DTYPE>> ids = readIds(sqlQueryBuilder, pk);
		
		Map<Class, Set<I>> idsPerSubclass = new HashMap<>();
		ids.forEach(id -> idsPerSubclass.computeIfAbsent(polymorphismPolicy.getClass(id.getRight()), k -> new HashSet<>()).add(id.getLeft()));
		
		Set<C> result = new HashSet<>();
		
		idsPerSubclass.forEach((k, v) -> result.addAll(persisterPerSubclass.get(k).select(v)));
		return result;
	}
	
	private List<Duo<I, DTYPE>> readIds(QuerySQLBuilder sqlQueryBuilder, Column<T, I> pk) {
		PreparedSQL preparedSQL = sqlQueryBuilder.toPreparedSQL(dialect.getColumnBinderRegistry());
		try (ReadOperation<Integer> closeableOperation = new ReadOperation<>(preparedSQL, connectionProvider)) {
			ResultSet resultSet = closeableOperation.execute();
			RowIterator rowIterator = new RowIterator(resultSet,
					Maps.asMap(PRIMARY_KEY_ALIAS, dialect.getColumnBinderRegistry().getBinder(pk))
							.add(DISCRIMINATOR_ALIAS, dialect.getColumnBinderRegistry().getBinder(discriminatorColumn)));
			return Iterables.collectToList(() -> rowIterator, row -> new Duo<>((I) row.get(PRIMARY_KEY_ALIAS), (DTYPE) row.get(DISCRIMINATOR_ALIAS)));
		} catch (RuntimeException e) {
			throw new SQLExecutionException(preparedSQL.getSQL(), e);
		}
	}
}
