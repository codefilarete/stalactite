package org.codefilarete.stalactite.engine.runtime;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.query.EntitySelectExecutor;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilder;
import org.codefilarete.stalactite.query.model.CriteriaChain;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.RowIterator;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.ReadOperation;
import org.codefilarete.stalactite.sql.statement.SQLExecutionException;
import org.codefilarete.stalactite.sql.statement.binder.ResultSetReader;
import org.codefilarete.tool.collection.Iterables;

/**
 * @author Guillaume Mary
 */
public class JoinTablePolymorphismEntitySelectExecutor<C, I, T extends Table> implements EntitySelectExecutor<C> {
	
	private final Map<Class<? extends C>, EntityConfiguredJoinedTablesPersister<? extends C, I>> persisterPerSubclass;
	private final T mainTable;
	private final EntityJoinTree<C, I> entityJoinTree;
	private final ConnectionProvider connectionProvider;
	private final Dialect dialect;
	
	public JoinTablePolymorphismEntitySelectExecutor(Map<Class<? extends C>, EntityConfiguredJoinedTablesPersister<? extends C, I>> persisterPerSubclass,
													 T mainTable,
													 EntityJoinTree<C, I> entityJoinTree,
													 ConnectionProvider connectionProvider,
													 Dialect dialect) {
		this.persisterPerSubclass = persisterPerSubclass;
		this.mainTable = mainTable;
		this.entityJoinTree = entityJoinTree;
		this.connectionProvider = connectionProvider;
		this.dialect = dialect;
	}
	
	@Override
	public List<C> loadGraph(CriteriaChain where) {
		Query query = new EntityTreeQueryBuilder<>(entityJoinTree, dialect.getColumnBinderRegistry()).buildSelectQuery().getQuery();
		
		Column<T, I> primaryKey = (Column<T, I>) Iterables.first(mainTable.getPrimaryKey().getColumns());
		persisterPerSubclass.values().forEach(subclassPersister -> {
			Column subclassPrimaryKey = Iterables.first(
					(Set<Column>) subclassPersister.getMainTable().getPrimaryKey().getColumns());
			query.select(subclassPrimaryKey, subclassPrimaryKey.getAlias());
			query.getFrom().leftOuterJoin(primaryKey, subclassPrimaryKey);
		});
		
		QuerySQLBuilder sqlQueryBuilder = EntitySelectExecutor.createQueryBuilder(where, query);
		
		// selecting ids and their entity type
		Map<String, ResultSetReader> aliases = new HashMap<>();
		Iterables.stream(query.getSelectSurrogate())
				.map(Column.class::cast)
				.forEach(c -> aliases.put(c.getAlias(), dialect.getColumnBinderRegistry().getBinder(c)));
		Map<Class, Set<I>> idsPerSubtype = readIds(sqlQueryBuilder, aliases, primaryKey);
		
		List<C> result = new ArrayList<>();
		idsPerSubtype.forEach((k, v) -> result.addAll(persisterPerSubclass.get(k).select(v)));
		return result;
	}
	
	private Map<Class, Set<I>> readIds(QuerySQLBuilder sqlQueryBuilder, Map<String, ResultSetReader> aliases,
									   Column<T, I> primaryKey) {
		Map<Class, Set<I>> result = new HashMap<>();
		PreparedSQL preparedSQL = sqlQueryBuilder.toPreparedSQL(dialect.getColumnBinderRegistry());
		try (ReadOperation readOperation = new ReadOperation<>(preparedSQL, connectionProvider)) {
			ResultSet resultSet = readOperation.execute();
			
			RowIterator resultSetIterator = new RowIterator(resultSet, aliases);
			ColumnedRow columnedRow = new ColumnedRow();
			resultSetIterator.forEachRemaining(row -> {
				
				// looking for entity type on row : we read each subclass PK and check for nullity. The non-null one is the 
				// right one
				Class<? extends C> entitySubclass;
				Set<Entry<Class<? extends C>, EntityConfiguredJoinedTablesPersister<? extends C, I>>> entries = persisterPerSubclass.entrySet();
				Entry<Class<? extends C>, EntityConfiguredJoinedTablesPersister<? extends C, I>> subclassEntityOnRow = Iterables.find(entries,
						e -> {
							boolean isPKEmpty = true;
							Iterator<Column> columnIt = e.getValue().getMainTable().getPrimaryKey().getColumns().iterator();
							while (isPKEmpty && columnIt.hasNext()) {
								Column column = columnIt.next();
								isPKEmpty = columnedRow.getValue(column, row) != null;
							}
							return isPKEmpty;
						});
				entitySubclass = subclassEntityOnRow.getKey();
				
				// adding identifier to subclass' ids
				result.computeIfAbsent(entitySubclass, k -> new HashSet<>())
						.add((I) columnedRow.getValue(primaryKey, row));
			});
			return result;
		} catch (RuntimeException e) {
			throw new SQLExecutionException(preparedSQL.getSQL(), e);
		}
		
	}
}
