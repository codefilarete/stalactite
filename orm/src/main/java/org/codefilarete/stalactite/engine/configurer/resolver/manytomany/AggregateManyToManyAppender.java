package org.codefilarete.stalactite.engine.configurer.resolver.manytomany;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.codefilarete.stalactite.engine.configurer.model.ResolvedManyToManyRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.GraftPoint;
import org.codefilarete.stalactite.engine.configurer.resolver.EntityReader;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.runtime.ConfiguredEntityReader;
import org.codefilarete.stalactite.engine.runtime.IndexedAssociationTable;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.Nullable;

/**
 * Handles SELECT-path join-tree wiring for a {@link ResolvedManyToManyRelation}.
 * <p>
 * Since many-to-many relations always use an intermediary association table, and since that table may or may not carry
 * an index column ({@link IndexedAssociationTable}), 4 combinations are possible, each of them being dispatched to a
 * dedicated class due to their own complexity, and to avoid long and complex methods:
 * <ul>
 * <li>{@link AggregateJoinedManyToManyAppender} and {@link AggregateJoinedIndexedManyToManyAppender} graft the relation onto the
 * aggregate's tree, hence everything is loaded by the very same query</li>
 * <li>{@link AggregateFetchSeparatelyManyToManyAppender} and {@link AggregateFetchSeparatelyIndexedManyToManyAppender} leave the
 * aggregate's tree untouched and load the relation through a dedicated second-phase query, so that the main query
 * doesn't return the cartesian product of all the aggregate relations</li>
 * </ul>
 * Note that the 2-phase load is not a lazy loading: no query is triggered in the background when accessing the
 * collection, everything is loaded eagerly so that the whole aggregate is available and coherent when returned.
 *
 * @author Guillaume Mary
 */
public class AggregateManyToManyAppender {
	
	private final AggregateJoinedManyToManyAppender joinedAppender = new AggregateJoinedManyToManyAppender();
	private final AggregateJoinedIndexedManyToManyAppender joinedIndexedAppender = new AggregateJoinedIndexedManyToManyAppender();
	private final AggregateFetchSeparatelyManyToManyAppender fetchSeparatelyAppender = new AggregateFetchSeparatelyManyToManyAppender();
	private final AggregateFetchSeparatelyIndexedManyToManyAppender fetchSeparatelyIndexedAppender = new AggregateFetchSeparatelyIndexedManyToManyAppender();
	
	/**
	 * Appends the given many-to-many relation to the aggregate persister by:
	 * <ol>
	 *   <li>Dispatching the join-tree wiring to the appender that matches the ordered / fetch-separately combination
	 *   of the relation.</li>
	 *   <li>Forwarding SELECT lifecycle events from the source persister to the target persister.</li>
	 * </ol>
	 *
	 * @return an {@link GraftPoint} for the target entity, ready to be pushed onto the assembly queue
	 * so that deeper relations are also resolved
	 */
	public <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>,
			LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
	GraftPoint append(ResolvedManyToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> relation,
	                  ConfiguredEntityReader<SRC, SRCID, LEFTTABLE> sourcePersister,
	                  ConfiguredEntityReader<TRGT, TRGTID, RIGHTTABLE> targetPersister,
	                  String mountPoint,
	                  EntityJoinTree<SRC, SRCID> aggregateTree,
	                  Dialect dialect,
	                  ConnectionProvider connectionProvider) {
		// Preparing for next iteration
		// Note that we can't set the correct generics types to the GraftPoint instance
		// because we go a step further in the relation by shifting the types from SRC to TRGT
		GraftPoint result;
		if (relation.isOrdered()) {
			if (relation.isFetchSeparately()) {
				result = fetchSeparatelyIndexedAppender.append(relation, sourcePersister, targetPersister, dialect, connectionProvider);
			} else {
				result = joinedIndexedAppender.append(relation, sourcePersister, targetPersister, relation.getAccessor(), mountPoint, aggregateTree);
			}
		} else {
			if (relation.isFetchSeparately()) {
				result = fetchSeparatelyAppender.append(relation, sourcePersister, targetPersister, dialect, connectionProvider);
			} else {
				result = joinedAppender.append(relation, sourcePersister, targetPersister, relation.getAccessor(), mountPoint, aggregateTree);
			}
		}
		
		// Forward SELECT lifecycle events from the source entity's persister down to the target persister
		SelectListener<TRGT, TRGTID> targetSelectListener = targetPersister.getSelectListener();
		sourcePersister.addSelectListener(new SelectListener<SRC, SRCID>() {
			@Override
			public void beforeSelect(Iterable<SRCID> ids) {
				targetSelectListener.beforeSelect(Collections.emptyList());
			}
			
			@Override
			public void afterSelect(Set<? extends SRC> result) {
				Set<TRGT> targets = Nullable.nullable(result)
						.map(r -> r.stream()
								.flatMap(src -> Nullable.nullable(relation.getAccessor().get(src))
										.map(Collection::stream)
										.getOr(Stream.empty()))
								.collect(Collectors.toSet()))
						.getOr(Collections.emptySet());
				targetSelectListener.afterSelect(targets);
			}
			
			@Override
			public void onSelectError(Iterable<SRCID> ids, RuntimeException exception) {
				targetSelectListener.onSelectError(Collections.emptyList(), exception);
			}
		});
		return result;
	}
}
