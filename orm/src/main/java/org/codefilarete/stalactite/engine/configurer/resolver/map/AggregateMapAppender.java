package org.codefilarete.stalactite.engine.configurer.resolver.map;

import java.util.Map;

import org.codefilarete.stalactite.engine.configurer.model.ResolvedMapRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.GraftPoint;
import org.codefilarete.stalactite.engine.configurer.resolver.EntityReader;
import org.codefilarete.stalactite.engine.configurer.resolver.map.EntryMapResolver.KeyValueRecordPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.Duo;

/**
 * Wires a {@link Map} relation to an aggregate's {@link EntityJoinTree}: depending on the need to fetch the relation
 * separately or not, the logic is dispatched to either {@link FetchSeparatelyMapAppender} or {@link JoinedMapAppender}.
 *
 * @author Guillaume Mary
 * @see FetchSeparatelyMapAppender
 */
public class AggregateMapAppender {
	
	private FetchSeparatelyMapAppender fetchSeparatelyMapAppender = new FetchSeparatelyMapAppender();
	private JoinedMapAppender joinedMapAppender = new JoinedMapAppender();
	
	public <X, Y, SRC, SRCID, K, KID, V, VID, M extends Map<K, V>, LEFTTABLE extends Table<LEFTTABLE>, MAPTABLE extends Table<MAPTABLE>, KTABLE extends Table<KTABLE>, VTABLE extends Table<VTABLE>>
	Duo<GraftPoint /* key assembly point */, GraftPoint /* value assembly point */> append(ResolvedMapRelation<SRC, SRCID, K, KID, V, VID, M, LEFTTABLE, MAPTABLE, KTABLE, VTABLE> relation,
	                                                                                       EntityJoinTree<SRC, SRCID> aggregateTree,
	                                                                                       EntityReader<SRC, SRCID, LEFTTABLE> sourcePersister,
	                                                                                       String mountPoint,
	                                                                                       KeyValueRecordPersister<X, Y, SRCID, MAPTABLE> keyValueRecordPersister,
	                                                                                       EntityReader<K, KID, KTABLE> keyEntityReader,
	                                                                                       EntityReader<V, VID, VTABLE> valueEntityReader,
	                                                                                       Dialect dialect,
	                                                                                       ConnectionProvider connectionProvider) {
		
		if (relation.isFetchSeparately()) {
			return fetchSeparatelyMapAppender.append(relation, aggregateTree, sourcePersister, mountPoint, keyValueRecordPersister, keyEntityReader, valueEntityReader, dialect, connectionProvider);
		} else {
			return joinedMapAppender.append(relation, aggregateTree, sourcePersister, mountPoint, keyValueRecordPersister, keyEntityReader, valueEntityReader);
		}
	}
}

