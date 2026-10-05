package org.codefilarete.stalactite.engine.configurer.resolver.map;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.codefilarete.stalactite.engine.SelectExecutor;
import org.codefilarete.stalactite.engine.configurer.map.KeyValueRecord;
import org.codefilarete.stalactite.engine.configurer.map.RecordId;
import org.codefilarete.stalactite.engine.configurer.resolver.map.EntryMapResolver.KeyValueRecordPersister;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.SeparateFetchQueryExecutor;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater.IdentityLinkedMap;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder.EntityTreeQuery;
import org.codefilarete.stalactite.mapping.DefaultEntityMapping;
import org.codefilarete.stalactite.mapping.IdMapping;
import org.codefilarete.stalactite.query.api.QualifiedSelectable;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.function.ThreadSafeLazyInitializer;

/**
 * Particular {@link SelectExecutor} that loads {@link KeyValueRecord} from a map table by the source identifier.
 * Made for the separate loading of a {@link Map} entry values (as {@link KeyValueRecord}) by the source identifier : we
 * could have done it through the {@link KeyValueRecordPersister}, but its select is made through {@link RecordId}
 * which we don't have on separate loading since we only have the left entity identifiers available, whereas
 * {@link RecordId} is made of the left entity identifier and the key value.
 *
 * @param <SRC>
 * @param <SRCID>
 * @param <K>
 * @param <V>
 * @param <LEFTTABLE>
 * @param <MAPTABLE>
 * @author Guillaume Mary
 */
public class MapEntryLoader<SRC, SRCID, K, V, LEFTTABLE extends Table<LEFTTABLE>, MAPTABLE extends Table<MAPTABLE>> implements SelectExecutor<KeyValueRecord<K, V, SRCID>, SRCID> {
	
	private final EntityJoinTree<KeyValueRecord<K, V, SRCID>, RecordId<K, SRCID>> entityJoinTree;
	private final IdMapping<SRC, SRCID> sourceIdMapping;
	private final Map<QualifiedSelectable<LEFTTABLE, ?>, QualifiedSelectable<MAPTABLE, ?>> reverseForeignKey;
	private final Dialect dialect;
	private final ConnectionProvider connectionProvider;
	// we use a lazy initializer to avoid creating the query several times :
	// - for potential performance issues
	// - to avoid unnecessary extra CPU work
	// - OVERALL to avoid a bug due to multiple instantiations of JoinRowConsumer instances which make the EntityTreeInflater
	// internal tree being out-of-sync with some RelationJoinNode. See TablePerClassAppender.addTablePerClassPolymorphicSubPersistersJoins
	// for some usage that may lead to this bug
	private final ThreadSafeLazyInitializer<SeparateFetchQueryExecutor<KeyValueRecord<K, V, SRCID>, SRCID, LEFTTABLE, MAPTABLE>> queryExecutorInitializer
			= new ThreadSafeLazyInitializer<SeparateFetchQueryExecutor<KeyValueRecord<K, V, SRCID>, SRCID, LEFTTABLE, MAPTABLE>>() {
		@Override
		protected SeparateFetchQueryExecutor<KeyValueRecord<K, V, SRCID>, SRCID, LEFTTABLE, MAPTABLE> createInstance() {
			EntityTreeQuery<KeyValueRecord<K, V, SRCID>> entityTreeQuery = new EntityTreeQueryBuilder<>(entityJoinTree, dialect.getColumnBinderRegistry()).buildSelectQuery();
			return new SeparateFetchQueryExecutor<>(entityTreeQuery, sourceIdMapping, reverseForeignKey, dialect, connectionProvider);
		}
	};
	
	public MapEntryLoader(IdMapping<SRC, SRCID> sourceIdMapping,
	                      KeyValueRecordPersister<K, V, SRCID, MAPTABLE> keyValueRecordPersister,
	                      Map<QualifiedSelectable<LEFTTABLE, ?>, QualifiedSelectable<MAPTABLE, ?>> reverseForeignKey,
	                      Dialect dialect,
	                      ConnectionProvider connectionProvider) {
		this.sourceIdMapping = sourceIdMapping;
		this.reverseForeignKey = reverseForeignKey;
		this.dialect = dialect;
		this.connectionProvider = connectionProvider;
		DefaultEntityMapping<KeyValueRecord<K, V, SRCID>, RecordId<K, SRCID>, MAPTABLE> mapping = new DefaultEntityMapping<>(
				(Class) KeyValueRecord.class,
				keyValueRecordPersister.getMainTable(),
				keyValueRecordPersister.getMapping().getPropertyToColumn(),
				keyValueRecordPersister.getMapping().getIdMapping());
		this.entityJoinTree = new EntityJoinTree<>(mapping);
	}
	
	public EntityJoinTree<KeyValueRecord<K, V, SRCID>, RecordId<K, SRCID>> getEntityJoinTree() {
		return entityJoinTree;
	}
	
	@Override
	public Set<KeyValueRecord<K, V, SRCID>> select(Iterable<SRCID> ids) {
		int estimatedResultSize = Iterables.size(ids);
		// we avoid relying on Entity equals/Hashcode by using a Map based on System.identityHashCode(..)
		Set<KeyValueRecord<K, V, SRCID>> result = Collections.newSetFromMap(new IdentityLinkedMap<>(estimatedResultSize));
		
		Iterables.forEachChunk(
				ids,
				dialect.getInOperatorMaxSize(),
				chunks -> {},
				chunkSize -> null,    // no particular initialization to do
				(context, chunk) -> {
					result.addAll(queryExecutorInitializer.get().select(chunk));
				},
				context -> {}
		);
		return result;
	}
}
