package org.codefilarete.stalactite.engine.configurer.resolver.elementcollection;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.codefilarete.stalactite.engine.SelectExecutor;
import org.codefilarete.stalactite.engine.configurer.elementcollection.ElementRecord;
import org.codefilarete.stalactite.engine.configurer.resolver.elementcollection.ElementCollectionResolver.ElementRecordPersister;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.SeparateFetchQueryExecutor;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater.IdentityLinkedMap;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder.EntityTreeQuery;
import org.codefilarete.stalactite.mapping.IdMapping;
import org.codefilarete.stalactite.query.api.JoinLink;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.Iterables;

/**
 * Particular {@link SelectExecutor} that loads {@link ElementRecord} from a collection table by the source identifier.
 * Made for the separate loading of a {@link Collection} elements (as {@link ElementRecord}) by the source identifier : we
 * could have done it through the {@link ElementRecordPersister}, but its select is made through {@link ElementRecord}
 * which we don't have on separate loading since we only have the left entity identifiers available, whereas
 * {@link ElementRecord} is made of the left entity identifier and the element value.
 * 
 * @param <SRC>
 * @param <SRCID>
 * @param <TRGT>
 * @param <LEFTTABLE>
 * @param <COLLECTIONTABLE>
 * @author Guillaume Mary
 */
public class ElementCollectionLoader<SRC, SRCID, TRGT, LEFTTABLE extends Table<LEFTTABLE>, COLLECTIONTABLE extends Table<COLLECTIONTABLE>> implements SelectExecutor<ElementRecord<TRGT, SRCID>, SRCID> {
	
	private final EntityJoinTree<ElementRecord<TRGT, SRCID>, ElementRecord<TRGT, SRCID>> entityJoinTree;
	private final IdMapping<SRC, SRCID> sourceIdMapping;
	private final Map<JoinLink<LEFTTABLE, ?>, JoinLink<COLLECTIONTABLE, ?>> reverseForeignKey;
	private final Dialect dialect;
	private final ConnectionProvider connectionProvider;
	
	public ElementCollectionLoader(IdMapping<SRC, SRCID> sourceIdMapping,
	                               ElementRecordPersister<TRGT, SRCID, COLLECTIONTABLE, ElementRecord<TRGT, SRCID>> collectionPersister,
	                               Map<JoinLink<LEFTTABLE, ?>, JoinLink<COLLECTIONTABLE, ?>> reverseForeignKey,
	                               Dialect dialect,
	                               ConnectionProvider connectionProvider) {
		this.sourceIdMapping = sourceIdMapping;
		this.reverseForeignKey = reverseForeignKey;
		this.dialect = dialect;
		this.connectionProvider = connectionProvider;
		this.entityJoinTree = new EntityJoinTree<>(collectionPersister.getMapping());
	}
	
	@Override
	public Set<ElementRecord<TRGT, SRCID>> select(Iterable<SRCID> ids) {
		int estimatedResultSize = Iterables.size(ids);
		// we avoid relying on Entity equals/Hashcode by using a Map based on System.identityHashCode(..)
		Set<ElementRecord<TRGT, SRCID>> result = Collections.newSetFromMap(new IdentityLinkedMap<>(estimatedResultSize));
		EntityTreeQuery<ElementRecord<TRGT, SRCID>> entityTreeQuery = new EntityTreeQueryBuilder<>(this.entityJoinTree, dialect.getColumnBinderRegistry()).buildSelectQuery();
		SeparateFetchQueryExecutor<ElementRecord<TRGT, SRCID>, SRCID, LEFTTABLE, COLLECTIONTABLE> queryExecutor =
				new SeparateFetchQueryExecutor<>(entityTreeQuery, sourceIdMapping, reverseForeignKey, dialect, connectionProvider);
		Iterables.forEachChunk(
				ids,
				dialect.getInOperatorMaxSize(),
				chunks -> {},
				chunkSize -> null,    // no particular initialization to do
				(context, chunk) -> {
					result.addAll(queryExecutor.select(chunk));
				},
				context -> {}
		);
		return result;
	}
}
