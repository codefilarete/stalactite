package org.codefilarete.stalactite.engine.configurer.resolver.elementcollection;

import java.util.Collection;
import java.util.Collections;

import org.codefilarete.stalactite.engine.configurer.elementcollection.ElementRecord;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedElementCollectionRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.elementcollection.ElementCollectionResolver.ElementRecordPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.OUTER;

public class AggregateElementCollectionAppender {
	
	public <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>, SRCTABLE extends Table<SRCTABLE>, COLLECTIONTABLE extends Table<COLLECTIONTABLE>>
	void append(ResolvedElementCollectionRelation<SRC, TRGT, S, SRCID, SRCTABLE, COLLECTIONTABLE, ElementRecord<TRGT, SRCID>> resolvedRelation,
	            EntityJoinTree<SRC, SRCID> aggregateTree,
				ElementRecordPersister<TRGT, SRCID, COLLECTIONTABLE, ElementRecord<TRGT, SRCID>> collectionPersister,
	            String mountPoint) {
		
		aggregateTree.addRelationJoin(
				mountPoint,
				new EntityMappingAdapter<>(collectionPersister.getMapping()),
				resolvedRelation.getAccessor(),
				resolvedRelation.getJoin().getLeftKey(),
				resolvedRelation.getJoin().getRightKey(),
				null,
				OUTER,
				resolvedRelation.getRelationFixer(),
				Collections.emptySet(),
				null);
	}
}
