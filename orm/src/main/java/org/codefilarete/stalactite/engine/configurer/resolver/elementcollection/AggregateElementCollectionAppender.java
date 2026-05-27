package org.codefilarete.stalactite.engine.configurer.resolver.elementcollection;

import java.util.Collection;
import java.util.Collections;

import org.codefilarete.stalactite.engine.configurer.elementcollection.ElementRecord;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedElementCollectionRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.AssemblyPoint;
import org.codefilarete.stalactite.engine.configurer.resolver.elementcollection.ElementCollectionResolver.ElementRecordPersister;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.OUTER;

public class AggregateElementCollectionAppender {
	
	private final ElementCollectionResolver elementCollectionResolver;
	
	public AggregateElementCollectionAppender(Dialect dialect, ConnectionConfiguration connectionConfiguration) {
		this.elementCollectionResolver = new ElementCollectionResolver(dialect, connectionConfiguration);
	}
	
	public <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>, SRCTABLE extends Table<SRCTABLE>, COLLECTIONTABLE extends Table<COLLECTIONTABLE>>
	void append(ConfiguredRelationalPersister<SRC, SRCID> rootPersister,
	                     ResolvedElementCollectionRelation<SRC, TRGT, S, SRCID, SRCTABLE, COLLECTIONTABLE, ElementRecord<TRGT, SRCID>> resolvedRelation,
	                     AssemblyPoint<SRC, SRCID, TRGT, SRCTABLE> assemblyPawn) {
		
		ElementRecordPersister<TRGT, SRCID, COLLECTIONTABLE, ElementRecord<TRGT, SRCID>> collectionPersister = elementCollectionResolver.resolve(resolvedRelation, assemblyPawn.getRelationOwnerPersister());
		
		rootPersister.getEntityJoinTree().addRelationJoin(
				assemblyPawn.getParentJoinPoint(),
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
