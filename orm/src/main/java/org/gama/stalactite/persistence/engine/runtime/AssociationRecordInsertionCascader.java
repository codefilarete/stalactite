package org.codefilarete.stalactite.persistence.engine.runtime;

import java.util.Collection;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.persistence.engine.cascade.AfterInsertCollectionCascader;
import org.codefilarete.stalactite.persistence.mapping.EntityMappingStrategy;

import static org.codefilarete.tool.collection.Iterables.stream;

/**
 * @author Guillaume Mary
 */
class AssociationRecordInsertionCascader<SRC, TRGT, SRCID, TRGTID, C extends Collection<TRGT>>
		extends AfterInsertCollectionCascader<SRC, AssociationRecord> {
	
	private final Function<SRC, C> collectionGetter;
	private final EntityMappingStrategy<SRC, SRCID, ?> mappingStrategy;
	private final EntityMappingStrategy<TRGT, TRGTID, ?> targetStrategy;
	
	public AssociationRecordInsertionCascader(AssociationRecordPersister<AssociationRecord, ?> persister,
											  Function<SRC, C> collectionGetter,
											  EntityMappingStrategy<SRC, SRCID, ?> mappingStrategy,
											  EntityMappingStrategy<TRGT, TRGTID, ?> targetStrategy) {
		super(persister);
		this.collectionGetter = collectionGetter;
		this.mappingStrategy = mappingStrategy;
		this.targetStrategy = targetStrategy;
	}
	
	@Override
	protected void postTargetInsert(Iterable<? extends AssociationRecord> entities) {
		// Nothing to do. Identified#isPersisted flag should be fixed by target persister
	}
	
	@Override
	protected Collection<AssociationRecord> getTargets(SRC src) {
		Collection<TRGT> targets = collectionGetter.apply(src);
		// We only insert non-persisted instances (for logic and to prevent duplicate primary key error)
		return stream(targets)
				.map(target -> new AssociationRecord(mappingStrategy.getId(src), targetStrategy.getId(target)))
				.collect(Collectors.toList());
	}
}
