package org.gama.stalactite.persistence.engine.runtime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import org.gama.lang.trace.ModifiableInt;
import org.gama.stalactite.persistence.engine.AssociationRecordPersister;
import org.gama.stalactite.persistence.engine.IndexedAssociationRecord;
import org.gama.stalactite.persistence.engine.cascade.AfterInsertCollectionCascader;
import org.gama.stalactite.persistence.mapping.IEntityMappingStrategy;

/**
 * @author Guillaume Mary
 */
class IndexedAssociationRecordInsertionCascader<SRC, TRGT, SRCID, TRGTID, C extends List<TRGT>>
		extends AfterInsertCollectionCascader<SRC, IndexedAssociationRecord> {
	
	private final Function<SRC, C> collectionGetter;
	private final IEntityMappingStrategy<SRC, SRCID, ?> mappingStrategy;
	private final IEntityMappingStrategy<TRGT, TRGTID, ?> targetStrategy;
	
	IndexedAssociationRecordInsertionCascader(AssociationRecordPersister<IndexedAssociationRecord, ?> persister,
											  Function<SRC, C> collectionGetter,
											  IEntityMappingStrategy<SRC, SRCID, ?> mappingStrategy,
											  IEntityMappingStrategy<TRGT, TRGTID, ?> targetStrategy) {
		super(persister);
		this.collectionGetter = collectionGetter;
		this.mappingStrategy = mappingStrategy;
		this.targetStrategy = targetStrategy;
	}
	
	@Override
	protected void postTargetInsert(Iterable<? extends IndexedAssociationRecord> entities) {
		// Nothing to do. Identified#isPersisted flag should be fixed by target persister
	}
	
	@Override
	protected Collection<IndexedAssociationRecord> getTargets(SRC src) {
		List<TRGT> targets = collectionGetter.apply(src);
		// We only insert non-persisted instances (for logic and to prevent duplicate primary key error)
		List<IndexedAssociationRecord> result = new ArrayList<>(targets.size());
		ModifiableInt index = new ModifiableInt(-1);    // we start at -1 because increment() increments before giving the value
		targets.forEach(target -> result.add(new IndexedAssociationRecord(mappingStrategy.getId(src), targetStrategy.getId(target), index.increment())));
		return result;
	}
}
