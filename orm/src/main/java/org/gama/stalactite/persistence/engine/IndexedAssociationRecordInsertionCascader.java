package org.gama.stalactite.persistence.engine;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import org.gama.lang.trace.ModifiableInt;
import org.gama.stalactite.persistence.engine.cascade.AfterInsertCollectionCascader;
import org.gama.stalactite.persistence.id.Identified;

/**
 * @author Guillaume Mary
 */
class IndexedAssociationRecordInsertionCascader<I extends Identified, O extends Identified, C extends List<O>>
		extends AfterInsertCollectionCascader<I, IndexedAssociationRecord> {
	
	private final Function<I, C> collectionGetter;
	
	IndexedAssociationRecordInsertionCascader(AssociationRecordPersister<IndexedAssociationRecord, ?> persister, Function<I, C> collectionGetter) {
		super(persister);
		this.collectionGetter = collectionGetter;
	}
	
	@Override
	protected void postTargetInsert(Iterable<? extends IndexedAssociationRecord> entities) {
		// Nothing to do. Identified#isPersisted flag should be fixed by target persister
	}
	
	@Override
	protected Collection<IndexedAssociationRecord> getTargets(I i) {
		List<O> targets = collectionGetter.apply(i);
		// We only insert non-persisted instances (for logic and to prevent duplicate primary key error)
		List<IndexedAssociationRecord> result = new ArrayList<>(targets.size());
		ModifiableInt index = new ModifiableInt(-1);    // we start at -1 because increment() increments before giving the value
		targets.forEach(o -> result.add(new IndexedAssociationRecord(i.getId(), o.getId(), index.increment())));
		return result;
	}
}
