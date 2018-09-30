package org.gama.stalactite.persistence.engine;

import java.util.Collection;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.gama.stalactite.persistence.engine.cascade.AfterInsertCollectionCascader;
import org.gama.stalactite.persistence.id.Identified;

import static org.gama.lang.collection.Iterables.stream;

/**
 * @author Guillaume Mary
 */
class AssociationRecordInsertionCascader<I extends Identified, O extends Identified, C extends Collection<O>>
		extends AfterInsertCollectionCascader<I, AssociationRecord> {
	
	private final Function<I, C> collectionGetter;
	
	public AssociationRecordInsertionCascader(AssociationRecordPersister<AssociationRecord, ?> persister, Function<I, C> collectionGetter) {
		super(persister);
		this.collectionGetter = collectionGetter;
	}
	
	@Override
	protected void postTargetInsert(Iterable<AssociationRecord> entities) {
		// Nothing to do. Identified#isPersisted flag should be fixed by target persister
	}
	
	@Override
	protected Collection<AssociationRecord> getTargets(I i) {
		Collection<O> targets = collectionGetter.apply(i);
		// We only insert non-persisted instances (for logic and to prevent duplicate primary key error)
		return stream(targets)
				.map(o -> new AssociationRecord(i.getId(), o.getId()))
				.collect(Collectors.toList());
	}
}
