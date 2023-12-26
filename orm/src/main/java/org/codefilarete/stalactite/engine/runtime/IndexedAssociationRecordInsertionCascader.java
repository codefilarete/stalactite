package org.codefilarete.stalactite.engine.runtime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import org.codefilarete.stalactite.engine.cascade.AfterInsertCollectionCascader;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.tool.trace.ModifiableInt;

import static org.codefilarete.stalactite.engine.runtime.onetomany.AbstractOneToManyWithAssociationTableEngine.INDEXED_COLLECTION_FIRST_INDEX_VALUE;

/**
 * @author Guillaume Mary
 */
public class IndexedAssociationRecordInsertionCascader<SRC, TRGT, SRCID, TRGTID, C extends Collection<TRGT>>
		extends AfterInsertCollectionCascader<SRC, IndexedAssociationRecord> {
	
	private final Function<SRC, C> collectionGetter;
	private final EntityMapping<SRC, SRCID, ?> mappingStrategy;
	private final EntityMapping<TRGT, TRGTID, ?> targetStrategy;
	
	public IndexedAssociationRecordInsertionCascader(AssociationRecordPersister<IndexedAssociationRecord, ?> persister,
													 Function<SRC, C> collectionGetter,
													 EntityMapping<SRC, SRCID, ?> mappingStrategy,
													 EntityMapping<TRGT, TRGTID, ?> targetStrategy) {
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
		Collection<TRGT> targets = collectionGetter.apply(src);
		// We only insert non-persisted instances (for logic and to prevent duplicate primary key error)
		List<IndexedAssociationRecord> result = new ArrayList<>(targets.size());
		ModifiableInt index = new ModifiableInt(INDEXED_COLLECTION_FIRST_INDEX_VALUE - 1);    // -1 because increment() increments before giving the value, though index will start at 1
		targets.forEach(target -> result.add(new IndexedAssociationRecord(mappingStrategy.getId(src), targetStrategy.getId(target), index.increment())));
		return result;
	}
}
