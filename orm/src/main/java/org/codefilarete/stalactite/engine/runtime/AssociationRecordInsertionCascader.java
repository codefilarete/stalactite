package org.codefilarete.stalactite.engine.runtime;

import java.util.Collection;
import java.util.stream.Collectors;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.stalactite.engine.cascade.AfterInsertCollectionCascader;
import org.codefilarete.stalactite.mapping.EntityMapping;

import static org.codefilarete.tool.collection.Iterables.stream;

/**
 * @author Guillaume Mary
 */
public class AssociationRecordInsertionCascader<SRC, TRGT, SRCID, TRGTID, C extends Collection<TRGT>, R extends AssociationRecord>
		extends AfterInsertCollectionCascader<SRC, R> {
	
	private final Accessor<SRC, C> collectionGetter;
	private final EntityMapping<SRC, SRCID, ?> mappingStrategy;
	private final EntityMapping<TRGT, TRGTID, ?> targetStrategy;
	
	public AssociationRecordInsertionCascader(AssociationRecordPersister<R, ?> persister,
											  Accessor<SRC, C> collectionGetter,
											  EntityMapping<SRC, SRCID, ?> mappingStrategy,
											  EntityMapping<TRGT, TRGTID, ?> targetStrategy) {
		super(persister);
		this.collectionGetter = collectionGetter;
		this.mappingStrategy = mappingStrategy;
		this.targetStrategy = targetStrategy;
	}
	
	@Override
	protected void postTargetInsert(Iterable<? extends R> entities) {
		// Nothing to do. Identified#isPersisted flag should be fixed by target persister
	}
	
	@Override
	protected Collection<R> getTargets(SRC src) {
		Collection<TRGT> targets = collectionGetter.get(src);
		// We only insert non-persisted instances (for logic and to prevent duplicate primary key error)
		return stream(targets)
				.map(target -> (R) new AssociationRecord(mappingStrategy.getId(src), targetStrategy.getId(target)))
				.collect(Collectors.toList());
	}
}
