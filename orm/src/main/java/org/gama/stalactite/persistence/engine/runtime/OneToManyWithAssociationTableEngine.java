package org.gama.stalactite.persistence.engine.runtime;

import java.util.Collection;
import java.util.function.Function;

import org.gama.stalactite.persistence.mapping.EntityMappingStrategy;

/**
 * @author Guillaume Mary
 */
public class OneToManyWithAssociationTableEngine<SRC, TRGT, SRCID, TRGTID, C extends Collection<TRGT>>
		extends AbstractOneToManyWithAssociationTableEngine<SRC, TRGT, SRCID, TRGTID, C, AssociationRecord, AssociationTable> {
	
	public OneToManyWithAssociationTableEngine(ConfiguredJoinedTablesPersister<SRC, SRCID> sourcePersister,
											   EntityConfiguredJoinedTablesPersister<TRGT, TRGTID> targetPersister,
											   ManyRelationDescriptor<SRC, TRGT, C> manyRelationDescriptor,
											   AssociationRecordPersister<AssociationRecord, AssociationTable> associationPersister) {
		super(sourcePersister, targetPersister, manyRelationDescriptor, associationPersister);
	}
	
	@Override
	protected AssociationRecordInsertionCascader<SRC, TRGT, SRCID, TRGTID, C> newRecordInsertionCascader(
			Function<SRC, C> collectionGetter,
			AssociationRecordPersister<AssociationRecord, AssociationTable> associationPersister,
			EntityMappingStrategy<SRC, SRCID, ?> mappingStrategy,
			EntityMappingStrategy<TRGT, TRGTID, ?> targetStrategy) {
		return new AssociationRecordInsertionCascader<>(associationPersister, collectionGetter, mappingStrategy, targetStrategy);
	}
	
	@Override
	protected AssociationRecord newRecord(SRC e, TRGT target, int index) {
		return new AssociationRecord(sourcePersister.getMappingStrategy().getId(e), targetPersister.getMappingStrategy().getId(target));
	}
}