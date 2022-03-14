package org.codefilarete.stalactite.engine.runtime;

import java.util.Collection;
import java.util.function.Function;

import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory;

/**
 * @author Guillaume Mary
 */
public class OneToManyWithAssociationTableEngine<SRC, TRGT, SRCID, TRGTID, C extends Collection<TRGT>>
		extends AbstractOneToManyWithAssociationTableEngine<SRC, TRGT, SRCID, TRGTID, C, AssociationRecord, AssociationTable> {
	
	public OneToManyWithAssociationTableEngine(ConfiguredJoinedTablesPersister<SRC, SRCID> sourcePersister,
											   EntityConfiguredJoinedTablesPersister<TRGT, TRGTID> targetPersister,
											   ManyRelationDescriptor<SRC, TRGT, C> manyRelationDescriptor,
											   AssociationRecordPersister<AssociationRecord, AssociationTable> associationPersister,
											   WriteOperationFactory writeOperationFactory) {
		super(sourcePersister, targetPersister, manyRelationDescriptor, associationPersister, writeOperationFactory);
	}
	
	@Override
	protected AssociationRecordInsertionCascader<SRC, TRGT, SRCID, TRGTID, C> newRecordInsertionCascader(
			Function<SRC, C> collectionGetter,
			AssociationRecordPersister<AssociationRecord, AssociationTable> associationPersister,
			EntityMapping<SRC, SRCID, ?> mappingStrategy,
			EntityMapping<TRGT, TRGTID, ?> targetStrategy) {
		return new AssociationRecordInsertionCascader<>(associationPersister, collectionGetter, mappingStrategy, targetStrategy);
	}
	
	@Override
	protected AssociationRecord newRecord(SRC e, TRGT target, int index) {
		return new AssociationRecord(sourcePersister.getMapping().getId(e), targetPersister.getMapping().getId(target));
	}
}