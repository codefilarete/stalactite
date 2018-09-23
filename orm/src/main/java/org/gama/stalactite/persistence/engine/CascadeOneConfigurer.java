package org.gama.stalactite.persistence.engine;

import java.util.function.Predicate;

import org.gama.lang.Duo;
import org.gama.lang.collection.Iterables;
import org.gama.reflection.Accessors;
import org.gama.reflection.IMutator;
import org.gama.reflection.PropertyAccessor;
import org.gama.stalactite.persistence.engine.CascadeOption.CascadeType;
import org.gama.stalactite.persistence.engine.FluentMappingBuilder.CascadeOne;
import org.gama.stalactite.persistence.engine.FluentMappingBuilder.MandatoryRelationCheckingBeforeInsertListener;
import org.gama.stalactite.persistence.engine.FluentMappingBuilder.MandatoryRelationCheckingBeforeUpdateListener;
import org.gama.stalactite.persistence.engine.FluentMappingBuilder.SetPersistedFlagAfterInsertListener;
import org.gama.stalactite.persistence.engine.cascade.AfterDeleteByIdCascader;
import org.gama.stalactite.persistence.engine.cascade.AfterDeleteCascader;
import org.gama.stalactite.persistence.engine.cascade.AfterUpdateCascader;
import org.gama.stalactite.persistence.engine.cascade.BeforeInsertCascader;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.listening.PersisterListener;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.manager.StatefullIdentifier;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * @author Guillaume Mary
 */
public class CascadeOneConfigurer<I extends Identified, O extends Identified, J extends StatefullIdentifier> {
	
	static final Predicate<Identified> NON_PERSISTED_PREDICATE = target -> target != null && !target.getId().isPersisted();
	
	static final Predicate<Identified> PERSISTED_PREDICATE = target -> target != null && target.getId().isPersisted();
	
	public <T extends Table<T>> void appendCascade(
			CascadeOne<I, O, J> cascadeOne, Persister<I, ?, T> localPersister,
			ClassMappingStrategy<I, O, T> mappingStrategy,
			JoinedTablesPersister<I, J, T> joinedTablesPersister,
			ForeignKeyNamingStrategy foreignKeyNamingStrategy) {
		Persister<Identified, StatefullIdentifier, ?> targetPersister = (Persister<Identified, StatefullIdentifier, ?>) cascadeOne.getPersister();
		
		// adding persistence flag setters on other side
		targetPersister.getPersisterListener().addInsertListener(SetPersistedFlagAfterInsertListener.INSTANCE);
		
		PropertyAccessor<Identified, Identified> propertyAccessor = Accessors.of(cascadeOne.getMember());
		// Finding joined columns:
		// - left one is given by current mapping strategy through the property accessor.
		// - Right one is target primary key because we don't yet support "not owner of the property"
		if (mappingStrategy.getTargetTable().getPrimaryKey().getColumns().size() > 1) {
			throw new NotYetSupportedOperationException("Joining tables on a composed primery key is not (yet) supported");
		}
		Column leftColumn = mappingStrategy.getMainMappingStrategy().getPropertyToColumn().get(propertyAccessor);
		// According to the nullable option, we specify the ddl schema option
		leftColumn.nullable(cascadeOne.isNullable());
		Column rightColumn = Iterables.first(targetPersister.getTargetTable().getPrimaryKey().getColumns());
		
		// adding foerign key constraint
		leftColumn.getTable().addForeignKey(foreignKeyNamingStrategy.giveName(leftColumn, rightColumn), leftColumn, rightColumn);
		
		PersisterListener<I, ?> persisterListener = localPersister.getPersisterListener();
		for (CascadeType cascadeType : cascadeOne.getCascadeTypes()) {
			switch (cascadeType) {
				case INSERT:
					// if cascade is mandatory, then adding nullability checking before insert
					if (!cascadeOne.isNullable()) {
						persisterListener.addInsertListener(
								new MandatoryRelationCheckingBeforeInsertListener<>(cascadeOne.getTargetProvider(), cascadeOne.getMember()));
					}
					// adding cascade treatment: after insert target is inserted too
					persisterListener.addInsertListener(new BeforeInsertCascader<I, Identified>(targetPersister) {
						
						@Override
						protected void postTargetInsert(Iterable<Identified> entities) {
							// Nothing to do. Identified#isPersisted flag should be fixed by target persister
						}
						
						@Override
						protected Identified getTarget(I o) {
							Identified target = cascadeOne.getTargetProvider().apply(o);
							// We only insert non-persisted instances (for logic and to prevent duplicate primary key error)
							return NON_PERSISTED_PREDICATE.test(target) ? target : null;
						}
					});
					break;
				case UPDATE:
					// if cascade is mandatory, then adding nullability checking before insert
					if (!cascadeOne.isNullable()) {
						persisterListener.addUpdateListener(
								new MandatoryRelationCheckingBeforeUpdateListener<>(cascadeOne.getMember(), cascadeOne.getTargetProvider()));
					}
					// adding cascade treatment: after update target is updated too
					persisterListener.addUpdateListener(new AfterUpdateCascader<I, Identified>(targetPersister) {
						
						@Override
						protected void postTargetUpdate(Iterable<UpdatePayload<Identified, ?>> entities) {
							// Nothing to do
						}
						
						@Override
						protected Duo<Identified, Identified> getTarget(I modifiedTrigger, I unmodifiedTrigger) {
							return new Duo<>(cascadeOne.getTargetProvider().apply(modifiedTrigger), cascadeOne.getTargetProvider().apply
									(unmodifiedTrigger));
						}
					});
					break;
				case DELETE:
					// adding cascade treatment: before delete target is deleted (done before because of foreign key constraint)
					persisterListener.addDeleteListener(new AfterDeleteCascader<I, Identified>(targetPersister) {
						
						@Override
						protected void postTargetDelete(Iterable<Identified> entities) {
							// no post treatment to do
						}
						
						@Override
						protected Identified getTarget(I i) {
							Identified target = cascadeOne.getTargetProvider().apply(i);
							// We only delete persisted instances (for logic and to prevent from non matching row count error)
							return PERSISTED_PREDICATE.test(target) ? target : null;
						}
					});
					// we add the deleteById event since we suppose that if delete is required then there's no reason that rough delete is not
					persisterListener.addDeleteByIdListener(new AfterDeleteByIdCascader<I, Identified>(targetPersister) {
						
						@Override
						protected void postTargetDelete(Iterable<Identified> entities) {
							// no post treatment to do
						}
						
						@Override
						protected Identified getTarget(I i) {
							Identified target = cascadeOne.getTargetProvider().apply(i);
							// We only delete persisted instances (for logic and to prevent from non matching row count error)
							return PERSISTED_PREDICATE.test(target) ? target : null;
						}
					});
					break;
				case SELECT:
					// configuring select for fetching relation
					IMutator<Identified, Identified> targetSetter = propertyAccessor.getMutator();
					joinedTablesPersister.addPersister(JoinedStrategiesSelect.FIRST_STRATEGY_NAME, targetPersister,
							BeanRelationFixer.of(targetSetter::set),
							leftColumn, rightColumn, cascadeOne.isNullable());
					break;
			}
		}
	}
	
}
