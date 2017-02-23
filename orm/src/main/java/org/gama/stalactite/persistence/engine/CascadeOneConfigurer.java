package org.gama.stalactite.persistence.engine;

import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;

import org.gama.reflection.IMutator;
import org.gama.reflection.PropertyAccessor;
import org.gama.stalactite.persistence.engine.CascadeOption.CascadeType;
import org.gama.stalactite.persistence.engine.FluentMappingBuilder.CascadeOne;
import org.gama.stalactite.persistence.engine.FluentMappingBuilder.MandatoryRelationCheckingBeforeInsertListener;
import org.gama.stalactite.persistence.engine.FluentMappingBuilder.MandatoryRelationCheckingBeforeUpdateListener;
import org.gama.stalactite.persistence.engine.FluentMappingBuilder.SetPersistedFlagAfterInsertListener;
import org.gama.stalactite.persistence.engine.cascade.AfterInsertCascader;
import org.gama.stalactite.persistence.engine.cascade.AfterUpdateCascader;
import org.gama.stalactite.persistence.engine.cascade.BeforeDeleteCascader;
import org.gama.stalactite.persistence.engine.cascade.BeforeDeleteRoughlyCascader;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.manager.StatefullIdentifier;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.structure.Table.Column;

/**
 * @author Guillaume Mary
 */
public class CascadeOneConfigurer<T extends Identified, I extends Identified, J extends StatefullIdentifier> {
	
	public void appendCascade(
			CascadeOne<T, I, J> cascadeOne, Persister<T, ?> localPersister,
			ClassMappingStrategy<T, I> mappingStrategy,
			JoinedTablesPersister<T, J> joinedTablesPersister) {
		Persister<Identified, StatefullIdentifier> targetPersister = (Persister<Identified, StatefullIdentifier>) cascadeOne.getPersister();
		
		// adding persistence flag setters on other side
		targetPersister.getPersisterListener().addInsertListener(SetPersistedFlagAfterInsertListener.INSTANCE);
		
		PropertyAccessor<Identified, Identified> propertyAccessor = PropertyAccessor.of(cascadeOne.getMember());
		// Finding joined columns:
		// - left one is given by current mapping strategy throught the property accessor.
		// - Right one is target primary key because we don't yet support "not owner of the property"
		Column leftColumn = mappingStrategy.getDefaultMappingStrategy().getPropertyToColumn().get(propertyAccessor);
		// According to the nullable option, we specify the ddl schema option
		leftColumn.nullable(cascadeOne.isNullable());
		Column rightColumn = targetPersister.getTargetTable().getPrimaryKey();
		
		for (CascadeType cascadeType : cascadeOne.getCascadeTypes()) {
			switch (cascadeType) {
				case INSERT:
					// if cascade is mandatory, then adding nullability checking before insert
					if (!cascadeOne.isNullable()) {
						localPersister.getPersisterListener().addInsertListener(
								new MandatoryRelationCheckingBeforeInsertListener<>(cascadeOne.getTargetProvider(), cascadeOne.getMember()));
					}
					// adding cascade treatment: after insert target is inserted too
					localPersister.getPersisterListener().addInsertListener(new AfterInsertCascader<T, Identified>(targetPersister) {
						
						@Override
						protected void postTargetInsert(Iterable<Identified> iterable) {
							// Nothing to do. Identified#isPersisted flag should be fixed by target persister
						}
						
						@Override
						protected Identified getTarget(T o) {
							Identified target = cascadeOne.getTargetProvider().apply(o);
							// We only insert non-persisted instances (for logic and to prevent duplicate primary key error)
							if (target != null && !target.getId().isPersisted()) {
								return target;
							} else {
								return null;
							}
						}
					});
					break;
				case UPDATE:
					// if cascade is mandatory, then adding nullability checking before insert
					if (!cascadeOne.isNullable()) {
						localPersister.getPersisterListener().addUpdateListener(
								new MandatoryRelationCheckingBeforeUpdateListener<>(cascadeOne.getMember(), cascadeOne.getTargetProvider()));
					}
					// adding cascade treatment: after update target is updated too
					localPersister.getPersisterListener().addUpdateListener(new AfterUpdateCascader<T, Identified>(targetPersister) {
						
						@Override
						protected void postTargetUpdate(Iterable<Entry<Identified, Identified>> iterable) {
							// Nothing to do
						}
						
						@Override
						protected Entry<Identified, Identified> getTarget(T modifiedTrigger, T unmodifiedTrigger) {
							return new SimpleEntry<>(cascadeOne.getTargetProvider().apply(modifiedTrigger), cascadeOne.getTargetProvider().apply
									(unmodifiedTrigger));
						}
					});
					break;
				case DELETE:
					// adding cascade treatment: before delete target is deleted (done before because of foreign key constraint)
					localPersister.getPersisterListener().addDeleteListener(new BeforeDeleteCascader<T, Identified>(targetPersister) {
						
						@Override
						protected void postTargetDelete(Iterable<Identified> iterable) {
						}
						
						@Override
						protected Identified getTarget(T o) {
							Identified target = cascadeOne.getTargetProvider().apply(o);
							// We only delete persisted instances (for logic and to prevent from non matching row count error)
							if (target != null && target.getId().isPersisted()) {
								return target;
							} else {
								return null;
							}
						}
					});
					// we add the delete roughly event since we suppose that if delete is required then there's no reason that roughly 
					// delete is not
					localPersister.getPersisterListener().addDeleteRoughlyListener(new BeforeDeleteRoughlyCascader<T, Identified>(targetPersister) {
						
						@Override
						protected void postTargetDelete(Iterable<Identified> iterable) {
						}
						
						@Override
						protected Identified getTarget(T o) {
							Identified target = cascadeOne.getTargetProvider().apply(o);
							// We only delete persisted instances (for logic and to prevent from non matching row count error)
							if (target != null && target.getId().isPersisted()) {
								return target;
							} else {
								return null;
							}
						}
					});
					break;
			}
		}
		
		IMutator targetSetter = propertyAccessor.getMutator();
		joinedTablesPersister.addPersister(JoinedStrategiesSelect.FIRST_STRATEGY_NAME, targetPersister,
				BeanRelationFixer.of(targetSetter::set),
				leftColumn, rightColumn, cascadeOne.isNullable());
	}
	
}
