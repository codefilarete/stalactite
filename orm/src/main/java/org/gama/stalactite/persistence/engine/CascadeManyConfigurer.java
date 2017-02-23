package org.gama.stalactite.persistence.engine;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.gama.lang.Reflections;
import org.gama.lang.collection.Iterables;
import org.gama.reflection.IMutator;
import org.gama.reflection.PropertyAccessor;
import org.gama.spy.MethodReferenceCapturer;
import org.gama.stalactite.persistence.engine.CascadeOption.CascadeType;
import org.gama.stalactite.persistence.engine.FluentMappingBuilder.CascadeMany;
import org.gama.stalactite.persistence.engine.FluentMappingBuilder.SetPersistedFlagAfterInsertListener;
import org.gama.stalactite.persistence.engine.cascade.AfterInsertCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.AfterUpdateCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.BeforeDeleteCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.BeforeDeleteRoughlyCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.listening.IInsertListener;
import org.gama.stalactite.persistence.engine.listening.PersisterListener;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.IdentifiedCollectionDiffer;
import org.gama.stalactite.persistence.id.IdentifiedCollectionDiffer.Diff;
import org.gama.stalactite.persistence.id.manager.StatefullIdentifier;
import org.gama.stalactite.persistence.structure.Table.Column;

/**
 * @author Guillaume Mary
 */
public class CascadeManyConfigurer<T extends Identified, I extends Identified, J extends StatefullIdentifier, C extends Collection<I>> {
	
	private final IdentifiedCollectionDiffer differ = new IdentifiedCollectionDiffer();
	
	public void appendCascade(CascadeMany<T, I, J, C> cascadeMany, Persister<T, ?> localPersister,
					 JoinedTablesPersister<T, J> joinedTablesPersister) {
		Persister<Identified, StatefullIdentifier> targetPersister = (Persister<Identified, StatefullIdentifier>) cascadeMany.getPersister();
		
		// adding persistence flag setters on both side
		joinedTablesPersister.getPersisterListener().addInsertListener((IInsertListener<T>) SetPersistedFlagAfterInsertListener.INSTANCE);
		targetPersister.getPersisterListener().addInsertListener(SetPersistedFlagAfterInsertListener.INSTANCE);
		
		// finding joined columns: left one is primary key. Right one is given by the target strategy throught the property accessor
		Column leftColumn = localPersister.getTargetTable().getPrimaryKey();
		Function targetProvider = cascadeMany.getTargetProvider();
		if (cascadeMany.getReverseMember() == null) {
			throw new NotYetSupportedOperationException("Collection mapping without reverse property is not (yet) supported,"
					+ " please used \"mappedBy\" option do declare one for "
					+ Reflections.toString(new MethodReferenceCapturer<>(localPersister.getMappingStrategy().getClassToPersist()).capture(targetProvider)));
		}
		
		Class<? extends Identified> targetClass = cascadeMany.getPersister().getMappingStrategy().getClassToPersist();
		MethodReferenceCapturer methodReferenceCapturer = new MethodReferenceCapturer<>(targetClass);
		Method reverseMember = methodReferenceCapturer.capture(cascadeMany.getReverseMember());
		Column rightColumn = targetPersister.getMappingStrategy().getDefaultMappingStrategy().getPropertyToColumn().get(PropertyAccessor.of(reverseMember));
		if (rightColumn == null) {
			throw new NotYetSupportedOperationException("Reverse side mapping is not declared, please add the mapping of a "
					+ localPersister.getMappingStrategy().getClassToPersist().getSimpleName()
					+ " to persister of " + cascadeMany.getPersister().getMappingStrategy().getClassToPersist().getName());
		}
		
		PersisterListener<T, ?> persisterListener = localPersister.getPersisterListener();
		for (CascadeType cascadeType : cascadeMany.getCascadeTypes()) {
			switch (cascadeType) {
				case INSERT:
					persisterListener.addInsertListener(new AfterInsertCollectionCascader<T, Identified>(targetPersister) {
						
						@Override
						protected void postTargetInsert(Iterable<Identified> iterables) {
							// Nothing to do. Identified#isPersisted flag should be fixed by target persister
						}
						
						@Override
						protected Collection<Identified> getTargets(T o) {
							Collection<Identified> targets = (Collection<Identified>) targetProvider.apply(o);
							// We only insert non-persisted instances (for logic and to prevent duplicate primary key error)
							return Iterables.stream(targets)
									.filter(CascadeOneConfigurer.NON_PERSISTED_PREDICATE)
									.collect(Collectors.toList());
						}
					});
					break;
				case UPDATE:
					persisterListener.addUpdateListener(new AfterUpdateCollectionCascader<T, Identified>(targetPersister) {
						
						@Override
						public void afterUpdate(Iterable<Map.Entry<T, T>> iterables, boolean allColumnsStatement) {
							iterables.forEach(entry -> {
								Set<Diff> diffSet = differ.diffSet(
										(Set) targetProvider.apply(entry.getValue()),
										(Set) targetProvider.apply(entry.getKey()));
								for (Diff diff : diffSet) {
									switch (diff.getState()) {
										case ADDED:
											targetPersister.insert(diff.getReplacingInstance());
											break;
										case HELD:
											// NB: update will only be done if necessary by target persister
											targetPersister.update(diff.getReplacingInstance(), diff.getSourceInstance(), allColumnsStatement);
											break;
										case REMOVED:
											targetPersister.delete(diff.getSourceInstance());
											break;
									}
								}
							});
						}
						
						@Override
						protected void postTargetUpdate(Iterable<Map.Entry<Identified, Identified>> iterables) {
							// Nothing to do
						}
						
						@Override
						protected Collection<Entry<Identified, Identified>> getTargets(T modifiedTrigger, T unmodifiedTrigger) {
							throw new NotYetSupportedOperationException();
						}
					});
					break;
				case DELETE:
					persisterListener.addDeleteListener(new BeforeDeleteCollectionCascader<T, Identified>(targetPersister) {
						
						@Override
						protected void postTargetDelete(Iterable<Identified> iterables) {
						}
						
						@Override
						protected Collection<Identified> getTargets(T o) {
							Collection<Identified> targets = (Collection<Identified>) targetProvider.apply(o);
							// We only delete persisted instances (for logic and to prevent from non matching row count exception)
							return Iterables.stream(targets)
									.filter(CascadeOneConfigurer.PERSISTED_PREDICATE)
									.collect(Collectors.toList());
						}
					});
					// we add the delete roughly event since we suppose that if delete is required then there's no reason that roughly delete is not
					persisterListener.addDeleteRoughlyListener(new BeforeDeleteRoughlyCollectionCascader<T, Identified>(targetPersister) {
						@Override
						protected void postTargetDelete(Iterable<Identified> iterables) {
						}
						
						@Override
						protected Collection<Identified> getTargets(T o) {
							Collection<Identified> targets = (Collection<Identified>) targetProvider.apply(o);
							// We only delete persisted instances (for logic and to prevent from non matching row count exception)
							return Iterables.stream(targets)
									.filter(CascadeOneConfigurer.PERSISTED_PREDICATE)
									.collect(Collectors.toList());
						}
					});
					break;
			}
		}
		
		IMutator targetSetter = PropertyAccessor.of(cascadeMany.getMember()).getMutator();
		joinedTablesPersister.addPersister(JoinedStrategiesSelect.FIRST_STRATEGY_NAME, targetPersister,
				BeanRelationFixer.of((BiConsumer) targetSetter::set, targetProvider, cascadeMany.getCollectionTargetClass(), cascadeMany.getReverseMember()),
				leftColumn, rightColumn, true);
	}
}
