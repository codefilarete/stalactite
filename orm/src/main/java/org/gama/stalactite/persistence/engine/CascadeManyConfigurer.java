package org.gama.stalactite.persistence.engine;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.gama.lang.Reflections;
import org.gama.lang.collection.Iterables;
import org.gama.reflection.Accessors;
import org.gama.reflection.IMutator;
import org.gama.reflection.MethodReferenceCapturer;
import org.gama.stalactite.persistence.engine.CascadeOption.CascadeType;
import org.gama.stalactite.persistence.engine.FluentMappingBuilder.CascadeMany;
import org.gama.stalactite.persistence.engine.FluentMappingBuilder.SetPersistedFlagAfterInsertListener;
import org.gama.stalactite.persistence.engine.cascade.AfterInsertCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.AfterUpdateCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.BeforeDeleteByIdCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.BeforeDeleteCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.listening.IInsertListener;
import org.gama.stalactite.persistence.engine.listening.PersisterListener;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.IdentifiedCollectionDiffer;
import org.gama.stalactite.persistence.id.IdentifiedCollectionDiffer.Diff;
import org.gama.stalactite.persistence.id.manager.StatefullIdentifier;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * @author Guillaume Mary
 */
public class CascadeManyConfigurer<I extends Identified, O extends Identified, J extends StatefullIdentifier, C extends Collection<O>> {
	
	/** {@link OneToManyOptions#mappedBy(SerializableBiConsumer)} method signature (for printing purpose) to help find usage by avoiding hard "mappedBy" String */
	private static final String MAPPED_BY_SIGNATURE;
	
	static {
		Method mappedByMethod = new MethodReferenceCapturer()
				.findMethod((SerializableBiConsumer<OneToManyOptions, SerializableBiConsumer>) OneToManyOptions::mappedBy);
		MAPPED_BY_SIGNATURE = mappedByMethod.getDeclaringClass().getSimpleName() + "." + mappedByMethod.getName();
	}
	
	private final IdentifiedCollectionDiffer differ = new IdentifiedCollectionDiffer();
	
	public <T extends Table> void appendCascade(CascadeMany<I, O, J, C> cascadeMany,
							  Persister<I, ?, T> localPersister,
							  JoinedTablesPersister<I, J, T> joinedTablesPersister,
							  ForeignKeyNamingStrategy foreignKeyNamingStrategy) {
		Persister<Identified, StatefullIdentifier, Table> targetPersister = (Persister<Identified, StatefullIdentifier, Table>) cascadeMany.getPersister();
		
		// adding persistence flag setters on both side
		joinedTablesPersister.getPersisterListener().addInsertListener((IInsertListener<I>) SetPersistedFlagAfterInsertListener.INSTANCE);
		targetPersister.getPersisterListener().addInsertListener(SetPersistedFlagAfterInsertListener.INSTANCE);
		
		// finding joined columns: left one is primary key. Right one is given by the target strategy through the property accessor
		Column leftColumn = localPersister.getTargetTable().getPrimaryKey();
		Function targetProvider = cascadeMany.getTargetProvider();
		if (cascadeMany.getReverseSetter() == null && cascadeMany.getReverseGetter() == null && cascadeMany.getReverseColumn() == null) {
			throw new NotYetSupportedOperationException("Collection mapping without reverse property is not (yet) supported,"
					+ " please use \"" + MAPPED_BY_SIGNATURE + "\" option do declare one for " 
					+ Reflections.toString(cascadeMany.getMember()));
		}
		
		Column rightColumn = cascadeMany.getReverseColumn();
		if (rightColumn == null) {
			// Here reverse side is surely defined by method reference (because of assertion some lines upper), we look for the matching column
			MethodReferenceCapturer methodReferenceCapturer = new MethodReferenceCapturer();
			Method reverseMember;
			if (cascadeMany.getReverseSetter() != null) {
				reverseMember = methodReferenceCapturer.findMethod(cascadeMany.getReverseSetter());
			} else {
				reverseMember = methodReferenceCapturer.findMethod(cascadeMany.getReverseGetter());
			}
			rightColumn = targetPersister.getMappingStrategy().getDefaultMappingStrategy().getPropertyToColumn().get(Accessors.of(reverseMember));
			if (rightColumn == null) {
				throw new NotYetSupportedOperationException("Reverse side mapping is not declared, please add the mapping of a "
						+ localPersister.getMappingStrategy().getClassToPersist().getSimpleName()
						+ " to persister of " + cascadeMany.getPersister().getMappingStrategy().getClassToPersist().getName());
			}
		}
		
		// adding foerign key constraint
		rightColumn.getTable().addForeignKey(foreignKeyNamingStrategy.giveName(rightColumn, leftColumn), rightColumn, leftColumn);
		
		PersisterListener<I, ?> persisterListener = localPersister.getPersisterListener();
		for (CascadeType cascadeType : cascadeMany.getCascadeTypes()) {
			switch (cascadeType) {
				case INSERT:
					persisterListener.addInsertListener(new AfterInsertCollectionCascader<I, Identified>(targetPersister) {
						
						@Override
						protected void postTargetInsert(Iterable<Identified> iterables) {
							// Nothing to do. Identified#isPersisted flag should be fixed by target persister
						}
						
						@Override
						protected Collection<Identified> getTargets(I o) {
							Collection<Identified> targets = (Collection<Identified>) targetProvider.apply(o);
							// We only insert non-persisted instances (for logic and to prevent duplicate primary key error)
							return Iterables.stream(targets)
									.filter(CascadeOneConfigurer.NON_PERSISTED_PREDICATE)
									.collect(Collectors.toList());
						}
					});
					break;
				case UPDATE:
					persisterListener.addUpdateListener(new AfterUpdateCollectionCascader<I, Identified>(targetPersister) {
						
						@Override
						public void afterUpdate(Iterable<Map.Entry<I, I>> iterables, boolean allColumnsStatement) {
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
											if (cascadeMany.shouldDeleteRemoved()) {
												targetPersister.delete(diff.getSourceInstance());
											}
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
						protected Collection<Entry<Identified, Identified>> getTargets(I modifiedTrigger, I unmodifiedTrigger) {
							throw new NotYetSupportedOperationException();
						}
					});
					break;
				case DELETE:
					persisterListener.addDeleteListener(new BeforeDeleteCollectionCascader<I, Identified>(targetPersister) {
						
						@Override
						protected void postTargetDelete(Iterable<Identified> iterables) {
							// no post treatment to do
						}
						
						@Override
						protected Collection<Identified> getTargets(I o) {
							Collection<Identified> targets = (Collection<Identified>) targetProvider.apply(o);
							// We only delete persisted instances (for logic and to prevent from non matching row count exception)
							return Iterables.stream(targets)
									.filter(CascadeOneConfigurer.PERSISTED_PREDICATE)
									.collect(Collectors.toList());
						}
					});
					// we add the deleteById event since we suppose that if delete is required then there's no reason that rough delete is not
					persisterListener.addDeleteByIdListener(new BeforeDeleteByIdCollectionCascader<I, Identified>(targetPersister) {
						@Override
						protected void postTargetDelete(Iterable<Identified> iterables) {
							// no post treatment to do
						}
						
						@Override
						protected Collection<Identified> getTargets(I o) {
							Collection<Identified> targets = (Collection<Identified>) targetProvider.apply(o);
							// We only delete persisted instances (for logic and to prevent from non matching row count exception)
							return Iterables.stream(targets)
									.filter(CascadeOneConfigurer.PERSISTED_PREDICATE)
									.collect(Collectors.toList());
						}
					});
					break;
				case SELECT:
					// configuring select for fetching relation
					IMutator targetSetter = Accessors.of(cascadeMany.getMember()).getMutator();
					SerializableBiConsumer<O, I> reverseMember = cascadeMany.getReverseSetter();
					if (reverseMember == null) {
						reverseMember = (SerializableBiConsumer<O, I>) (o, i) -> { /* we can'i do anything, so we do ... nothing */ };
					}
					joinedTablesPersister.addPersister(JoinedStrategiesSelect.FIRST_STRATEGY_NAME, targetPersister,
							BeanRelationFixer.of((BiConsumer) targetSetter::set, targetProvider, cascadeMany.getCollectionTargetClass(), reverseMember),
							leftColumn, rightColumn, true);
					break;
			}
		}
	}
}
