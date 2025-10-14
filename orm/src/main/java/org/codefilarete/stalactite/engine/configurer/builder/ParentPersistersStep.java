package org.codefilarete.stalactite.engine.configurer.builder;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfiguration;
import org.codefilarete.stalactite.engine.cascade.AfterDeleteByIdSupport;
import org.codefilarete.stalactite.engine.cascade.AfterDeleteSupport;
import org.codefilarete.stalactite.engine.cascade.AfterUpdateSupport;
import org.codefilarete.stalactite.engine.cascade.BeforeInsertSupport;
import org.codefilarete.stalactite.engine.configurer.AbstractIdentification;
import org.codefilarete.stalactite.engine.configurer.builder.InheritanceMappingStep.Mapping;
import org.codefilarete.stalactite.engine.configurer.builder.InheritanceMappingStep.MappingPerTable;
import org.codefilarete.stalactite.engine.listener.PersisterListenerCollection;
import org.codefilarete.stalactite.engine.listener.UpdateByIdListener;
import org.codefilarete.stalactite.engine.runtime.SimpleRelationalEntityPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityMerger.EntityMergerAdapter;
import org.codefilarete.stalactite.mapping.DefaultEntityMapping;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.collection.ReadOnlyIterator;
import org.codefilarete.tool.function.Hanger.Holder;

import static org.codefilarete.stalactite.engine.configurer.builder.MainPersisterStep.createEntityMapping;
import static org.codefilarete.tool.collection.Iterables.first;

/**
 * Build parent persisters and add cascade between child and parent table.
 *
 * @param <C>
 * @param <I>
 * @author Guillaume Mary
 */
public class ParentPersistersStep<C, I> {
	
	void buildParentPersisters(SimpleRelationalEntityPersister<C, I, ?> mainPersister,
							   AbstractIdentification<C, I> identification,
							   MappingPerTable<C> inheritanceMappingPerTable,
							   Dialect dialect,
							   ConnectionConfiguration connectionConfiguration) {
		Mapping<C, ?> mainMapping = first(inheritanceMappingPerTable.getMappings());
		// parent persister must be kept in ascending order for below treatment
		ReadOnlyIterator<Mapping<C, ?>> inheritedMappingIterator = Iterables.reverseIterator(inheritanceMappingPerTable.getMappings().getDelegate());
		Iterator<Mapping<C, ?>> mappings = Iterables.filter(inheritedMappingIterator,
				m -> !mainMapping.equals(m) && !m.isMappedSuperClass());
		KeepOrderSet<SimpleRelationalEntityPersister<C, I, ?>> parentPersisters = this.buildParentPersisters(() -> mappings,
				identification, mainPersister, dialect, connectionConfiguration
		);
		
		addCascadesBetweenChildAndParentTable(mainPersister, parentPersisters);
	}
	
	private <T extends Table<T>, TT extends Table<TT>> KeepOrderSet<SimpleRelationalEntityPersister<C, I, ?>> buildParentPersisters(Iterable<Mapping<C, ?>> mappings,
																																	AbstractIdentification<C, I> identification,
																																	SimpleRelationalEntityPersister<C, I, T> mainPersister,
																																	Dialect dialect,
																																	ConnectionConfiguration connectionConfiguration) {
		KeepOrderSet<SimpleRelationalEntityPersister<C, I, ?>> result = new KeepOrderSet<>();
		PrimaryKey<T, I> superclassPK = mainPersister.getMainTable().getPrimaryKey();
		Holder<Table> currentTable = new Holder<>(mainPersister.getMainTable());
		mappings.forEach(mapping -> {
			// we skip already configured inherited persister to avoid getting an error while adding results of buildParentPersisters(..) method
			// to the persister registry due to prohibition to add twice persister dealing with same entity type
			if (mapping.getMappingConfiguration() instanceof EmbeddableMappingConfiguration) {
				Class<?> entityType = ((EmbeddableMappingConfiguration<?>) mapping.getMappingConfiguration()).getBeanType();
				if (PersisterBuilderContext.CURRENT.get().getPersisterRegistry().getPersister(entityType) != null) {
					return;
				}
			}
			
			Mapping<C, TT> castedMapping = (Mapping<C, TT>) mapping;
			PrimaryKey<TT, I> subclassPK = castedMapping.getTargetTable().getPrimaryKey();
			boolean isIdentifyingConfiguration = identification.getIdentificationDefiner().getPropertiesMapping() == mapping.giveEmbeddableConfiguration();
			DefaultEntityMapping<C, I, TT> currentMappingStrategy = createEntityMapping(
					isIdentifyingConfiguration,
					castedMapping.getTargetTable(),
					castedMapping.getMapping(),
					castedMapping.getReadonlyMapping(),
					castedMapping.getReadConverters(),
					castedMapping.getWriteConverters(),
					castedMapping.getPropertiesSetByConstructor(),
					identification,
					mapping.giveEmbeddableConfiguration().getBeanType(),
					null);
			
			SimpleRelationalEntityPersister<C, I, TT> currentPersister = new SimpleRelationalEntityPersister<>(currentMappingStrategy, dialect, connectionConfiguration);
			result.add(currentPersister);
			// a join is necessary to select entity, only if target table changes
			if (!currentPersister.getMainTable().equals(currentTable.get())) {
				mainPersister.getEntityJoinTree().addMergeJoin(EntityJoinTree.ROOT_JOIN_NAME, new EntityMergerAdapter<>(currentMappingStrategy), superclassPK, subclassPK);
				currentTable.set(currentPersister.getMainTable());
			}
		});
		return result;
	}
	
	/**
	 *
	 * @param mainPersister main persister
	 * @param superPersisters persisters in ascending order
	 */
	private <T extends Table<T>> void addCascadesBetweenChildAndParentTable(SimpleRelationalEntityPersister<C, I, T> mainPersister,
																			KeepOrderSet<SimpleRelationalEntityPersister<C, I, ?>> superPersisters) {
		// we add cascade only on persister with different table : we keep the "lowest" one because it gets all inherited properties,
		// upper ones are superfluous
		KeepOrderSet<SimpleRelationalEntityPersister<C, I, ?>> superPersistersWithChangingTable = new KeepOrderSet<>();
		Holder<Table> lastTable = new Holder<>(mainPersister.getMainTable());
		superPersisters.forEach(p -> {
			if (!p.getMainTable().equals(lastTable.get())) {
				superPersistersWithChangingTable.add(p);
			}
			lastTable.set(p.getMainTable());
		});
		PersisterListenerCollection<C, I> persisterListener = mainPersister.getPersisterListener();
		superPersistersWithChangingTable.forEach(superPersister -> {
			// Before insert of child we must insert parent
			persisterListener.addInsertListener(new BeforeInsertSupport<>(superPersister::insert, Function.identity()));
			
			// On child update, parent must be updated too, no constraint on order for this, after is arbitrarily chosen
			persisterListener.addUpdateListener(new AfterUpdateSupport<>(superPersister::update, Function.identity()));
			// idem for updateById
			persisterListener.addUpdateByIdListener(new UpdateByIdListener<C>() {
				@Override
				public void afterUpdateById(Iterable<? extends C> entities) {
					superPersister.updateById(entities);
				}
			});
		});
		
		List<SimpleRelationalEntityPersister<C, I, ?>> copy = Iterables.copy(superPersistersWithChangingTable);
		Collections.reverse(copy);
		copy.forEach(superPersister -> {
			// On child deletion, parent must be deleted after
			persisterListener.addDeleteListener(new AfterDeleteSupport<>(superPersister::delete, Function.identity()));
			// idem for deleteById
			persisterListener.addDeleteByIdListener(new AfterDeleteByIdSupport<>(superPersister::deleteById, Function.identity()));
		});
	}
}
