package org.codefilarete.stalactite.engine.configurer.manytomany;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

import org.codefilarete.stalactite.engine.configurer.CascadeConfigurationResult;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImpl.PostInitializer;
import org.codefilarete.stalactite.engine.configurer.manytomany.ManyToManyRelationConfigurer.ManyToManyWithAssociationTableConfigurer;
import org.codefilarete.stalactite.engine.configurer.onetomany.FirstPhaseCycleLoadListener;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.cycle.ManyToManyCycleLoader;

/**
 * Container of {@link ManyToManyRelationConfigurer}s of same entity type and their relation name (through {@link RelationConfigurer}).
 * Expected to exist as a one-per-entity-type.
 * 
 * As a {@link PostInitializer}, will invoke every registered {@link ManyToManyRelationConfigurer}
 * {@link ManyToManyWithAssociationTableConfigurer#configureWithSelectIn2Phases(ConfiguredRelationalPersister, FirstPhaseCycleLoadListener)} configureWithSelectIn2Phases method}
 * with a {@link ManyToManyCycleLoader}.
 * 
 * @param <TRGT> type of all registered {@link ManyToManyRelationConfigurer}
 */
public class ManyToManyCycleConfigurer<TRGT> extends PostInitializer<TRGT> {
	
	// instantiated as a LinkedHashSet only for steady debugging purpose, could be replaced by a HashSet
	private final Set<RelationConfigurer<?, ?, ?>> relations = new LinkedHashSet<>();
	
	public ManyToManyCycleConfigurer(Class<TRGT> entityType) {
		super(entityType);
	}
	
	public <SRC> void addCycleSolver(String relationIdentifier,
									 ManyToManyWithAssociationTableConfigurer<SRC, TRGT, ?, ?, ? extends Collection<TRGT>, ? extends Collection<SRC>, ?, ?> manyToManyRelationConfigurer) {
		this.relations.add(new RelationConfigurer<>(relationIdentifier, manyToManyRelationConfigurer));
	}
	
	@Override
	public void consume(ConfiguredRelationalPersister<TRGT, ?> targetPersister) {
		registerRelationLoader(targetPersister);
	}
	
	private <SRC, TRGTID> void registerRelationLoader(ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister) {
		ManyToManyCycleLoader<SRC, TRGT, TRGTID> manyToManyCycleLoader = new ManyToManyCycleLoader<>(targetPersister);
		targetPersister.addSelectListener(manyToManyCycleLoader);
		((Set<RelationConfigurer<SRC, ?, TRGTID>>) (Set) relations).forEach(c -> {
			String tableAlias = c.relationName.replaceAll("\\W", "_");
			CascadeConfigurationResult<SRC, TRGT> configurationResult = c.cascadeManyConfigurer.configureWithSelectIn2Phases(
					targetPersister, manyToManyCycleLoader.buildRowReader(c.relationName));
			manyToManyCycleLoader.addRelation(c.relationName, configurationResult);
		});
	}
	
	private class RelationConfigurer<SRC, SRCID, TRGTID> {
		
		private final String relationName;
		private final ManyToManyWithAssociationTableConfigurer<SRC, TRGT, SRCID, TRGTID, Collection<TRGT>, Collection<SRC>, ?, ?> cascadeManyConfigurer;
		
		public RelationConfigurer(String relationName,
								  ManyToManyWithAssociationTableConfigurer<SRC, TRGT, SRCID, TRGTID, ? extends Collection<TRGT>, ? extends Collection<SRC>, ?, ?> cascadeManyConfigurer) {
			this.relationName = relationName;
			this.cascadeManyConfigurer = (ManyToManyWithAssociationTableConfigurer<SRC, TRGT, SRCID, TRGTID, Collection<TRGT>, Collection<SRC>, ?, ?>) cascadeManyConfigurer;
		}
	}
}