package org.codefilarete.stalactite.engine.runtime.cycle;

import java.util.LinkedHashSet;
import java.util.Set;

import org.codefilarete.stalactite.engine.runtime.EntityConfiguredJoinedTablesPersister;
import org.codefilarete.stalactite.engine.configurer.CascadeConfigurationResult;
import org.codefilarete.stalactite.engine.configurer.OneToOneRelationConfigurer;
import org.codefilarete.stalactite.engine.configurer.OneToOneRelationConfigurer.FirstPhaseCycleLoadListener;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImpl.PostInitializer;

/**
 * Container of {@link OneToOneRelationConfigurer}s of same entity type and their relation name (through {@link RelationConfigurer}).
 * Expected to exist as a one-per-entity-type.
 * 
 * As a {@link PostInitializer}, will invoke every registered {@link OneToOneRelationConfigurer}
 * {@link OneToOneRelationConfigurer#configureWithSelectIn2Phases(String, EntityConfiguredJoinedTablesPersister, FirstPhaseCycleLoadListener) configureWithSelectIn2Phases method}
 * with a {@link OneToOneCycleLoader}.
 * 
 * @param <TRGT> type of all registered {@link OneToOneRelationConfigurer}
 */
public class OneToOneCycleConfigurer<TRGT> extends PostInitializer<TRGT> {
	
	// instantiated as a LinkedHashSet only for steady debugging purpose, could be replaced by a HashSet
	private final Set<RelationConfigurer<?, ?, ?>> relations = new LinkedHashSet<>();
	
	public OneToOneCycleConfigurer(Class<TRGT> entityType) {
		super(entityType);
	}
	
	public <SRC> void addCycleSolver(String relationIdentifier,
									 OneToOneRelationConfigurer<SRC, TRGT, ?, ?> oneToOneRelationConfigurer) {
		this.relations.add(new RelationConfigurer<>(relationIdentifier, oneToOneRelationConfigurer));
	}
	
	@Override
	public void consume(EntityConfiguredJoinedTablesPersister<TRGT, ?> targetPersister) {
		registerRelationLoader(targetPersister);
	}
	
	private <SRC, TRGTID> void registerRelationLoader(EntityConfiguredJoinedTablesPersister<TRGT, TRGTID> targetPersister) {
		OneToOneCycleLoader<SRC, TRGT, TRGTID> oneToOneCycleLoader = new OneToOneCycleLoader<>(targetPersister);
		targetPersister.addSelectListener(oneToOneCycleLoader);
		relations.forEach((RelationConfigurer c) -> {
			String tableAlias = c.relationName.replaceAll("\\W", "_");
			CascadeConfigurationResult<SRC, TRGT> configurationResult = c.oneToOneRelationConfigurer.configureWithSelectIn2Phases(
					tableAlias, targetPersister, oneToOneCycleLoader);
			oneToOneCycleLoader.addRelation(c.relationName, configurationResult);
		});
	}
	
	private class RelationConfigurer<SRC, SRCID, TRGTID> {
		
		private final String relationName;
		private final OneToOneRelationConfigurer<SRC, TRGT, SRCID, TRGTID> oneToOneRelationConfigurer;
		
		public RelationConfigurer(String relationName,
								  OneToOneRelationConfigurer<SRC, TRGT, SRCID, TRGTID> oneToOneRelationConfigurer) {
			this.relationName = relationName;
			this.oneToOneRelationConfigurer = oneToOneRelationConfigurer;
		}
	}
}