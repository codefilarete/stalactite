package org.codefilarete.stalactite.engine.runtime.cycle;

import java.util.LinkedHashSet;
import java.util.Set;

import org.codefilarete.stalactite.engine.runtime.EntityConfiguredJoinedTablesPersister;
import org.codefilarete.stalactite.engine.configurer.CascadeConfigurationResult;
import org.codefilarete.stalactite.engine.configurer.CascadeOneConfigurer;
import org.codefilarete.stalactite.engine.configurer.CascadeOneConfigurer.FirstPhaseCycleLoadListener;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImpl.PostInitializer;

/**
 * Container of {@link CascadeOneConfigurer}s of same entity type and their relation name (through {@link RelationConfigurer}).
 * Expected to exist as a one-per-entity-type.
 * 
 * As a {@link PostInitializer}, will invoke every registered {@link CascadeOneConfigurer}
 * {@link CascadeOneConfigurer#appendCascadesWith2PhasesSelect(String, EntityConfiguredJoinedTablesPersister, FirstPhaseCycleLoadListener) appendCascadesWith2PhasesSelect method}
 * with a {@link OneToOneCycleLoader}.
 * 
 * @param <TRGT> type of all registered {@link CascadeOneConfigurer}
 */
public class OneToOneCycleConfigurer<TRGT> extends PostInitializer<TRGT> {
	
	// instantiated as a LinkedHashSet only for steady debugging purpose, could be replaced by a HashSet
	private final Set<RelationConfigurer<?, ?, ?>> relations = new LinkedHashSet<>();
	
	public OneToOneCycleConfigurer(Class<TRGT> entityType) {
		super(entityType);
	}
	
	public <SRC> void addCycleSolver(String relationIdentifier,
									 CascadeOneConfigurer<SRC, TRGT, ?, ?> cascadeOneConfigurer) {
		this.relations.add(new RelationConfigurer<>(relationIdentifier, cascadeOneConfigurer));
	}
	
	@Override
	public void consume(EntityConfiguredJoinedTablesPersister<TRGT, Object> targetPersister) {
		registerRelationLoader(targetPersister);
	}
	
	private <SRC, TRGTID> void registerRelationLoader(EntityConfiguredJoinedTablesPersister<TRGT, TRGTID> targetPersister) {
		OneToOneCycleLoader<SRC, TRGT, TRGTID> oneToOneCycleLoader = new OneToOneCycleLoader<>(targetPersister);
		targetPersister.addSelectListener(oneToOneCycleLoader);
		relations.forEach((RelationConfigurer c) -> {
			String tableAlias = c.relationName.replaceAll("\\W", "_");
			CascadeConfigurationResult<SRC, TRGT> configurationResult = c.cascadeOneConfigurer.appendCascadesWith2PhasesSelect(
					tableAlias, targetPersister, oneToOneCycleLoader);
			oneToOneCycleLoader.addRelation(c.relationName, configurationResult);
		});
	}
	
	private class RelationConfigurer<SRC, SRCID, TRGTID> {
		
		private final String relationName;
		private final CascadeOneConfigurer<SRC, TRGT, SRCID, TRGTID> cascadeOneConfigurer;
		
		public RelationConfigurer(String relationName,
								  CascadeOneConfigurer<SRC, TRGT, SRCID, TRGTID> cascadeOneConfigurer) {
			this.relationName = relationName;
			this.cascadeOneConfigurer = cascadeOneConfigurer;
		}
	}
}