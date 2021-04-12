package org.gama.stalactite.persistence.engine.runtime.cycle;

import java.util.LinkedHashSet;
import java.util.Set;

import org.gama.stalactite.persistence.engine.configurer.CascadeOneConfigurer;
import org.gama.stalactite.persistence.engine.configurer.CascadeOneConfigurer.ConfigurationResult;
import org.gama.stalactite.persistence.engine.configurer.CascadeOneConfigurer.FirstPhaseCycleLoadListener;
import org.gama.stalactite.persistence.engine.configurer.PersisterBuilderImpl.PostInitializer;
import org.gama.stalactite.persistence.engine.runtime.IEntityConfiguredJoinedTablesPersister;

/**
 * Container of {@link CascadeOneConfigurer}s of same entity type and their relation name (throught {@link RelationConfigurer}).
 * Expected to exist as a one-per-entity-type.
 * 
 * As a {@link PostInitializer}, will invoke every registered {@link CascadeOneConfigurer}
 * {@link CascadeOneConfigurer#appendCascadesWith2PhasesSelect(String, IEntityConfiguredJoinedTablesPersister, FirstPhaseCycleLoadListener) appendCascadesWith2PhasesSelect method}
 * with a {@link OneToOneCycleLoader}.
 * 
 * @param <TRGT> type of all registered {@link CascadeOneConfigurer}
 */
public class OneToOneCycleConfigurer<TRGT> extends PostInitializer<TRGT> {
	
	// instanciated as a LinkedHashSet only for steady debugging purpose, could be replaced by a HashSet
	private final Set<RelationConfigurer<?, ?, ?>> relations = new LinkedHashSet<>();
	
	public OneToOneCycleConfigurer(Class<TRGT> entityType) {
		super(entityType);
	}
	
	public <SRC> void addCycleSolver(String relationIdentifier,
									 CascadeOneConfigurer<SRC, TRGT, ?, ?> cascadeOneConfigurer) {
		this.relations.add(new RelationConfigurer<>(relationIdentifier, cascadeOneConfigurer));
	}
	
	@Override
	public void consume(IEntityConfiguredJoinedTablesPersister<TRGT, Object> targetPersister) {
		registerRelationLoader(targetPersister);
	}
	
	private <SRC, TRGTID> void registerRelationLoader(IEntityConfiguredJoinedTablesPersister<TRGT, TRGTID> targetPersister) {
		OneToOneCycleLoader<SRC, TRGT, TRGTID> oneToOneCycleLoader = new OneToOneCycleLoader<>(targetPersister);
		targetPersister.addSelectListener(oneToOneCycleLoader);
		relations.forEach((RelationConfigurer c) -> {
			String tableAlias = c.relationName.replaceAll("\\W", "_");
			ConfigurationResult<SRC, TRGT> configurationResult = c.cascadeOneConfigurer.appendCascadesWith2PhasesSelect(
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