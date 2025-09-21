package org.codefilarete.stalactite.engine.configurer.manyToOne;

import java.util.LinkedHashSet;
import java.util.Set;

import org.codefilarete.stalactite.engine.configurer.CascadeConfigurationResult;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImpl.PostInitializer;
import org.codefilarete.stalactite.engine.configurer.onetoone.FirstPhaseCycleLoadListener;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.cycle.ManyToOneCycleLoader;

/**
 * Container of {@link ManyToOneRelationConfigurer}s of same entity type and their relation name (through {@link RelationConfigurer}).
 * Expected to exist as a one-per-entity-type.
 * 
 * As a {@link PostInitializer}, will invoke every registered {@link ManyToOneRelationConfigurer}
 * {@link ManyToOneOwnedBySourceConfigurer#configureWithSelectIn2Phases(String, ConfiguredRelationalPersister, FirstPhaseCycleLoadListener) configureWithSelectIn2Phases method}
 * with a {@link ManyToOneCycleLoader}.
 * 
 * @param <TRGT> type of all registered {@link ManyToOneRelationConfigurer}
 */
public class ManyToOneCycleConfigurer<TRGT> extends PostInitializer<TRGT> {
	
	// instantiated as a LinkedHashSet only for steady debugging purpose, could be replaced by a HashSet
	private final Set<RelationConfigurer<?, ?, ?>> relations = new LinkedHashSet<>();
	
	public ManyToOneCycleConfigurer(Class<TRGT> entityType) {
		super(entityType);
	}
	
	public <SRC> void addCycleSolver(String relationIdentifier,
									 ManyToOneOwnedBySourceConfigurer<SRC, TRGT, ?, ?, ?, ?, ?> manyToOneRelationConfigurer) {
		this.relations.add(new RelationConfigurer<>(relationIdentifier, manyToOneRelationConfigurer));
	}
	
	@Override
	public void consume(ConfiguredRelationalPersister<TRGT, ?> targetPersister) {
		registerRelationLoader(targetPersister);
	}
	
	private <SRC, TRGTID> void registerRelationLoader(ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister) {
		ManyToOneCycleLoader<SRC, TRGT, TRGTID> manyToOneCycleLoader = new ManyToOneCycleLoader<>(targetPersister);
		targetPersister.addSelectListener(manyToOneCycleLoader);
		relations.forEach((RelationConfigurer c) -> {
			String tableAlias = c.relationName.replaceAll("\\W", "_");
			CascadeConfigurationResult<SRC, TRGT> configurationResult = c.manyToOneRelationConfigurer.configureWithSelectIn2Phases(
					tableAlias, targetPersister, manyToOneCycleLoader);
			manyToOneCycleLoader.addRelation(c.relationName, configurationResult);
		});
	}
	
	private class RelationConfigurer<SRC, SRCID, TRGTID> {
		
		private final String relationName;
		private final ManyToOneOwnedBySourceConfigurer<SRC, TRGT, SRCID, TRGTID, ?, ?, ?> manyToOneRelationConfigurer;
		
		public RelationConfigurer(String relationName,
								  ManyToOneOwnedBySourceConfigurer<SRC, TRGT, SRCID, TRGTID, ?, ?, ?> manyToOneRelationConfigurer) {
			this.relationName = relationName;
			this.manyToOneRelationConfigurer = manyToOneRelationConfigurer;
		}
	}
}