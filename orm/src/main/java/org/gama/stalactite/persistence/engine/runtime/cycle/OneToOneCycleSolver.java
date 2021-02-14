package org.gama.stalactite.persistence.engine.runtime.cycle;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.gama.reflection.AccessorDefinition;
import org.gama.reflection.IReversibleAccessor;
import org.gama.stalactite.persistence.engine.configurer.CascadeOneConfigurer;
import org.gama.stalactite.persistence.engine.configurer.CascadeOneConfigurer.ConfigurationResult;
import org.gama.stalactite.persistence.engine.configurer.PersisterBuilderImpl.PostInitializer;
import org.gama.stalactite.persistence.engine.runtime.IEntityConfiguredJoinedTablesPersister;

public class OneToOneCycleSolver<SRC, TRGT> extends PostInitializer<TRGT> {
	
	private final Set<CyclingPersisterSolver<?>> cycleSolvers = new LinkedHashSet<>();
	
	public OneToOneCycleSolver(Class<TRGT> entityType) {
		super(entityType);
	}
	
	public void addCycleSolver(IReversibleAccessor<SRC, TRGT> targetProvider,
							   CascadeOneConfigurer<SRC, TRGT, ?, ?> cascadeOneConfigurer) {
		this.cycleSolvers.add(new CyclingPersisterSolver<>(targetProvider, cascadeOneConfigurer));
	}
	
	@Override
	public void consume(IEntityConfiguredJoinedTablesPersister<TRGT, Object> targetPersister) {
		List<ConfigurationResult> configurationResults = new ArrayList<>();
		OneToOneCycleLoader<SRC, TRGT, Object> oneToOneCycleLoader = new OneToOneCycleLoader<>(targetPersister);
		targetPersister.addSelectListener(oneToOneCycleLoader);
		cycleSolvers.forEach((CyclingPersisterSolver c) -> {
			ConfigurationResult configurationResult = c.cascadeOneConfigurer.appendCascadesWith2PhasesSelect(targetPersister, oneToOneCycleLoader);
			oneToOneCycleLoader.addRelation(c.relationName, configurationResult);
			configurationResults.add(configurationResult);
			
		});
	}
	
	private class CyclingPersisterSolver<SRCID> {
		
		private final String relationName;
		private final CascadeOneConfigurer<SRC, TRGT, SRCID, Object> cascadeOneConfigurer;
		
		public CyclingPersisterSolver(IReversibleAccessor<SRC, TRGT> targetProvider,
									  CascadeOneConfigurer<SRC, TRGT, SRCID, ?> cascadeOneConfigurer) {
			this.relationName = AccessorDefinition.toString(targetProvider);
			this.cascadeOneConfigurer = (CascadeOneConfigurer<SRC, TRGT, SRCID, Object>) cascadeOneConfigurer;
		}
	}
}