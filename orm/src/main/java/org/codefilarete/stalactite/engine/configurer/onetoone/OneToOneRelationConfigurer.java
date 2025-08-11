package org.codefilarete.stalactite.engine.configurer.onetoone;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.stalactite.engine.EntityMappingConfiguration;
import org.codefilarete.stalactite.engine.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.engine.JoinColumnNamingStrategy;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderContext;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImpl;
import org.codefilarete.stalactite.engine.configurer.RelationConfigurer.GraphLoadingRelationRegisterer;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.Iterables;

import static org.codefilarete.tool.Nullable.nullable;

/**
 * Wrapper for {@link OneToOneOwnedBySourceConfigurer} and {@link OneToOneOwnedByTargetConfigurer} to make them available for cycle.
 *
 * @param <C> type of input (left/source entities)
 * @param <I> identifier type of source entities
 * @author Guillaume Mary
 */
public class OneToOneRelationConfigurer<C, I> {
	private final Dialect dialect;
	private final ConnectionConfiguration connectionConfiguration;
	private final ConfiguredRelationalPersister<C, I> sourcePersister;
	private final JoinColumnNamingStrategy joinColumnNamingStrategy;
	private final ForeignKeyNamingStrategy foreignKeyNamingStrategy;
	
	public OneToOneRelationConfigurer(Dialect dialect,
			  ConnectionConfiguration connectionConfiguration,
			  ConfiguredRelationalPersister<C, I> sourcePersister,
			  JoinColumnNamingStrategy joinColumnNamingStrategy,
			  ForeignKeyNamingStrategy foreignKeyNamingStrategy) {
		this.dialect = dialect;
		this.connectionConfiguration = connectionConfiguration;
		this.sourcePersister = sourcePersister;
		this.joinColumnNamingStrategy = joinColumnNamingStrategy;
		this.foreignKeyNamingStrategy = foreignKeyNamingStrategy;
	}
	
	/**
	 * Setup given one-to-one relation by adding write cascade as well as creating appropriate joins in 
	 *
	 * @param oneToOneRelation the relation to be configured
	 * @param <TRGT> type of output (right/target entities)
	 * @param <TRGTID>> identifier type of target entities
	 */
	public <TRGT, TRGTID> void configure(OneToOneRelation<C, TRGT, TRGTID> oneToOneRelation) {
		PersisterBuilderContext currentBuilderContext = PersisterBuilderContext.CURRENT.get();
		
		OneToOneConfigurerTemplate<C, TRGT, I, TRGTID, ?, ?, I> configurer;
		if (oneToOneRelation.isRelationOwnedByTarget()) {
			configurer = new OneToOneOwnedByTargetConfigurer<>(sourcePersister, oneToOneRelation, joinColumnNamingStrategy, foreignKeyNamingStrategy, dialect, connectionConfiguration);
		} else {
			configurer = new OneToOneOwnedBySourceConfigurer<>(sourcePersister, oneToOneRelation, joinColumnNamingStrategy, foreignKeyNamingStrategy);
		}
		
		String relationName = AccessorDefinition.giveDefinition(oneToOneRelation.getTargetProvider()).getName();
		
		EntityMappingConfiguration<TRGT, TRGTID> targetMappingConfiguration = oneToOneRelation.getTargetMappingConfiguration();
		if (currentBuilderContext.isCycling(targetMappingConfiguration)) {
			// cycle detected
			// we had a second phase load because cycle can hardly be supported by simply joining things together because at one time we will
			// fall into infinite loop (think to SQL generation of a cycling graph ...)
			Class<TRGT> targetEntityType = targetMappingConfiguration.getEntityType();
			// adding the relation to an eventually already existing cycle configurer for the entity
			OneToOneCycleConfigurer<TRGT> cycleSolver = (OneToOneCycleConfigurer<TRGT>)
					Iterables.find(currentBuilderContext.getBuildLifeCycleListeners(), p -> p instanceof OneToOneCycleConfigurer && ((OneToOneCycleConfigurer<?>) p).getEntityType() == targetEntityType);
			if (cycleSolver == null) {
				cycleSolver = new OneToOneCycleConfigurer<>(targetEntityType);
				currentBuilderContext.addBuildLifeCycleListener(cycleSolver);
			}
			cycleSolver.addCycleSolver(relationName, configurer);
			// Registering relation to EntityCriteria so one can use it as a criteria. Declared as a lazy initializer to work with lazy persister building such as cycling ones
			currentBuilderContext.addBuildLifeCycleListener(new GraphLoadingRelationRegisterer<C, I, TRGT>(targetMappingConfiguration.getEntityType(),
					oneToOneRelation.getTargetProvider(), sourcePersister.getClassToPersist(), null));
		} else {
			// please note that even if no table is found in configuration, build(..) will create one
			Table targetTable = nullable(oneToOneRelation.getTargetTable()).getOr(nullable(oneToOneRelation.getReverseColumn()).map(Column::getTable).get());
			ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister = new PersisterBuilderImpl<>(targetMappingConfiguration)
					.build(dialect, connectionConfiguration, targetTable);
			String relationJoinNodeName = configurer.configure(relationName, targetPersister, oneToOneRelation.isFetchSeparately());
			// Registering relation to EntityCriteria so one can use it as a criteria. Declared as a lazy initializer to work with lazy persister building such as cycling ones
			currentBuilderContext.addBuildLifeCycleListener(new GraphLoadingRelationRegisterer<C, I, TRGT>(targetMappingConfiguration.getEntityType(),
					oneToOneRelation.getTargetProvider(), sourcePersister.getClassToPersist(), relationJoinNodeName));
		}
		
	}
}