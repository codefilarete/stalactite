package org.codefilarete.stalactite.engine.configurer.manyToOne;

import java.util.Collection;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.dsl.naming.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.JoinColumnNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.TableNamingStrategy;
import org.codefilarete.stalactite.engine.configurer.AbstractRelationConfigurer;
import org.codefilarete.stalactite.engine.configurer.EntityMappingConfigurationWithTable;
import org.codefilarete.stalactite.engine.configurer.builder.PersisterBuilderContext;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.Iterables;

/**
 * Wrapper for {@link ManyToOneOwnedBySourceConfigurer} to make them available for cycle.
 *
 * @param <C> type of input (left/source entities)
 * @param <I> identifier type of source entities
 * @param <TRGT> type of output (right/target entities)
 * @param <TRGTID>> identifier type of target entities
 * @author Guillaume Mary
 */
public class ManyToOneRelationConfigurer<C, I, TRGT, TRGTID> extends AbstractRelationConfigurer<C, I, TRGT, TRGTID> {
	private final JoinColumnNamingStrategy joinColumnNamingStrategy;
	private final ForeignKeyNamingStrategy foreignKeyNamingStrategy;
	
	public ManyToOneRelationConfigurer(Dialect dialect,
									   ConnectionConfiguration connectionConfiguration,
									   ConfiguredRelationalPersister<C, I> sourcePersister,
									   TableNamingStrategy tableNamingStrategy,
									   JoinColumnNamingStrategy joinColumnNamingStrategy,
									   ForeignKeyNamingStrategy foreignKeyNamingStrategy,
									   PersisterBuilderContext currentBuilderContext) {
		super(dialect, connectionConfiguration, sourcePersister, tableNamingStrategy, currentBuilderContext);
		this.joinColumnNamingStrategy = joinColumnNamingStrategy;
		this.foreignKeyNamingStrategy = foreignKeyNamingStrategy;
	}
	
	/**
	 * Setup given one-to-one relation by adding write cascade as well as creating appropriate joins in 
	 *
	 * @param manyToOneRelation the relation to be configured
	 */
	public void configure(ManyToOneRelation<C, TRGT, TRGTID, Collection<C>> manyToOneRelation) {
		ManyToOneOwnedBySourceConfigurer<C, TRGT, I, TRGTID, ?, ?, I> configurer;
		configurer = new ManyToOneOwnedBySourceConfigurer<>(sourcePersister, manyToOneRelation, joinColumnNamingStrategy, foreignKeyNamingStrategy);
		
		String relationName = AccessorDefinition.giveDefinition(manyToOneRelation.getTargetProvider()).getName();
		
		EntityMappingConfiguration<TRGT, TRGTID> targetMappingConfiguration = manyToOneRelation.getTargetMappingConfiguration();
		if (currentBuilderContext.isCycling(targetMappingConfiguration)) {
			// cycle detected
			// we had a second phase load because cycle can hardly be supported by simply joining things together because at one time we will
			// fall into infinite loop (think to SQL generation of a cycling graph ...)
			Class<TRGT> targetEntityType = targetMappingConfiguration.getEntityType();
			// adding the relation to an eventually already existing cycle configurer for the entity
			ManyToOneCycleConfigurer<TRGT> cycleSolver = (ManyToOneCycleConfigurer<TRGT>)
					Iterables.find(currentBuilderContext.getBuildLifeCycleListeners(), p -> p instanceof ManyToOneCycleConfigurer && ((ManyToOneCycleConfigurer<?>) p).getEntityType() == targetEntityType);
			if (cycleSolver == null) {
				cycleSolver = new ManyToOneCycleConfigurer<>(targetEntityType);
				currentBuilderContext.addBuildLifeCycleListener(cycleSolver);
			}
			cycleSolver.addCycleSolver(relationName, configurer);
		} else {
			// please note that even if no table is found in configuration, build(..) will create one
			Table targetTable = determineTargetTable(manyToOneRelation);
			ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister = persisterBuilder.build(new EntityMappingConfigurationWithTable<>(targetMappingConfiguration, targetTable));
			configurer.configure(relationName, targetPersister, manyToOneRelation.isFetchSeparately());
		}
	}
	
	private Table determineTargetTable(ManyToOneRelation<C, TRGT, TRGTID, Collection<C>> manyToOneRelation) {
		EntityMappingConfiguration<TRGT, TRGTID> targetMappingConfiguration = manyToOneRelation.getTargetMappingConfiguration();
		Table targetTable = targetMappingConfiguration.getTable();
		if (targetTable == null) {
			targetTable = lookupTableInRegisteredPersisters(targetMappingConfiguration.getEntityType());
		}
		return targetTable;
	}
}