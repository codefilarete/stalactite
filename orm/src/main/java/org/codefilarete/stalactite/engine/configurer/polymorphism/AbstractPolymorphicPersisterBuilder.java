package org.codefilarete.stalactite.engine.configurer.polymorphism;

import java.util.Map;
import java.util.Set;

import org.codefilarete.stalactite.dsl.PolymorphismPolicy;
import org.codefilarete.stalactite.dsl.RelationalMappingConfiguration;
import org.codefilarete.stalactite.dsl.subentity.SubEntityMappingConfiguration;
import org.codefilarete.stalactite.engine.configurer.AbstractIdentification;
import org.codefilarete.stalactite.engine.configurer.AbstractIdentification.SingleColumnIdentification;
import org.codefilarete.stalactite.engine.configurer.NamingConfiguration;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderContext;
import org.codefilarete.stalactite.engine.configurer.RelationConfigurer;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;

/**
 * @author Guillaume Mary
 */
abstract class AbstractPolymorphicPersisterBuilder<C, I, T extends Table<T>> implements PolymorphismBuilder<C, I, T> {
	
	protected final PolymorphismPolicy<C> polymorphismPolicy;
	protected final ConfiguredRelationalPersister<C, I> mainPersister;
	protected final AbstractIdentification<C, I> identification;
	protected final ColumnBinderRegistry columnBinderRegistry;
	
	protected final NamingConfiguration namingConfiguration;
	protected final PersisterBuilderContext persisterBuilderContext;
	
	protected AbstractPolymorphicPersisterBuilder(PolymorphismPolicy<C> polymorphismPolicy,
												  AbstractIdentification<C, I> identification,
												  ConfiguredRelationalPersister<C, I> mainPersister,
												  ColumnBinderRegistry columnBinderRegistry,
												  NamingConfiguration namingConfiguration,
												  PersisterBuilderContext persisterBuilderContext) {
		this.polymorphismPolicy = polymorphismPolicy;
		this.identification = identification;
		this.mainPersister = mainPersister;
		this.columnBinderRegistry = columnBinderRegistry;
		this.namingConfiguration = namingConfiguration;
		this.persisterBuilderContext = persisterBuilderContext;
	}
	
	/**
	 * Adds relations to given persisters which are expected to be subclass's one. Relations are not only one-to-one and one-to-many ones but also
	 * Polymorphism ones (subclass can also be polymorphic, resulting in a kind of recursive call to create a polymorphism tree)
	 *
	 * <strong>Given persister Map may be modified</strong> by this method by replacing persisters by new ones.
	 *
	 * @param persisterPerSubclass persisters that need relation
	 * @param dialect the {@link Dialect} use for type binding
	 * @param connectionConfiguration the connection configuration
	 */
	protected <D extends C> void registerSubEntitiesRelations(Map<Class<D>, ConfiguredRelationalPersister<D, I>> persisterPerSubclass,
															  Dialect dialect,
															  ConnectionConfiguration connectionConfiguration) {
		// we surround our relation configuration with cycle detection (see registerRelationCascades(..) implementation), this may seem too wide and
		// could be closer to registerRelationCascades(..) method call (which actually requires it) but as doing such we also cover the case of 2
		// subconfigurations using same entity in their relation 
		persisterBuilderContext.runInContext(mainPersister, () -> {
			for (SubEntityMappingConfiguration<D> subConfiguration : (Set<SubEntityMappingConfiguration<D>>) (Set) this.polymorphismPolicy.getSubClasses()) {
				ConfiguredRelationalPersister<D, I> subEntityPersister = persisterPerSubclass.get(subConfiguration.getEntityType());
				
				if (subConfiguration.getPolymorphismPolicy() != null) {
					assertSubPolymorphismIsSupported(subConfiguration.getPolymorphismPolicy());
					subEntityPersister = buildSubPolymorphicPersister(subEntityPersister, subConfiguration.getPolymorphismPolicy(), dialect, connectionConfiguration);
					persisterPerSubclass.put(subConfiguration.getEntityType(), subEntityPersister);
				}
				
				// We register relation of subclass persister to take into account its specific one-to-ones, one-to-manys and element collection mapping
				this.registerRelationCascades(
						subConfiguration,
						dialect,
						connectionConfiguration,
						subEntityPersister);
			}
		});
	}
	
	abstract void assertSubPolymorphismIsSupported(PolymorphismPolicy<? extends C> subPolymorphismPolicy);
	
	/**
	 * Creates a polymorphic persister for an already-polymorphic case (this class) : used when main persister subclasses are also polymorphic.
	 *
	 * @param subPersister a subclass persister of our main persister
	 * @param subPolymorphismPolicy the sub persister {@link PolymorphismPolicy}
	 * @param dialect the {@link Dialect} use for type binding
	 * @param connectionConfiguration the connection configuration
	 */
	private <D extends C> ConfiguredRelationalPersister<D, I> buildSubPolymorphicPersister(ConfiguredRelationalPersister<D, I> subPersister,
																						   PolymorphismPolicy<D> subPolymorphismPolicy,
																						   Dialect dialect,
																						   ConnectionConfiguration connectionConfiguration) {
		// we only have to call a polymorphic builder with given methods arguments, and same configuration values as this instance
		PolymorphismPersisterBuilder<D, I, T> polymorphismPersisterBuilder = new PolymorphismPersisterBuilder<>(
				subPolymorphismPolicy,
				(SingleColumnIdentification<D, I>) identification,
				subPersister,
				columnBinderRegistry,
				(Map) subPersister.getMapping().getPropertyToColumn(),
				(Map) subPersister.getMapping().getReadonlyPropertyToColumn(),
				subPersister.getMapping().getReadConverters(),
				subPersister.getMapping().getWriteConverters(),
				namingConfiguration,
				persisterBuilderContext);
		return polymorphismPersisterBuilder.build(dialect, connectionConfiguration);
	}
	
	private <D extends C> void registerRelationCascades(RelationalMappingConfiguration<D> entityMappingConfiguration,
														Dialect dialect,
														ConnectionConfiguration connectionConfiguration,
														ConfiguredRelationalPersister<D, I> subEntityPersister) {
		// Note that for now polymorphism configuration doesn't support many-to-many nor Map relation
		RelationConfigurer<D, I> relationConfigurer = new RelationConfigurer<>(dialect,
				connectionConfiguration,
				subEntityPersister,
				namingConfiguration,
				persisterBuilderContext);
		relationConfigurer.configureRelations(entityMappingConfiguration);
	}
}
