package org.codefilarete.stalactite.engine.configurer.polymorphism;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.stalactite.engine.MappingConfigurationException;
import org.codefilarete.stalactite.engine.PersisterRegistry;
import org.codefilarete.stalactite.engine.PolymorphismPolicy;
import org.codefilarete.stalactite.engine.SubEntityMappingConfiguration;
import org.codefilarete.stalactite.engine.configurer.AbstractIdentification;
import org.codefilarete.stalactite.engine.configurer.AbstractIdentification.Identification;
import org.codefilarete.stalactite.engine.configurer.NamingConfiguration;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderContext;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImpl;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImpl.PostInitializer;
import org.codefilarete.stalactite.engine.configurer.elementcollection.ElementCollectionRelation;
import org.codefilarete.stalactite.engine.configurer.elementcollection.ElementCollectionRelationConfigurer;
import org.codefilarete.stalactite.engine.configurer.onetomany.OneToManyRelation;
import org.codefilarete.stalactite.engine.configurer.onetomany.OneToManyRelationConfigurer;
import org.codefilarete.stalactite.engine.configurer.onetoone.OneToOneRelation;
import org.codefilarete.stalactite.engine.configurer.onetoone.OneToOneRelationConfigurer;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.cycle.OneToOneCycleConfigurer;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;

/**
 * @author Guillaume Mary
 */
abstract class AbstractPolymorphicPersisterBuilder<C, I, T extends Table<T>> implements PolymorphismBuilder<C, I, T> {
	
	/**
	 * Asserts that given arguments are null, or equal
	 *
	 * @param table1 any table, null accepted (that's the purpose of the method)
	 * @param table2 any table, null accepted (that's the purpose of the method)
	 */
	protected static void assertNullOrEqual(Table table1, Table table2) {
		Set<Table> availableTables = Arrays.asHashSet(table1, table2);
		availableTables.remove(null);
		if (availableTables.size() > 1) {
			class TableAppender extends StringAppender {
				@Override
				public StringAppender cat(Object o) {
					if (o instanceof Table) {
						return super.cat(((Table) o).getName());
					} else {
						return super.cat(o);
					}
				}
			}
			throw new MappingConfigurationException("Table declared in inheritance is different from given one in embeddable properties override : "
					+ new TableAppender().ccat(availableTables, ", "));
		}
	}
	
	protected final PolymorphismPolicy<C> polymorphismPolicy;
	protected final ConfiguredRelationalPersister<C, I> mainPersister;
	protected final AbstractIdentification<C, I> identification;
	protected final ColumnBinderRegistry columnBinderRegistry;
	
	protected final NamingConfiguration namingConfiguration;
	
	protected AbstractPolymorphicPersisterBuilder(PolymorphismPolicy<C> polymorphismPolicy,
												  AbstractIdentification<C, I> identification,
												  ConfiguredRelationalPersister<C, I> mainPersister,
												  ColumnBinderRegistry columnBinderRegistry,
												  NamingConfiguration namingConfiguration) {
		this.polymorphismPolicy = polymorphismPolicy;
		this.identification = identification;
		this.mainPersister = mainPersister;
		this.columnBinderRegistry = columnBinderRegistry;
		this.namingConfiguration = namingConfiguration;
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
	 * @param persisterRegistry {@link PersisterRegistry} used to check for already defined persister
	 */
	protected <D extends C> void registerCascades(Map<Class<D>, ConfiguredRelationalPersister<D, I>> persisterPerSubclass,
												  Dialect dialect,
												  ConnectionConfiguration connectionConfiguration,
												  PersisterRegistry persisterRegistry) {
		// we surround our relation configuration with cycle detection (see registerRelationCascades(..) implementation), this may seem too wide and
		// could be closer to registerRelationCascades(..) method call (which actually requires it) but as doing such we also cover the case of 2
		// subconfigurations using same entity in their relation 
		PersisterBuilderContext.CURRENT.get().runInContext(mainPersister, () -> {
			for (SubEntityMappingConfiguration<D> subConfiguration : (Set<SubEntityMappingConfiguration<D>>) (Set) this.polymorphismPolicy.getSubClasses()) {
				ConfiguredRelationalPersister<D, I> subEntityPersister = persisterPerSubclass.get(subConfiguration.getEntityType());
				
				if (subConfiguration.getPolymorphismPolicy() != null) {
					registerPolymorphismCascades(persisterPerSubclass, dialect, connectionConfiguration, persisterRegistry, subConfiguration, subEntityPersister);
				}
				
				// We register relation of subclass persister to take into account its specific one-to-ones, one-to-manys and element collection mapping
				registerRelationCascades(
						subConfiguration,
						dialect,
						connectionConfiguration,
						persisterRegistry,
						subEntityPersister);
			}
		});
	}
	
	private <D extends C> void registerPolymorphismCascades(Map<Class<D>, ConfiguredRelationalPersister<D, I>> persisterPerSubclass,
															Dialect dialect,
															ConnectionConfiguration connectionConfiguration,
															PersisterRegistry persisterRegistry,
															SubEntityMappingConfiguration<D> subConfiguration,
															ConfiguredRelationalPersister<D, I> subEntityPersister) {
		assertSubPolymorphismIsSupported(subConfiguration.getPolymorphismPolicy());
		ConfiguredRelationalPersister<D, I> subclassPersister =
				buildSubPolymorphicPersister(subEntityPersister, subConfiguration.getPolymorphismPolicy(), dialect, connectionConfiguration,
						persisterRegistry);
		persisterPerSubclass.put(subConfiguration.getEntityType(), subclassPersister);
	}
	
	abstract void assertSubPolymorphismIsSupported(PolymorphismPolicy<? extends C> subPolymorphismPolicy);
	
	/**
	 * Creates a polymorphic persister for an already-polymorphic case (this class) : used when main persister subclasses are also polymorphic.
	 *
	 * @param subPersister a subclass persister of our main persister
	 * @param subPolymorphismPolicy the sub persister {@link PolymorphismPolicy}
	 * @param dialect the {@link Dialect} use for type binding
	 * @param connectionConfiguration the connection configuration
	 * @param persisterRegistry {@link PersisterRegistry} used to check for already defined persister
	 */
	private <D extends C> ConfiguredRelationalPersister<D, I> buildSubPolymorphicPersister(ConfiguredRelationalPersister<D, I> subPersister,
																								 PolymorphismPolicy<D> subPolymorphismPolicy,
																								 Dialect dialect,
																								 ConnectionConfiguration connectionConfiguration,
																								 PersisterRegistry persisterRegistry) {
		// we only have to call a polymorphic builder with given methods arguments, and same configuration values as this instance
		PolymorphismPersisterBuilder<D, I, T> polymorphismPersisterBuilder = new PolymorphismPersisterBuilder<>(
				subPolymorphismPolicy,
				(Identification<D, I>) identification,
				subPersister,
				columnBinderRegistry,
				(Map) subPersister.getMapping().getPropertyToColumn(),
				(Map) subPersister.getMapping().getReadonlyPropertyToColumn(),
				subPersister.getMapping().getReadConverters(),
				subPersister.getMapping().getWriteConverters(),
				namingConfiguration);
		return polymorphismPersisterBuilder.build(dialect, connectionConfiguration, persisterRegistry);
	}
	
	private <D extends C, TRGT> void registerRelationCascades(SubEntityMappingConfiguration<D> entityMappingConfiguration,
															  Dialect dialect,
															  ConnectionConfiguration connectionConfiguration,
															  PersisterRegistry persisterRegistry,
															  ConfiguredRelationalPersister<D, I> subEntityPersister) {
		
		PersisterBuilderContext currentBuilderContext = PersisterBuilderContext.CURRENT.get();
		
		for (OneToOneRelation<D, TRGT, Object> oneToOneRelation : (List<OneToOneRelation<D, TRGT, Object>>) (List) entityMappingConfiguration.getOneToOnes()) {
			OneToOneRelationConfigurer<D, TRGT, I, Object> oneToOneRelationConfigurer = new OneToOneRelationConfigurer<>(
					oneToOneRelation,
					subEntityPersister,
					dialect,
					connectionConfiguration,
					persisterRegistry,
					this.namingConfiguration.getForeignKeyNamingStrategy(),
					this.namingConfiguration.getJoinColumnNamingStrategy());
			
			String relationName = AccessorDefinition.giveDefinition(oneToOneRelation.getTargetProvider()).getName();
			
			if (currentBuilderContext.isCycling(oneToOneRelation.getTargetMappingConfiguration())) {
				// cycle detected
				// we had a second phase load because cycle can hardly be supported by simply joining things together because at one time we will
				// fall into infinite loop (think to SQL generation of a cycling graph ...)
				Class<TRGT> targetEntityType = oneToOneRelation.getTargetMappingConfiguration().getEntityType();
				// adding the relation to an eventually already existing cycle configurer for the entity
				OneToOneCycleConfigurer<TRGT> cycleSolver = (OneToOneCycleConfigurer<TRGT>)
						Iterables.find(currentBuilderContext.getBuildLifeCycleListeners(), p -> p instanceof OneToOneCycleConfigurer && ((OneToOneCycleConfigurer<?>) p).getEntityType() == targetEntityType);
				if (cycleSolver == null) {
					cycleSolver = new OneToOneCycleConfigurer<>(targetEntityType);
					currentBuilderContext.addBuildLifeCycleListener(cycleSolver);
				}
				cycleSolver.addCycleSolver(relationName, oneToOneRelationConfigurer);
			} else {
				oneToOneRelationConfigurer.configure(relationName, new PersisterBuilderImpl<>(oneToOneRelation.getTargetMappingConfiguration()), oneToOneRelation.isFetchSeparately());
			}
		}
		for (OneToManyRelation<D, ?, ?, ? extends Collection> oneToManyRelation : entityMappingConfiguration.getOneToManys()) {
			OneToManyRelationConfigurer oneToManyRelationConfigurer = new OneToManyRelationConfigurer<>(oneToManyRelation, subEntityPersister,
					dialect,
					connectionConfiguration,
					persisterRegistry,
					this.namingConfiguration.getForeignKeyNamingStrategy(),
					this.namingConfiguration.getJoinColumnNamingStrategy(),
					this.namingConfiguration.getAssociationTableNamingStrategy(),
					this.namingConfiguration.getIndexColumnNamingStrategy());
			if (currentBuilderContext.isCycling(oneToManyRelation.getTargetMappingConfiguration())) {
				// cycle detected
				// we add a second phase load because cycle can hardly be supported by simply joining things together, in particular due to that
				// Query and SQL generation don't support several instances of table and columns in them (aliases generation must be enhanced), and
				// overall column reading will be messed up because of that (to avoid all of this we should have mapping strategy clones)
				PostInitializer postInitializer = new PostInitializer(oneToManyRelation.getTargetMappingConfiguration().getEntityType()) {
					@Override
					public void consume(ConfiguredRelationalPersister targetPersister) {
						oneToManyRelationConfigurer.configure((PersisterBuilderImpl) targetPersister);
					}
				};
				PersisterBuilderContext.CURRENT.get().addBuildLifeCycleListener(postInitializer);
			} else {
				oneToManyRelationConfigurer.configure(
						new PersisterBuilderImpl<>(oneToManyRelation.getTargetMappingConfiguration()));
			}
		}
		// Please note that as a difference with PersisterBuilderImpl, we don't need to register relation in select because polymorphic selection
		// is made in two phases, see JoinTablePolymorphismEntitySelectExecutor (instantiated in JoinTablePolymorphismPersister)
		
		// taking element collections into account
		for (ElementCollectionRelation<D, ?, ? extends Collection> elementCollection : entityMappingConfiguration.getElementCollections()) {
			ElementCollectionRelationConfigurer<D, ?, I, ? extends Collection> elementCollectionRelationConfigurer = new ElementCollectionRelationConfigurer<>(
					elementCollection,
					subEntityPersister,
					this.namingConfiguration.getForeignKeyNamingStrategy(),
					this.namingConfiguration.getColumnNamingStrategy(),
					this.namingConfiguration.getElementCollectionTableNamingStrategy(),
					dialect,
					connectionConfiguration);
			elementCollectionRelationConfigurer.configure();
		}
	}
}
