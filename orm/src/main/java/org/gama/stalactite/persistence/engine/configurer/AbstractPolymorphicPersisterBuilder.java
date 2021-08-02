package org.gama.stalactite.persistence.engine.configurer;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.gama.lang.StringAppender;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.reflection.AccessorDefinition;
import org.gama.stalactite.persistence.engine.AssociationTableNamingStrategy;
import org.gama.stalactite.persistence.engine.ColumnNamingStrategy;
import org.gama.stalactite.persistence.engine.ElementCollectionTableNamingStrategy;
import org.gama.stalactite.persistence.engine.ForeignKeyNamingStrategy;
import org.gama.stalactite.persistence.engine.MappingConfigurationException;
import org.gama.stalactite.persistence.engine.PersisterRegistry;
import org.gama.stalactite.persistence.engine.PolymorphismPolicy;
import org.gama.stalactite.persistence.engine.SubEntityMappingConfiguration;
import org.gama.stalactite.persistence.engine.TableNamingStrategy;
import org.gama.stalactite.persistence.engine.configurer.BeanMappingBuilder.ColumnNameProvider;
import org.gama.stalactite.persistence.engine.configurer.PersisterBuilderImpl.Identification;
import org.gama.stalactite.persistence.engine.configurer.PersisterBuilderImpl.PolymorphismBuilder;
import org.gama.stalactite.persistence.engine.configurer.PersisterBuilderImpl.PostInitializer;
import org.gama.stalactite.persistence.engine.runtime.EntityConfiguredJoinedTablesPersister;
import org.gama.stalactite.persistence.engine.runtime.cycle.OneToOneCycleConfigurer;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.ConnectionConfiguration;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * @author Guillaume Mary
 */
abstract class AbstractPolymorphicPersisterBuilder<C, I, T extends Table> implements PolymorphismBuilder<C, I, T> {
	
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
	protected final EntityConfiguredJoinedTablesPersister<C, I> mainPersister;
	protected final Identification identification;
	protected final ColumnBinderRegistry columnBinderRegistry;
	protected final ColumnNameProvider columnNameProvider;
	
	protected final ColumnNamingStrategy columnNamingStrategy;
	protected final ForeignKeyNamingStrategy foreignKeyNamingStrategy;
	protected final ElementCollectionTableNamingStrategy elementCollectionTableNamingStrategy;
	protected final ColumnNamingStrategy joinColumnNamingStrategy;
	protected final ColumnNamingStrategy indexColumnNamingStrategy;
	protected final AssociationTableNamingStrategy associationTableNamingStrategy;
	protected final TableNamingStrategy tableNamingStrategy;
	
	protected AbstractPolymorphicPersisterBuilder(PolymorphismPolicy<C> polymorphismPolicy,
												  Identification identification,
												  EntityConfiguredJoinedTablesPersister<C, I> mainPersister,
												  ColumnBinderRegistry columnBinderRegistry,
												  ColumnNameProvider columnNameProvider,
												  ColumnNamingStrategy columnNamingStrategy,
												  ForeignKeyNamingStrategy foreignKeyNamingStrategy,
												  ElementCollectionTableNamingStrategy elementCollectionTableNamingStrategy,
												  ColumnNamingStrategy joinColumnNamingStrategy,
												  ColumnNamingStrategy indexColumnNamingStrategy,
												  AssociationTableNamingStrategy associationTableNamingStrategy,
												  TableNamingStrategy tableNamingStrategy) {
		this.polymorphismPolicy = polymorphismPolicy;
		this.identification = identification;
		this.mainPersister = mainPersister;
		this.columnBinderRegistry = columnBinderRegistry;
		this.columnNameProvider = columnNameProvider;
		this.columnNamingStrategy = columnNamingStrategy;
		this.foreignKeyNamingStrategy = foreignKeyNamingStrategy;
		this.elementCollectionTableNamingStrategy = elementCollectionTableNamingStrategy;
		this.joinColumnNamingStrategy = joinColumnNamingStrategy;
		this.indexColumnNamingStrategy = indexColumnNamingStrategy;
		this.associationTableNamingStrategy = associationTableNamingStrategy;
		this.tableNamingStrategy = tableNamingStrategy;
	}
	
	/**
	 * Adds relations to given persisters which are expected to be subclass's one. Relations are not only one-to-one and one-to-many ones but also
	 * Polymorphism ones (sub class can also be polymophic, resulting in a kind of resursive call to create a polymorphism tree)
	 *
	 * <strong>Given persiter Map may be modified</strong> by this method by replacing persisters by new ones.
	 *
	 * @param persisterPerSubclass persisters that need relation
	 * @param dialect the {@link Dialect} use for type binding
	 * @param connectionConfiguration the connection configuration
	 * @param persisterRegistry {@link PersisterRegistry} used to check for already defined persister
	 */
	protected <D extends C> void registerCascades(Map<Class<? extends C>, EntityConfiguredJoinedTablesPersister<C, I>> persisterPerSubclass,
									Dialect dialect,
									ConnectionConfiguration connectionConfiguration,
									PersisterRegistry persisterRegistry) {
		// we surround our relation configuration with cycle detection (see registerRelationCascades(..) implementation), this may seem too wide and
		// could be closer to registerRelationCascades(..) method call (which actually requires it) but as doing such we also cover the case of 2
		// subconfigurations using same entity in their relation 
		PersisterBuilderContext.CURRENT.get().runInContext(mainPersister, () -> {
			for (SubEntityMappingConfiguration<? extends C> subConfiguration : this.polymorphismPolicy.getSubClasses()) {
				EntityConfiguredJoinedTablesPersister<C, I> subEntityPersister = persisterPerSubclass.get(subConfiguration.getEntityType());
				
				if (subConfiguration.getPolymorphismPolicy() != null) {
					registerPolymorphismCascades(persisterPerSubclass, dialect, connectionConfiguration, persisterRegistry, subConfiguration, subEntityPersister);
				}
				
				// We register relation of sub class persister to take into account its specific one-to-ones, one-to-manys and element collection mapping
				registerRelationCascades(
						(SubEntityMappingConfiguration<D>) subConfiguration,
						dialect,
						connectionConfiguration,
						persisterRegistry,
						(EntityConfiguredJoinedTablesPersister<D, I>) subEntityPersister);
			}
		});
	}
	
	private void registerPolymorphismCascades(Map<Class<? extends C>, EntityConfiguredJoinedTablesPersister<C, I>> persisterPerSubclass,
											  Dialect dialect,
											  ConnectionConfiguration connectionConfiguration,
											  PersisterRegistry persisterRegistry,
											  SubEntityMappingConfiguration<? extends C> subConfiguration,
											  EntityConfiguredJoinedTablesPersister<C, I> subEntityPersister) {
		assertSubPolymorphismIsSupported(subConfiguration.getPolymorphismPolicy());
		EntityConfiguredJoinedTablesPersister<? extends C, I> subclassPersister =
				buildSubPolymorphicPersister(subEntityPersister, subConfiguration.getPolymorphismPolicy(), dialect, connectionConfiguration,
						persisterRegistry);
		persisterPerSubclass.put(subConfiguration.getEntityType(), (EntityConfiguredJoinedTablesPersister<C, I>) subclassPersister);
	}
	
	abstract void assertSubPolymorphismIsSupported(PolymorphismPolicy<? extends C> subPolymorphismPolicy);
	
	/**
	 * Creates a polymorphic persister for an already-polymorphic case (this class) : used when main persister subclasses are also polymorphic.
	 *
	 * @param subPersister a sub class persister of our main persister
	 * @param subPolymorphismPolicy the sub persister {@link PolymorphismPolicy}
	 * @param dialect the {@link Dialect} use for type binding
	 * @param connectionConfiguration the connection configuration
	 * @param persisterRegistry {@link PersisterRegistry} used to check for already defined persister
	 */
	private EntityConfiguredJoinedTablesPersister<? extends C, I> buildSubPolymorphicPersister(EntityConfiguredJoinedTablesPersister<C, I> subPersister,
																							   PolymorphismPolicy<? extends C> subPolymorphismPolicy,
																							   Dialect dialect,
																							   ConnectionConfiguration connectionConfiguration,
																							   PersisterRegistry persisterRegistry) {
		// we only have to call a polymmoprhic builder with given methods arguments, and same configuration values as this instance
		PolymorphismPersisterBuilder<? extends C, I, T> polymorphismPersisterBuilder = new PolymorphismPersisterBuilder(
				subPolymorphismPolicy,
				identification,
				subPersister,
				columnBinderRegistry,
				columnNameProvider,
				columnNamingStrategy,
				foreignKeyNamingStrategy,
				elementCollectionTableNamingStrategy,
				joinColumnNamingStrategy,
				indexColumnNamingStrategy,
				associationTableNamingStrategy,
				subPersister.getMappingStrategy().getPropertyToColumn(),
				tableNamingStrategy);
		return polymorphismPersisterBuilder.build(dialect, connectionConfiguration, persisterRegistry);
	}
	
	private <D extends C, TRGT> void registerRelationCascades(SubEntityMappingConfiguration<D> entityMappingConfiguration,
															  Dialect dialect,
															  ConnectionConfiguration connectionConfiguration,
															  PersisterRegistry persisterRegistry,
															  EntityConfiguredJoinedTablesPersister<D, I> sourcePersister) {
		
		PersisterBuilderContext currentBuilderContext = PersisterBuilderContext.CURRENT.get();
		
		for (CascadeOne<D, TRGT, Object> cascadeOne : (List<CascadeOne<D, TRGT, Object>>) (List) entityMappingConfiguration.getOneToOnes()) {
			CascadeOneConfigurer<D, TRGT, I, Object> cascadeOneConfigurer = new CascadeOneConfigurer<>(
					cascadeOne,
					sourcePersister,
					dialect,
					connectionConfiguration,
					persisterRegistry,
					this.foreignKeyNamingStrategy,
					this.joinColumnNamingStrategy);
			
			String relationName = AccessorDefinition.giveDefinition(cascadeOne.getTargetProvider()).getName();
			
			if (currentBuilderContext.isCycling(cascadeOne.getTargetMappingConfiguration())) {
				// cycle detected
				// we had a second phase load because cycle can hardly be supported by simply joining things together because at one time we will
				// fall into infinite loop (think to SQL generation of a cycling graph ...)
				Class<TRGT> targetEntityType = cascadeOne.getTargetMappingConfiguration().getEntityType();
				// adding the relation to an eventually already existing cycle configurer for the entity
				OneToOneCycleConfigurer<TRGT> cycleSolver = (OneToOneCycleConfigurer<TRGT>)
						Iterables.find(currentBuilderContext.getPostInitializers(), p -> p instanceof OneToOneCycleConfigurer && p.getEntityType() == targetEntityType);
				if (cycleSolver == null) {
					cycleSolver = new OneToOneCycleConfigurer<>(targetEntityType);
					currentBuilderContext.addPostInitializers(cycleSolver);
				}
				cycleSolver.addCycleSolver(relationName, cascadeOneConfigurer);
			} else {
				cascadeOneConfigurer.appendCascades(relationName, new PersisterBuilderImpl<>(cascadeOne.getTargetMappingConfiguration()));
			}
		}
		for (CascadeMany<D, ?, ?, ? extends Collection> cascadeMany : entityMappingConfiguration.getOneToManys()) {
			CascadeManyConfigurer cascadeManyConfigurer = new CascadeManyConfigurer<>(cascadeMany, sourcePersister,
					dialect,
					connectionConfiguration,
					persisterRegistry,
					this.foreignKeyNamingStrategy,
					this.joinColumnNamingStrategy,
					this.associationTableNamingStrategy,
					this.indexColumnNamingStrategy)
					// we must give primary key else reverse foreign key will target subclass table, which creates 2 fk in case of reuse of target persister
					.setSourcePrimaryKey((Column) Iterables.first(mainPersister.getMappingStrategy().getTargetTable().getPrimaryKey().getColumns()));
			if (currentBuilderContext.isCycling(cascadeMany.getTargetMappingConfiguration())) {
				// cycle detected
				// we add a second phase load because cycle can hardly be supported by simply joining things together, in particular due to that
				// Query and SQL generation don't support several instances of table and columns in them (aliases generation must be inhanced), and
				// overall column reading will be messed up because of that (to avoid all of this we should have mapping strategy clones)
				PostInitializer postInitializer = new PostInitializer(cascadeMany.getTargetMappingConfiguration().getEntityType()) {
					@Override
					public void consume(EntityConfiguredJoinedTablesPersister targetPersister) {
						cascadeManyConfigurer.appendCascade(cascadeMany,
								sourcePersister,
								foreignKeyNamingStrategy,
								joinColumnNamingStrategy,
								indexColumnNamingStrategy,
								associationTableNamingStrategy,
								targetPersister);
					}
				};
				PersisterBuilderContext.CURRENT.get().addPostInitializers(postInitializer);
			} else {
				cascadeManyConfigurer.appendCascade(cascadeMany, sourcePersister,
						this.foreignKeyNamingStrategy,
						this.joinColumnNamingStrategy,
						this.indexColumnNamingStrategy,
						this.associationTableNamingStrategy,
						new PersisterBuilderImpl<>(cascadeMany.getTargetMappingConfiguration()));
			}
		}
		// Please note that as a difference with PersisterBuilderImpl, we don't need to register relation in select because polymorphic selection
		// is made in two phases, see JoinedTablesPolymorphismEntitySelectExecutor (instanciated in JoinedTablesPolymorphicPersister)
		
		// taking element collections into account
		for (ElementCollectionLinkage<D, ?, ? extends Collection> elementCollection : entityMappingConfiguration.getElementCollections()) {
			ElementCollectionCascadeConfigurer elementCollectionCascadeConfigurer = new ElementCollectionCascadeConfigurer(dialect,
					connectionConfiguration);
			elementCollectionCascadeConfigurer.appendCascade(elementCollection, sourcePersister, this.foreignKeyNamingStrategy,
					this.columnNamingStrategy,
					this.elementCollectionTableNamingStrategy);
		}
	}
}
