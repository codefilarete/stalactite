package org.gama.stalactite.persistence.engine.configurer;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.gama.lang.StringAppender;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
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
import org.gama.stalactite.persistence.engine.runtime.IEntityConfiguredJoinedTablesPersister;
import org.gama.stalactite.persistence.mapping.IEntityMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.IConnectionConfiguration;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * @author Guillaume Mary
 */
abstract class AbstractPolymorphicPersisterBuilder<C, I, T extends Table> implements PolymorphismBuilder<C, I, T> {
	
	/**
	 * Asserts that all given arguments are null, or all equals
	 *
	 * @param table1 any table, null accepted (that's the purpose of the method)
	 * @param table2 any table, null accepted (that's the purpose of the method)
	 */
	protected static void assertAllAreEqual(Table table1, Table table2) {
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
	protected final IEntityConfiguredJoinedTablesPersister<C, I> mainPersister;
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
												  IEntityConfiguredJoinedTablesPersister<C, I> mainPersister,
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
	protected void registerCascades(Map<Class<? extends C>, IEntityConfiguredJoinedTablesPersister<C, I>> persisterPerSubclass,
									Dialect dialect,
									IConnectionConfiguration connectionConfiguration,
									PersisterRegistry persisterRegistry) {
		for (SubEntityMappingConfiguration<? extends C> subConfiguration : this.polymorphismPolicy.getSubClasses()) {
			IEntityConfiguredJoinedTablesPersister<C, I> subEntityPersister = persisterPerSubclass.get(subConfiguration.getEntityType());
			
			if (subConfiguration.getPolymorphismPolicy() != null) {
				registerPolymorphismCascades(persisterPerSubclass, dialect, connectionConfiguration, persisterRegistry, subConfiguration,
						subEntityPersister);
			}
			
			// We register relation of sub class persister to take into account its specific one-to-ones, one-to-manys and element collection mapping
			registerRelationCascades(subConfiguration, dialect, connectionConfiguration, persisterRegistry, subEntityPersister);
		}
	}
	
	private void registerPolymorphismCascades(Map<Class<? extends C>, IEntityConfiguredJoinedTablesPersister<C, I>> persisterPerSubclass,
											  Dialect dialect,
											  IConnectionConfiguration connectionConfiguration,
											  PersisterRegistry persisterRegistry,
											  SubEntityMappingConfiguration<? extends C> subConfiguration,
											  IEntityConfiguredJoinedTablesPersister<C, I> subEntityPersister) {
		assertSubPolymorphismIsSupported(subConfiguration.getPolymorphismPolicy());
		IEntityConfiguredJoinedTablesPersister<? extends C, I> subclassPersister =
				buildSubPolymorphicPersister(subEntityPersister, subConfiguration.getPolymorphismPolicy(), dialect, connectionConfiguration,
						persisterRegistry);
		persisterPerSubclass.put(subConfiguration.getEntityType(), (IEntityConfiguredJoinedTablesPersister<C, I>) subclassPersister);
	}
	
	abstract void assertSubPolymorphismIsSupported(PolymorphismPolicy<? extends C> subPolymorphismPolicy);
	
	/**
	 * Creates a polymorphic persister for an already-polymorphic one.
	 *
	 * @param subPersister a sub class persister of our main persister
	 * @param subPolymorphismPolicy the sub persister {@link PolymorphismPolicy}
	 * @param dialect the {@link Dialect} use for type binding
	 * @param connectionConfiguration the connection configuration
	 * @param persisterRegistry {@link PersisterRegistry} used to check for already defined persister
	 */
	private IEntityConfiguredJoinedTablesPersister<? extends C, I> buildSubPolymorphicPersister(IEntityConfiguredJoinedTablesPersister<C, I> subPersister,
																								PolymorphismPolicy<? extends C> subPolymorphismPolicy,
																								Dialect dialect,
																								IConnectionConfiguration connectionConfiguration,
																								PersisterRegistry persisterRegistry) {
		IEntityMappingStrategy<C, I, T> mappingStrategy = subPersister.getMappingStrategy();
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
				mappingStrategy.getPropertyToColumn(),
				tableNamingStrategy);
		return polymorphismPersisterBuilder.build(dialect, connectionConfiguration, persisterRegistry);
	}
	
	private <D extends C> void registerRelationCascades(SubEntityMappingConfiguration<D> entityMappingConfiguration,
														Dialect dialect,
														IConnectionConfiguration connectionConfiguration,
														PersisterRegistry persisterRegistry,
														IEntityConfiguredJoinedTablesPersister<C, I> sourcePersister) {
		for (CascadeOne<D, ?, ?> cascadeOne : entityMappingConfiguration.getOneToOnes()) {
			CascadeOneConfigurer cascadeOneConfigurer = new CascadeOneConfigurer<>(dialect, connectionConfiguration, persisterRegistry,
					new PersisterBuilderImpl<>(cascadeOne.getTargetMappingConfiguration()));
			cascadeOneConfigurer.appendCascade(cascadeOne, sourcePersister, this.foreignKeyNamingStrategy, this.joinColumnNamingStrategy);
		}
		for (CascadeMany<D, ?, ?, ? extends Collection> cascadeMany : entityMappingConfiguration.getOneToManys()) {
			CascadeManyConfigurer cascadeManyConfigurer = new CascadeManyConfigurer<>(dialect, connectionConfiguration, persisterRegistry,
					new PersisterBuilderImpl<>(cascadeMany.getTargetMappingConfiguration()))
					// we must give primary key else reverse foreign key will target subclass table, which creates 2 fk in case of reuse of target persister
					.setSourcePrimaryKey((Column) Iterables.first(mainPersister.getMappingStrategy().getTargetTable().getPrimaryKey().getColumns()));
			cascadeManyConfigurer.appendCascade(cascadeMany, sourcePersister,
					this.foreignKeyNamingStrategy,
					this.joinColumnNamingStrategy,
					this.indexColumnNamingStrategy,
					this.associationTableNamingStrategy);
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
