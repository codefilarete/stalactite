package org.codefilarete.stalactite.engine.configurer.polymorphism;

import java.util.Map;
import java.util.Set;

import org.codefilarete.stalactite.engine.MappingConfigurationException;
import org.codefilarete.stalactite.engine.PolymorphismPolicy;
import org.codefilarete.stalactite.engine.RelationalMappingConfiguration;
import org.codefilarete.stalactite.engine.SubEntityMappingConfiguration;
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
import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.collection.Arrays;

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
	 */
	protected <D extends C> void registerCascades(Map<Class<D>, ConfiguredRelationalPersister<D, I>> persisterPerSubclass,
												  Dialect dialect,
												  ConnectionConfiguration connectionConfiguration) {
		// we surround our relation configuration with cycle detection (see registerRelationCascades(..) implementation), this may seem too wide and
		// could be closer to registerRelationCascades(..) method call (which actually requires it) but as doing such we also cover the case of 2
		// subconfigurations using same entity in their relation 
		PersisterBuilderContext.CURRENT.get().runInContext(mainPersister, () -> {
			for (SubEntityMappingConfiguration<D> subConfiguration : (Set<SubEntityMappingConfiguration<D>>) (Set) this.polymorphismPolicy.getSubClasses()) {
				ConfiguredRelationalPersister<D, I> subEntityPersister = persisterPerSubclass.get(subConfiguration.getEntityType());
				
				if (subConfiguration.getPolymorphismPolicy() != null) {
					registerPolymorphismCascades(persisterPerSubclass, dialect, connectionConfiguration, subConfiguration, subEntityPersister);
				}
				
				// We register relation of subclass persister to take into account its specific one-to-ones, one-to-manys and element collection mapping
				this.<D>registerRelationCascades(
						subConfiguration,
						dialect,
						connectionConfiguration,
						subEntityPersister);
			}
		});
	}
	
	private <D extends C> void registerPolymorphismCascades(Map<Class<D>, ConfiguredRelationalPersister<D, I>> persisterPerSubclass,
															Dialect dialect,
															ConnectionConfiguration connectionConfiguration,
															SubEntityMappingConfiguration<D> subConfiguration,
															ConfiguredRelationalPersister<D, I> subEntityPersister) {
		assertSubPolymorphismIsSupported(subConfiguration.getPolymorphismPolicy());
		ConfiguredRelationalPersister<D, I> subclassPersister =
				buildSubPolymorphicPersister(subEntityPersister, subConfiguration.getPolymorphismPolicy(), dialect, connectionConfiguration);
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
				namingConfiguration);
		return polymorphismPersisterBuilder.build(dialect, connectionConfiguration);
	}
	
	private <D extends C> void registerRelationCascades(RelationalMappingConfiguration<D> entityMappingConfiguration,
														Dialect dialect,
														ConnectionConfiguration connectionConfiguration,
														ConfiguredRelationalPersister<D, I> subEntityPersister) {
		// Note that for now polymorphism configuration doesn't support many-to-many nor Map relation
		RelationConfigurer<D, I> relationConfigurer = new RelationConfigurer<>(dialect, connectionConfiguration, subEntityPersister, namingConfiguration);
		relationConfigurer.configureRelations(entityMappingConfiguration);
	}
}
