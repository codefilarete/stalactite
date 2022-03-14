package org.codefilarete.stalactite.engine.configurer;

import java.util.HashMap;
import java.util.Map;

import org.codefilarete.stalactite.engine.AssociationTableNamingStrategy;
import org.codefilarete.stalactite.engine.ColumnNamingStrategy;
import org.codefilarete.stalactite.engine.ElementCollectionTableNamingStrategy;
import org.codefilarete.stalactite.engine.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.engine.PolymorphismPolicy;
import org.codefilarete.stalactite.engine.PolymorphismPolicy.SingleTablePolymorphism;
import org.codefilarete.stalactite.engine.SubEntityMappingConfiguration;
import org.codefilarete.stalactite.engine.TableNamingStrategy;
import org.codefilarete.stalactite.engine.configurer.BeanMappingBuilder.ColumnNameProvider;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImpl.Identification;
import org.codefilarete.stalactite.engine.runtime.EntityConfiguredJoinedTablesPersister;
import org.codefilarete.stalactite.engine.runtime.SimpleRelationalEntityPersister;
import org.codefilarete.stalactite.engine.runtime.SingleTablePolymorphismPersister;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.exception.NotImplementedException;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ValueAccessPointSet;
import org.codefilarete.stalactite.engine.PersisterRegistry;
import org.codefilarete.stalactite.mapping.ClassMapping;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * @author Guillaume Mary
 */
class SingleTablePolymorphismBuilder<C, I, T extends Table, D> extends AbstractPolymorphicPersisterBuilder<C, I, T> {
	
	private final SingleTablePolymorphism<C, D> polymorphismPolicy;
	private final Map<ReversibleAccessor, Column> mainMapping;
	
	SingleTablePolymorphismBuilder(SingleTablePolymorphism<C, D> polymorphismPolicy,
								   Identification identification,
								   EntityConfiguredJoinedTablesPersister<C, I> mainPersister,
								   Map<ReversibleAccessor, Column> mainMapping,
								   ColumnBinderRegistry columnBinderRegistry,
								   ColumnNameProvider columnNameProvider,
								   TableNamingStrategy tableNamingStrategy, ColumnNamingStrategy columnNamingStrategy,
								   ForeignKeyNamingStrategy foreignKeyNamingStrategy,
								   ElementCollectionTableNamingStrategy elementCollectionTableNamingStrategy,
								   ColumnNamingStrategy joinColumnNamingStrategy,
								   ColumnNamingStrategy indexColumnNamingStrategy,
								   AssociationTableNamingStrategy associationTableNamingStrategy
	) {
		super(polymorphismPolicy, identification, mainPersister, columnBinderRegistry, columnNameProvider, columnNamingStrategy, foreignKeyNamingStrategy,
				elementCollectionTableNamingStrategy, joinColumnNamingStrategy, indexColumnNamingStrategy, associationTableNamingStrategy, tableNamingStrategy);
		this.polymorphismPolicy = polymorphismPolicy;
		this.mainMapping = mainMapping;
	}
	
	@Override
	public EntityConfiguredJoinedTablesPersister<C, I> build(Dialect dialect, ConnectionConfiguration connectionConfiguration, PersisterRegistry persisterRegistry) {
		Map<Class<? extends C>, EntityConfiguredJoinedTablesPersister<C, I>> persisterPerSubclass = new HashMap<>();
		
		BeanMappingBuilder beanMappingBuilder = new BeanMappingBuilder();
		T mainTable = (T) mainPersister.getMapping().getTargetTable();
		for (SubEntityMappingConfiguration<? extends C> subConfiguration : polymorphismPolicy.getSubClasses()) {
			// first we'll use table of columns defined in embedded override
			// then the one defined by inheritance
			// if both are null we'll create a new one
			Table tableDefinedByColumnOverride = BeanMappingBuilder.giveTargetTable(subConfiguration.getPropertiesMapping());
			
			assertNullOrEqual(tableDefinedByColumnOverride, mainTable);
			
			Map<ReversibleAccessor, Column> subEntityPropertiesMapping = beanMappingBuilder.build(subConfiguration.getPropertiesMapping(), mainTable,
					this.columnBinderRegistry, this.columnNameProvider);
			// in single-table polymorphism, main properties must be given to sub-entities ones, because CRUD operations are dipatched to them
			// by a proxy and main persister is not so much used
			subEntityPropertiesMapping.putAll(mainMapping);
			ClassMapping<? extends C, I, T> classMappingStrategy = PersisterBuilderImpl.createClassMappingStrategy(
					false,
					mainTable,
					subEntityPropertiesMapping,
					new ValueAccessPointSet(),	// TODO: implement properties set by constructor feature in single-table polymorphism
					identification,
					subConfiguration.getPropertiesMapping().getBeanType(),
					null);
			// we need to copy also shadow columns, made in particular for one-to-one owned by source side because foreign key is maintained through it
			classMappingStrategy.addShadowColumns((ClassMapping) mainPersister.getMapping());
			
			// no primary key to add nor foreign key since table is the same as main one (single table strategy)
			SimpleRelationalEntityPersister subclassPersister = new SimpleRelationalEntityPersister(classMappingStrategy, dialect, connectionConfiguration);
			
			persisterPerSubclass.put(subConfiguration.getEntityType(), subclassPersister);
		}
		
		Column<T, D> discriminatorColumn = ensureDiscriminatorColumn();
		// NB: persisters are not registered into PersistenceContext because it may break implicit polymorphism principle (persisters are then
		// available by PersistenceContext.getPersister(..)) and it is one sure that they are perfect ones (all their features should be tested)
		SingleTablePolymorphismPersister<C, I, ?, ?> surrogate = new SingleTablePolymorphismPersister<>(
				mainPersister, persisterPerSubclass, connectionConfiguration.getConnectionProvider(), dialect,
				discriminatorColumn, polymorphismPolicy);
		
		registerCascades(persisterPerSubclass, dialect, connectionConfiguration, persisterRegistry);
		
		return surrogate;
	}
	
	@Override
	protected void assertSubPolymorphismIsSupported(PolymorphismPolicy<? extends C> subPolymorphismPolicy) {
		// Everything else than joined-tables is not implemented
		// - single-table with single-table is non sensence
		// - single-table with table-per-class is not implemented
		// Written as a negative condition to explicitly say what we support
		if (!(subPolymorphismPolicy instanceof PolymorphismPolicy.JoinTablePolymorphism)) {
			throw new NotImplementedException("Combining joined-tables polymorphism policy with " + Reflections.toString(subPolymorphismPolicy.getClass()));
		}
	}
	
	private Column<T, D> ensureDiscriminatorColumn() {
		Column<T, D> result = mainPersister.getMapping().getTargetTable().addColumn(
				polymorphismPolicy.getDiscriminatorColumn(),
				polymorphismPolicy.getDiscrimintorType());
		result.setNullable(false);
		return result;
	}
	
}
