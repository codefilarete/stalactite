package org.codefilarete.stalactite.engine.configurer.builder;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.codefilarete.reflection.ValueAccessPointSet;
import org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfiguration.Linkage;
import org.codefilarete.stalactite.engine.cascade.AfterInsertSupport;
import org.codefilarete.stalactite.engine.cascade.AfterUpdateByIdSupport;
import org.codefilarete.stalactite.engine.cascade.AfterUpdateSupport;
import org.codefilarete.stalactite.engine.cascade.BeforeDeleteByIdSupport;
import org.codefilarete.stalactite.engine.cascade.BeforeDeleteSupport;
import org.codefilarete.stalactite.engine.configurer.AbstractIdentification;
import org.codefilarete.stalactite.engine.configurer.builder.BeanMappingBuilder.BeanMapping;
import org.codefilarete.stalactite.engine.configurer.FluentEmbeddableMappingConfigurationSupport;
import org.codefilarete.stalactite.engine.configurer.NamingConfiguration;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.SimpleRelationalEntityPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.engine.runtime.load.EntityMerger.EntityMergerAdapter;
import org.codefilarete.stalactite.mapping.DefaultEntityMapping;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.KeepOrderSet;

/**
 * Class aimed at handling extra tables options by adding cascade on them.
 * 
 * @author Guillaume Mary
 */
public class ExtraTableConfigurer<C, I, T extends Table<T>> {
	
	private final PrimaryKey<T, I> mainTablePrimaryKey;
	private final ConfiguredRelationalPersister<C, I> mainPersister;
	private final AbstractIdentification<C, I> identification;
	private final Map<String, Set<Linkage>> extraTableLinkages;
	private final ColumnBinderRegistry columnBinderRegistry;
	
	private final NamingConfiguration namingConfiguration;
	
	public ExtraTableConfigurer(AbstractIdentification<C, I> identification,
								ConfiguredRelationalPersister<C, I> mainPersister,
								Map<String, Set<Linkage>> extraTableLinkages,
								ColumnBinderRegistry columnBinderRegistry,
								NamingConfiguration namingConfiguration) {
		this.identification = identification;
		this.mainPersister = mainPersister;
		this.extraTableLinkages = extraTableLinkages;
		this.columnBinderRegistry = columnBinderRegistry;
		this.namingConfiguration = namingConfiguration;
		this.mainTablePrimaryKey = (PrimaryKey<T, I>) mainPersister.getMapping().getTargetTable().getPrimaryKey();
	}
	
	public void configure(Dialect dialect, ConnectionConfiguration connectionConfiguration) {
		
		// Note that we use KeepOrderSet only for simplicity and better debug (can be changed, not a great expectation)
		Set<SimpleRelationalEntityPersister<C, I, ?>> extraTablePersisters = new KeepOrderSet<>();
		
		this.extraTableLinkages.forEach((extraTableName, linkages) -> {
			DefaultEntityMapping<C, I, ?> extratableEntityMapping = buildExtraTableClassMapping(extraTableName, linkages);
			extraTablePersisters.add(new SimpleRelationalEntityPersister<>(extratableEntityMapping, dialect, connectionConfiguration));
		});
		
		// we handle cascade on other table by adding listeners to main persister, other manner would be to override
		// main persister CRUD methods but it's more complex and less decoupled as this.
		extraTablePersisters.forEach(extraTablePersister -> {
			mainPersister.addInsertListener(new AfterInsertSupport<>(extraTablePersister::insert, Function.identity()));
			mainPersister.addUpdateListener(new AfterUpdateSupport<>(extraTablePersister::update, Function.identity()));
			mainPersister.addUpdateByIdListener(new AfterUpdateByIdSupport<>(extraTablePersister::updateById, Function.identity()));
			mainPersister.addDeleteListener(new BeforeDeleteSupport<>(extraTablePersister::delete, Function.identity()));
			mainPersister.addDeleteByIdListener(new BeforeDeleteByIdSupport<>(extraTablePersister::deleteById, Function.identity()));
		});
	}
	
	private <EXTRATABLE extends Table<EXTRATABLE>> DefaultEntityMapping<C, I, EXTRATABLE> buildExtraTableClassMapping(String extraTableName, Set<Linkage> linkages) {
		EXTRATABLE extraTable = (EXTRATABLE) new Table(extraTableName);
		addPrimaryKey(extraTable);
		addForeignKey(extraTable);
		
		FluentEmbeddableMappingConfigurationSupport<C> fluentEmbeddableMappingConfigurationSupport = new FluentEmbeddableMappingConfigurationSupport<>(mainPersister.getClassToPersist());
		fluentEmbeddableMappingConfigurationSupport.getPropertiesMapping().addAll(linkages);
		BeanMappingBuilder<C, EXTRATABLE> beanMappingBuilder = new BeanMappingBuilder<>(fluentEmbeddableMappingConfigurationSupport, extraTable, columnBinderRegistry, namingConfiguration.getColumnNamingStrategy());
		BeanMapping<C, EXTRATABLE> build = beanMappingBuilder.build(true);
		
		// we create the DefaultEntityDefaultEntityMapping from the complex method, not from one of its constructor, because it would
		// require the IdMapping which can be taken from mainPersister but which is wrong since PK column is not the
		// right one and create exception at runtime (update case for example)
		DefaultEntityMapping<C, I, EXTRATABLE> extratableEntityMapping = MainPersisterStep.createEntityMapping(
				false,
				extraTable,
				build.getMapping(),
				build.getReadonlyMapping(),
				build.getReadConverters(),
				build.getWriteConverters(),
				// we don't care a bit of those argument since they won't be used since we join with main persister with a merge join (no instance creation)
				new ValueAccessPointSet<>(),
				identification,
				mainPersister.getClassToPersist(),
				null);
		mainPersister.getEntityJoinTree().addMergeJoin(EntityJoinTree.ROOT_JOIN_NAME,
				new EntityMergerAdapter<>(extratableEntityMapping),
				mainPersister.getMainTable().getPrimaryKey(),
				extraTable.getPrimaryKey(),
				JoinType.OUTER
		);
		return extratableEntityMapping;
	}
	
	private void addPrimaryKey(Table table) {
		PrimaryKeyPropagationStep.propagatePrimaryKey(this.mainTablePrimaryKey, Arrays.asSet(table));
	}
	
	private void addForeignKey(Table table) {
		PrimaryKeyPropagationStep.applyForeignKeys(this.mainTablePrimaryKey, this.namingConfiguration.getForeignKeyNamingStrategy(), Arrays.asSet(table));
	}
	
}
