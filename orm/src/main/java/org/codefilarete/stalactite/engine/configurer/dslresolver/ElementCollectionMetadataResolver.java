package org.codefilarete.stalactite.engine.configurer.dslresolver;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.AccessorDefinitionDefiner;
import org.codefilarete.reflection.Mutator;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ReadWriteAccessorChain;
import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfiguration;
import org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfigurationProvider;
import org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.ElementCollectionTableNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.dsl.property.CascadeOptions;
import org.codefilarete.stalactite.engine.configurer.NamingConfiguration;
import org.codefilarete.stalactite.engine.configurer.builder.embeddable.EmbeddableLinkage;
import org.codefilarete.stalactite.engine.configurer.builder.embeddable.EmbeddableMappingBuilder;
import org.codefilarete.stalactite.engine.configurer.dslresolver.InheritanceConfigurationResolver.ResolvedConfiguration;
import org.codefilarete.stalactite.engine.configurer.dslresolver.MetadataSolvingCache.EntitySource;
import org.codefilarete.stalactite.engine.configurer.elementcollection.ElementCollectionRelation;
import org.codefilarete.stalactite.engine.configurer.elementcollection.ElementRecord;
import org.codefilarete.stalactite.engine.configurer.model.DirectRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.Entity;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedElementCollectionRelation;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.Size;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.ForeignKey;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Key.KeyBuilder;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.collection.PairIterator;

import static org.codefilarete.stalactite.engine.configurer.elementcollection.ElementRecord.IDENTIFIER_ACCESSOR;
import static org.codefilarete.stalactite.engine.configurer.elementcollection.IndexedElementRecord.INDEX_ACCESSOR;
import static org.codefilarete.tool.Nullable.nullable;
import static org.codefilarete.tool.bean.Objects.preventNull;
import static org.codefilarete.tool.collection.Iterables.first;

public class ElementCollectionMetadataResolver {
	
	private static final AccessorDefinition ELEMENT_RECORD_ID_ACCESSOR_DEFINITION = AccessorDefinition.giveDefinition(IDENTIFIER_ACCESSOR);
	private static final AccessorDefinition ELEMENT_RECORD_INDEX_ACCESSOR_DEFINITION = AccessorDefinition.giveDefinition(INDEX_ACCESSOR);
	
	private final Dialect dialect;
	
	public ElementCollectionMetadataResolver(Dialect dialect) {
		this.dialect = dialect;
	}
	
	<C, I> Set<EntitySource<?, ?>> resolve(EntitySource<C, I> source) {
		KeepOrderSet<EntitySource<?, ?>> targetEntities = new KeepOrderSet<>();
		// configuring collection of elements owned by this entity
		source.getResolvedConfigurations().forEach(resolvedConfiguration -> {
			targetEntities.addAll(resolve(source.getEntity(), resolvedConfiguration));
		});
		return targetEntities;
	}
	
	private <C, I> Set<EntitySource<?, ?>> resolve(Entity<C, I, ?> entity, ResolvedConfiguration<C, I> resolvedConfiguration) {
		KeepOrderSet<EntitySource<?, ?>> targetEntities = new KeepOrderSet<>();
		resolvedConfiguration.getMappingConfiguration().getElementCollections().forEach(elementCollection -> {
			resolve(entity, resolvedConfiguration, elementCollection);
		});
		// treating relations embedded in insets
		resolvedConfiguration.getMappingConfiguration().getPropertiesMapping().getInsets().forEach(inset -> {
			inset.getConfigurationProvider().getConfiguration().getElementCollections().forEach(elementCollection -> {
				resolve(entity, resolvedConfiguration, elementCollection.embedInto(inset.getAccessor(), inset.getEmbeddedClass()));
			});
		});
		return targetEntities;
	}
	
	private <SRC, TRGT, SRCID, S extends Collection<TRGT>, SRCTABLE extends Table<SRCTABLE>, COLLECTIONTABLE extends Table<COLLECTIONTABLE>, ER extends ElementRecord<TRGT, SRCID>>
	void resolve(Entity<SRC, SRCID, SRCTABLE> source,
	             ResolvedConfiguration<SRC, SRCID> resolvedConfiguration,
	             ElementCollectionRelation<SRC, TRGT, S> collectionRelation) {
		
		ResolvedElementCollectionRelation<SRC, TRGT, S, SRCID, SRCTABLE, COLLECTIONTABLE, ElementRecord<TRGT, SRCID>> relation = resolveRelation(source, resolvedConfiguration, collectionRelation);
		source.addRelation(relation);
	}
	
	private <SRC, TRGT, SRCID, S extends Collection<TRGT>, SRCTABLE extends Table<SRCTABLE>, COLLECTIONTABLE extends Table<COLLECTIONTABLE>, ER extends ElementRecord<TRGT, SRCID>>
	ResolvedElementCollectionRelation<SRC, TRGT, S, SRCID, SRCTABLE, COLLECTIONTABLE, ER> resolveRelation(Entity<SRC, SRCID, SRCTABLE> source,
	                                                                                                      ResolvedConfiguration<SRC, SRCID> resolvedConfiguration,
	                                                                                                      ElementCollectionRelation<SRC, TRGT, S> collectionRelation) {
		
		AccessorDefinition collectionProviderDefinition = AccessorDefinition.giveDefinition(collectionRelation.getCollectionAccessor());
		PrimaryKey<SRCTABLE, SRCID> sourcePK = source.getTable().getPrimaryKey();
		
		// Note that table will participate to DDL while cascading selection thanks to its join on foreignKey 
		NamingConfiguration namingConfiguration = resolvedConfiguration.getNamingConfiguration();
		COLLECTIONTABLE targetTable = determineTable(collectionRelation, collectionProviderDefinition, namingConfiguration.getElementCollectionTableNamingStrategy());
		Map<Column<SRCTABLE, ?>, Column<COLLECTIONTABLE, ?>> primaryKeyForeignKeyColumnMapping = buildPrimaryKeyForeignKeyColumnMapping(collectionRelation, targetTable, sourcePK, namingConfiguration.getColumnNamingStrategy(), namingConfiguration.getForeignKeyNamingStrategy());
		
		Map<ReadWritePropertyAccessPoint<ER, ?>, Column<COLLECTIONTABLE, ?>> targetColumnMapping = buildCollectionTableMapping(collectionRelation, collectionProviderDefinition, namingConfiguration, targetTable);
		
		
		// managing primary key
		Column<COLLECTIONTABLE, Integer> indexColumn = null;
		if (collectionRelation.isOrdered()) {
			String indexingColumnName = nullable(collectionRelation.getIndexingColumnName()).getOr(() -> namingConfiguration.getIndexColumnNamingStrategy().giveName(ELEMENT_RECORD_INDEX_ACCESSOR_DEFINITION));
			indexColumn = targetTable.addColumn(indexingColumnName, Integer.class);
			// adding a constraint on the index column, not on the element one (like for Set), to allow duplicates
			indexColumn.primaryKey();
			targetColumnMapping.put((ReadWritePropertyAccessPoint) INDEX_ACCESSOR, indexColumn);
		} else {
			// adding a constraint on the element column because Sets don't allow duplicates
			targetColumnMapping.values().forEach(Column::primaryKey);
		}
		
		// a particular collection fixer that gets raw values (elements) from ElementRecord
		// because elementRecordPersister manages ElementRecord, so it gives them as input of the relation,
		// hence an adaption is needed to "convert" it.
		// Note that this code is wrongly typed: the relationFixer should be of <SRC, C> to access the property, whereas it is typed with
		// ElementRecord<TRGT, I> to fulfill the adapter argument. There's a kind of magic here that make it works (generics type erasure, and wrong
		// ofAdapter(..) type deduction by compiler to match the relationFixer variable.
		Supplier<S> collectionFactory = preventNull(
				collectionRelation.getCollectionFactory(),
				Reflections.giveCollectionFactory(collectionProviderDefinition.getMemberType()));
		BeanRelationFixer<SRC, ER> relationFixer = BeanRelationFixer.ofAdapter(
				collectionRelation.getCollectionAccessor(),
				collectionFactory,
				(bean, input, collection) -> collection.add(input.getElement()));    // element value is taken from ElementRecord
		
		KeyBuilder<COLLECTIONTABLE, SRCID> targetKeyBuilder = Key.from(targetTable);
		targetKeyBuilder.addAllColumns(primaryKeyForeignKeyColumnMapping.values());
		DirectRelationJoin<SRCTABLE, COLLECTIONTABLE, SRCID> join = new DirectRelationJoin<>(sourcePK, targetKeyBuilder.build());
		return new ResolvedElementCollectionRelation<>(collectionRelation.getCollectionAccessor(),
				CascadeOptions.RelationMode.ALL_ORPHAN_REMOVAL,
				false,
				join,
				relationFixer,
				collectionRelation.getCollectionFactory(),
				targetColumnMapping,
				primaryKeyForeignKeyColumnMapping,
				indexColumn);
	}
	
	private <SRC, TRGT, SRCID, S extends Collection<TRGT>, COLLECTIONTABLE extends Table<COLLECTIONTABLE>>
	COLLECTIONTABLE determineTable(ElementCollectionRelation<SRC, TRGT, S> collectionRelation, AccessorDefinition collectionProviderDefinition, ElementCollectionTableNamingStrategy elementCollectionTableNamingStrategy) {
		// Note that table will participate to DDL while cascading selection thanks to its join on foreignKey 
		COLLECTIONTABLE targetTable = (COLLECTIONTABLE) nullable(collectionRelation.getTargetTable()).getOr(
				() -> {
					String tableName = nullable(collectionRelation.getTargetTableName()).getOr(() -> {
						String generatedTableName = elementCollectionTableNamingStrategy.giveName(collectionProviderDefinition);
						// we replace dot character by underscore one to take embedded relation properties into account: their accessor is an AccessorChain
						// which is printed with dots by AccessorDefinition
						return generatedTableName.replace('.', '_');
					});
					return new Table(tableName);
				});
		return targetTable;
	}
	
	private <SRC, TRGT, SRCID, S extends Collection<TRGT>, SRCTABLE extends Table<SRCTABLE>, COLLECTIONTABLE extends Table<COLLECTIONTABLE>, ER extends ElementRecord<TRGT, SRCID>>
	Map<Column<SRCTABLE, ?>, Column<COLLECTIONTABLE, ?>>
	buildPrimaryKeyForeignKeyColumnMapping(ElementCollectionRelation<SRC, TRGT, S> collectionRelation, COLLECTIONTABLE targetTable, PrimaryKey<SRCTABLE, SRCID> sourcePK, ColumnNamingStrategy columnNamingStrategy, ForeignKeyNamingStrategy foreignKeyNamingStrategy) {
		Map<Column<SRCTABLE, ?>, Column<COLLECTIONTABLE, ?>> primaryKeyForeignColumnMapping = new HashMap<>();
		if (!sourcePK.isComposed() && collectionRelation.getReverseColumn() != null) {
			primaryKeyForeignColumnMapping.put(first(sourcePK.getColumns()), (Column) collectionRelation.getReverseColumn());
		} else {
			sourcePK.getColumns().forEach(col -> {
				String reverseColumnName = nullable(collectionRelation.getReverseColumnName()).getOr(() ->
						columnNamingStrategy.giveName(ELEMENT_RECORD_ID_ACCESSOR_DEFINITION));
				Column<COLLECTIONTABLE, ?> reverseCol = targetTable.addColumn(reverseColumnName, col.getJavaType())
						.primaryKey();
				primaryKeyForeignColumnMapping.put(col, reverseCol);
			});
		}
		
		KeyBuilder<COLLECTIONTABLE, SRCID> keyBuilder = Key.from(targetTable);
		keyBuilder.addAllColumns(primaryKeyForeignColumnMapping.values());
		Key<COLLECTIONTABLE, SRCID> reverseKey = keyBuilder.build();
		ForeignKey<COLLECTIONTABLE, SRCTABLE, SRCID> reverseForeignKey = targetTable.addForeignKey(foreignKeyNamingStrategy::giveName, reverseKey, sourcePK);
		registerColumnBinder(reverseForeignKey, sourcePK);    // because sourcePk binder might have been overloaded by column so we need to adjust to it
		
		return primaryKeyForeignColumnMapping;
	}
	
	private <SRC, TRGT, SRCID, S extends Collection<TRGT>, SRCTABLE extends Table<SRCTABLE>, COLLECTIONTABLE extends Table<COLLECTIONTABLE>, ER extends ElementRecord<TRGT, SRCID>>
	Map<ReadWritePropertyAccessPoint<ER, ?>, Column<COLLECTIONTABLE, ?>>
	buildCollectionTableMapping(ElementCollectionRelation<SRC, TRGT, S> collectionRelation, AccessorDefinition collectionProviderDefinition, NamingConfiguration namingConfiguration, COLLECTIONTABLE targetTable) {
		
		EmbeddableMappingConfiguration<TRGT> embeddableConfiguration = nullable(collectionRelation.getEmbeddableConfigurationProvider()).map(EmbeddableMappingConfigurationProvider::getConfiguration).get();
		Map<ReadWritePropertyAccessPoint<ER, ?>, Column<COLLECTIONTABLE, ?>> targetColumnMapping = new HashMap<>();
		
		if (embeddableConfiguration == null) {
			String columnName = nullable(collectionRelation.getElementColumnName())
					.getOr(() -> namingConfiguration.getColumnNamingStrategy().giveName(collectionProviderDefinition));
			Column<COLLECTIONTABLE, TRGT> elementColumn = targetTable.addColumn(columnName, collectionRelation.getComponentType(), collectionRelation.getElementColumnSize());
			// the mapping is only made of the element accessor
			ReadWriteAccessorChain<ER, ER, Object> key = new ReadWriteAccessorChain<>(new XX<>(), (ReadWritePropertyAccessPoint<ER, Object>) ElementRecord.ELEMENT_ACCESSOR);
			targetColumnMapping.put(key, elementColumn);
		} else {
			// a special configuration was given, we compute a EmbeddedClassMapping from it
			EmbeddableMappingBuilder<TRGT, COLLECTIONTABLE> elementCollectionMappingBuilder = new EmbeddableMappingBuilder<TRGT, COLLECTIONTABLE>(embeddableConfiguration, targetTable,
					dialect.getColumnBinderRegistry(), namingConfiguration.getColumnNamingStrategy(), namingConfiguration.getUniqueConstraintNamingStrategy()) {
				@Override
				protected <O> String determineColumnName(EmbeddableLinkage<TRGT, O> linkage, @javax.annotation.Nullable String overriddenColumName) {
					return super.determineColumnName(linkage, collectionRelation.getOverriddenColumnNames().get(linkage.getAccessor()));
				}
				
				@Override
				protected <O> Size determineColumnSize(EmbeddableLinkage<TRGT, O> linkage, @javax.annotation.Nullable Size overriddenColumSize) {
					return super.determineColumnSize(linkage, collectionRelation.getOverriddenColumnSizes().get(linkage.getAccessor()));
				}
			};
			Map<ReadWritePropertyAccessPoint<TRGT, ?>, Column<COLLECTIONTABLE, ?>> embeddableColumnMapping = elementCollectionMappingBuilder.build().getMapping();
			
			embeddableColumnMapping.forEach((propertyAccessor, column) -> {
				List<ReadWritePropertyAccessPoint<?, ?>> shifter = Arrays.asList(ElementRecord.ELEMENT_ACCESSOR, propertyAccessor);
				AccessorChain<ER, Object> accessorChain = AccessorChain.fromAccessorsWithNullSafe(shifter, (accessor, valueType) -> {
					if (accessor == ElementRecord.ELEMENT_ACCESSOR) {
						// on getElement(), bean type can't be deduced by reflection due to generic type erasure : default mechanism returns Object
						// so we have to specify our bean type, else a simple Object is instantiated which throws a ClassCastException further
						return Reflections.newInstance(embeddableConfiguration.getBeanType());
					} else {
						// default mechanism
						return Reflections.newInstance(valueType);
					}
				});
				
				targetColumnMapping.put(new ReadWriteAccessorChain<>(accessorChain), column);
			});
		}
		
		return targetColumnMapping;
		
	}
	
	private static class XX<TRGT, SRCID, ER extends ElementRecord<TRGT, SRCID>> implements PropertyAccessor<ER, ER>, AccessorDefinitionDefiner<ER>, ReversibleAccessor<ER, ER> {
		
		@Override
		public ER get(ER er) {
			return er;
		}
		
		@Override
		public AccessorDefinition asAccessorDefinition() {
			return new AccessorDefinition(ElementRecord.class, "self", Object.class);
		}
		
		@Override
		public Mutator<ER, ER> toMutator() {
			return (er, er2) -> {
				// does nothing because it is actually never invoked since we never set the ElementRecord on any other Object
				// => implementing "ReversibleAccessor" is only made to fulfill ReadWriteAccessorChain construction
			};
		}
	}
	
	private <SRCID> void registerColumnBinder(ForeignKey<?, ?, SRCID> reverseColumn, PrimaryKey<?, SRCID> sourcePK) {
		PairIterator<? extends Column<?, ?>, ? extends Column<?, ?>> pairIterator = new PairIterator<>(reverseColumn.getColumns(), sourcePK.getColumns());
		pairIterator.forEachRemaining(col -> {
			dialect.getColumnBinderRegistry().register(col.getLeft(), dialect.getColumnBinderRegistry().getBinder(col.getRight()));
			dialect.getSqlTypeRegistry().put(col.getLeft(), dialect.getSqlTypeRegistry().getTypeName(col.getRight()));
		});
	}
}
