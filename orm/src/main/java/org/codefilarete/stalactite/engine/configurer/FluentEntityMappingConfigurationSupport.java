package org.codefilarete.stalactite.engine.configurer;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import org.codefilarete.stalactite.engine.AssociationTableNamingStrategy;
import org.codefilarete.stalactite.engine.ColumnNamingStrategy;
import org.codefilarete.stalactite.engine.ColumnOptions;
import org.codefilarete.stalactite.engine.ColumnOptions.IdentifierPolicy;
import org.codefilarete.stalactite.engine.ElementCollectionOptions;
import org.codefilarete.stalactite.engine.ElementCollectionTableNamingStrategy;
import org.codefilarete.stalactite.engine.EmbeddableMappingConfiguration;
import org.codefilarete.stalactite.engine.EmbeddableMappingConfigurationProvider;
import org.codefilarete.stalactite.engine.EntityMappingConfiguration;
import org.codefilarete.stalactite.engine.EntityMappingConfigurationProvider;
import org.codefilarete.stalactite.engine.EnumOptions;
import org.codefilarete.stalactite.engine.FluentEmbeddableMappingBuilder.FluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions;
import org.codefilarete.stalactite.engine.FluentEmbeddableMappingBuilder.FluentEmbeddableMappingBuilderEnumOptions;
import org.codefilarete.stalactite.engine.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.engine.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.engine.ImportedEmbedWithColumnOptions;
import org.codefilarete.stalactite.engine.IndexableCollectionOptions;
import org.codefilarete.stalactite.engine.InheritanceOptions;
import org.codefilarete.stalactite.engine.OneToManyOptions;
import org.codefilarete.stalactite.engine.OneToOneOptions;
import org.codefilarete.stalactite.engine.PolymorphismPolicy;
import org.codefilarete.stalactite.engine.TableNamingStrategy;
import org.codefilarete.stalactite.engine.configurer.FluentEmbeddableMappingConfigurationSupport.LinkageSupport;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableBiFunction;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.codefilarete.tool.Nullable;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.exception.NotImplementedException;
import org.codefilarete.tool.function.SerializableTriFunction;
import org.codefilarete.tool.function.Serie;
import org.codefilarete.tool.function.TriFunction;
import org.codefilarete.tool.reflect.MethodDispatcher;
import org.codefilarete.reflection.AccessorByMethod;
import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.MethodReferenceCapturer;
import org.codefilarete.reflection.MethodReferenceDispatcher;
import org.codefilarete.reflection.MutatorByMethod;
import org.codefilarete.reflection.MutatorByMethodReference;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ValueAccessPointByMethodReference;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.VersioningStrategy;
import org.codefilarete.stalactite.engine.runtime.AbstractVersioningStrategy.VersioningStrategySupport;
import org.codefilarete.stalactite.engine.runtime.EntityConfiguredPersister;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

import static org.codefilarete.tool.Reflections.propertyName;

/**
 * A class that stores configuration made through a {@link FluentEntityMappingBuilder}
 * 
 * @author Guillaume Mary
 */
public class FluentEntityMappingConfigurationSupport<C, I> implements FluentEntityMappingBuilder<C, I>, EntityMappingConfiguration<C, I> {
	
	private final Class<C> persistedClass;
	
	private IdentifierPolicy<I> identifierPolicy;
	
	private TableNamingStrategy tableNamingStrategy = TableNamingStrategy.DEFAULT;
	
	private ReversibleAccessor<C, I> identifierAccessor;
	
	private final MethodReferenceCapturer methodSpy;
	
	private final List<CascadeOne<C, Object, Object>> cascadeOnes = new ArrayList<>();
	
	private final List<CascadeMany<C, ?, ?, ? extends Collection>> cascadeManys = new ArrayList<>();
	
	private final List<ElementCollectionLinkage<C, ?, ? extends Collection>> elementCollections = new ArrayList<>();
	
	private final EntityDecoratedEmbeddableConfigurationSupport<C, I> propertiesMappingConfigurationSurrogate;
	
	private ForeignKeyNamingStrategy foreignKeyNamingStrategy = ForeignKeyNamingStrategy.DEFAULT;
	
	private ColumnNamingStrategy joinColumnNamingStrategy = ColumnNamingStrategy.JOIN_DEFAULT;
	
	private ColumnNamingStrategy indexColumnNamingStrategy;
	
	private AssociationTableNamingStrategy associationTableNamingStrategy = AssociationTableNamingStrategy.DEFAULT;
	
	private ElementCollectionTableNamingStrategy elementCollectionTableNamingStrategy = ElementCollectionTableNamingStrategy.DEFAULT;
	
	private OptimisticLockOption optimisticLockOption;
	
	private InheritanceConfigurationSupport<? super C, I> inheritanceConfiguration;
	
	private PolymorphismPolicy<C> polymorphismPolicy;
	
	private EntityFactoryProvider<C> entityFactoryProvider;
	
	/**
	 * Creates a builder to map the given class for persistence
	 *
	 * @param persistedClass the class to create a mapping for
	 */
	public FluentEntityMappingConfigurationSupport(Class<C> persistedClass) {
		this.persistedClass = persistedClass;
		
		// Helper to capture Method behind method reference
		this.methodSpy = new MethodReferenceCapturer();
		
		this.propertiesMappingConfigurationSurrogate = new EntityDecoratedEmbeddableConfigurationSupport<>(this, persistedClass);
	}
	
	@Override
	public Class<C> getEntityType() {
		return persistedClass;
	}
	
	@Override
	public EntityFactoryProvider<C> getEntityFactoryProvider() {
		return entityFactoryProvider;
	}
	
	@Override
	public TableNamingStrategy getTableNamingStrategy() {
		return tableNamingStrategy;
	}
	
	@Override
	public ColumnNamingStrategy getJoinColumnNamingStrategy() {
		return joinColumnNamingStrategy;
	}
	
	@Override
	public ColumnNamingStrategy getIndexColumnNamingStrategy() {
		return indexColumnNamingStrategy;
	}
	
	private Method captureMethod(SerializableFunction getter) {
		return this.methodSpy.findMethod(getter);
	}
	
	private Method captureMethod(SerializableBiConsumer setter) {
		return this.methodSpy.findMethod(setter);
	}
	
	@Override
	public IdentifierPolicy getIdentifierPolicy() {
		return identifierPolicy;
	}
	
	@Override
	public ReversibleAccessor<C, I> getIdentifierAccessor() {
		return this.identifierAccessor;
	}
	
	@Override
	public EmbeddableMappingConfiguration<C> getPropertiesMapping() {
		return propertiesMappingConfigurationSurrogate;
	}
	
	@Override
	public VersioningStrategy getOptimisticLockOption() {
		return Nullable.nullable(this.optimisticLockOption).map(OptimisticLockOption::getVersioningStrategy).get();
	}
	
	@Override
	public <TRGT, TRGTID> List<CascadeOne<C, TRGT, TRGTID>> getOneToOnes() {
		return (List) cascadeOnes;
	}
	
	@Override
	public <TRGT, TRGTID> List<CascadeMany<C, TRGT, TRGTID, ? extends Collection<TRGT>>> getOneToManys() {
		return (List) cascadeManys;
	}
	
	@Override
	public List<ElementCollectionLinkage<C, ?, ? extends Collection>> getElementCollections() {
		return elementCollections;
	}
	
	@Override
	public InheritanceConfiguration<? super C, I> getInheritanceConfiguration() {
		return inheritanceConfiguration;
	}
	
	@Override
	public ForeignKeyNamingStrategy getForeignKeyNamingStrategy() {
		return this.foreignKeyNamingStrategy;
	}
	
	@Override
	public AssociationTableNamingStrategy getAssociationTableNamingStrategy() {
		return this.associationTableNamingStrategy;
	}
	
	@Override
	public ElementCollectionTableNamingStrategy getElementCollectionTableNamingStrategy() {
		return this.elementCollectionTableNamingStrategy;
	}
	
	@Override
	public EntityMappingConfiguration<C, I> getConfiguration() {
		return this;
	}
	
	@Override
	public PolymorphismPolicy<C> getPolymorphismPolicy() {
		return polymorphismPolicy;
	}
	
	@Override
	public FluentEntityMappingBuilderKeyOptions<C, I> mapKey(SerializableFunction<C, I> getter, IdentifierPolicy<I> identifierPolicy) {
		LinkageSupport<C, I> mapping = propertiesMappingConfigurationSurrogate.addKeyMapping(getter, identifierPolicy);
		return this.propertiesMappingConfigurationSurrogate.wrapForKeyOptions(mapping);
	}
	
	@Override
	public <T extends Table> FluentEntityMappingBuilderKeyOptions<C, I> mapKey(SerializableFunction<C, I> getter, IdentifierPolicy<I> identifierPolicy,
																			   Column<T, I> column) {
		LinkageSupport<C, I> mapping = propertiesMappingConfigurationSurrogate.addKeyMapping(getter, identifierPolicy, column);
		return this.propertiesMappingConfigurationSurrogate.wrapForKeyOptions(mapping);
	}
	
	@Override
	public FluentEntityMappingBuilderKeyOptions<C, I> mapKey(SerializableFunction<C, I> getter, IdentifierPolicy<I> identifierPolicy,
																			   String columnName) {
		LinkageSupport<C, I> mapping = propertiesMappingConfigurationSurrogate.addKeyMapping(getter, identifierPolicy, columnName);
		return this.propertiesMappingConfigurationSurrogate.wrapForKeyOptions(mapping);
	}
	
	@Override
	public FluentEntityMappingBuilderKeyOptions<C, I> mapKey(SerializableBiConsumer<C, I> setter, IdentifierPolicy<I> identifierPolicy) {
		LinkageSupport<C, I> mapping = propertiesMappingConfigurationSurrogate.addKeyMapping(setter, identifierPolicy);
		return this.propertiesMappingConfigurationSurrogate.wrapForKeyOptions(mapping);
	}
	
	@Override
	public <T extends Table> FluentEntityMappingBuilderKeyOptions<C, I> mapKey(SerializableBiConsumer<C, I> setter, IdentifierPolicy<I> identifierPolicy, Column<T, I> column) {
		LinkageSupport<C, I> mapping = propertiesMappingConfigurationSurrogate.addKeyMapping(setter, identifierPolicy, column);
		return this.propertiesMappingConfigurationSurrogate.wrapForKeyOptions(mapping);
	}
	
	@Override
	public FluentEntityMappingBuilderKeyOptions<C, I> mapKey(SerializableBiConsumer<C, I> setter, IdentifierPolicy<I> identifierPolicy, String columnName) {
		LinkageSupport<C, I> mapping = propertiesMappingConfigurationSurrogate.addKeyMapping(setter, identifierPolicy, columnName);
		return this.propertiesMappingConfigurationSurrogate.wrapForKeyOptions(mapping);
	}
	
	@Override
	public <O> FluentMappingBuilderPropertyOptions<C, I> map(SerializableBiConsumer<C, O> setter) {
		return map(setter, (String) null);
	}
	
	@Override
	public <O> FluentMappingBuilderPropertyOptions<C, I> map(SerializableFunction<C, O> getter) {
		return map(getter, (String) null);
	}
	
	@Override
	public <O> FluentMappingBuilderPropertyOptions<C, I> map(SerializableBiConsumer<C, O> setter, String columnName) {
		LinkageSupport<C, O> mapping = propertiesMappingConfigurationSurrogate.addMapping(setter, columnName);
		return this.propertiesMappingConfigurationSurrogate.wrapForAdditionalOptions(mapping);
	}
	
	@Override
	public <O> FluentMappingBuilderPropertyOptions<C, I> map(SerializableFunction<C, O> getter, String columnName) {
		LinkageSupport<C, O> mapping = propertiesMappingConfigurationSurrogate.addMapping(getter, columnName);
		return this.propertiesMappingConfigurationSurrogate.wrapForAdditionalOptions(mapping);
	}
	
	@Override
	public <O> FluentMappingBuilderPropertyOptions<C, I> map(SerializableBiConsumer<C, O> setter, Column<? extends Table, O> column) {
		LinkageSupport<C, O> mapping = propertiesMappingConfigurationSurrogate.addMapping(setter, column);
		return this.propertiesMappingConfigurationSurrogate.wrapForAdditionalOptions(mapping);
	}
	
	@Override
	public <O> FluentMappingBuilderPropertyOptions<C, I> map(SerializableFunction<C, O> getter, Column<? extends Table, O> column) {
		LinkageSupport<C, O> mapping = propertiesMappingConfigurationSurrogate.addMapping(getter, column);
		return this.propertiesMappingConfigurationSurrogate.wrapForAdditionalOptions(mapping);
	}
	
	@Override
	public <E extends Enum<E>> FluentMappingBuilderEnumOptions<C, I> mapEnum(SerializableBiConsumer<C, E> setter) {
		return mapEnum(setter, (String) null);
	}
	
	@Override
	public <E extends Enum<E>> FluentMappingBuilderEnumOptions<C, I> mapEnum(SerializableFunction<C, E> getter) {
		return mapEnum(getter, (String) null);
	}
	
	@Override
	public <E extends Enum<E>> FluentMappingBuilderEnumOptions<C, I> mapEnum(SerializableBiConsumer<C, E> setter, @javax.annotation.Nullable String columnName) {
		LinkageSupport<C, E> linkage = propertiesMappingConfigurationSurrogate.addMapping(setter, columnName);
		return handleEnumOptions(propertiesMappingConfigurationSurrogate.addEnumOptions(linkage));
	}
	
	@Override
	public <E extends Enum<E>> FluentMappingBuilderEnumOptions<C, I> mapEnum(SerializableFunction<C, E> getter, @javax.annotation.Nullable String columnName) {
		LinkageSupport<C, E> linkage = propertiesMappingConfigurationSurrogate.addMapping(getter, columnName);
		return handleEnumOptions(propertiesMappingConfigurationSurrogate.addEnumOptions(linkage));
	}
	
	@Override
	public <E extends Enum<E>> FluentMappingBuilderEnumOptions<C, I> mapEnum(SerializableBiConsumer<C, E> setter, Column<? extends Table, E> column) {
		LinkageSupport<C, E> linkage = propertiesMappingConfigurationSurrogate.addMapping(setter, column);
		return handleEnumOptions(propertiesMappingConfigurationSurrogate.addEnumOptions(linkage));
	}
	
	@Override
	public <E extends Enum<E>> FluentMappingBuilderEnumOptions<C, I> mapEnum(SerializableFunction<C, E> getter, Column<? extends Table, E> column) {
		LinkageSupport<C, E> linkage = propertiesMappingConfigurationSurrogate.addMapping(getter, column);
		return handleEnumOptions(propertiesMappingConfigurationSurrogate.addEnumOptions(linkage));
	}
	
	private FluentMappingBuilderEnumOptions<C, I> handleEnumOptions(FluentEmbeddableMappingBuilderEnumOptions<C> enumOptionsHandler) {
		// we redirect all EnumOptions methods to the instance that can handle them, returning the dispatcher on these methods so one can chain
		// with some other methods, any methods out of EnumOptions are redirected to "this" because it can handle them.
		return new MethodDispatcher()
				.redirect(EnumOptions.class, enumOptionsHandler, true)
				.fallbackOn(this)
				.build((Class<FluentMappingBuilderEnumOptions<C, I>>) (Class) FluentMappingBuilderEnumOptions.class);
	}
	
	@Override
	public <O, S extends Collection<O>> FluentMappingBuilderElementCollectionOptions<C, I, O, S> mapCollection(SerializableFunction<C, S> getter,
																											   Class<O> componentType) {
		ElementCollectionLinkage<C, O, S> elementCollectionLinkage = new ElementCollectionLinkage<>(getter, componentType,
				propertiesMappingConfigurationSurrogate, null);
		elementCollections.add(elementCollectionLinkage);
		return new MethodReferenceDispatcher()
				.redirect((SerializableBiFunction<FluentMappingBuilderElementCollectionOptions, String, FluentMappingBuilderElementCollectionOptions>)
								FluentMappingBuilderElementCollectionOptions::override,
						elementCollectionLinkage::overrideColumnName)
				.redirect(ElementCollectionOptions.class, wrapAsOptions(elementCollectionLinkage), true)
				.fallbackOn(this)
				.build((Class<FluentMappingBuilderElementCollectionOptions<C, I, O, S>>) (Class) FluentMappingBuilderElementCollectionOptions.class);
	}
	
	@Override
	public <O, S extends Collection<O>> FluentMappingBuilderElementCollectionOptions<C, I, O, S> mapCollection(SerializableBiConsumer<C, S> setter,
																											   Class<O> componentType) {
		ElementCollectionLinkage<C, O, S> elementCollectionLinkage = new ElementCollectionLinkage<>(setter, componentType, null);
		elementCollections.add(elementCollectionLinkage);
		return new MethodReferenceDispatcher()
				.redirect((SerializableBiFunction<FluentMappingBuilderElementCollectionOptions, String, FluentMappingBuilderElementCollectionOptions>)
								FluentMappingBuilderElementCollectionOptions::override,
						elementCollectionLinkage::overrideColumnName)
				.redirect(ElementCollectionOptions.class, wrapAsOptions(elementCollectionLinkage), true)
				.fallbackOn(this)
				.build((Class<FluentMappingBuilderElementCollectionOptions<C, I, O, S>>) (Class) FluentMappingBuilderElementCollectionOptions.class);
	}
	
	@Override
	public <O, S extends Collection<O>> FluentMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> mapCollection(SerializableFunction<C, S> getter,
																														  Class<O> componentType,
																														  EmbeddableMappingConfigurationProvider<O> embeddableConfiguration) {
		ElementCollectionLinkage<C, O, S> elementCollectionLinkage = new ElementCollectionLinkage<>(getter, componentType,
				propertiesMappingConfigurationSurrogate,
				embeddableConfiguration);
		elementCollections.add(elementCollectionLinkage);
		return new MethodReferenceDispatcher()
				.redirect((SerializableTriFunction<FluentMappingBuilderElementCollectionImportEmbedOptions, SerializableFunction, String, FluentMappingBuilderElementCollectionImportEmbedOptions>)
								FluentMappingBuilderElementCollectionImportEmbedOptions::overrideName,
						(BiConsumer<SerializableFunction, String>) elementCollectionLinkage::overrideName)
				.redirect(ElementCollectionOptions.class, wrapAsOptions(elementCollectionLinkage), true)
				.fallbackOn(this)
				.build((Class<FluentMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S>>) (Class) FluentMappingBuilderElementCollectionImportEmbedOptions.class);
	}
	
	@Override
	public <O, S extends Collection<O>> FluentMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> mapCollection(SerializableBiConsumer<C, S> setter,
																														  Class<O> componentType,
																														  EmbeddableMappingConfigurationProvider<O> embeddableConfiguration) {
		ElementCollectionLinkage<C, O, S> elementCollectionLinkage = new ElementCollectionLinkage<>(setter, componentType, embeddableConfiguration);
		elementCollections.add(elementCollectionLinkage);
		return new MethodReferenceDispatcher()
				.redirect((SerializableTriFunction<FluentMappingBuilderElementCollectionImportEmbedOptions, SerializableFunction, String, FluentMappingBuilderElementCollectionImportEmbedOptions>)
								FluentMappingBuilderElementCollectionImportEmbedOptions::overrideName,
						(BiConsumer<SerializableFunction, String>) elementCollectionLinkage::overrideName)
				.redirect(ElementCollectionOptions.class, wrapAsOptions(elementCollectionLinkage), true)
				.fallbackOn(this)
				.build((Class<FluentMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S>>) (Class) FluentMappingBuilderElementCollectionImportEmbedOptions.class);
	}
	
	private <O, S extends Collection<O>> ElementCollectionOptions<C, O, S> wrapAsOptions(ElementCollectionLinkage<C, O, S> elementCollectionLinkage) {
		return new ElementCollectionOptions<C, O, S>() {
			
			@Override
			public ElementCollectionOptions<C, O, S> withCollectionFactory(Supplier<? extends S> collectionFactory) {
				elementCollectionLinkage.setCollectionFactory(collectionFactory);
				return null;
			}
			
			@Override
			public FluentMappingBuilderElementCollectionOptions<C, I, O, S> mappedBy(String name) {
				elementCollectionLinkage.setReverseColumnName(name);
				return null;
			}
			
			@Override
			public ElementCollectionOptions<C, O, S> withTable(Table table) {
				elementCollectionLinkage.setTargetTable(table);
				return null;
			}
			
			@Override
			public ElementCollectionOptions<C, O, S> withTable(String tableName) {
				elementCollectionLinkage.setTargetTableName(tableName);
				return null;
			}
		};
	}
	
	@Override
	public FluentMappingBuilderInheritanceOptions<C, I> mapInheritance(EntityMappingConfiguration<? super C, I> mappingConfiguration) {
		inheritanceConfiguration = new InheritanceConfigurationSupport<>(mappingConfiguration);
		return new MethodReferenceDispatcher()
				.redirect((SerializableFunction<InheritanceOptions, InheritanceOptions>) InheritanceOptions::withJoinedTable,
						() -> this.inheritanceConfiguration.joinTable = true)
				.redirect((SerializableBiFunction<InheritanceOptions, Table, InheritanceOptions>) InheritanceOptions::withJoinedTable,
						t -> { this.inheritanceConfiguration.joinTable = true; this.inheritanceConfiguration.table = t;})
				.fallbackOn(this)
				.build((Class<FluentMappingBuilderInheritanceOptions<C, I>>) (Class) FluentMappingBuilderInheritanceOptions.class);
	}
	
	@Override
	public FluentEntityMappingBuilder<C, I> mapSuperClass(EmbeddableMappingConfigurationProvider<? super C> superMappingConfiguration) {
		this.propertiesMappingConfigurationSurrogate.mapSuperClass(superMappingConfiguration);
		return this;
	}
	
	@Override
	public <O, J, T extends Table> FluentMappingBuilderOneToOneOptions<C, I, T> mapOneToOne(
			SerializableFunction<C, O> getter,
			EntityMappingConfigurationProvider<O, J> mappingConfiguration) {
		return mapOneToOne(getter, mappingConfiguration, null);
	}
	
	@Override
	public <O, J, T extends Table> FluentMappingBuilderOneToOneOptions<C, I, T> mapOneToOne(
			SerializableBiConsumer<C, O> setter,
			EntityMappingConfigurationProvider<O, J> mappingConfiguration) {
		return mapOneToOne(setter, mappingConfiguration, null);
	}
	
	@Override
	public <O, J, T extends Table> FluentMappingBuilderOneToOneOptions<C, I, T> mapOneToOne(
			SerializableBiConsumer<C, O> setter,
			EntityMappingConfigurationProvider<O, J> mappingConfiguration,
			T table) {
		MutatorByMethodReference<C, O> mutatorByMethodReference = Accessors.mutatorByMethodReference(setter);
		PropertyAccessor<C, O> propertyAccessor = new PropertyAccessor<>(
				// we keep close to user demand : we keep its method reference ...
				new MutatorByMethod<C, O>(captureMethod(setter)).toAccessor(),
				// ... but we can't do it for mutator, so we use the most equivalent manner : a mutator based on setter method (fallback to property if not present)
				mutatorByMethodReference);
		CascadeOne<C, O, J> cascadeOne = new CascadeOne<>(propertyAccessor, mappingConfiguration.getConfiguration(), table);
		this.cascadeOnes.add((CascadeOne<C, Object, Object>) cascadeOne);
		return wrapForAdditionalOptions(cascadeOne);
	}
	
	@Override
	public <O, J, T extends Table> FluentMappingBuilderOneToOneOptions<C, I, T> mapOneToOne(
			SerializableFunction<C, O> getter,
			EntityMappingConfigurationProvider<O, J> mappingConfiguration,
			T table) {
		AccessorByMethodReference<C, O> accessorByMethodReference = Accessors.accessorByMethodReference(getter);
		PropertyAccessor<C, O> propertyAccessor = new PropertyAccessor<>(
				// we keep close to user demand : we keep its method reference ...
				accessorByMethodReference,
				// ... but we can't do it for mutator, so we use the most equivalent manner : a mutator based on setter method (fallback to property if not present)
				new AccessorByMethod<C, O>(captureMethod(getter)).toMutator());
		CascadeOne<C, O, J> cascadeOne = new CascadeOne<>(propertyAccessor, mappingConfiguration, table);
		this.cascadeOnes.add((CascadeOne<C, Object, Object>) cascadeOne);
		return wrapForAdditionalOptions(cascadeOne);
	}
	
	private <O, J, T extends Table> FluentMappingBuilderOneToOneOptions<C, I, T> wrapForAdditionalOptions(final CascadeOne<C, O, J> cascadeOne) {
		// then we return an object that allows fluent settings over our OneToOne cascade instance
		return new MethodDispatcher()
				.redirect(OneToOneOptions.class, new OneToOneOptions() {
					@Override
					public OneToOneOptions cascading(RelationMode relationMode) {
						cascadeOne.setRelationMode(relationMode);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public OneToOneOptions mandatory() {
						cascadeOne.setNullable(false);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public OneToOneOptions mappedBy(SerializableFunction reverseLink) {
						cascadeOne.setReverseGetter(reverseLink);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public OneToOneOptions mappedBy(SerializableBiConsumer reverseLink) {
						cascadeOne.setReverseSetter(reverseLink);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public OneToOneOptions mappedBy(Column reverseLink) {
						cascadeOne.setReverseColumn(reverseLink);
						return null;	// we can return null because dispatcher will return proxy
					}
				}, true)	// true to allow "return null" in implemented methods
				.fallbackOn(this)
				.build((Class<FluentMappingBuilderOneToOneOptions<C, I, T>>) (Class) FluentMappingBuilderOneToOneOptions.class);
	}
	
	@Override
	public <O, J, S extends Set<O>> FluentMappingBuilderOneToManyOptions<C, I, O, S> mapOneToManySet(
			SerializableFunction<C, S> getter,
			EntityMappingConfigurationProvider<O, J> mappingConfiguration) {
		return mapOneToManySet(getter, mappingConfiguration, null);
	}
		
	@Override
	public <O, J, S extends Set<O>, T extends Table> FluentMappingBuilderOneToManyOptions<C, I, O, S> mapOneToManySet(
			SerializableFunction<C, S> getter,
			EntityMappingConfigurationProvider<O, J> mappingConfiguration,
			@javax.annotation.Nullable T table) {
		
		AccessorByMethodReference<C, S> getterReference = Accessors.accessorByMethodReference(getter);
		ReversibleAccessor<C, S> propertyAccessor = new PropertyAccessor<>(
				// we keep close to user demand : we keep its method reference ...
				getterReference,
				// ... but we can't do it for mutator, so we use the most equivalent manner : a mutator based on setter method (fallback to property if not present)
				new AccessorByMethod<C, S>(captureMethod(getter)).toMutator());
		return mapOneToManySet(propertyAccessor, getterReference, mappingConfiguration, table);
	}
	
	@Override
	public <O, J, S extends Set<O>, T extends Table> FluentMappingBuilderOneToManyOptions<C, I, O, S> mapOneToManySet(
			SerializableBiConsumer<C, S> setter,
			EntityMappingConfigurationProvider<O, J> mappingConfiguration,
			@javax.annotation.Nullable T table) {
		
		MutatorByMethodReference<C, S> setterReference = Accessors.mutatorByMethodReference(setter);
		PropertyAccessor<C, S> propertyAccessor = new PropertyAccessor<>(
				Accessors.accessor(setterReference.getDeclaringClass(), propertyName(setterReference.getMethodName())),
				setterReference
		);
		return mapOneToManySet(propertyAccessor, setterReference, mappingConfiguration, table);
	}
	
	private <O, J, S extends Set<O>, T extends Table> FluentMappingBuilderOneToManyOptions<C, I, O, S> mapOneToManySet(
			ReversibleAccessor<C, S> propertyAccessor,
			ValueAccessPointByMethodReference methodReference,
			EntityMappingConfigurationProvider<O, J> mappingConfiguration,
			@javax.annotation.Nullable T table) {
		CascadeMany<C, O, J, S> cascadeMany = new CascadeMany<>(propertyAccessor, methodReference, mappingConfiguration, table);
		this.cascadeManys.add(cascadeMany);
		return new MethodDispatcher()
				.redirect(OneToManyOptions.class, new OneToManyOptionsSupport<>(cascadeMany), true)	// true to allow "return null" in implemented methods
				.fallbackOn(this)
				.build((Class<FluentMappingBuilderOneToManyOptions<C, I, O, S>>) (Class) FluentMappingBuilderOneToManyOptions.class);
	}
	
	@Override
	public <O, J, S extends List<O>> FluentMappingBuilderOneToManyListOptions<C, I, O, S> mapOneToManyList(
			SerializableFunction<C, S> getter,
			EntityMappingConfigurationProvider<O, J> mappingConfiguration) {
		return mapOneToManyList(getter, mappingConfiguration, null);
	}
		
	@Override
	public <O, J, S extends List<O>, T extends Table> FluentMappingBuilderOneToManyListOptions<C, I, O, S> mapOneToManyList(
			SerializableFunction<C, S> getter,
			EntityMappingConfigurationProvider<O, J> mappingConfiguration,
			@javax.annotation.Nullable T table) {
		
		AccessorByMethodReference<C, S> getterReference = Accessors.accessorByMethodReference(getter);
		ReversibleAccessor<C, S> propertyAccessor = new PropertyAccessor<>(
				// we keep close to user demand : we keep its method reference ...
				getterReference,
				// ... but we can't do it for mutator, so we use the most equivalent manner : a mutator based on setter method (fallback to property if not present)
				new AccessorByMethod<C, S>(captureMethod(getter)).toMutator());
		return mapOneToManyList(propertyAccessor, getterReference, mappingConfiguration, table);
	}
	
	@Override
	public <O, J, S extends List<O>, T extends Table> FluentMappingBuilderOneToManyListOptions<C, I, O, S> mapOneToManyList(
			SerializableBiConsumer<C, S> setter,
			EntityMappingConfigurationProvider<O, J> mappingConfiguration,
			@javax.annotation.Nullable T table) {
		
		MutatorByMethodReference<C, S> setterReference = Accessors.mutatorByMethodReference(setter);
		PropertyAccessor<C, S> propertyAccessor = new PropertyAccessor<>(
				Accessors.accessor(setterReference.getDeclaringClass(), propertyName(setterReference.getMethodName())),
				setterReference
		);
		return mapOneToManyList(propertyAccessor, setterReference, mappingConfiguration, table);
	}
	
	private <O, J, S extends List<O>, T extends Table> FluentMappingBuilderOneToManyListOptions<C, I, O, S> mapOneToManyList(
			ReversibleAccessor<C, S> propertyAccessor,
			ValueAccessPointByMethodReference methodReference,
			EntityMappingConfigurationProvider<? extends O, J> mappingConfiguration,
			@javax.annotation.Nullable T table) {
		CascadeManyList<C, O, J, ? extends List<O>> cascadeMany = new CascadeManyList<>(propertyAccessor, methodReference, mappingConfiguration.getConfiguration(), table);
		this.cascadeManys.add(cascadeMany);
		return new MethodDispatcher()
				.redirect(OneToManyOptions.class, new OneToManyOptionsSupport<>(cascadeMany), true)	// true to allow "return null" in implemented methods
				.redirect(IndexableCollectionOptions.class, orderingColumn -> {
					cascadeMany.setIndexingColumn(orderingColumn);
					return null;
				}, true)	// true to allow "return null" in implemented methods
				.fallbackOn(this)
				.build((Class<FluentMappingBuilderOneToManyListOptions<C, I, O, S>>) (Class) FluentMappingBuilderOneToManyListOptions.class);
	}
	
	@Override
	public <O> FluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O> embed(SerializableFunction<C, O> getter,
																									 EmbeddableMappingConfigurationProvider<? extends O> embeddableMappingBuilder) {
		return embed(propertiesMappingConfigurationSurrogate.embed(getter, embeddableMappingBuilder));
	}
	
	@Override
	public <O> FluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O> embed(SerializableBiConsumer<C, O> setter,
																									 EmbeddableMappingConfigurationProvider<? extends O> embeddableMappingBuilder) {
		return embed(propertiesMappingConfigurationSurrogate.embed(setter, embeddableMappingBuilder));
	}
	
	private <O> FluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O> embed(FluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, O> support) {
		return new MethodDispatcher()
				.redirect(ImportedEmbedWithColumnOptions.class, new ImportedEmbedWithColumnOptions() {
					@Override
					public ImportedEmbedWithColumnOptions overrideName(SerializableBiConsumer setter, String columnName) {
						support.overrideName(setter, columnName);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public ImportedEmbedWithColumnOptions overrideName(SerializableFunction getter, String columnName) {
						support.overrideName(getter, columnName);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public ImportedEmbedWithColumnOptions override(SerializableBiConsumer setter, Column targetColumn) {
						propertiesMappingConfigurationSurrogate.currentInset().override(setter, targetColumn);
						return null;
					}
					
					@Override
					public ImportedEmbedWithColumnOptions override(SerializableFunction getter, Column targetColumn) {
						propertiesMappingConfigurationSurrogate.currentInset().override(getter, targetColumn);
						return null;
					}
					
					@Override
					public ImportedEmbedWithColumnOptions exclude(SerializableBiConsumer setter) {
						support.exclude(setter);
						return null;	// we can return null because dispatcher will return proxy
					}
					
					@Override
					public ImportedEmbedWithColumnOptions exclude(SerializableFunction getter) {
						support.exclude(getter);
						return null;	// we can return null because dispatcher will return proxy
					}
				}, true)
				.fallbackOn(this)
				.build((Class<FluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O>>) (Class) FluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions.class);
	}
	
	@Override
	public FluentEntityMappingBuilder<C, I> withElementCollectionTableNaming(ElementCollectionTableNamingStrategy tableNamingStrategy) {
		this.elementCollectionTableNamingStrategy = tableNamingStrategy;
		return this;
	}
	
	@Override
	public FluentEntityMappingBuilder<C, I> withForeignKeyNaming(ForeignKeyNamingStrategy foreignKeyNamingStrategy) {
		this.foreignKeyNamingStrategy = foreignKeyNamingStrategy;
		return this;
	}
	
	@Override
	public FluentEntityMappingBuilder<C, I> withColumnNaming(ColumnNamingStrategy columnNamingStrategy) {
		this.propertiesMappingConfigurationSurrogate.withColumnNaming(columnNamingStrategy);
		return this;
	}
	
	@Override
	public FluentEntityMappingBuilder<C, I> withJoinColumnNaming(ColumnNamingStrategy columnNamingStrategy) {
		this.joinColumnNamingStrategy = columnNamingStrategy;
		return this;
	}
	
	@Override
	public FluentEntityMappingBuilder<C, I> withIndexColumnNamingStrategy(ColumnNamingStrategy columnNamingStrategy) {
		this.indexColumnNamingStrategy = columnNamingStrategy;
		return this;
	}
	
	@Override
	public FluentEntityMappingBuilder<C, I> withAssociationTableNaming(AssociationTableNamingStrategy associationTableNamingStrategy) {
		this.associationTableNamingStrategy = associationTableNamingStrategy;
		return this;
	}
	
	/**
	 * Defines the versioning property of beans. This implies that Optmistic Locking will be applied on those beans.
	 * Versioning policy is supported for following types:
	 * <ul>
	 * <li>{@link Integer} : a "+1" policy will be applied, see {@link Serie#INTEGER_SERIE}</li>
	 * <li>{@link Long} : a "+1" policy will be applied, see {@link Serie#LONG_SERIE}</li>
	 * <li>{@link Date} : a "now" policy will be applied, see {@link Serie#NOW_SERIE}</li>
	 * </ul>
	 * 
	 * @param getter the funciton that gives access to the versioning property
	 * @param <V> type of the versioning property, determines versioning policy
	 * @return this
	 * @see #versionedBy(SerializableFunction, Serie)
	 */
	@Override
	public <V> FluentEntityMappingBuilder<C, I> versionedBy(SerializableFunction<C, V> getter) {
		AccessorByMethodReference methodReference = Accessors.accessorByMethodReference(getter);
		Serie<V> serie;
		if (Integer.class.isAssignableFrom(methodReference.getPropertyType())) {
			serie = (Serie<V>) Serie.INTEGER_SERIE;
		} else if (Long.class.isAssignableFrom(methodReference.getPropertyType())) {
			serie = (Serie<V>) Serie.LONG_SERIE;
		} else if (Date.class.isAssignableFrom(methodReference.getPropertyType())) {
			serie = (Serie<V>) Serie.NOW_SERIE;
		} else {
			throw new NotImplementedException("Type of versioned property is not implemented, please provide a "
					+ Serie.class.getSimpleName() + " for it : " + Reflections.toString(methodReference.getPropertyType()));
		}
		return versionedBy(getter, methodReference, serie);
	}
	
	@Override
	public <V> FluentEntityMappingBuilder<C, I> versionedBy(SerializableFunction<C, V> getter, Serie<V> serie) {
		return versionedBy(getter, new AccessorByMethodReference<>(getter), serie);
	}
	
	private <V> FluentEntityMappingBuilder<C, I> versionedBy(SerializableFunction<C, V> getter, AccessorByMethodReference methodReference, Serie<V> serie) {
		optimisticLockOption = new OptimisticLockOption<>(methodReference, serie);
		map(getter);
		return this;
	}
	
	@Override
	public FluentEntityMappingBuilder<C, I> mapPolymorphism(PolymorphismPolicy<C> polymorphismPolicy) {
		this.polymorphismPolicy = polymorphismPolicy;
		return this;
	}
	
	@Override
	public EntityConfiguredPersister<C, I> build(PersistenceContext persistenceContext) {
		return build(persistenceContext, null);
	}
	
	@Override
	public EntityConfiguredPersister<C, I> build(PersistenceContext persistenceContext, @javax.annotation.Nullable Table targetTable) {
		return new PersisterBuilderImpl<>(this.getConfiguration()).build(persistenceContext, targetTable);
	}
	
	/**
	 * Class very close to {@link FluentEmbeddableMappingConfigurationSupport}, but with dedicated methods to entity mapping such as
	 * identifier definition or configuration override by {@link Column}
	 */
	static class EntityDecoratedEmbeddableConfigurationSupport<C, I> extends FluentEmbeddableMappingConfigurationSupport<C> {
		
		private final FluentEntityMappingConfigurationSupport<C, I> entityConfigurationSupport;
		
		/**
		 * Creates a builder to map the given class for persistence
		 *
		 * @param persistedClass the class to create a mapping for
		 */
		public EntityDecoratedEmbeddableConfigurationSupport(FluentEntityMappingConfigurationSupport<C, I> entityConfigurationSupport, Class<C> persistedClass) {
			super(persistedClass);
			this.entityConfigurationSupport = entityConfigurationSupport;
		}
		
		<E> LinkageSupport<C, E> addMapping(SerializableBiConsumer<C, E> setter, Column column) {
			return addMapping(Accessors.mutator(setter), column);
		}
		
		<E> LinkageSupport<C, E> addMapping(SerializableFunction<C, E> getter, Column column) {
			return addMapping(Accessors.accessor(getter), column);
		}
		
		/**
		 * Equivalent of {@link #addMapping(ReversibleAccessor, String)} with a {@link Column}
		 * 
		 * @return a new Column added to the target table, throws an exception if already mapped
		 */
		<E> LinkageSupport<C, E> addMapping(ReversibleAccessor<C, ?> propertyAccessor, Column column) {
			LinkageSupport<C, E> newLinkage = new LinkageSupport<>(propertyAccessor);
			newLinkage.setColumnOptions(new ColumnLinkageOptionsByColumn(column));
			mapping.add(newLinkage);
			return newLinkage;
		}
		
		private <O> FluentMappingBuilderPropertyOptions<C, I> wrapForAdditionalOptions(LinkageSupport<C, O> newMapping) {
			return new MethodDispatcher()
					.redirect(ColumnOptions.class, new ColumnOptions() {
						@Override
						public ColumnOptions mandatory() {
							newMapping.setNullable(false);
							return null;
						}
						
						@Override
						public ColumnOptions setByConstructor() {
							newMapping.setByConstructor();
							return null;
						}
					}, true)
					.fallbackOn(entityConfigurationSupport)
					.build((Class<FluentMappingBuilderPropertyOptions<C, I>>) (Class) FluentMappingBuilderPropertyOptions.class);
		}
		
		<E> LinkageSupport<C, I> addKeyMapping(SerializableFunction<C, E> getter, IdentifierPolicy<I> identifierPolicy) {
			return addKeyMapping(Accessors.accessor(getter), identifierPolicy);
		}
		
		<E> LinkageSupport<C, I> addKeyMapping(SerializableFunction<C, E> getter, IdentifierPolicy<I> identifierPolicy, Column<?, E> column) {
			LinkageSupport<C, I> linkage = addKeyMapping(Accessors.accessor(getter), identifierPolicy);
			linkage.setColumnOptions(new ColumnLinkageOptionsByColumn(column));
			return linkage;
		}
		
		<E> LinkageSupport<C, I> addKeyMapping(SerializableFunction<C, E> getter, IdentifierPolicy<I> identifierPolicy, String columnName) {
			LinkageSupport<C, I> linkage = addKeyMapping(Accessors.accessor(getter), identifierPolicy);
			linkage.setColumnOptions(new ColumnLinkageOptionsByName(columnName));
			return linkage;
		}
		
		<E> LinkageSupport<C, I> addKeyMapping(SerializableBiConsumer<C, E> setter, IdentifierPolicy<I> identifierPolicy) {
			return addKeyMapping(Accessors.mutator(setter), identifierPolicy);
		}
		
		<E> LinkageSupport<C, I> addKeyMapping(SerializableBiConsumer<C, E> setter, IdentifierPolicy<I> identifierPolicy, Column<?, E> column) {
			LinkageSupport<C, I> linkage = addKeyMapping(Accessors.mutator(setter), identifierPolicy);
			linkage.setColumnOptions(new ColumnLinkageOptionsByColumn(column));
			return linkage;
		}
		
		<E> LinkageSupport<C, I> addKeyMapping(SerializableBiConsumer<C, E> setter, IdentifierPolicy<I> identifierPolicy, String columnName) {
			LinkageSupport<C, I> linkage = addKeyMapping(Accessors.mutator(setter), identifierPolicy);
			linkage.setColumnOptions(new ColumnLinkageOptionsByName(columnName));
			return linkage;
		}
		
		/**
		 * 
		 * @param propertyAccessor
		 * @param identifierPolicy
		 * @return
		 */
		LinkageSupport<C, I> addKeyMapping(ReversibleAccessor<C, ?> propertyAccessor, IdentifierPolicy<I> identifierPolicy) {
			
			// Please note that we don't check for any id presence in inheritance since this will override parent one (see final build()) 
			if (entityConfigurationSupport.identifierAccessor != null) {
				throw new IllegalArgumentException("Identifier is already defined by " + AccessorDefinition.toString(entityConfigurationSupport.identifierAccessor));
			}
			LinkageSupport<C, I> newLinkage = new LinkageSupport<>(propertyAccessor);
			entityConfigurationSupport.identifierAccessor = newLinkage.getAccessor();
			mapping.add(newLinkage);
			entityConfigurationSupport.identifierPolicy = identifierPolicy;
			return newLinkage;
		}
		
		private FluentEntityMappingBuilderKeyOptions<C, I> wrapForKeyOptions(LinkageSupport<C, I> keyMapping) {
			return new MethodDispatcher()
					.redirect(KeyOptions.class, new KeyOptions<C, I>() {
						
						@Override
						public KeyOptions<C, I> usingConstructor(Supplier<C> factory) {
							entityConfigurationSupport.entityFactoryProvider = table -> row -> factory.get();
							return null;
						}
						
						@Override
						public KeyOptions<C, I> usingConstructor(Function<? super I, C> factory) {
							keyMapping.setByConstructor();
							entityConfigurationSupport.entityFactoryProvider = table -> {
								Column<?, I> primaryKey = Iterables.first(((Table<?>)table) .getPrimaryKey().getColumns());
								return row -> factory.apply((I) row.apply(primaryKey));
							};
							return null;
						}
						
						@Override
						public <T extends Table> KeyOptions<C, I> usingConstructor(Function<? super I, C> factory, Column<T, I> input) {
							keyMapping.setColumnOptions(new ColumnLinkageOptionsByColumn(input));
							keyMapping.setByConstructor();
							entityConfigurationSupport.entityFactoryProvider = table -> row -> factory.apply((I) row.apply(input));
							return null;
						}
						
						@Override
						public KeyOptions<C, I> usingConstructor(Function<? super I, C> factory, String columnName) {
							keyMapping.setColumnOptions(new ColumnLinkageOptionsByName(columnName));
							keyMapping.setByConstructor();
							entityConfigurationSupport.entityFactoryProvider = table -> row -> factory.apply((I) row.apply(table.getColumn(columnName)));
							return null;
						}
						
						@Override
						public <X, T extends Table> KeyOptions<C, I> usingConstructor(BiFunction<? super I, X, C> factory,
																					  Column<T, I> input1,
																					  Column<T, X> input2) {
							keyMapping.setColumnOptions(new ColumnLinkageOptionsByColumn(input1));
							keyMapping.setByConstructor();
							entityConfigurationSupport.entityFactoryProvider = table -> row -> factory.apply((I) row.apply(input1),
																											 (X) row.apply(input2));
							return null;
						}
						
						@Override
						public <X> KeyOptions<C, I> usingConstructor(BiFunction<? super I, X, C> factory,
																	 String columnName1,
																	 String columnName2) {
							keyMapping.setColumnOptions(new ColumnLinkageOptionsByName(columnName1));
							keyMapping.setByConstructor();
							entityConfigurationSupport.entityFactoryProvider = table -> row -> factory.apply((I) row.apply(table.getColumn(columnName1)),
																											 (X) row.apply(table.getColumn(columnName2)));
							return null;
						}
						
						
						@Override
						public <X, Y, T extends Table> KeyOptions<C, I> usingConstructor(TriFunction<? super I, X, Y, C> factory,
																												   Column<T, I> input1,
																												   Column<T, X> input2,
																												   Column<T, Y> input3) {
							keyMapping.setColumnOptions(new ColumnLinkageOptionsByColumn(input1));
							keyMapping.setByConstructor();
							entityConfigurationSupport.entityFactoryProvider = table -> row -> factory.apply((I) row.apply(input1),
																											 (X) row.apply(input2),
																											 (Y) row.apply(input3));
							return null;
						}
						
						@Override
						public <X, Y> KeyOptions<C, I> usingConstructor(TriFunction<? super I, X, Y, C> factory,
																		String columnName1,
																		String columnName2,
																		String columnName3) {
							keyMapping.setColumnOptions(new ColumnLinkageOptionsByName(columnName1));
							keyMapping.setByConstructor();
							entityConfigurationSupport.entityFactoryProvider = table -> row -> factory.apply((I) row.apply(table.getColumn(columnName1)),
																											 (X) row.apply(table.getColumn(columnName2)),
																											 (Y) row.apply(table.getColumn(columnName3)));
							return null;
						}
						
						@Override
						public <T extends Table> KeyOptions<C, I> usingFactory(Function<Function<Column<T, ?>, ?>, C> factory) {
							keyMapping.setByConstructor();
							entityConfigurationSupport.entityFactoryProvider = table -> row -> (C) factory.apply((Function) row);
							return null;
						}
					}, true)
					.fallbackOn(entityConfigurationSupport)
					.build((Class<FluentEntityMappingBuilderKeyOptions<C, I>>) (Class) FluentEntityMappingBuilderKeyOptions.class);
		}
	}
	
	private static class OptimisticLockOption<C> {
		
		private final VersioningStrategy<Object, C> versioningStrategy;
		
		public OptimisticLockOption(AccessorByMethodReference<Object, C> versionAccessor, Serie<C> serie) {
			this.versioningStrategy = new VersioningStrategySupport<>(new PropertyAccessor<>(
					versionAccessor,
					Accessors.mutator(versionAccessor.getDeclaringClass(), propertyName(versionAccessor.getMethodName()), versionAccessor.getPropertyType())
			), serie);
		}
		
		public VersioningStrategy getVersioningStrategy() {
			return versioningStrategy;
		}
	}
	
	/**
	 * A small class for one-to-many options storage into a {@link CascadeMany}. Acts as a wrapper over it.
	 */
	static class OneToManyOptionsSupport<C, I, O, S extends Collection<O>>
			implements OneToManyOptions<C, I, O, S> {
		
		private final CascadeMany<C, O, I, S> cascadeMany;
		
		public OneToManyOptionsSupport(CascadeMany<C, O, I, S> cascadeMany) {
			this.cascadeMany = cascadeMany;
		}
		
		@Override
		public FluentMappingBuilderOneToManyOptions<C, I, O, S> mappedBy(SerializableBiConsumer<O, ? super C> reverseLink) {
			cascadeMany.setReverseSetter(reverseLink);
			return null;	// we can return null because dispatcher will return proxy
		}
		
		@Override
		public FluentMappingBuilderOneToManyOptions<C, I, O, S> mappedBy(SerializableFunction<O, ? super C> reverseLink) {
			cascadeMany.setReverseGetter(reverseLink);
			return null;	// we can return null because dispatcher will return proxy
		}
		
		@Override
		public FluentMappingBuilderOneToManyOptions<C, I, O, S> mappedBy(Column<Table, ?> reverseLink) {
			cascadeMany.setReverseColumn(reverseLink);
			return null;	// we can return null because dispatcher will return proxy
		}
		
		@Override
		public OneToManyOptions<C, I, O, S> reverselySetBy(SerializableBiConsumer<O, C> reverseLink) {
			cascadeMany.setReverseLink(reverseLink);
			return null;	// we can return null because dispatcher will return proxy
		}
		
		@Override
		public FluentMappingBuilderOneToManyOptions<C, I, O, S> initializeWith(Supplier<S> collectionFactory) {
			cascadeMany.setCollectionFactory(collectionFactory);
			return null;	// we can return null because dispatcher will return proxy
		}
		
		@Override
		public FluentMappingBuilderOneToManyOptions<C, I, O, S> cascading(RelationMode relationMode) {
			cascadeMany.setRelationMode(relationMode);
			return null;	// we can return null because dispatcher will return proxy
		}
	}
	
	/**
	 * Stores informations of {@link InheritanceConfiguration}
	 * 
	 * @param <E> entity type
	 * @param <I> identifier type
	 */
	static class InheritanceConfigurationSupport<E, I> implements InheritanceConfiguration<E, I> {
		
		private final EntityMappingConfiguration<E, I> configuration;
		
		private boolean joinTable = false;
		
		private Table table;
		
		InheritanceConfigurationSupport(EntityMappingConfiguration<E, I> configuration) {
			this.configuration = configuration;
		}
		
		@Override
		public EntityMappingConfiguration<E, I> getConfiguration() {
			return configuration;
		}
		
		@Override
		public boolean isJoinTable() {
			return this.joinTable;
		}
		
		@Override
		public Table getTable() {
			return this.table;
		}
	}
	
	private static class EntityFactoryConfiguration<C> implements EntityFactoryProvider<C> {

		private final Function<Function<Column, Object>, C> entityFactory;
		
		private EntityFactoryConfiguration(Function<Function<Column, Object>, C> entityFactory) {
			this.entityFactory = entityFactory;
		}

		@Override
		public Function<Function<Column, Object>, C> giveEntityFactory(Table table) {
			return this.entityFactory;
		}
	}
}