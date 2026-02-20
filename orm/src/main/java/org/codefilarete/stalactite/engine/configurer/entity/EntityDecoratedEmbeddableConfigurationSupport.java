package org.codefilarete.stalactite.engine.configurer.entity;

import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.dsl.ExtraTablePropertyOptions;
import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.dsl.idpolicy.IdentifierPolicy;
import org.codefilarete.stalactite.dsl.key.CompositeKeyMappingConfigurationProvider;
import org.codefilarete.stalactite.dsl.key.CompositeKeyOptions;
import org.codefilarete.stalactite.dsl.key.FluentEntityMappingBuilderCompositeKeyOptions;
import org.codefilarete.stalactite.dsl.key.FluentEntityMappingBuilderKeyOptions;
import org.codefilarete.stalactite.dsl.key.KeyOptions;
import org.codefilarete.stalactite.dsl.property.ColumnOptions;
import org.codefilarete.stalactite.dsl.property.PropertyOptions;
import org.codefilarete.stalactite.engine.configurer.embeddable.FluentEmbeddableMappingConfigurationSupport;
import org.codefilarete.stalactite.engine.configurer.embeddable.LinkageSupport;
import org.codefilarete.stalactite.engine.configurer.property.ColumnLinkageOptionsByColumn;
import org.codefilarete.stalactite.engine.configurer.property.ColumnLinkageOptionsSupport;
import org.codefilarete.stalactite.sql.ddl.Size;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.function.Converter;
import org.codefilarete.tool.function.TriFunction;
import org.codefilarete.tool.reflect.MethodDispatcher;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * Class very close to {@link FluentEmbeddableMappingConfigurationSupport}, but with dedicated methods to entity mapping such as
 * identifier definition or configuration override by {@link Column}
 */
class EntityDecoratedEmbeddableConfigurationSupport<C, I> extends FluentEmbeddableMappingConfigurationSupport<C> {
	
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
	
	<E> LinkageSupport<C, E> addMapping(SerializableBiConsumer<C, E> setter) {
		LinkageSupport<C, E> newLinkage = new LinkageSupport<>(setter);
		mapping.add(newLinkage);
		return newLinkage;
	}
	
	<E> LinkageSupport<C, E> addMapping(SerializableFunction<C, E> getter) {
		LinkageSupport<C, E> newLinkage = new LinkageSupport<>(getter);
		mapping.add(newLinkage);
		return newLinkage;
	}
	
	<E> LinkageSupport<C, E> addMapping(String fieldName) {
		LinkageSupport<C, E> newLinkage = new LinkageSupport<>(getEntityType(), fieldName);
		mapping.add(newLinkage);
		return newLinkage;
	}
	
	public <O> FluentEntityMappingBuilder.FluentMappingBuilderPropertyOptions<C, I, O> wrapWithAdditionalPropertyOptions(LinkageSupport<C, O> newMapping) {
		return new MethodDispatcher()
				.redirect(ColumnOptions.class, new ColumnOptions<O>() {
					@Override
					public ColumnOptions<O> mandatory() {
						newMapping.setNullable(false);
						return null;
					}
					
					@Override
					public ColumnOptions<O> nullable() {
						newMapping.setNullable(true);
						return null;
					}

					@Override
					public ColumnOptions<O> unique() {
						newMapping.setUnique(true);
						return null;
					}
					
					@Override
					public ColumnOptions<O> setByConstructor() {
						newMapping.setByConstructor();
						return null;
					}
					
					@Override
					public ColumnOptions<O> readonly() {
						newMapping.readonly();
						return null;
					}
					
					@Override
					public ColumnOptions<O> columnName(String name) {
						newMapping.getColumnOptions().setColumnName(name);
						return null;
					}
					
					@Override
					public ColumnOptions<O> columnSize(Size size) {
						newMapping.getColumnOptions().setColumnSize(size);
						return null;
					}
					
					@Override
					public ColumnOptions<O> column(Column<? extends Table, ? extends O> column) {
						newMapping.setColumnOptions(new ColumnLinkageOptionsByColumn(column));
						return null;
					}
					
					@Override
					public ColumnOptions<O> fieldName(String name) {
						newMapping.setField(EntityDecoratedEmbeddableConfigurationSupport.this.entityConfigurationSupport.getEntityType(), name);
						return null;
					}
					
					@Override
					public <X> ColumnOptions<O> readConverter(Converter<X, O> converter) {
						newMapping.setReadConverter(converter);
						return null;
					}
					
					@Override
					public <X> ColumnOptions<O> writeConverter(Converter<O, X> converter) {
						newMapping.setWriteConverter(converter);
						return null;
					}
					
					@Override
					public <V> PropertyOptions<O> sqlBinder(ParameterBinder<V> parameterBinder) {
						newMapping.setParameterBinder(parameterBinder);
						return null;
					}
				}, true)
				.redirect(ExtraTablePropertyOptions.class, name -> {
					newMapping.setExtraTableName(name);
					return null;
				}, true)
				.fallbackOn(entityConfigurationSupport)
				.build((Class<FluentEntityMappingBuilder.FluentMappingBuilderPropertyOptions<C, I, O>>) (Class) FluentEntityMappingBuilder.FluentMappingBuilderPropertyOptions.class);
	}
	
	SingleKeyLinkageSupport<C, I> addKeyMapping(SerializableFunction<C, I> getter, IdentifierPolicy<I> identifierPolicy) {
		return addKeyMapping(new SingleKeyLinkageSupport<>(getter, identifierPolicy));
	}
	
	SingleKeyLinkageSupport<C, I> addKeyMapping(SerializableFunction<C, I> getter, IdentifierPolicy<I> identifierPolicy, Column<?, I> column) {
		SingleKeyLinkageSupport<C, I> linkage = addKeyMapping(new SingleKeyLinkageSupport<>(getter, identifierPolicy));
		linkage.setColumnOptions(new ColumnLinkageOptionsByColumn(column));
		return linkage;
	}
	
	SingleKeyLinkageSupport<C, I> addKeyMapping(SerializableFunction<C, I> getter, IdentifierPolicy<I> identifierPolicy, String columnName) {
		SingleKeyLinkageSupport<C, I> linkage = addKeyMapping(new SingleKeyLinkageSupport<>(getter, identifierPolicy));
		linkage.setColumnOptions(new ColumnLinkageOptionsSupport(columnName));
		return linkage;
	}
	
	SingleKeyLinkageSupport<C, I> addKeyMapping(SerializableBiConsumer<C, I> setter, IdentifierPolicy<I> identifierPolicy) {
		return addKeyMapping(new SingleKeyLinkageSupport<>(setter, identifierPolicy));
	}
	
	SingleKeyLinkageSupport<C, I> addKeyMapping(SerializableBiConsumer<C, I> setter, IdentifierPolicy<I> identifierPolicy, Column<?, I> column) {
		SingleKeyLinkageSupport<C, I> linkage = addKeyMapping(new SingleKeyLinkageSupport<>(setter, identifierPolicy));
		linkage.setColumnOptions(new ColumnLinkageOptionsByColumn(column));
		return linkage;
	}
	
	SingleKeyLinkageSupport<C, I> addKeyMapping(SerializableBiConsumer<C, I> setter, IdentifierPolicy<I> identifierPolicy, String columnName) {
		SingleKeyLinkageSupport<C, I> linkage = addKeyMapping(new SingleKeyLinkageSupport<>(setter, identifierPolicy));
		linkage.setColumnOptions(new ColumnLinkageOptionsSupport(columnName));
		return linkage;
	}
	
	/**
	 *
	 * @param newLinkage
	 * @return
	 */
	private SingleKeyLinkageSupport<C, I> addKeyMapping(SingleKeyLinkageSupport<C, I> newLinkage) {
		// Please note that we don't check for any id presence in inheritance since this will override parent one (see final build())
		if (entityConfigurationSupport.getKeyMapping() != null) {
			throw new IllegalArgumentException("Identifier is already defined by " + AccessorDefinition.toString(entityConfigurationSupport.getKeyMapping().getAccessor()));
		}
		entityConfigurationSupport.setKeyMapping(newLinkage);
		return newLinkage;
	}
	
	/**
	 *
	 * @param propertyAccessor
	 * @return
	 */
	public CompositeKeyLinkageSupport<C, I> addCompositeKeyMapping(ReversibleAccessor<C, I> propertyAccessor,
																   CompositeKeyMappingConfigurationProvider<I> compositeKeyMappingBuilder,
																   Consumer<C> markAsPersistedFunction,
																   Function<C, Boolean> isPersistedFunction) {
		// Please note that we don't check for any id presence in inheritance since this will override parent one (see final build())
		if (entityConfigurationSupport.getKeyMapping() != null) {
			throw new IllegalArgumentException("Identifier is already defined by " + AccessorDefinition.toString(entityConfigurationSupport.getKeyMapping().getAccessor()));
		}
		CompositeKeyLinkageSupport<C, I> newLinkage = new CompositeKeyLinkageSupport<>(propertyAccessor, compositeKeyMappingBuilder, markAsPersistedFunction, isPersistedFunction);
		entityConfigurationSupport.setKeyMapping(newLinkage);
		return newLinkage;
	}
	
	public FluentEntityMappingBuilderKeyOptions<C, I> wrapWithKeyOptions(SingleKeyLinkageSupport<C, I> keyMapping) {
		return new MethodDispatcher()
				.redirect(KeyOptions.class, new KeyOptions<C, I>() {
					
					@Override
					public KeyOptions<C, I> columnName(String name) {
						keyMapping.getColumnOptions().setColumnName(name);
						return null;
					}
					
					@Override
					public KeyOptions<C, I> columnSize(Size size) {
						keyMapping.getColumnOptions().setColumnSize(size);
						return null;
					}
					
					@Override
					public KeyOptions<C, I> column(Column<? extends Table, ? extends I> column) {
						keyMapping.setColumnOptions(new ColumnLinkageOptionsByColumn(column));
						return null;
					}
					
					@Override
					public KeyOptions<C, I> fieldName(String name) {
						keyMapping.setField(entityConfigurationSupport.getEntityType(), name);
						return null;
					}
					
					@Override
					public KeyOptions<C, I> usingConstructor(Supplier<C> factory) {
						entityConfigurationSupport.setEntityFactoryProvider(new EntityFactoryProviderSupport<>(table -> row -> factory.get(), false));
						return null;
					}
					
					@Override
					public KeyOptions<C, I> usingConstructor(Function<? super I, C> factory) {
						keyMapping.setByConstructor();
						entityConfigurationSupport.setEntityFactoryProvider(new EntityFactoryProviderSupport<>(table -> {
							Column<?, I> primaryKey = (Column<?, I>) Iterables.first(((Table<?>) table).getPrimaryKey().getColumns());
							return row -> factory.apply((I) row.get(primaryKey));
						}, true));
						return null;
					}
					
					@Override
					public <T extends Table<T>> KeyOptions<C, I> usingConstructor(Function<? super I, C> factory, Column<T, I> input) {
						keyMapping.setColumnOptions(new ColumnLinkageOptionsByColumn(input));
						keyMapping.setByConstructor();
						entityConfigurationSupport.setEntityFactoryProvider(new EntityFactoryProviderSupport<>(table -> row -> factory.apply((I) row.get(input)), true));
						return null;
					}
					
					@Override
					public KeyOptions<C, I> usingConstructor(Function<? super I, C> factory, String columnName) {
						keyMapping.setColumnOptions(new ColumnLinkageOptionsSupport(columnName));
						keyMapping.setByConstructor();
						entityConfigurationSupport.setEntityFactoryProvider(new EntityFactoryProviderSupport<>(table -> row -> factory.apply((I) row.get(table.getColumn(columnName))), true));
						return null;
					}
					
					@Override
					public <X, T extends Table<T>> KeyOptions<C, I> usingConstructor(BiFunction<? super I, X, C> factory,
																					 Column<T, I> input1,
																					 Column<T, X> input2) {
						keyMapping.setColumnOptions(new ColumnLinkageOptionsByColumn(input1));
						keyMapping.setByConstructor();
						entityConfigurationSupport.setEntityFactoryProvider(new EntityFactoryProviderSupport<>(
								table -> row -> factory.apply(
										(I) row.get(input1),
										(X) row.get(input2)),
								true));
						return null;
					}
					
					@Override
					public <X> KeyOptions<C, I> usingConstructor(BiFunction<? super I, X, C> factory,
																 String columnName1,
																 String columnName2) {
						keyMapping.setColumnOptions(new ColumnLinkageOptionsSupport(columnName1));
						keyMapping.setByConstructor();
						entityConfigurationSupport.setEntityFactoryProvider(new EntityFactoryProviderSupport<>(
								table -> row -> factory.apply(
										(I) row.get(table.getColumn(columnName1)),
										(X) row.get(table.getColumn(columnName2))),
								true));
						return null;
					}
					
					
					@Override
					public <X, Y, T extends Table<T>> KeyOptions<C, I> usingConstructor(TriFunction<? super I, X, Y, C> factory,
																						Column<T, I> input1,
																						Column<T, X> input2,
																						Column<T, Y> input3) {
						keyMapping.setColumnOptions(new ColumnLinkageOptionsByColumn(input1));
						keyMapping.setByConstructor();
						entityConfigurationSupport.setEntityFactoryProvider(new EntityFactoryProviderSupport<>(
								table -> row -> factory.apply(
										(I) row.get(input1),
										(X) row.get(input2),
										(Y) row.get(input3)),
								true));
						return null;
					}
					
					@Override
					public <X, Y> KeyOptions<C, I> usingConstructor(TriFunction<? super I, X, Y, C> factory,
																	String columnName1,
																	String columnName2,
																	String columnName3) {
						keyMapping.setColumnOptions(new ColumnLinkageOptionsSupport(columnName1));
						keyMapping.setByConstructor();
						entityConfigurationSupport.setEntityFactoryProvider(new EntityFactoryProviderSupport<>(
								table -> row -> factory.apply(
										(I) row.get(table.getColumn(columnName1)),
										(X) row.get(table.getColumn(columnName2)),
										(Y) row.get(table.getColumn(columnName3))),
								true));
						return null;
					}
					
					@Override
					public KeyOptions<C, I> usingFactory(Function<ColumnedRow, C> factory) {
						keyMapping.setByConstructor();
						entityConfigurationSupport.setEntityFactoryProvider(new EntityFactoryProviderSupport<>(table -> row -> (C) factory.apply(row), true));
						return null;
					}
				}, true)
				.fallbackOn(entityConfigurationSupport)
				.build((Class<FluentEntityMappingBuilderKeyOptions<C, I>>) (Class) FluentEntityMappingBuilderKeyOptions.class);
	}
	
	public FluentEntityMappingBuilderCompositeKeyOptions<C, I> wrapWithKeyOptions(CompositeKeyLinkageSupport<C, I> keyMapping) {
		return new MethodDispatcher()
				.redirect(CompositeKeyOptions.class, new CompositeKeyOptions<C, I>() {
					
					@Override
					public CompositeKeyOptions<C, I> usingConstructor(Supplier<C> factory) {
						entityConfigurationSupport.setEntityFactoryProvider(new EntityFactoryProviderSupport<>(table -> row -> factory.get(), false));
						return null;
					}
					
					@Override
					public <X, T extends Table> CompositeKeyOptions<C, I> usingConstructor(Function<X, C> factory, Column<T, X> input) {
						keyMapping.setByConstructor();
						entityConfigurationSupport.setEntityFactoryProvider(new EntityFactoryProviderSupport<>(table -> row -> factory.apply(row.get(input)), true));
						return null;
					}
					
					@Override
					public <X> CompositeKeyOptions<C, I> usingConstructor(Function<X, C> factory, String columnName) {
						keyMapping.setByConstructor();
						entityConfigurationSupport.setEntityFactoryProvider(new EntityFactoryProviderSupport<>(table -> row -> factory.apply((X) row.get(table.getColumn(columnName))), true));
						return null;
					}
					
					@Override
					public <X, Y, T extends Table> CompositeKeyOptions<C, I> usingConstructor(BiFunction<X, Y, C> factory,
																								 Column<T, X> input1,
																								 Column<T, Y> input2) {
						keyMapping.setByConstructor();
						entityConfigurationSupport.setEntityFactoryProvider(new EntityFactoryProviderSupport<>(table -> row -> factory.apply(row.get(input1), row.get(input2)), true));
						return null;
					}
					
					@Override
					public <X, Y> CompositeKeyOptions<C, I> usingConstructor(BiFunction<X, Y, C> factory,
																			 String columnName1,
																			 String columnName2) {
						keyMapping.setByConstructor();
						entityConfigurationSupport.setEntityFactoryProvider(new EntityFactoryProviderSupport<>(
								table -> row -> factory.apply(
										(X) row.get(table.getColumn(columnName1)),
										(Y) row.get(table.getColumn(columnName2))),
								true));
						return null;
					}
					
					
					@Override
					public <X, Y, Z, T extends Table> CompositeKeyOptions<C, I> usingConstructor(TriFunction<X, Y, Z, C> factory,
																									Column<T, X> column1,
																									Column<T, Y> column2,
																									Column<T, Z> column3) {
						keyMapping.setByConstructor();
						entityConfigurationSupport.setEntityFactoryProvider(new EntityFactoryProviderSupport<>(
								table -> row -> factory.apply(
										row.get(column1),
										row.get(column2),
										row.get(column3)),
								true));
						return null;
					}
					
					@Override
					public <X, Y, Z> CompositeKeyOptions<C, I> usingConstructor(TriFunction<X, Y, Z, C> factory,
																				String columnName1,
																				String columnName2,
																				String columnName3) {
						keyMapping.setByConstructor();
						entityConfigurationSupport.setEntityFactoryProvider(new EntityFactoryProviderSupport<>(
								table -> row -> factory.apply(
										(X) row.get(table.getColumn(columnName1)),
										(Y) row.get(table.getColumn(columnName2)),
										(Z) row.get(table.getColumn(columnName3))),
								true));
						return null;
					}
					
					@Override
					public CompositeKeyOptions<C, I> usingFactory(Function<ColumnedRow, C> factory) {
						keyMapping.setByConstructor();
						entityConfigurationSupport.setEntityFactoryProvider(new EntityFactoryProviderSupport<>(table -> row -> (C) factory.apply(row), true));
						return null;
					}
				}, true)
				.fallbackOn(entityConfigurationSupport)
				.build((Class<FluentEntityMappingBuilderCompositeKeyOptions<C, I>>) (Class) FluentEntityMappingBuilderCompositeKeyOptions.class);
	}
}
