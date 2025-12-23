package org.codefilarete.stalactite.engine.configurer.elementcollection;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;
import java.util.function.Supplier;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.AccessorByMethod;
import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.AccessorChain.ValueInitializerOnNullValue;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.MutatorByMethodReference;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.reflection.ValueAccessPointMap;
import org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfigurationProvider;
import org.codefilarete.stalactite.engine.configurer.LambdaMethodUnsheller;
import org.codefilarete.stalactite.sql.ddl.Size;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.Reflections;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

import static org.codefilarete.tool.Reflections.propertyName;

/**
 * Support for element-collection configuration
 * 
 * @author Guillaume Mary
 */
public class ElementCollectionRelation<SRC, TRGT, S extends Collection<TRGT>> {
	
	/** The method that gives the entities from the "root" entity */
	private final ReversibleAccessor<SRC, S> collectionAccessor;
	private final Class<TRGT> componentType;
	/** Optional provider of collection instance to be used if collection value is null */
	private Supplier<S> collectionFactory;
	
	private Table targetTable;
	private String targetTableName;
	private Column<Table, ?> reverseColumn;
	private String reverseColumnName;
	
	/** Element column name override, used in simple case : {@link EmbeddableMappingConfigurationProvider} null, aka not when element is a complex type */
	private String elementColumnName;
	
	/** Complex type mapping, optional */
	@Nullable
	private final EmbeddableMappingConfigurationProvider<TRGT> embeddableConfigurationProvider;
	
	/** Complex type mapping override, to be used when {@link EmbeddableMappingConfigurationProvider} is not null */
	private final ValueAccessPointMap<SRC, String> overriddenColumnNames = new ValueAccessPointMap<>();
	
	/** Complex type mapping override, to be used when {@link EmbeddableMappingConfigurationProvider} is not null */
	private final ValueAccessPointMap<SRC, Size> overriddenColumnSizes = new ValueAccessPointMap<>();
	
	private Size elementColumnSize;
	
	/**
	 * @param setter collection accessor
	 * @param componentType element type in collection
	 * @param embeddableConfigurationProvider complex type mapping, null when element is a simple type (String, Integer, ...)
	 */
	public ElementCollectionRelation(SerializableBiConsumer<SRC, S> setter,
									 Class<TRGT> componentType,
									 @Nullable EmbeddableMappingConfigurationProvider<TRGT> embeddableConfigurationProvider) {
		MutatorByMethodReference<SRC, S> setterReference = Accessors.mutatorByMethodReference(setter);
		this.collectionAccessor = new PropertyAccessor<>(
				Accessors.accessor(setterReference.getDeclaringClass(), propertyName(setterReference.getMethodName())),
				setterReference
		);
		this.componentType = componentType;
		this.embeddableConfigurationProvider = embeddableConfigurationProvider;
	}
	
	/**
	 * @param getter collection accessor
	 * @param componentType element type in collection
	 * @param lambdaMethodUnsheller engine to capture getter method reference
	 * @param embeddableConfigurationProvider complex type mapping, null when element is a simple type (String, Integer, ...)
	 */
	public ElementCollectionRelation(SerializableFunction<SRC, S> getter,
									 Class<TRGT> componentType,
									 LambdaMethodUnsheller lambdaMethodUnsheller,
									 @Nullable EmbeddableMappingConfigurationProvider<TRGT> embeddableConfigurationProvider) {
		AccessorByMethodReference<SRC, S> getterReference = Accessors.accessorByMethodReference(getter);
		this.collectionAccessor = new PropertyAccessor<>(
				// we keep close to user demand : we keep its method reference ...
				getterReference,
				// ... but we can't do it for mutator, so we use the most equivalent manner : a mutator based on setter method (fallback to property if not present)
				new AccessorByMethod<SRC, S>(lambdaMethodUnsheller.captureLambdaMethod(getter)).toMutator());
		this.componentType = componentType;
		this.embeddableConfigurationProvider = embeddableConfigurationProvider;
	}
	
	public ElementCollectionRelation(ReversibleAccessor<SRC, S> collectionAccessor ,
									 Class<TRGT> componentType,
									 @Nullable EmbeddableMappingConfigurationProvider<TRGT> embeddableConfigurationProvider) {
		this.collectionAccessor = collectionAccessor;
		this.componentType = componentType;
		this.embeddableConfigurationProvider = embeddableConfigurationProvider;
	}
	
	public ReversibleAccessor<SRC, S> getCollectionAccessor() {
		return collectionAccessor;
	}
	
	public Supplier<S> getCollectionFactory() {
		return collectionFactory;
	}
	
	public ElementCollectionRelation<SRC, TRGT, S> setCollectionFactory(Supplier<? extends S> collectionFactory) {
		this.collectionFactory = (Supplier<S>) collectionFactory;
		return this;
	}

	public ValueAccessPointMap<SRC, String> getOverriddenColumnNames() {
		return this.overriddenColumnNames;
	}
	
	public void overrideName(SerializableFunction methodRef, String columnName) {
		this.overriddenColumnNames.put(new AccessorByMethodReference(methodRef), columnName);
	}
	
	public void overrideName(SerializableBiConsumer methodRef, String columnName) {
		this.overriddenColumnNames.put(new MutatorByMethodReference(methodRef), columnName);
	}
	
	public void overrideSize(SerializableFunction methodRef, Size columnSize) {
		this.overriddenColumnSizes.put(new AccessorByMethodReference(methodRef), columnSize);
	}
	
	public void overrideSize(SerializableBiConsumer methodRef, Size columnSize) {
		this.overriddenColumnSizes.put(new MutatorByMethodReference(methodRef), columnSize);
	}
	
	public ValueAccessPointMap<SRC, Size> getOverriddenColumnSizes() {
		return this.overriddenColumnSizes;
	}
	
	public Table getTargetTable() {
		return targetTable;
	}
	
	public void setTargetTable(Table targetTable) {
		this.targetTable = targetTable;
	}
	
	public String getTargetTableName() {
		return targetTableName;
	}
	
	public ElementCollectionRelation<SRC, TRGT, S> setTargetTableName(String targetTableName) {
		this.targetTableName = targetTableName;
		return this;
	}
	
	public <I> Column<Table, I> getReverseColumn() {
		return (Column<Table, I>) reverseColumn;
	}
	
	public void setReverseColumn(Column<Table, ?> reverseColumn) {
		this.reverseColumn = reverseColumn;
	}
	
	public String getReverseColumnName() {
		return reverseColumnName;
	}
	
	public ElementCollectionRelation<SRC, TRGT, S> setReverseColumnName(String reverseColumnName) {
		this.reverseColumnName = reverseColumnName;
		return this;
	}
	
	public Class<TRGT> getComponentType() {
		return componentType;
	}
	
	public EmbeddableMappingConfigurationProvider<TRGT> getEmbeddableConfigurationProvider() {
		return embeddableConfigurationProvider;
	}
	
	public void setElementColumnName(String columnName) {
		this.elementColumnName = columnName;
	}
	
	public String getElementColumnName() {
		return elementColumnName;
	}
	
	public void setElementColumnSize(Size elementColumnSize) {
		this.elementColumnSize = elementColumnSize;
	}
	
	public Size getElementColumnSize() {
		return elementColumnSize;
	}
	
	/**
	 * Clones this object to make one with the given accessor as prefix of current one.
	 * Made to "slide" current instance with an accessor prefix. Used for embeddable objects with relation to make the relation being accessible
	 * from the "root" entity.
	 *
	 * @param accessor the prefix of the clone to be created
	 * @param embeddedType the concrete type of the embeddable bean, because accessor may provide an abstraction
	 * @return a clones of this instance prefixed with the given accessor
	 * @param <C> the root entity type that owns the embeddable which has this relation
	 */
	public <C> ElementCollectionRelation<C, TRGT, S> embedInto(Accessor<C, SRC> accessor, Class<SRC> embeddedType) {
		AccessorChain<C, S> shiftedTargetProvider = new AccessorChain<>(accessor, collectionAccessor);
		shiftedTargetProvider.setNullValueHandler(new ValueInitializerOnNullValue() {
			@Override
			protected <T> T newInstance(Accessor<?, T> segmentAccessor, Class<T> valueType) {
				if (segmentAccessor == accessor) {
					return (T) Reflections.newInstance(embeddedType);
				} else if (segmentAccessor == collectionAccessor){
					if (collectionFactory != null) {
						return (T) collectionFactory.get();
					} else {
						return super.newInstance(segmentAccessor, valueType);
					}
				} else {
					return super.newInstance(segmentAccessor, valueType);
				}
			}
		});
		ElementCollectionRelation<C, TRGT, S> result = new ElementCollectionRelation<C, TRGT, S>(shiftedTargetProvider, componentType, embeddableConfigurationProvider);
		
		result.setElementColumnName(this.getElementColumnName());
		result.setElementColumnSize(this.getElementColumnSize());
		result.setTargetTable(this.getTargetTable());
		result.setTargetTableName(this.getTargetTableName());
		result.setReverseColumn(this.getReverseColumn());
		result.setReverseColumnName(this.getReverseColumnName());
		result.getOverriddenColumnNames().putAll((Map<? extends ValueAccessPoint<C>, ? extends String>) this.getOverriddenColumnNames());
		result.getOverriddenColumnSizes().putAll((Map<? extends ValueAccessPoint<C>, ? extends Size>) this.getOverriddenColumnSizes());
		result.setCollectionFactory(this.getCollectionFactory());
		return result;
	}
}
