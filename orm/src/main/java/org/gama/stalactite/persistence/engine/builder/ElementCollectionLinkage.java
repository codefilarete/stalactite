package org.gama.stalactite.persistence.engine.builder;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.function.Supplier;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.reflection.AccessorByMethod;
import org.gama.reflection.AccessorByMethodReference;
import org.gama.reflection.Accessors;
import org.gama.reflection.IReversibleAccessor;
import org.gama.reflection.MutatorByMethodReference;
import org.gama.reflection.PropertyAccessor;
import org.gama.reflection.ValueAccessPointMap;
import org.gama.stalactite.persistence.engine.EmbeddableMappingConfigurationProvider;
import org.gama.stalactite.persistence.engine.LambdaMethodUnsheller;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

import static org.gama.lang.Reflections.propertyName;

/**
 * Support for element-collection configuration
 * 
 * @author Guillaume Mary
 */
public class ElementCollectionLinkage<SRC, TRGT, C extends Collection<TRGT>> {
	
	/** The method that gives the entities from the "root" entity */
	private final IReversibleAccessor<SRC, C> collectionProvider;
	private final Class<TRGT> componentType;
	/** Optional provider of collection instance to be used if collection value is null */
	private Supplier<C> collectionFactory;
	
	private Table targetTable;
	private String targetTableName;
	private Column<Table, TRGT> reverseColumn;
	private String reverseColumnName;
	
	/** Element column name override, used in simple case : {@link EmbeddableMappingConfigurationProvider} null, aka not when element is a complex type */
	private String elementColumnName;
	
	/** Complext type mapping, optional */
	private final EmbeddableMappingConfigurationProvider<TRGT> embeddableConfigurationProvider;
	
	/** Complext type mapping override, to be used when {@link EmbeddableMappingConfigurationProvider} is not null */
	private final ValueAccessPointMap<String> overridenColumnNames = new ValueAccessPointMap<>();
	
	/**
	 * @param setter collection accessor
	 * @param componentType element type in collection
	 * @param embeddableConfigurationProvider complex type mapping, null when element is a simple type (String, Integer, ...)
	 */
	public ElementCollectionLinkage(SerializableBiConsumer<SRC, C> setter,
									Class<TRGT> componentType,
									@Nullable EmbeddableMappingConfigurationProvider<TRGT> embeddableConfigurationProvider) {
		MutatorByMethodReference<SRC, C> setterReference = Accessors.mutatorByMethodReference(setter);
		this.collectionProvider = new PropertyAccessor<>(
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
	public ElementCollectionLinkage(SerializableFunction<SRC, C> getter,
									Class<TRGT> componentType,
									LambdaMethodUnsheller lambdaMethodUnsheller,
									@Nullable EmbeddableMappingConfigurationProvider<TRGT> embeddableConfigurationProvider) {
		AccessorByMethodReference<SRC, C> getterReference = Accessors.accessorByMethodReference(getter);
		this.collectionProvider = new PropertyAccessor<>(
				// we keep close to user demand : we keep its method reference ...
				getterReference,
				// ... but we can't do it for mutator, so we use the most equivalent manner : a mutator based on setter method (fallback to property if not present)
				new AccessorByMethod<SRC, C>(lambdaMethodUnsheller.captureLambdaMethod(getter)).toMutator());
		this.componentType = componentType;
		this.embeddableConfigurationProvider = embeddableConfigurationProvider;
	}
	
	public IReversibleAccessor<SRC, C> getCollectionProvider() {
		return collectionProvider;
	}
	
	public Supplier<C> getCollectionFactory() {
		return collectionFactory;
	}
	
	public ElementCollectionLinkage<SRC, TRGT, C> setCollectionFactory(Supplier<? extends C> collectionFactory) {
		this.collectionFactory = (Supplier<C>) collectionFactory;
		return this;
	}

	public ValueAccessPointMap<String> getOverridenColumnNames() {
		return this.overridenColumnNames;
	}
	
	public void overrideName(SerializableFunction methodRef, String columnName) {
		this.overridenColumnNames.put(new AccessorByMethodReference(methodRef), columnName);
	}
	
	public void overrideName(SerializableBiConsumer methodRef, String columnName) {
		this.overridenColumnNames.put(new MutatorByMethodReference(methodRef), columnName);
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
	
	public ElementCollectionLinkage<SRC, TRGT, C> setTargetTableName(String targetTableName) {
		this.targetTableName = targetTableName;
		return this;
	}
	
	public Column<Table, TRGT> getReverseColumn() {
		return reverseColumn;
	}
	
	public void setReverseColumn(Column<Table, TRGT> reverseColumn) {
		this.reverseColumn = reverseColumn;
	}
	
	public String getReverseColumnName() {
		return reverseColumnName;
	}
	
	public ElementCollectionLinkage<SRC, TRGT, C> setReverseColumnName(String reverseColumnName) {
		this.reverseColumnName = reverseColumnName;
		return this;
	}
	
	public Class<TRGT> getComponentType() {
		return componentType;
	}
	
	public EmbeddableMappingConfigurationProvider<TRGT> getEmbeddableConfigurationProvider() {
		return embeddableConfigurationProvider;
	}
	
	public void overrideColumnName(String columnName) {
		this.elementColumnName = columnName;
	}
	
	public String getElementColumnName() {
		return elementColumnName;
	}
}
