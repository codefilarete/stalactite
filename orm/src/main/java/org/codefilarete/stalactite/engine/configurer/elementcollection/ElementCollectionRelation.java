package org.codefilarete.stalactite.engine.configurer.elementcollection;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.function.Supplier;

import org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfigurationProvider;
import org.codefilarete.stalactite.engine.configurer.LambdaMethodUnsheller;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.codefilarete.reflection.AccessorByMethod;
import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.MutatorByMethodReference;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ValueAccessPointMap;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

import static org.codefilarete.tool.Reflections.propertyName;

/**
 * Support for element-collection configuration
 * 
 * @author Guillaume Mary
 */
public class ElementCollectionRelation<SRC, TRGT, C extends Collection<TRGT>> {
	
	/** The method that gives the entities from the "root" entity */
	private final ReversibleAccessor<SRC, C> collectionProvider;
	private final Class<TRGT> componentType;
	/** Optional provider of collection instance to be used if collection value is null */
	private Supplier<C> collectionFactory;
	
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
	private String elementColumn;
	
	/**
	 * @param setter collection accessor
	 * @param componentType element type in collection
	 * @param embeddableConfigurationProvider complex type mapping, null when element is a simple type (String, Integer, ...)
	 */
	public ElementCollectionRelation(SerializableBiConsumer<SRC, C> setter,
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
	public ElementCollectionRelation(SerializableFunction<SRC, C> getter,
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
	
	public ReversibleAccessor<SRC, C> getCollectionProvider() {
		return collectionProvider;
	}
	
	public Supplier<C> getCollectionFactory() {
		return collectionFactory;
	}
	
	public ElementCollectionRelation<SRC, TRGT, C> setCollectionFactory(Supplier<? extends C> collectionFactory) {
		this.collectionFactory = (Supplier<C>) collectionFactory;
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
	
	public Table getTargetTable() {
		return targetTable;
	}
	
	public void setTargetTable(Table targetTable) {
		this.targetTable = targetTable;
	}
	
	public String getTargetTableName() {
		return targetTableName;
	}
	
	public ElementCollectionRelation<SRC, TRGT, C> setTargetTableName(String targetTableName) {
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
	
	public ElementCollectionRelation<SRC, TRGT, C> setReverseColumnName(String reverseColumnName) {
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
}
