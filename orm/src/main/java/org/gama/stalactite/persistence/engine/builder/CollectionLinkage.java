package org.gama.stalactite.persistence.engine.builder;

import java.util.Collection;
import java.util.Map;
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
import org.gama.reflection.ValueAccessPointSet;
import org.gama.stalactite.persistence.engine.ElementCollectionTableNamingStrategy;
import org.gama.stalactite.persistence.engine.LambdaMethodUnsheller;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

import static org.gama.lang.Reflections.propertyName;

/**
 * Support for element-collection configuration
 * 
 * @author Guillaume Mary
 */
public class CollectionLinkage<SRC, TRGT, C extends Collection<TRGT>> {
	
	/** The method that gives the entities from the "root" entity */
	private final IReversibleAccessor<SRC, C> collectionProvider;
	private final Class<TRGT> componentType;
	/** Optional provider of collection instance to be used if collection value is null */
	private Supplier<C> collectionFactory;
	
	private Table targetTable;
	private Column<Table, TRGT> foreignKeyColumn;
	private Map<IReversibleAccessor, String> columnNames;
	
//	private final Class<TRGT> embeddedClass;
//	private final Class<C> collectionType;
//	private final Method insetAccessor;
//	/** Equivalent of {@link #insetAccessor} as a {@link PropertyAccessor}  */
//	private final PropertyAccessor<SRC, TRGT> accessor;
	private final ValueAccessPointMap<String> overridenColumnNames = new ValueAccessPointMap<>();
	private final ValueAccessPointSet excludedProperties = new ValueAccessPointSet();
	private ElementCollectionTableNamingStrategy tableNamingStrategy = ElementCollectionTableNamingStrategy.DEFAULT;
	
	public CollectionLinkage(SerializableBiConsumer<SRC, C> setter, Class<TRGT> componentType, LambdaMethodUnsheller lambdaMethodUnsheller) {
		MutatorByMethodReference<SRC, C> setterReference = Accessors.mutatorByMethodReference((SerializableBiConsumer<SRC, C>) setter);
		this.collectionProvider = new PropertyAccessor<>(
				Accessors.accessor(setterReference.getDeclaringClass(), propertyName(setterReference.getMethodName())),
				setterReference
		);
		this.componentType = componentType;
	}
	
	public CollectionLinkage(SerializableFunction<SRC, C> getter, Class<TRGT> componentType, LambdaMethodUnsheller lambdaMethodUnsheller) {
		AccessorByMethodReference<SRC, C> getterReference = Accessors.accessorByMethodReference((SerializableFunction<SRC, C>) getter);
		this.collectionProvider = new PropertyAccessor<>(
				// we keep close to user demand : we keep its method reference ...
				getterReference,
				// ... but we can't do it for mutator, so we use the most equivalent manner : a mutator based on setter method (fallback to property if not present)
				new AccessorByMethod<SRC, C>(lambdaMethodUnsheller.captureLambdaMethod(getter)).toMutator());
		this.componentType = componentType;
	}
	
	public IReversibleAccessor<SRC, C> getCollectionProvider() {
		return collectionProvider;
	}
	
	public Supplier<C> getCollectionFactory() {
		return collectionFactory;
	}
	
	public ElementCollectionTableNamingStrategy getTableNamingStrategy() {
		return tableNamingStrategy;
	}
	
	//	/**
//	 * Equivalent of {@link #insetAccessor} as a {@link PropertyAccessor}
//	 */
//	public PropertyAccessor<SRC, TRGT> getAccessor() {
//		return accessor;
//	}
//	
//	/**
//	 * Equivalent of given getter or setter at construction time as a {@link Method}
//	 */
//	public Method getInsetAccessor() {
//		return insetAccessor;
//	}
//	
//	public Class<TRGT> getEmbeddedClass() {
//		return embeddedClass;
//	}
//	
//	public ValueAccessPointSet getExcludedProperties() {
//		return this.excludedProperties;
//	}
//	
//	public ValueAccessPointMap<String> getOverridenColumnNames() {
//		return this.overridenColumnNames;
//	}
	
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
	
	public Column<Table, TRGT> getForeignKeyColumn() {
		return foreignKeyColumn;
	}
	
	public void setForeignKeyColumn(Column<Table, TRGT> foreignKeyColumn) {
		this.foreignKeyColumn = foreignKeyColumn;
	}
	
	public Class<TRGT> getComponentType() {
		return componentType;
	}
}
