package org.gama.stalactite.persistence.engine.builder;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.Reflections;
import org.gama.stalactite.persistence.engine.CascadeOption.CascadeType;
import org.gama.stalactite.persistence.engine.OneToManyOptions.RelationshipMaintenanceMode;
import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.manager.StatefullIdentifier;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * 
 * @param <SRC> the "one" type
 * @param <O> the "many" type
 * @param <J> identifier type of O
 * @param <C> the "many" collection type
 */
public class CascadeMany<SRC extends Identified, O extends Identified, J extends StatefullIdentifier, C extends Collection<O>> {
	
	/** the method that gets the "many" entities" from the "one" entity */
	private final Function<SRC, C> targetProvider;
	/** {@link Persister} for the "one" entity */
	private final Persister<O, J, ? extends Table> persister;
	private final Method member;
	private final Class<C> collectionTargetClass;
	/** the method that sets the "one" entity onto the "many" entities */
	private SerializableBiConsumer<O, SRC> reverseSetter;
	/** the method that gets the "one" entity from the "many" entities */
	private SerializableFunction<O, SRC> reverseGetter;
	private Column<Table, SRC> reverseColumn;
	private final Set<CascadeType> cascadeTypes = new HashSet<>();
	/** Should we delete removed entities from the Collection (for UPDATE cascade) */
	private boolean deleteRemoved = false;
	private RelationshipMaintenanceMode maintenanceMode;
	
	public CascadeMany(Function<SRC, C> targetProvider, Persister<O, J, ? extends Table> persister, Method method) {
		this(targetProvider, persister, (Class<C>) Reflections.javaBeanTargetType(method), method);
	}
	
	protected CascadeMany(Function<SRC, C> targetProvider, Persister<O, J, ? extends Table> persister, Class<C> collectionTargetClass, Method method) {
		this.targetProvider = targetProvider;
		this.persister = persister;
		this.member = method;
		this.collectionTargetClass = collectionTargetClass;
	}
	
	public Function<SRC, C> getTargetProvider() {
		return targetProvider;
	}
	
	public Persister<O, J, ?> getPersister() {
		return persister;
	}
	
	public Method getMember() {
		return member;
	}
	
	public Class<C> getCollectionTargetClass() {
		return collectionTargetClass;
	}
	
	public SerializableBiConsumer<O, SRC> getReverseSetter() {
		return reverseSetter;
	}
	
	public void setReverseSetter(SerializableBiConsumer<O, SRC> reverseSetter) {
		this.reverseSetter = reverseSetter;
	}
	
	public Column<Table, SRC> getReverseColumn() {
		return reverseColumn;
	}
	
	public void setReverseColumn(Column<Table, SRC> reverseColumn) {
		this.reverseColumn = reverseColumn;
	}
	
	public SerializableFunction<O, SRC> getReverseGetter() {
		return reverseGetter;
	}
	
	public void setReverseGetter(SerializableFunction<O, SRC> reverseGetter) {
		this.reverseGetter = reverseGetter;
	}
	
	public Set<CascadeType> getCascadeTypes() {
		return cascadeTypes;
	}
	
	public void addCascadeType(CascadeType cascadeType) {
		this.cascadeTypes.add(cascadeType);
	}
	
	public boolean shouldDeleteRemoved() {
		return deleteRemoved;
	}
	
	public RelationshipMaintenanceMode getMaintenanceMode() {
		return maintenanceMode;
	}
	
	public void setMaintenanceMode(RelationshipMaintenanceMode maintenanceMode) {
		this.maintenanceMode = maintenanceMode;
	}
}
