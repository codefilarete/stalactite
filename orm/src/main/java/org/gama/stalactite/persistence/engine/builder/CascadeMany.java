package org.gama.stalactite.persistence.engine.builder;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.function.Function;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.Reflections;
import org.gama.stalactite.persistence.engine.CascadeOptions.RelationshipMode;
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
	
	/** The method that gives the "many" entities" from the "one" entity */
	private final Function<SRC, C> collectionProvider;
	/** Same as {@link #collectionProvider}, but with the reflection API */
	private final Method collectionGetter;
	/** {@link Persister} for the "many" entity */
	private final Persister<O, J, ? extends Table> persister;
	private final Class<C> collectionTargetClass;
	/** the method that sets the "one" entity onto the "many" entities */
	private SerializableBiConsumer<O, SRC> reverseSetter;
	/** the method that gets the "one" entity from the "many" entities */
	private SerializableFunction<O, SRC> reverseGetter;
	private Column<Table, SRC> reverseColumn;
	/** Default relationship mode is readonly */
	private RelationshipMode relationshipMode = RelationshipMode.READ_ONLY;
	
	public CascadeMany(Function<SRC, C> collectionProvider, Persister<O, J, ? extends Table> persister, Method collectionGetter) {
		this(collectionProvider, persister, (Class<C>) Reflections.javaBeanTargetType(collectionGetter), collectionGetter);
	}
	
	protected CascadeMany(Function<SRC, C> collectionProvider, Persister<O, J, ? extends Table> persister, Class<C> collectionTargetClass, Method collectionGetter) {
		this.collectionProvider = collectionProvider;
		this.persister = persister;
		this.collectionGetter = collectionGetter;
		this.collectionTargetClass = collectionTargetClass;
	}
	
	public Function<SRC, C> getCollectionProvider() {
		return collectionProvider;
	}
	
	public Method getCollectionGetter() {
		return collectionGetter;
	}
	
	public Persister<O, J, ?> getPersister() {
		return persister;
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
	
	public RelationshipMode getRelationshipMode() {
		return relationshipMode;
	}
	
	public void setRelationshipMode(RelationshipMode relationshipMode) {
		this.relationshipMode = relationshipMode;
	}
}
