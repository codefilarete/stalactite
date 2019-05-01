package org.gama.stalactite.persistence.engine.builder;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.function.Function;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.Reflections;
import org.gama.stalactite.persistence.engine.CascadeOptions.RelationshipMode;
import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * 
 * @param <SRC> the "one" type
 * @param <TRGT> the "many" type
 * @param <TRGTID> identifier type of TRGT
 * @param <C> the "many" collection type
 */
public class CascadeMany<SRC, TRGT, TRGTID, C extends Collection<TRGT>> {
	
	/** The method that gives the "many" entities from the "one" entity */
	private final Function<SRC, C> collectionProvider;
	
	/** Same as {@link #collectionProvider}, but with the reflection API */
	private final Method collectionGetter;
	
	/** "many" entity {@link Persister} */
	private final Persister<TRGT, TRGTID, ? extends Table> persister;
	
	private final Class<C> collectionTargetClass;
	
	/** the method that gets the "one" entity from the "many" entities */
	private SerializableFunction<TRGT, SRC> reverseGetter;
	
	/** the method that sets the "one" entity onto the "many" entities */
	private SerializableBiConsumer<TRGT, SRC> reverseSetter;
	
	private Column<Table, SRC> reverseColumn;
	
	/** Default relationship mode is readonly */
	private RelationshipMode relationshipMode = RelationshipMode.READ_ONLY;
	
	public CascadeMany(Function<SRC, C> collectionProvider, Persister<TRGT, TRGTID, ? extends Table> persister, Method collectionGetter) {
		this(collectionProvider, persister, (Class<C>) Reflections.javaBeanTargetType(collectionGetter), collectionGetter);
	}
	
	protected CascadeMany(Function<SRC, C> collectionProvider, Persister<TRGT, TRGTID, ? extends Table> persister, Class<C> collectionTargetClass, Method collectionGetter) {
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
	
	public Persister<TRGT, TRGTID, ?> getPersister() {
		return persister;
	}
	
	public Class<C> getCollectionTargetClass() {
		return collectionTargetClass;
	}
	
	public SerializableFunction<TRGT, SRC> getReverseGetter() {
		return reverseGetter;
	}
	
	public void setReverseGetter(SerializableFunction<TRGT, SRC> reverseGetter) {
		this.reverseGetter = reverseGetter;
	}
	
	public SerializableBiConsumer<TRGT, SRC> getReverseSetter() {
		return reverseSetter;
	}
	
	public void setReverseSetter(SerializableBiConsumer<TRGT, SRC> reverseSetter) {
		this.reverseSetter = reverseSetter;
	}
	
	public Column<Table, SRC> getReverseColumn() {
		return reverseColumn;
	}
	
	public void setReverseColumn(Column<Table, SRC> reverseColumn) {
		this.reverseColumn = reverseColumn;
	}
	
	public RelationshipMode getRelationshipMode() {
		return relationshipMode;
	}
	
	public void setRelationshipMode(RelationshipMode relationshipMode) {
		this.relationshipMode = relationshipMode;
	}
}
