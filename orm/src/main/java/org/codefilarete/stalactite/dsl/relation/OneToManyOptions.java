package org.codefilarete.stalactite.dsl.relation;

import java.util.Collection;
import java.util.function.Supplier;

import org.codefilarete.stalactite.dsl.property.CascadeOptions;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.codefilarete.stalactite.engine.configurer.onetomany.OneToManyRelationConfigurer;

/**
 * Interface for one-to-many relation options
 *
 * @param <C> entity type
 * @param <O> type of {@link Collection} element
 * @param <S> refined {@link Collection} type
 *
 * @author Guillaume Mary
 */
public interface OneToManyOptions<C, O, S extends Collection<O>> extends CascadeOptions {
	
	/**
	 * Defines the bidirectional relation.
	 * No need to additionally call {@link #mappedBy(SerializableFunction)}.
	 *
	 * If the relation is already defined through {@link #mappedBy(SerializableFunction)} then there's no
	 * guaranty about which one will be taken first. Algorithm is defined in {@link OneToManyRelationConfigurer}.
	 *
	 * Signature note : given consumer accepts "? super C" to allow given method to return an abstraction of current mapping definition, especially
	 * in case of inheritance where current mapping is made of inheritance and target entities only maps it as an upper (ancestor) class
	 *
	 * @param reverseLink opposite owner of the relation (setter)
	 * @return the global mapping configurer
	 */
	OneToManyOptions<C, O, S> mappedBy(SerializableBiConsumer<O, ? super C> reverseLink);
	
	/**
	 * Defines the bidirectional relation stored in target entity table.
	 * No need to additionally call {@link #mappedBy(SerializableBiConsumer)}.
	 *
	 * If the relation is already defined through {@link #mappedBy(SerializableBiConsumer)} then there's no
	 * guaranty about which one will be taken first. Algorithm is defined in {@link OneToManyRelationConfigurer}.
	 *
	 *
	 * Signature note : given function accepts "? super C" to allow given method to return an abstraction of current mapped entity, for example
	 * if current mapped entity inherits from an abstraction and target entity only maps the ancestor.
	 *
	 * @param reverseLink opposite owner of the relation (getter)
	 * @return the global mapping configurer
	 */
	OneToManyOptions<C, O, S> mappedBy(SerializableFunction<O, ? super C> reverseLink);
	
	/**
	 * Defines reverse side owning column name.
	 * Note that defining it this way will not allow relation to be fixed in memory (after select in database), prefer {@link #mappedBy(SerializableBiConsumer)}.
	 * Use this method to define unidirectional relation.
	 *
	 * If the relation is already defined through {@link #mappedBy(SerializableFunction)} or {@link #mappedBy(SerializableBiConsumer)} then there's no
	 * guaranty about which one will be taken first. Algorithm is defined in {@link OneToManyRelationConfigurer}.
	 *
	 * @param reverseColumnName opposite owner of the relation
	 * @return the global mapping configurer
	 */
	OneToManyOptions<C, O, S> mappedBy(String reverseColumnName);
	
	/**
	 * Defines setter of current entity on target entity, which is only interesting while dealing with relation mapped
	 * through an association table (no use of {@link #mappedBy(SerializableFunction)}) because reverse setter can't be
	 * deduced.
	 * This method has no consequence on database mapping since it only interacts in memory.
	 * If used with owned association ({@link #mappedBy(SerializableFunction)} already used) it would have no consequence
	 * and won't be taken into account.
	 *
	 * @param reverseLink opposite owner of the relation
	 * @return the global mapping configurer
	 */
	OneToManyOptions<C, O, S> reverselySetBy(SerializableBiConsumer<O, C> reverseLink);
	
	/**
	 * Defines the collection factory to be used at load time to initialize property if it is null.
	 * Useful for cases where property is lazily initialized in bean.
	 *
	 * @param collectionFactory a collection factory
	 * @return the global mapping configurer
	 */
	OneToManyOptions<C, O, S> initializeWith(Supplier<S> collectionFactory);
	
	/**
	 * Asks to load relation in some separate query (actually may use several queries according to association table presence or polymorphism)
	 *
	 * @return the global mapping configurer
	 */
	OneToManyOptions<C, O, S> fetchSeparately();
	
	/**
	 * Activates entity order persistence and indicates column name to be used for it.
	 * Collection that stores data is expected to support ordering by index (as List or LinkedHashSet)
	 *
	 * @param columnName the column name to be used for order persistence
	 * @return the global mapping configurer
	 */
	OneToManyOptions<C, O, S> indexedBy(String columnName);
	
	/**
	 * Activates entity order persistence.
	 * Collection that stores data is expected to support ordering by index (as List or LinkedHashSet)
	 *
	 * @return the global mapping configurer
	 */
	OneToManyOptions<C, O, S> indexed();
	
	/**
	 * {@inheritDoc}
	 * Overridden for type accuracy
	 *
	 * @param relationMode any {@link RelationMode}
	 * @return the global mapping configurer
	 */
	OneToManyOptions<C,  O, S> cascading(RelationMode relationMode);
}
