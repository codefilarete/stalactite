package org.codefilarete.stalactite.spring.repository;

import java.util.Optional;

import org.codefilarete.stalactite.spring.repository.config.EnableStalactiteRepositories;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.NoRepositoryBean;
import org.springframework.data.repository.Repository;

/**
 * Stalactite specific extension of {@link org.springframework.data.repository.Repository}.
 * <ul>As a difference with {@link CrudRepository} it doesn't define :
 * <li> {@link CrudRepository#count()} because it's a bit complex to implement for all cases (table-per-class polymorphism in mind) and doesn't seem to be highly required feature </li>
 * <li> {@link CrudRepository#findAll()} because it may consume too many resources </li>
 * <li> {@link CrudRepository#deleteAll()} because it may consume too many resources and it's quite dangerous </li>
 * <li> {@link CrudRepository#existsById(Object)} because it's a bit complex to implement and one can use {@link #findById(Object)} as a substitute </li>
 * <li> {@link CrudRepository#deleteById(Object)} because optimistic lock can't be applied </li>
 * <li> {@link CrudRepository#deleteAllById(Iterable)} because optimistic lock can't be applied </li>
 * </ul>
 * 
 * This interface must implement {@link Repository} to let Spring detect it as a candidate repository when
 * {@link EnableStalactiteRepositories} is used.
 * 
 * @param <C> entity type
 * @param <I> entity identifier type
 * @author Guillaume Mary
 */
@NoRepositoryBean
public interface StalactiteRepository<C, I> extends Repository<C, I> {
	
	/**
	 * Saves a given entity (creates it in database or updates it). Returned instance is same reference as given one as argument.
	 * This operation might have changed the entity instance, for example, has set an identifier (depending on policy identifier).
	 * Depending on persister, some exception may occur, as one for optimistic lock failure, unsupported type, etc.
	 *
	 * @param entity any object of {@code C} type
	 * @return the given entity
	 */
	<D extends C> D save(D entity);
	
	/**
	 * Saves all given entities. Same rules apply as ones of {@link #save(Object)};
	 *
	 * @param entities any objects of {@code C} type
	 * @return given entities. The size can be equal or less than the number of given {@literal entities}.
	 */
	<D extends C> Iterable<D> saveAll(Iterable<D> entities);
	
	/**
	 * Retrieves all instances of the type.
	 *
	 * @return all entities
	 */
	Iterable<C> findAll();
	
	/**
	 * Retrieves an entity by its id.
	 *
	 * @param id any object of type {@code I}
	 * @return the entity with the given id or an {@link Optional} returning {@link Optional#isPresent()} false.
	 */
	Optional<C> findById(I id);
	
	/**
	 * Returns all mapped instances of type {@code C} with the given IDs.
	 * <p>
	 * If some ids are not found, no entities are returned for these IDs.
	 * <p>
	 * Note that the order of elements in the result is not guaranteed.
	 *
	 * @param ids any objects of type {@code I}
	 * @return the entities with the given ids. The size can be equal or less than the number of given {@literal ids}.
	 */
	Iterable<C> findAllById(Iterable<I> ids);
	
	/**
	 * Deletes a given entity.
	 * Depending on persister, some exception may occur, as one for optimistic lock failure, unsupported type, etc
	 *
	 * @param entity any object of type {@code C}
	 */
	void delete(C entity);
	
	/**
	 * Deletes the given entities.
	 * Depending on persister, some exception may occur, as one for optimistic lock failure, unsupported type, etc
	 *
	 * @param entities any objects of type {@code C}
	 */
	void deleteAll(Iterable<? extends C> entities);
}