package org.codefilarete.stalactite.sql.spring.repository;

import java.util.Optional;

import org.codefilarete.stalactite.engine.EntityPersister;
import org.springframework.stereotype.Repository;

/**
 * Default implementation of the {@link StalactiteRepository} interface.
 * Mimics SimpleJpaRepository 
 * 
 * @param <C> entity type
 * @param <I> identifier type
 * @author Guillaume Mary
 */
@Repository
public class SimpleStalactiteRepository<C, I> implements StalactiteRepository<C, I> {
	
	private final EntityPersister<C, I> persister;
	
	public SimpleStalactiteRepository(EntityPersister<C, I> persister) {
		this.persister = persister;
	}
	
	@Override
	public <S extends C> S save(S entity) {
		persister.persist(entity);
		return entity;
	}
	
	@Override
	public <S extends C> Iterable<S> saveAll(Iterable<S> entities) {
		persister.persist(entities);
		return entities;
	}
	
	@Override
	public Optional<C> findById(I id) {
		return Optional.ofNullable(persister.select(id));
	}
	
	@Override
	public Iterable<C> findAllById(Iterable<I> ids) {
		return persister.select(ids);
	}
	
	@Override
	public void delete(C entity) {
		persister.delete(entity);
	}
	
	@Override
	public void deleteAll(Iterable<? extends C> entities) {
		persister.delete(entities);
	}
}
