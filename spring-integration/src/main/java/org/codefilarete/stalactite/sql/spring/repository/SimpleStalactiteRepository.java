package org.codefilarete.stalactite.sql.spring.repository;

import java.util.Optional;

import org.codefilarete.stalactite.engine.EntityPersister;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/**
 * Default implementation of the {@link StalactiteRepository} interface.
 * Mimics SimpleJpaRepository 
 * 
 * @param <C> entity type
 * @param <I> identifier type
 * @author Guillaume Mary
 */
@Repository
// Adding transaction for all methods of this class. Read-only by default, those which require some write access
// are annotated without readOnly = true
@Transactional(readOnly = true)
public class SimpleStalactiteRepository<C, I> implements StalactiteRepository<C, I> {
	
	private final EntityPersister<C, I> persister;
	
	public SimpleStalactiteRepository(EntityPersister<C, I> persister) {
		this.persister = persister;
	}
	
	@Transactional
	@Override
	public <S extends C> S save(S entity) {
		persister.persist(entity);
		return entity;
	}
	
	@Transactional
	@Override
	public <S extends C> Iterable<S> saveAll(Iterable<S> entities) {
		persister.persist(entities);
		return entities;
	}
	
	@Override
	public Iterable<C> findAll() {
		return persister.selectAll();
	}
	
	@Override
	public Optional<C> findById(I id) {
		return Optional.ofNullable(persister.select(id));
	}
	
	@Override
	public Iterable<C> findAllById(Iterable<I> ids) {
		return persister.select(ids);
	}
	
	@Transactional
	@Override
	public void delete(C entity) {
		persister.delete(entity);
	}
	
	@Transactional
	@Override
	public void deleteAll(Iterable<? extends C> entities) {
		persister.delete(entities);
	}
}
