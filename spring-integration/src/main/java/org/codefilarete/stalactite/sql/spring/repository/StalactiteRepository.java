package org.codefilarete.stalactite.sql.spring.repository;

import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.NoRepositoryBean;

/**
 * Stalactite specific extension of {@link org.springframework.data.repository.Repository}.
 * 
 * @param <T> entity type
 * @param <ID> entity identifier type
 * @author Guillaume Mary
 */
@NoRepositoryBean
public interface StalactiteRepository<T, ID> extends CrudRepository<T, ID> {
	
}