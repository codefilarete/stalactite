package org.codefilarete.stalactite.sql.spring.repository.query;

import java.util.Set;

import org.codefilarete.stalactite.engine.model.Republic;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.sql.spring.repository.StalactiteRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Repository;

/**
 * @author Guillaume Mary
 */
@Repository
public interface DerivedQueriesWithoutMappedCollectionRepository extends StalactiteRepository<Republic, Identifier<Long>> {
	
	Republic findFirstByOrderByNameAsc();
	
	Set<Republic> findTop2ByOrderByNameAsc();
	
	Set<Republic> findByNameLike(String name, Sort sort);
	
	Page<Republic> findByNameLikeOrderByIdAsc(String name, Pageable pageable);
	
	Page<Republic> findByNameLike(String name, Pageable pageable);
	
	Slice<Republic> searchByNameLikeOrderByIdAsc(String name, Pageable pageable);
	
	Slice<Republic> searchByNameLike(String name, Pageable pageable);
	
}
