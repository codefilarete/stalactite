package org.codefilarete.stalactite.spring.repository.query;

import java.util.List;
import java.util.Set;

import org.codefilarete.stalactite.engine.model.Republic;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.spring.repository.StalactiteRepository;
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
	
	Set<NamesOnly> getByNameLikeOrderByPresidentNameAsc(String code);
	
	Slice<NamesOnly> getByNameLikeOrderByPresidentNameAsc(String code, Pageable pageable);
	
	interface NamesOnly {
		
		String getName();
		
		//		@Value("#{target.president.name}")
//		String getPresidentName();
		
		SimplePerson getPresident();
		
		interface SimplePerson {
			
			String getName();
			
		}
	}
	
}
