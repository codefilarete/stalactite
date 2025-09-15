package org.codefilarete.stalactite.spring.repository.query;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.codefilarete.stalactite.engine.model.Republic;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.spring.repository.StalactiteRepository;
import org.codefilarete.stalactite.spring.repository.query.nativ.NativeQueriesRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Repository;

/**
 * Repository of tests for orderBy and limit cases : both work only with a mapping that doesn't imply Collection property
 * 
 * @author Guillaume Mary
 */
@Repository
public interface DerivedQueriesWithoutMappedCollectionRepository extends StalactiteRepository<Republic, Identifier<Long>> {
	
	Republic findFirstByOrderByNameAsc();
	
	Set<Republic> findTop2ByOrderByNameAsc();
	
	Set<Republic> findByNameLike(String name, Sort sort);
	
	Page<Republic> findByNameLikeOrderByIdAsc(String name, Pageable pageable);
	
	Page<Republic> findByNameLike(String name, Pageable pageable);
	
	Slice<Republic> searchByNameLikeOrderByIdAsc(String name);
	
	Slice<Republic> searchByNameLikeOrderByIdAsc(String name, Pageable pageable);
	
	Slice<Republic> searchByNameLike(String name, Pageable pageable);
	
	// Note that "stream" keyword doesn't imply returning a Stream, it is used to distinguish the same method behavior from the other returning a Slice
	Stream<Republic> streamByNameLikeOrderByIdAsc(String name);
	
	// Note that "stream" keyword doesn't imply returning a Stream, it is used to distinguish the same method behavior from the other returning a Slice
	Stream<Republic> streamByNameLikeOrderByIdAsc(String name, Pageable pageable);
	
	// projection tests
	NamesOnly getByName(String name);
	
	<T> Collection<T> getByName(String name, Class<T> type);
	
	Set<NamesOnly> getByNameLikeOrderByPresidentNameAsc(String name);
	
	Slice<NamesOnly> getByNameLikeOrderByPresidentNameAsc(String name, Pageable pageable);
	
	interface NamesOnly {
		
		String getName();
		
		SimplePerson getPresident();
		
		interface SimplePerson {
			
			String getName();
			
		}
	}
	
	// This class should create a query that retrieves the whole aggregate because it has a @Value annotation
	// Indeed, values required by the @Value annotations can't be known in advance, so the query must retrieve the whole aggregate
	interface NamesOnlyWithValue extends NamesOnly {
		
		@Value("#{target.president.name + '-' + target.president.id.delegate}")
		String getPresidentName();
	}
	
}
