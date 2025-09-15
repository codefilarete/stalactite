package org.codefilarete.stalactite.spring.repository.query;

import java.util.Set;

import org.codefilarete.stalactite.engine.model.Republic;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.spring.repository.StalactiteRepository;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Repository;

@Repository
public interface ThreadSafetyRepository extends StalactiteRepository<Republic, Identifier<Long>> {
	
	Set<Republic> findByIdIn(Iterable<Identifier<Long>> ids);
	
	Set<Republic> findByNameLike(String name, Sort sort);
	
}