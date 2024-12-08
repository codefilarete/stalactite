package org.codefilarete.stalactite.spring.repository.query.bean;

import java.util.Set;

import org.codefilarete.stalactite.engine.model.Republic;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.spring.repository.StalactiteRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

/**
 * @author Guillaume Mary
 */
@Repository
public interface BeanQueriesRepository extends StalactiteRepository<Republic, Identifier<Long>> {
	
	Set<Republic> findEuropeanMemberWithPresidentName(@Param("presidentName") String presidentName);
}
