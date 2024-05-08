package org.codefilarete.stalactite.sql.spring.repository.query;

import java.util.Set;

import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.sql.spring.repository.StalactiteRepository;
import org.springframework.stereotype.Repository;

/**
 * @author Guillaume Mary
 */
@Repository
public interface DerivedQueriesRepository extends StalactiteRepository<Country, Identifier<Long>> {
	
	Country findByName(String name);
	
	Country findByIdAndName(Identifier<Long> id, String name);
	
	Set<Country> findByDescriptionLike(String name);
}
