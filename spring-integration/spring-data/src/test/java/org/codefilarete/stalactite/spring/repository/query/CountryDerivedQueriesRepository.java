package org.codefilarete.stalactite.spring.repository.query;

import java.util.Set;

import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.spring.repository.StalactiteRepository;
import org.springframework.stereotype.Repository;

/**
 * @author Guillaume Mary
 */
@Repository
public interface CountryDerivedQueriesRepository extends StalactiteRepository<Country, Identifier<Long>> {
	
	Set<Country> findByNameIn(String... name);
}