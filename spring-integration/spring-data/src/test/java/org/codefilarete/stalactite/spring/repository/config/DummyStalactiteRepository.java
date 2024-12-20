package org.codefilarete.stalactite.spring.repository.config;

import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.spring.repository.StalactiteRepository;
import org.springframework.stereotype.Repository;

/**
 * @author Guillaume Mary
 */
@Repository
public interface DummyStalactiteRepository extends StalactiteRepository<Person, Identifier<Long>> {
}
