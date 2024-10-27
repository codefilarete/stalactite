package org.codefilarete.stalactite.spring.repository.config;

import org.codefilarete.stalactite.engine.model.Person;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * @author Guillaume Mary
 */
@Repository
public interface DummyJpaRepository extends JpaRepository<Person, Long> {
}
