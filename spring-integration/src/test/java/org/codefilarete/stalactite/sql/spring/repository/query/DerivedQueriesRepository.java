package org.codefilarete.stalactite.sql.spring.repository.query;

import java.util.Date;
import java.util.Set;

import org.codefilarete.stalactite.engine.model.Color;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.sql.spring.repository.StalactiteRepository;
import org.springframework.stereotype.Repository;

/**
 * @author Guillaume Mary
 */
@Repository
public interface DerivedQueriesRepository extends StalactiteRepository<Country, Identifier<Long>> {
	
	Set<Country> findByIdIn(Iterable<Identifier<Long>> ids);
	
	Set<Country> findByIdNotIn(Iterable<Identifier<Long>> ids);
	
	Country findByName(String name);
	
	Country findByNameNot(String name);
	
	Country findByIdAndName(Identifier<Long> id, String name);
	
	Set<Country> findByDescriptionLike(String name);
	
	Set<Country> findByDescriptionNotLike(String name);
	
	Set<Country> findByDescriptionStartsWith(String name);
	
	Set<Country> findByDescriptionEndsWith(String name);
	
	Set<Country> findByNameIsNull();
	
	Set<Country> findByNameIsNotNull();
	
	Set<Country> findByIdLessThan(Identifier<Long> id);
	
	Set<Country> findByIdLessThanEqual(Identifier<Long> id);
	
	Set<Country> findByIdGreaterThan(Identifier<Long> id);
	
	Set<Country> findByIdGreaterThanEqual(Identifier<Long> id);
	
	Set<Country> findByIdBetween(Identifier<Long> id1, Identifier<Long> id2);
	
	Country findByPresidentId(Identifier<Long> longPersistedIdentifier);
	
	Country findByPresidentName(String name);
	
	Country findByPresidentVehicleColor(Color color);
	
	Country findByStatesIdIn(Iterable<Identifier<Long>> ids);
	
	Country findByPresidentNicknamesIn(Iterable<String> names);
	
	Country findByTimestampCreationDateLessThan(Date date);
	
	Set<Country> findByLanguagesCodeIs(String code);
}
