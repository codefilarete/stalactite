package org.codefilarete.stalactite.sql.spring.repository.query;

import java.util.Collection;
import java.util.Date;
import java.util.Set;

import org.codefilarete.stalactite.engine.model.Color;
import org.codefilarete.stalactite.engine.model.Republic;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.sql.spring.repository.StalactiteRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Repository;

/**
 * @author Guillaume Mary
 */
@Repository
public interface DerivedQueriesRepository extends StalactiteRepository<Republic, Identifier<Long>> {
	
	Set<Republic> findByIdIn(Iterable<Identifier<Long>> ids);
	
	Set<Republic> findByIdNotIn(Iterable<Identifier<Long>> ids);
	
	Republic findByName(String name);
	
	Republic findByNameNot(String name);
	
	Republic findByEuMemberIsTrue();
	
	Republic findByEuMemberIsFalse();
	
	Republic findByIdAndName(Identifier<Long> id, String name);
	
	Set<Republic> findByDescriptionLike(String name);
	
	Set<Republic> findByDescriptionNotLike(String name);
	
	Set<Republic> findByDescriptionStartsWith(String name);
	
	Set<Republic> findByDescriptionEndsWith(String name);
	
	Set<Republic> findByDescriptionContains(String name);
	
	Set<Republic> findByDescriptionNotContains(String name);
	
	Set<Republic> findByNameIsNull();
	
	Set<Republic> findByNameIsNotNull();
	
	Set<Republic> findByIdLessThan(Identifier<Long> id);
	
	Set<Republic> findByIdLessThanEqual(Identifier<Long> id);
	
	Set<Republic> findByIdGreaterThan(Identifier<Long> id);
	
	Set<Republic> findByIdGreaterThanEqual(Identifier<Long> id);
	
	Set<Republic> findByIdBefore(Identifier<Long> id);
	
	Set<Republic> findByIdAfter(Identifier<Long> id);
	
	Set<Republic> findByIdBetween(Identifier<Long> id1, Identifier<Long> id2);
	
	Republic findByNameIgnoreCase(String name);
	
	Republic findByNameIgnoringCase(String name);
	
	Set<Republic> findByNameLikeIgnoreCase(String name);
	
	Republic findByPresidentId(Identifier<Long> longPersistedIdentifier);
	
	Republic findByPresidentName(String name);
	
	Republic findByPresidentVehicleColor(Color color);
	
	Republic findByStatesIdIn(Iterable<Identifier<Long>> ids);
	
	Republic findByPresidentNicknamesIn(Iterable<String> names);
	
	Republic findByTimestampCreationDateLessThan(Date date);
	
	Set<Republic> findByLanguagesCodeIs(String code);
	
	Set<Republic> findByLanguagesCodeIsOrderByNameDesc(String code);
	
	Set<Republic> findByLanguagesCodeIsOrderByNameAsc(String code);
	
	Set<Republic> findByLanguagesCodeIs(String code, Sort sort);
	
	Set<Republic> findByNameLikeOrderByPresidentNameAsc(String code);
	
	Set<Republic> findByLanguagesCodeIsOrderByPresidentNameAsc(String code);
	
	Republic findFirstByLanguagesCodeIs(String code);
	
	long deleteByLanguagesCodeIs(String code);
	
	long countByLanguagesCodeIs(String code);
	
	long countDistinctByLanguagesCodeIs(String code);
	
	NamesOnly getByName(String name);
	
	<T> Collection<T> getByName(String name, Class<T> type);
	
	boolean existsByName(String name);
	
	interface NamesOnly {
		
		String getName();
		
		@Value("#{target.president.name}")
		String getPresidentName();
		
		SimplePerson getPresident();
		
		interface SimplePerson {
			
			String getName();
			
		}
	}
}
