package org.codefilarete.stalactite.spring.repository.query;

import java.util.Collection;
import java.util.Date;
import java.util.Set;
import java.util.stream.Stream;

import org.codefilarete.stalactite.engine.model.Color;
import org.codefilarete.stalactite.engine.model.Republic;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.spring.repository.StalactiteRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Repository;

/**
 * @author Guillaume Mary
 */
@Repository
public interface DerivedQueriesRepository extends StalactiteRepository<Republic, Identifier<Long>> {
	
	Set<Republic> findByIdIn(Iterable<Identifier<Long>> ids);
	
	Set<Republic> findByIdIn(Identifier<Long>... ids);
	
	Set<Republic> findByNameIn(String... name);
	
	Set<Republic> findByNameIgnoreCaseIn(String... name);
	
	Set<Republic> findByNameIgnoreCaseAndDescriptionLike(String name, String description);
	
	Set<Republic> findByNameAndDescriptionLikeAllIgnoreCase(String name, String description);
	
	Set<Republic> findByIdNotIn(Iterable<Identifier<Long>> ids);
	
	Set<Republic> findByNameIgnoreCaseNotIn(String... name);
	
	Set<Republic> findByNameLike(String name, Sort sort);
	
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
	
	Set<Republic> findByNameNotLikeIgnoreCase(String name);
	
	Republic findByPresidentId(Identifier<Long> longPersistedIdentifier);
	
	Republic findByPresidentName(String name);
	
	Republic findByPresidentVehicleColor(Color color);
	
	Republic findByStatesIdIn(Iterable<Identifier<Long>> ids);
	
	Republic findByPresidentNicknamesIn(Iterable<String> names);
	
	Set<Republic> findByPresidentPhoneNumbersIs(String phoneType);
	
	Republic findByTimestampCreationDateLessThan(Date date);
	
	Set<Republic> findByLanguagesCodeIs(String code);
	
	Set<Republic> findByLanguagesCodeIsOrderByNameDesc(String code);
	
	Set<Republic> findByLanguagesCodeIsOrderByNameAsc(String code);
	
	Set<Republic> findByLanguagesCodeIs(String code, Sort sort);
	
	Set<Republic> findByLanguagesCodeLike(String code, Sort sort);
	
	Set<Republic> findByNameLikeOrderByPresidentNameAsc(String code);
	
	Set<Republic> findTop2ByNameLikeOrderByPresidentNameAsc(String code);
	
	Set<Republic> findByLanguagesCodeIsOrderByPresidentNameAsc(String code);
	
	Republic findFirstByLanguagesCodeIs(String code);
	
	long deleteByLanguagesCodeIs(String code);
	
	long countByLanguagesCodeIs(String code);
	
	long countDistinctByLanguagesCodeIs(String code);
	
	boolean existsByName(String name);

	Set<Republic> findByNameOrDescription(String name, String description);

	Set<Republic> findByNameOrDescriptionAndEuMemberOrPresidentName(String name, String description, boolean euMember, String presidentName);
	
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

	Set<NamesOnly> getByNameLikeOrderByName(String name);
	
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
