package org.gama.stalactite.persistence.engine;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.function.Predicates;
import org.gama.lang.test.Assertions;
import org.gama.stalactite.sql.ConnectionProvider;
import org.gama.stalactite.sql.binder.DefaultParameterBinders;
import org.gama.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.gama.stalactite.persistence.engine.ColumnOptions.IdentifierPolicy;
import org.gama.stalactite.persistence.engine.IFluentEntityMappingBuilder.IFluentMappingBuilderPropertyOptions;
import org.gama.stalactite.persistence.engine.PersistenceContext.ExecutableSelect;
import org.gama.stalactite.persistence.engine.listening.UpdateListener;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.PersistableIdentifier;
import org.gama.stalactite.persistence.id.PersistedIdentifier;
import org.gama.stalactite.persistence.id.manager.StatefullIdentifier;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.HSQLDBDialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.test.JdbcConnectionProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.gama.lang.function.Functions.chain;
import static org.gama.stalactite.persistence.engine.CascadeOptions.RelationMode.ALL;
import static org.gama.stalactite.persistence.engine.CascadeOptions.RelationMode.ALL_ORPHAN_REMOVAL;
import static org.gama.stalactite.persistence.engine.MappingEase.entityBuilder;
import static org.gama.stalactite.persistence.id.Identifier.LONG_TYPE;
import static org.gama.stalactite.query.model.QueryEase.select;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * @author Guillaume Mary
 */
class FluentEntityMappingConfigurationSupportIndexedCollectionTest {
	
	private static final Dialect DIALECT = new HSQLDBDialect();
	private final DataSource dataSource = new HSQLDBInMemoryDataSource();
	private final ConnectionProvider connectionProvider = new JdbcConnectionProvider(dataSource);
	private PersistenceContext persistenceContext;
	
	@BeforeAll
	static void initBinders() {
		// binder creation for our identifier
		DIALECT.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		DIALECT.getJavaTypeToSqlTypeMapping().put(Identifier.class, "int");
		DIALECT.getColumnBinderRegistry().register((Class) Identified.class, Identified.identifiedBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		DIALECT.getJavaTypeToSqlTypeMapping().put(Identified.class, "int");
	}
	
	@Test
	void oneToManyList_insert_cascadeIsTriggered() {
		persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
		
		Table choiceTable = new Table("Choice");
		// we declare the column that will store our List index
		Column<Table, Identifier> id = choiceTable.addColumn("id", Identifier.class).primaryKey();
		Column<Table, Integer> idx = choiceTable.addColumn("idx", int.class);
		
		IFluentMappingBuilderPropertyOptions<Choice, Identifier<Long>> choiceMappingConfiguration = entityBuilder(Choice.class, LONG_TYPE)
				.add(Choice::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Choice::getName)
				.add(Choice::getQuestion);
		
		IEntityPersister<Question, Identifier<Long>> questionPersister = entityBuilder(Question.class, LONG_TYPE)
				.add(Question::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.addOneToManyList(Question::getChoices, choiceMappingConfiguration).mappedBy(Choice::getQuestion).indexedBy(idx).cascading(ALL)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Question newQuestion = new Question(1L);
		newQuestion.setChoices(Arrays.asList(
				new Choice(10L),
				new Choice(20L),
				new Choice(30L)));
		questionPersister.insert(newQuestion);
		
		List<Result> persistedChoices = persistenceContext.newQuery(select(id, idx).from(choiceTable).getSelectQuery().orderBy(id), Result.class)
				.mapKey(Result::new, id)
				.map(idx, (SerializableBiConsumer<Result, Integer>) Result::setIdx)
				.execute();
		assertEquals(Arrays.asList(10L, 20L, 30L), Iterables.collectToList(persistedChoices, Result::getId));
		// stating that indexes are in same order than instances
		assertEquals(Arrays.asList(0, 1, 2), Iterables.collectToList(persistedChoices, Result::getIdx));
	}
	
	@Nested
	class Update {
		
		@Test
		void update_entitySwapping() {
			UpdateTestData updateTestData = new UpdateTestData().build();
			Table choiceTable = updateTestData.getChoiceTable();
			Column<Table, Identifier> id = updateTestData.getId();
			Column<Table, Integer> idx = updateTestData.getIdx();
			IEntityPersister<Question, Identifier<Long>> questionPersister = updateTestData.getQuestionPersister();
			Question newQuestion = updateTestData.getNewQuestion();
			Choice choice1 = updateTestData.getChoice1();
			Choice choice2 = updateTestData.getChoice2();
			Choice choice3 = updateTestData.getChoice3();
			
			// creating a clone to test instance swapping
			Question modifiedQuestion = new Question(newQuestion.getId().getSurrogate());
			// little swap between 2 elements
			modifiedQuestion.setChoices(Arrays.asList(choice2, choice1, choice3));
			
			questionPersister.update(modifiedQuestion, newQuestion, true);
			List<Result> persistedChoices = persistenceContext.newQuery(select(id, idx).from(choiceTable).getSelectQuery().orderBy(id), Result.class)
					.mapKey(Result::new, id)
					.map(idx, (SerializableBiConsumer<Result, Integer>) Result::setIdx)
					.execute();
			// id should be left unmodified
			assertEquals(Arrays.asList(10L, 20L, 30L), Iterables.collectToList(persistedChoices, Result::getId));
			// but indexes must reflect swap done on instances
			assertEquals(Arrays.asList(1, 0, 2), Iterables.collectToList(persistedChoices, Result::getIdx));
		}
		
		@Test
		void update_noChange() {
			UpdateTestData updateTestData = new UpdateTestData().build();
			Table choiceTable = updateTestData.getChoiceTable();
			Column<Table, Identifier> id = updateTestData.getId();
			Column<Table, Integer> idx = updateTestData.getIdx();
			IEntityPersister<Question, Identifier<Long>> questionPersister = updateTestData.getQuestionPersister();
			Question newQuestion = updateTestData.getNewQuestion();
			
			// creating a clone to test for no change
			Question modifiedQuestion = new Question(newQuestion.getId().getSurrogate());
			// no modifications
			modifiedQuestion.setChoices(newQuestion.getChoices());
			
			UpdateListener<Choice> updateListener = Mockito.mock(UpdateListener.class);
			persistenceContext.getPersister(Choice.class).addUpdateListener(updateListener);
			
			questionPersister.update(modifiedQuestion, newQuestion, true);
			// No change on List so no call to listener
			verifyNoMoreInteractions(updateListener);
			List<Result> persistedChoices = persistenceContext.newQuery(select(id, idx).from(choiceTable).getSelectQuery().orderBy(id), Result.class)
					.mapKey(Result::new, id)
					.map(idx, (SerializableBiConsumer<Result, Integer>) Result::setIdx)
					.execute();
			// nothing should have changed
			assertEquals(Arrays.asList(10L, 20L, 30L), Iterables.collectToList(persistedChoices, Result::getId));
			assertEquals(Arrays.asList(0, 1, 2), Iterables.collectToList(persistedChoices, Result::getIdx));
		}
		
		@Test
		void update_entityAddition() {
			UpdateTestData updateTestData = new UpdateTestData().build();
			Table choiceTable = updateTestData.getChoiceTable();
			Column<Table, Identifier> id = updateTestData.getId();
			Column<Table, Integer> idx = updateTestData.getIdx();
			IEntityPersister<Question, Identifier<Long>> questionPersister = updateTestData.getQuestionPersister();
			Question newQuestion = updateTestData.getNewQuestion();
			Choice choice1 = updateTestData.getChoice1();
			Choice choice2 = updateTestData.getChoice2();
			Choice choice3 = updateTestData.getChoice3();
			Choice choice4 = new Choice(40L);
			
			// creating a clone to test instance swaping
			Question modifiedQuestion = new Question(newQuestion.getId().getSurrogate());
			// addition of an element and little swap
			modifiedQuestion.setChoices(Arrays.asList(choice3, choice4, choice2, choice1));
			
			questionPersister.update(modifiedQuestion, newQuestion, true);
			List<Result> persistedChoices = persistenceContext.newQuery(select(id, idx).from(choiceTable).getSelectQuery().orderBy(id), Result.class)
					.mapKey(Result::new, id)
					.map(idx, (SerializableBiConsumer<Result, Integer>) Result::setIdx)
					.execute();
			// id should left unmodified
			assertEquals(Arrays.asList(10L, 20L, 30L, 40L), Iterables.collectToList(persistedChoices, Result::getId));
			// but indexes must reflect modifications
			assertEquals(Arrays.asList(3, 2, 0, 1), Iterables.collectToList(persistedChoices, Result::getIdx));
		}
		
		@Test
		void update_entityRemoval() {
			UpdateTestData updateTestData = new UpdateTestData().build();
			Table choiceTable = updateTestData.getChoiceTable();
			Column<Table, Identifier> id = updateTestData.getId();
			Column<Table, Integer> idx = updateTestData.getIdx();
			IEntityPersister<Question, Identifier<Long>> questionPersister = updateTestData.getQuestionPersister();
			Question newQuestion = updateTestData.getNewQuestion();
			Choice choice1 = updateTestData.getChoice1();
			Choice choice3 = updateTestData.getChoice3();
			
			// creating a clone to test instance swaping
			Question modifiedQuestion = new Question(newQuestion.getId().getSurrogate());
			Choice choice3Copy = new Choice(new PersistedIdentifier<>(choice3.getId().getSurrogate()));
			Choice choice1Copy = new Choice(new PersistedIdentifier<>(choice1.getId().getSurrogate()));
			// little swap between 2 elements
			modifiedQuestion.setChoices(Arrays.asList(choice3Copy, choice1Copy));
			
			questionPersister.update(modifiedQuestion, newQuestion, true);
			List<Result> persistedChoices = persistenceContext.newQuery(select(id, idx).from(choiceTable).getSelectQuery().orderBy(id), Result.class)
					.mapKey(Result::new, id)
					.map(idx, (SerializableBiConsumer<Result, Integer>) Result::setIdx)
					.execute();
			// the removed id must be missing (entity asked for deletion)
			assertEquals(Arrays.asList(10L, 30L), Iterables.collectToList(persistedChoices, Result::getId));
			// choice 1 (10) is last, choice 3 (30) is first
			assertEquals(Arrays.asList(1, 0), Iterables.collectToList(persistedChoices, Result::getIdx));
		}
		
		/**
		 * Quite the same as {@link #update_entityRemoval()} but where swapped references are not clones but initial objects
		 * This is not an expected use case and may be removed, but as it works it is "documented"
		 */
		@Test
		void update_entityRemoval_entitiesAreSameObjectReference() {
			UpdateTestData updateTestData = new UpdateTestData().build();
			Table choiceTable = updateTestData.getChoiceTable();
			Column<Table, Identifier> id = updateTestData.getId();
			Column<Table, Integer> idx = updateTestData.getIdx();
			IEntityPersister<Question, Identifier<Long>> questionPersister = updateTestData.getQuestionPersister();
			Question newQuestion = updateTestData.getNewQuestion();
			Choice choice1 = updateTestData.getChoice1();
			Choice choice3 = updateTestData.getChoice3();
			
			// creating a clone to test instance swaping
			Question modifiedQuestion = new Question(newQuestion.getId().getSurrogate());
			// little swap between 2 elements
			modifiedQuestion.setChoices(Arrays.asList(choice3, choice1));
			
			questionPersister.update(modifiedQuestion, newQuestion, true);
			List<Result> persistedChoices = persistenceContext.newQuery(select(id, idx).from(choiceTable).getSelectQuery().orderBy(id), Result.class)
					.mapKey(Result::new, id)
					.map(idx, (SerializableBiConsumer<Result, Integer>) Result::setIdx)
					.execute();
			// the removed id must be missing (entity asked for deletion)
			assertEquals(Arrays.asList(10L, 30L), Iterables.collectToList(persistedChoices, Result::getId));
			// choice 1 (10) is last, choice 3 (30) is first
			assertEquals(Arrays.asList(1, 0), Iterables.collectToList(persistedChoices, Result::getIdx));
		}
	}
	
	@Test
	void oneToManyList_select() {
		persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
		
		Table choiceTable = new Table("Choice");
		// we declare the column that will store our List index
		Column<Table, Identifier> id = choiceTable.addColumn("id", Identifier.class).primaryKey();
		Column<Table, Integer> idx = choiceTable.addColumn("idx", int.class);
		
		IFluentMappingBuilderPropertyOptions<Choice, Identifier<Long>> choiceMappingConfiguration = entityBuilder(Choice.class, LONG_TYPE)
				.add(Choice::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Choice::getName)
				.add(Choice::getQuestion);
		
		IEntityPersister<Question, Identifier<Long>> questionPersister = entityBuilder(Question.class, LONG_TYPE)
				.add(Question::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.addOneToManyList(Question::getChoices, choiceMappingConfiguration).mappedBy(Choice::getQuestion).indexedBy(idx).cascading(ALL)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Question newQuestion = new Question(1L);
		Choice choice1 = new Choice(10L);
		Choice choice2 = new Choice(20L);
		Choice choice3 = new Choice(30L);
		newQuestion.setChoices(Arrays.asList(choice1, choice2, choice3));
		questionPersister.insert(newQuestion);
		
		Question select = questionPersister.select(new PersistedIdentifier<>(1L));
		assertEquals(Arrays.asSet(10L, 20L, 30L), Iterables.collect(select.getChoices(), chain(Choice::getId, StatefullIdentifier::getSurrogate), HashSet::new));
	}
	
	@Test
	void oneToManyList_delete_reverseSideIsNotMapped_relationRecordsMustBeDeleted() throws SQLException {
		persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
		
		DuplicatesTestData duplicatesTestData = new DuplicatesTestData().build();
		
		IEntityPersister<Question, Identifier<Long>> questionPersister = duplicatesTestData.getQuestionPersister();
		
		Question newQuestion = new Question(1L);
		Choice choice1 = new Choice(10L);
		Choice choice2 = new Choice(20L);
		Choice choice3 = new Choice(30L);
		newQuestion.setChoices(Arrays.asList(choice1, choice2, choice3));
		questionPersister.insert(newQuestion);
		
		assertTrue(choice1.getId().isPersisted());
		
		IEntityPersister<Answer, Identifier<Long>> answerPersister = duplicatesTestData.getAnswerPersister();
		Answer answer = new Answer(1L);
		answer.takeChoices(Arrays.asList(choice1, choice2, choice3));
		answerPersister.persist(answer);
		
		answerPersister.delete(answer);
		
		ResultSet resultSet;
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select id from Answer");
		assertFalse(resultSet.next());
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select * from Answer_Choices");
		assertFalse(resultSet.next());
		// NB: target entities are not deleted with ASSOCIATION_ONLY cascading
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select id from Choice");
		assertTrue(resultSet.next());
	}
	
	@Test
	void oneToManyList_withOwnerButWithoutIndexedBy_throwsException() {
		persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
		
		IFluentMappingBuilderPropertyOptions<Choice, Identifier<Long>> choiceMappingConfiguration = entityBuilder(Choice.class, LONG_TYPE)
				.add(Choice::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Choice::getName)
				.add(Choice::getQuestion);
		
		assertEquals("Missing indexing column : relation is mapped by " +
						"o.g.s.p.e.FluentEntityMappingConfigurationSupportIndexedCollectionTest$Choice.getQuestion() " +
						"but no indexing property is defined",
				assertThrows(UnsupportedOperationException.class, () ->
						entityBuilder(Question.class, LONG_TYPE)
								.add(Question::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
								// in next statement there's no call to indexedBy(), so configuration will fail because it requires it
								.addOneToManyList(Question::getChoices, choiceMappingConfiguration).mappedBy(Choice::getQuestion)
								.build(persistenceContext)).getMessage());
	}
	
	@Test
	void oneToManyList_insert_mappedByNonExistingGetter_throwsException() {
		persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
		
		Table choiceTable = new Table("Choice");
		// we declare the column that will store our List index
		Column<Table, Identifier> id = choiceTable.addColumn("id", Identifier.class).primaryKey();
		Column<Table, Integer> idx = choiceTable.addColumn("idx", int.class);
		
		IFluentMappingBuilderPropertyOptions<Choice, Identifier<Long>> choiceMappingConfiguration = entityBuilder(Choice.class, LONG_TYPE)
				.add(Choice::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Choice::getName)
				.add(Choice::setQuestionWithNoGetter);
		
		IEntityPersister<Question, Identifier<Long>> persisterWithNonExistingSetter = entityBuilder(Question.class, LONG_TYPE)
				.add(Question::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.addOneToManyList(Question::getChoices, choiceMappingConfiguration).indexedBy(idx).mappedBy(Choice::setQuestionWithNoGetter).cascading(ALL)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Question question = new Question(1L);
		Choice choice = new Choice(4L);
		question.addChoice(choice);
		
		Assertions.assertThrows(() -> persisterWithNonExistingSetter.insert(question), Assertions.hasExceptionInCauses(RuntimeMappingException.class)
				.andProjection(Assertions.hasMessage("Can't get index : " + choice.toString() + " is not associated with a o.g.s.p.e.FluentEntityMappingConfigurationSupportIndexedCollectionTest$Question : "
				+ "accessor for field" +
				" o.g.s.p.e.FluentEntityMappingConfigurationSupportIndexedCollectionTest$Choice.questionWithNoGetter returned null")));
	}
		
	@Test
	void oneToManyList_insert_targetEntitiesAreNotLinked_throwsException() {
		persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
		
		Table choiceTable = new Table("Choice");
		// we declare the column that will store our List index
		Column<Table, Identifier> id = choiceTable.addColumn("id", Identifier.class).primaryKey();
		Column<Table, Integer> idx = choiceTable.addColumn("idx", int.class);
		
		IFluentMappingBuilderPropertyOptions<Choice, Identifier<Long>> choiceMappingConfiguration = entityBuilder(Choice.class, LONG_TYPE)
				.add(Choice::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Choice::getName)
				.add(Choice::getQuestion);
		
		IEntityPersister<Question, Identifier<Long>> persister = entityBuilder(Question.class, LONG_TYPE)
				.add(Question::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.addOneToManyList(Question::getChoices, choiceMappingConfiguration).indexedBy(idx).mappedBy(Choice::setQuestion).cascading(ALL)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Question question = new Question(1L);
		question.getChoices().add(new Choice(4L));
		
		Assertions.assertThrows(() -> persister.insert(question), Assertions.hasExceptionInCauses(RuntimeMappingException.class)
				.andProjection(Assertions.hasMessage("Can't get index : Choice{id=4, question=null, name='null'} is not associated with a o.g.s.p.e.FluentEntityMappingConfigurationSupportIndexedCollectionTest$Question : "
						+ "o.g.s.p.e.FluentEntityMappingConfigurationSupportIndexedCollectionTest$Choice.getQuestion() returned null")));
	}
		
	@Test
	void oneToManyList_withoutOwnerButWithIndexedBy_throwsException() {
		persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
		
		Table choiceTable = new Table("Choice");
		// we declare the column that will store our List index
		Column<Table, Identifier> id = choiceTable.addColumn("id", Identifier.class).primaryKey();
		Column<Table, Integer> idx = choiceTable.addColumn("idx", int.class);
		
		IFluentMappingBuilderPropertyOptions<Choice, Identifier<Long>> choiceMappingConfiguration = entityBuilder(Choice.class, LONG_TYPE)
				.add(Choice::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Choice::getName)
				.add(Choice::getQuestion);
		
		assertEquals("Indexing column is defined without owner : relation is only declared by Question::getChoices",
				assertThrows(UnsupportedOperationException.class, () ->
				entityBuilder(Question.class, LONG_TYPE)
						.add(Question::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
						// in next statement there's no call to indexedBy(), so configuration will fail because it requires it
						.addOneToManyList(Question::getChoices, choiceMappingConfiguration).indexedBy(idx)
						.build(persistenceContext)).getMessage());
	}
	
	@Nested
	class WithDuplicates {
		
		@Test
		void insert() {
			persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
			
			DuplicatesTestData duplicatesTestData = new DuplicatesTestData().build();
			
			IEntityPersister<Question, Identifier<Long>> questionPersister = duplicatesTestData.getQuestionPersister();
			IEntityPersister<Answer, Identifier<Long>> answerPersister = duplicatesTestData.getAnswerPersister();
			Table answerChoicesTable = duplicatesTestData.getAnswerChoicesTable();
			Column<Table, Identifier> answerChoicesTableId = duplicatesTestData.getAnswerChoicesTableId();
			Column<Table, Integer> answerChoicesTableIdx = duplicatesTestData.getAnswerChoicesTableIdx();
			Column<Table, Identifier> answerChoicesTableChoiceId = duplicatesTestData.getAnswerChoicesTableChoiceId();
			
			Question newQuestion = new Question(1L);
			Choice choice1 = new Choice(10L);
			Choice choice2 = new Choice(20L);
			Choice choice3 = new Choice(30L);
			newQuestion.setChoices(Arrays.asList(choice1, choice2, choice3));
			questionPersister.insert(newQuestion);
			
			Answer answer = new Answer(1L);
			answer.takeChoices(Arrays.asList(choice1, choice2, choice2, choice3));
			answerPersister.insert(answer);
			
			ExecutableSelect<RawAnswer> query = persistenceContext.newQuery(
					select(answerChoicesTableId, answerChoicesTableIdx, answerChoicesTableChoiceId)
							.from(answerChoicesTable).getSelectQuery().orderBy(answerChoicesTableIdx), RawAnswer.class);
			List<RawAnswer> persistedChoices = query
					.mapKey(RawAnswer::new, answerChoicesTableId, answerChoicesTableIdx, answerChoicesTableChoiceId)
					.execute();
			assertEquals(Arrays.asList(10L, 20L, 20L, 30L), Iterables.collectToList(persistedChoices, RawAnswer::getChoiceId));
			// stating that indexes are in same order than instances
			assertEquals(Arrays.asList(0, 1, 2, 3), Iterables.collectToList(persistedChoices, RawAnswer::getChoiceIdx));
		}
		
		@Test
		void select_default() {
			persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
			
			DuplicatesTestData duplicatesTestData = new DuplicatesTestData().build();
			
			IEntityPersister<Question, Identifier<Long>> questionPersister = duplicatesTestData.getQuestionPersister();
			IEntityPersister<Answer, Identifier<Long>> answerPersister = duplicatesTestData.getAnswerPersister();
			
			Question newQuestion = new Question(1L);
			Choice choice1 = new Choice(10L);
			choice1.setName("toto");
			Choice choice2 = new Choice(20L);
			Choice choice3 = new Choice(30L);
			newQuestion.setChoices(Arrays.asList(choice1, choice2, choice3));
			questionPersister.insert(newQuestion);
			
			Answer answer = new Answer(1L);
			List<Choice> choices = Arrays.asList(choice1, choice2, choice2, choice3);
			// we shuffle choices so there's no risk to fall onto a green case due to unexpected database insertion or select behavior
			Collections.shuffle(choices);
			answer.takeChoices(choices);
			answerPersister.insert(answer);
			
			Answer selectedAnswer = answerPersister.select(new PersistableIdentifier<>(1L));
			
			assertEquals((Long) 1L, selectedAnswer.getId().getSurrogate());
			org.gama.lang.test.Assertions.assertAllEquals(Arrays.asList(choice1, choice2, choice2, choice3), selectedAnswer.getChoices(),
					(c1, c2) -> Predicates.equalOrNull(c1.getId(), c2.getId()) && Predicates.equalOrNull(c1.getName(), c2.getName()));
		}
		
		/** Test to check that loading a target entity from its persister still work (no use of the aggregate persister) */
		@Test
		void select_targets() {
			persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
			
			DuplicatesTestData duplicatesTestData = new DuplicatesTestData().build();
			
			IEntityPersister<Question, Identifier<Long>> questionPersister = duplicatesTestData.getQuestionPersister();
			
			Question newQuestion = new Question(1L);
			Choice choice1 = new Choice(10L);
			Choice choice2 = new Choice(20L);
			Choice choice3 = new Choice(30L);
			List<Choice> randomizedOrderChoices = Arrays.asList(choice2, choice1, choice3);
			// we shuffle choices so there's no risk to fall onto a green case due to unexpected database insertion or select behavior
			Collections.shuffle(randomizedOrderChoices);
			newQuestion.setChoices(randomizedOrderChoices);
			questionPersister.insert(newQuestion);
			
			// does loading the target entity from its persister work ?
			Choice loadedChoice = persistenceContext.getPersister(Choice.class).select(new PersistableIdentifier<>(10L));
			assertEquals(choice1, loadedChoice);
			
			// loading the target entity from its aggregate persister
			Question loadedQuestion = duplicatesTestData.questionPersister.select(new PersistableIdentifier<>(1L));
			assertEquals(newQuestion.getChoices(), loadedQuestion.getChoices());
		}
		
		@Test
		void delete() {
			persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
			
			DuplicatesTestData duplicatesTestData = new DuplicatesTestData().build();
			
			IEntityPersister<Question, Identifier<Long>> questionPersister = duplicatesTestData.getQuestionPersister();
			IEntityPersister<Answer, Identifier<Long>> answerPersister = duplicatesTestData.getAnswerPersister();
			
			Question newQuestion = new Question(1L);
			Choice choice1 = new Choice(10L);
			Choice choice2 = new Choice(20L);
			Choice choice3 = new Choice(30L);
			newQuestion.setChoices(Arrays.asList(choice1, choice2, choice3));
			questionPersister.insert(newQuestion);
			
			Answer answer = new Answer(1L);
			answer.takeChoices(Arrays.asList(choice1, choice2, choice2, choice3));
			answerPersister.insert(answer);
			
			int deletedAnswerCount = answerPersister.delete(answer);
			
			assertEquals(1, deletedAnswerCount);
			
			Table answerChoicesTable = duplicatesTestData.getAnswerChoicesTable();
			List<Long> persistedChoices = persistenceContext.newQuery(select("count(*) as c").from(answerChoicesTable), Long.class)
					.mapKey(SerializableFunction.identity(), "c", long.class)
					.execute();
			assertEquals((Long) 0L, persistedChoices.get(0));
			
			// No choice must be deleted
			// NB : we use choiceReader instead of choicePersister because the latter needs the idx column which is not mapped for Answer -> Choice
			List<Long> remainingChoices = persistenceContext.newQuery("select id from Choice", Long.class)
					.mapKey(SerializableFunction.identity(), "id", long.class)
					.execute();
			assertEquals(Arrays.asSet(choice1.getId().getSurrogate(), choice2.getId().getSurrogate(), choice3.getId().getSurrogate()), new HashSet<>(remainingChoices));
		}
		
		@Test
		void deleteById() {
			persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
			
			DuplicatesTestData duplicatesTestData = new DuplicatesTestData().build();
			
			IEntityPersister<Question, Identifier<Long>> questionPersister = duplicatesTestData.getQuestionPersister();
			IEntityPersister<Answer, Identifier<Long>> answerPersister = duplicatesTestData.getAnswerPersister();
			
			Question newQuestion = new Question(1L);
			Choice choice1 = new Choice(10L);
			Choice choice2 = new Choice(20L);
			Choice choice3 = new Choice(30L);
			newQuestion.setChoices(Arrays.asList(choice1, choice2, choice3));
			questionPersister.insert(newQuestion);
			
			Answer answer = new Answer(1L);
			answer.takeChoices(Arrays.asList(choice1, choice2, choice2, choice3));
			answerPersister.insert(answer);
			
			int deletedAnswerCount = answerPersister.deleteById(answer);
			
			assertEquals(1, deletedAnswerCount);
			
			Table answerChoicesTable = duplicatesTestData.getAnswerChoicesTable();
			List<Long> persistedChoices = persistenceContext.newQuery(select("count(*) as c").from(answerChoicesTable).getSelectQuery(), Long.class)
					.mapKey(SerializableFunction.identity(), "c", long.class)
					.execute();
			assertEquals((Long) 0L, persistedChoices.get(0));
			
			// No choice must be deleted
			// NB : we use choiceReader instead of choicePersister because the latter needs the idx column which is not mapped for Answer -> Choice
			List<Long> remainingChoices = persistenceContext.newQuery("select id from Choice", Long.class)
					.mapKey(SerializableFunction.identity(), "id", long.class)
					.execute();
			assertEquals(Arrays.asSet(choice1.getId().getSurrogate(), choice2.getId().getSurrogate(), choice3.getId().getSurrogate()), new HashSet<>(remainingChoices));
		}

		@Test
		void update() {
			persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
			
			DuplicatesTestData duplicatesTestData = new DuplicatesTestData().build();
			
			IEntityPersister<Question, Identifier<Long>> questionPersister = duplicatesTestData.getQuestionPersister();
			IEntityPersister<Answer, Identifier<Long>> answerPersister = duplicatesTestData.getAnswerPersister();
			Table answerChoicesTable = duplicatesTestData.getAnswerChoicesTable();
			Column<Table, Identifier> answerChoicesTableId = duplicatesTestData.getAnswerChoicesTableId();
			Column<Table, Integer> answerChoicesTableIdx = duplicatesTestData.getAnswerChoicesTableIdx();
			Column<Table, Identifier> answerChoicesTableChoiceId = duplicatesTestData.getAnswerChoicesTableChoiceId();
			
			
			Question newQuestion = new Question(1L);
			Choice choice1 = new Choice(10L);
			Choice choice2 = new Choice(20L);
			Choice choice3 = new Choice(30L);
			Choice choice4 = new Choice(40L);
			newQuestion.setChoices(Arrays.asList(choice1, choice2, choice3, choice4));
			questionPersister.insert(newQuestion);
			
			Answer answer = new Answer(1L);
			List<Choice> choices = Arrays.asList(choice1, choice2, choice2, choice3);
			answer.takeChoices(choices);
			answerPersister.insert(answer);
			
			
			// test with addition of entity
			Choice newChoice = new Choice(50L);
			newChoice.setQuestion(newQuestion);
			persistenceContext.getPersister(Choice.class).persist(newChoice);
			Answer selectedAnswer = answerPersister.select(new PersistableIdentifier<>(1L));
			// NB: difference with previous state is addition on newChoice (in the middle), choice1 & choice4 to the end
			selectedAnswer.takeChoices(Arrays.asList(choice1, choice2, newChoice, choice2, choice3, choice1, choice4));
			answerPersister.update(selectedAnswer, answer, true);
			
			ExecutableSelect<RawAnswer> query = persistenceContext.newQuery(
					select(answerChoicesTableId, answerChoicesTableIdx, answerChoicesTableChoiceId)
							.from(answerChoicesTable).getSelectQuery().orderBy(answerChoicesTableIdx), RawAnswer.class);
			List<RawAnswer> persistedChoices = query
					.mapKey(RawAnswer::new, answerChoicesTableId, answerChoicesTableIdx, answerChoicesTableChoiceId)
					.execute();
			assertEquals(Arrays.asList(10L, 20L, 50L, 20L, 30L, 10L, 40L), Iterables.collectToList(persistedChoices, RawAnswer::getChoiceId));
			// stating that indexes are in same order than instances
			assertEquals(Arrays.asList(0, 1, 2, 3, 4, 5, 6), Iterables.collectToList(persistedChoices, RawAnswer::getChoiceIdx));
			
			// test with entity removal
			Answer selectedAnswer1 = answerPersister.select(new PersistableIdentifier<>(1L));
			selectedAnswer1.takeChoices(Arrays.asList(choice1, choice2, choice1));
			answerPersister.update(selectedAnswer1, selectedAnswer, true);
			
			query = persistenceContext.newQuery(
					select(answerChoicesTableId, answerChoicesTableIdx, answerChoicesTableChoiceId)
							.from(answerChoicesTable).getSelectQuery().orderBy(answerChoicesTableIdx), RawAnswer.class);
			persistedChoices = query
					.mapKey(RawAnswer::new, answerChoicesTableId, answerChoicesTableIdx, answerChoicesTableChoiceId)
					.execute();
			assertEquals(Arrays.asList(10L, 20L, 10L), Iterables.collectToList(persistedChoices, RawAnswer::getChoiceId));
			// stating that indexes are in same order than instances
			assertEquals(Arrays.asList(0, 1, 2), Iterables.collectToList(persistedChoices, RawAnswer::getChoiceIdx));
		}
	}
	
	/**
	 * Simple class to store index of an entity. Very general implementation. 
	 */
	private static class Result {
		private final long id;
		private int idx;
		
		private Result(Identifier<Long> id) {
			this.id = id.getSurrogate();
		}
		
		public long getId() {
			return id;
		}
		
		private int getIdx() {
			return idx;
		}
		
		private void setIdx(Integer idx) {
			this.idx = idx;
		}
	}
	
	private static class Question implements Identified<Long> {
		
		private Identifier<Long> id;
		
		private List<Choice> choices = new ArrayList<>();
		
		private Question() {
		}
		
		private Question(Long id) {
			this(new PersistableIdentifier<>(id));
		}
		
		private Question(Identifier<Long> id) {
			this.id = id;
		}
		
		@Override
		public Identifier<Long> getId() {
			return id;
		}
		
		public List<Choice> getChoices() {
			return choices;
		}
		
		public void setChoices(List<Choice> choices) {
			this.choices = choices;
			choices.forEach(choice -> choice.setQuestion(this));
		}
		
		public void addChoice(Choice choice) {
			choice.setQuestion(this);
			choices.add(choice);
		}
		
		public void removeChoice(Choice choice) {
			choices.remove(choice);
			choice.setQuestion(null);
		}
	}
	
	public static class Answer implements Identified<Long> {
		
		private Identifier<Long> id;
		
		private List<AnswerChoice> choices = new ArrayList<>();
		
		private Answer() {
		}
		
		private Answer(Long id) {
			this(new PersistableIdentifier<>(id));
		}
		
		private Answer(Identifier<Long> id) {
			this.id = id;
		}
		
		@Override
		public Identifier<Long> getId() {
			return id;
		}
		
		public List<AnswerChoice> getChoices() {
			return choices;
		}
		
		public void takeChoices(List<Choice> choices) {
			this.choices = Iterables.collectToList(choices, AnswerChoice::new);
		}
		
		public void setChoices(List<AnswerChoice> choices) {
			this.choices = choices;
		}
		
	}
	
	public static class AnswerChoice implements Identified<Long> {
		
		private Identifier<Long> id;
		
		private String name;
		
		public AnswerChoice() {
		}
		
		private AnswerChoice(Choice choice) {
			this(choice.getId());
			setName(choice.getName());
		}
		
		private AnswerChoice(long id) {
			this.id = new PersistableIdentifier<>(id);
		}
		
		private AnswerChoice(Identifier<Long> id) {
			this.id = id;
		}
		
		@Override
		public Identifier<Long> getId() {
			return id;
		}
		
		public String getName() {
			return name;
		}
		
		public void setName(String name) {
			this.name = name;
		}
		
		@Override
		public String toString() {
			return "AnswerChoice{id=" + id.getSurrogate() + ", name='" + name + '\'' + '}';
		}
		
	}
	
	private static class RawAnswer {
		private Long answerId;
		private Integer choiceIdx;
		private Long choiceId;
		
		public RawAnswer() {
		}
		
		private RawAnswer(long answerId, int choiceIdx, long choiceId) {
			this.answerId = answerId;
			this.choiceIdx = choiceIdx;
			this.choiceId = choiceId;
		}
		
		private RawAnswer(Identifier<Long> answerId, int choiceIdx, Identifier<Long> choiceId) {
			this(answerId.getSurrogate(), choiceIdx, choiceId.getSurrogate());
		}
		
		public Long getAnswerId() {
			return answerId;
		}
		
		public void setAnswerId(long answerId) {
			this.answerId = answerId;
		}
		
		public Integer getChoiceIdx() {
			return choiceIdx;
		}
		
		public void setChoiceIdx(int choiceIdx) {
			this.choiceIdx = choiceIdx;
		}
		
		public Long getChoiceId() {
			return choiceId;
		}
		
		public void setChoiceId(long choiceId) {
			this.choiceId = choiceId;
		}
	}
	
	private static class Choice implements Identified<Long> {
		
		private Identifier<Long> id;
		
		private Question question;
		
		private Question questionWithNoGetter;
		
		private String name;
		
		public Choice() {
		}
		
		private Choice(long id) {
			this.id = new PersistableIdentifier<>(id);
		}
		
		public Choice(Identifier<Long> id) {
			this.id = id;
		}
		
		@Override
		public Identifier<Long> getId() {
			return id;
		}
		
		public Question getQuestion() {
			return question;
		}
		
		public Choice setQuestion(Question question) {
			this.question = question;
			return this;
		}
		
		public void setQuestionWithNoGetter(Question question) {
			questionWithNoGetter = question;
		}
		
		public String getName() {
			return name;
		}
		
		public void setName(String name) {
			this.name = name;
		}
		
		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (!(o instanceof Choice)) return false;
			Choice choice = (Choice) o;
			return Objects.equals(id, choice.id);
		}
		
		@Override
		public int hashCode() {
			return Objects.hash(id);
		}
		
		@Override
		public String toString() {
			return "Choice{id=" + id.getSurrogate() + ", question=" + question + ", name='" + name + '\'' + '}';
		}
	}
	
	private class UpdateTestData {
		private Table choiceTable;
		private Column<Table, Identifier> id;
		private Column<Table, Integer> idx;
		private IEntityPersister<Question, Identifier<Long>> questionPersister;
		private Question newQuestion;
		private Choice choice1;
		private Choice choice2;
		private Choice choice3;
		
		public Table getChoiceTable() {
			return choiceTable;
		}
		
		public Column<Table, Identifier> getId() {
			return id;
		}
		
		public Column<Table, Integer> getIdx() {
			return idx;
		}
		
		public IEntityPersister<Question, Identifier<Long>> getQuestionPersister() {
			return questionPersister;
		}
		
		public Question getNewQuestion() {
			return newQuestion;
		}
		
		public Choice getChoice1() {
			return choice1;
		}
		
		public Choice getChoice2() {
			return choice2;
		}
		
		public Choice getChoice3() {
			return choice3;
		}
		
		public UpdateTestData build() {
			persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
			
			choiceTable = new Table("Choice");
			// we declare the column that will store our List index
			id = choiceTable.addColumn("id", Identifier.class).primaryKey();
			idx = choiceTable.addColumn("idx", int.class);
			
			IFluentMappingBuilderPropertyOptions<Choice, Identifier<Long>> choiceMappingConfiguration = entityBuilder(Choice.class, LONG_TYPE)
					.add(Choice::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
					.add(Choice::getName)
					.add(Choice::getQuestion);
			
			questionPersister = entityBuilder(Question.class, LONG_TYPE)
					.add(Question::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
					.addOneToManyList(Question::getChoices, choiceMappingConfiguration).mappedBy(Choice::getQuestion).indexedBy(idx)
					.cascading(ALL_ORPHAN_REMOVAL)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			newQuestion = new Question(1L);
			choice1 = new Choice(10L);
			choice2 = new Choice(20L);
			choice3 = new Choice(30L);
			newQuestion.setChoices(Arrays.asList(choice1, choice2, choice3));
			// creating initial state
			questionPersister.insert(newQuestion);
			
			List<Result> persistedChoices = persistenceContext.newQuery(select(id, idx).from(choiceTable).getSelectQuery().orderBy(id), Result.class)
					.mapKey(Result::new, id)
					.map(idx, (SerializableBiConsumer<Result, Integer>) Result::setIdx)
					.execute();
			assertEquals(Arrays.asList(10L, 20L, 30L), Iterables.collectToList(persistedChoices, Result::getId));
			// stating that indexes are in same order than instances
			assertEquals(Arrays.asList(0, 1, 2), Iterables.collectToList(persistedChoices, Result::getIdx));
			return this;
		}
	}
	
	private class DuplicatesTestData {
		
		private IEntityPersister<Question, Identifier<Long>> questionPersister;
		private IEntityPersister<Answer, Identifier<Long>> answerPersister;
		private Table answerChoicesTable;
		private Column<Table, Identifier> answerChoicesTableId;
		private Column<Table, Integer> answerChoicesTableIdx;
		private Column<Table, Identifier> answerChoicesTableChoiceId;
		
		public IEntityPersister<Question, Identifier<Long>> getQuestionPersister() {
			return questionPersister;
		}
		
		public IEntityPersister<Answer, Identifier<Long>> getAnswerPersister() {
			return answerPersister;
		}
		
		public Table getAnswerChoicesTable() {
			return answerChoicesTable;
		}
		
		public Column<Table, Identifier> getAnswerChoicesTableId() {
			return answerChoicesTableId;
		}
		
		public Column<Table, Integer> getAnswerChoicesTableIdx() {
			return answerChoicesTableIdx;
		}
		
		public Column<Table, Identifier> getAnswerChoicesTableChoiceId() {
			return answerChoicesTableChoiceId;
		}
		
		public DuplicatesTestData build() {
			Table choiceTable = new Table("Choice");
			// we declare the column that will store our List index
			Column<Table, Identifier> id = choiceTable.addColumn("id", Identifier.class).primaryKey();
			Column<Table, Integer> idx = choiceTable.addColumn("idx", int.class);
			
			IFluentMappingBuilderPropertyOptions<Choice, Identifier<Long>> choiceMappingConfiguration = entityBuilder(Choice.class, LONG_TYPE)
					.add(Choice::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
					.add(Choice::getName)
					.add(Choice::getQuestion);
			
			questionPersister = entityBuilder(Question.class, LONG_TYPE)
					.add(Question::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
					.addOneToManyList(Question::getChoices, choiceMappingConfiguration).mappedBy(Choice::getQuestion).indexedBy(idx)
					.cascading(ALL)
					.build(persistenceContext);
			
			// We create another choices persister dedicated to Answer association because usages are not the same :
			// like Aggregate (in Domain Driven Design) or CQRS, Answers are not in the same context than Questions so it requires a different
			// mapping. For instance there's no need of Question relationship mapping.
			// BE AWARE THAT mapping Choice a second time is a bad practise
			IFluentMappingBuilderPropertyOptions<AnswerChoice, Identifier<Long>> answerChoiceMappingConfiguration = entityBuilder(AnswerChoice.class, LONG_TYPE)
					.add(AnswerChoice::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
					.add(AnswerChoice::getName);
			
			answerPersister = entityBuilder(Answer.class, LONG_TYPE)
					.add(Answer::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
					.addOneToManyList(Answer::getChoices, answerChoiceMappingConfiguration, choiceTable).cascading(ALL)
					.build(persistenceContext);
			
			// We declare the table that will store our relationship, and overall our List index
			// NB: names are hardcoded here because they are hardly accessible from outside of CascadeManyConfigurer
			answerChoicesTable = new Table("Answer_Choices");
			answerChoicesTableId = answerChoicesTable.addColumn("answer_Id", Identifier.class).primaryKey();
			answerChoicesTableIdx = answerChoicesTable.addColumn("idx", Integer.class).primaryKey();
			answerChoicesTableChoiceId = answerChoicesTable.addColumn("choice_Id", Identifier.class).primaryKey();
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.getDdlGenerator().addTables(answerChoicesTable);
			ddlDeployer.deployDDL();
			return this;
		}
	}
}
