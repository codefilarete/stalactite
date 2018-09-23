package org.gama.stalactite.persistence.engine;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.gama.lang.Duo;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.sql.ConnectionProvider;
import org.gama.sql.binder.DefaultParameterBinders;
import org.gama.sql.test.HSQLDBInMemoryDataSource;
import org.gama.stalactite.persistence.engine.FluentMappingBuilder.IdentifierPolicy;
import org.gama.stalactite.persistence.engine.PersisterTest.PayloadPredicate;
import org.gama.stalactite.persistence.engine.listening.IUpdateListener;
import org.gama.stalactite.persistence.engine.listening.IUpdateListener.UpdatePayload;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.PersistableIdentifier;
import org.gama.stalactite.persistence.id.PersistedIdentifier;
import org.gama.stalactite.persistence.id.manager.StatefullIdentifier;
import org.gama.stalactite.persistence.sql.HSQLDBDialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.test.JdbcConnectionProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.gama.lang.function.Functions.chain;
import static org.gama.stalactite.persistence.engine.CascadeOption.CascadeType.INSERT;
import static org.gama.stalactite.persistence.engine.CascadeOption.CascadeType.SELECT;
import static org.gama.stalactite.persistence.engine.CascadeOption.CascadeType.UPDATE;
import static org.gama.stalactite.persistence.engine.FluentMappingBuilder.from;
import static org.gama.stalactite.persistence.id.Identifier.LONG_TYPE;
import static org.gama.stalactite.query.model.QueryEase.select;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * @author Guillaume Mary
 */
public class FluentMappingBuilderIndexedCollectionTest {
	
	private static final HSQLDBDialect DIALECT = new HSQLDBDialect();
	private final DataSource dataSource = new HSQLDBInMemoryDataSource();
	private final ConnectionProvider connectionProvider = new JdbcConnectionProvider(dataSource);
	private PersistenceContext persistenceContext;
	
	@BeforeAll
	public static void initBinders() {
		// binder creation for our identifier
		DIALECT.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		DIALECT.getJavaTypeToSqlTypeMapping().put(Identifier.class, "int");
		DIALECT.getColumnBinderRegistry().register((Class) Identified.class, Identified.identifiedBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		DIALECT.getJavaTypeToSqlTypeMapping().put(Identified.class, "int");
	}
	
	@Test
	public void testInsert() {
		persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
		
		Table choiceTable = new Table("Choice");
		// we declare the column that will store our List index
		Column<Table, Identifier> id = choiceTable.addColumn("id", Identifier.class).primaryKey();
		Column<Table, Integer> idx = choiceTable.addColumn("idx", int.class);
		
		Persister<Choice, Identifier<Long>, ?> choicePersister = from(Choice.class, LONG_TYPE, choiceTable)
				.add(Choice::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Choice::getName)
				.add(Choice::getQuestion)
				.build(persistenceContext);
		
		// We need to rebuild our cityPersister before each test because some of them alter it on country relationship.
		// So schema contains FK twice with same name, ending in duplicate FK name exception
		Persister<Question, Identifier<Long>, ?> questionPersister = from(Question.class, LONG_TYPE)
				.add(Question::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.addOneToManyList(Question::getChoices, choicePersister).mappedBy(Choice::getQuestion).indexedBy(idx).cascade(INSERT)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Question newQuestion = new Question(1L);
		newQuestion.setChoices(Arrays.asList(
				new Choice(10L).setQuestion(newQuestion),
				new Choice(20L).setQuestion(newQuestion),
				new Choice(30L).setQuestion(newQuestion)));
		questionPersister.insert(newQuestion);
		
		List<Result> persistedChoices = persistenceContext.newQuery(select(id, idx).from(choiceTable).getSelectQuery().orderBy(id), Result.class)
				.mapKey(id, Result::new)
				.map(idx, (SerializableBiConsumer<Result, Integer>) Result::setIdx)
				.execute(connectionProvider);
		assertEquals(Arrays.asList(10L, 20L, 30L), Iterables.collectToList(persistedChoices, Result::getId));
		// stating that indexes are in same order than instances
		assertEquals(Arrays.asList(0, 1, 2), Iterables.collectToList(persistedChoices, Result::getIdx));
	}
	
	@Test
	public void testUpdate_entitySwapping() {
		UpdateTestData updateTestData = new UpdateTestData().build();
		Table choiceTable = updateTestData.getChoiceTable();
		Column<Table, Identifier> id = updateTestData.getId();
		Column<Table, Integer> idx = updateTestData.getIdx();
		Persister<Question, Identifier<Long>, ?> questionPersister = updateTestData.getQuestionPersister();
		Question newQuestion = updateTestData.getNewQuestion();
		Choice choice1 = updateTestData.getChoice1();
		Choice choice2 = updateTestData.getChoice2();
		Choice choice3 = updateTestData.getChoice3();
		
		// creating a clone to test instance swapping
		Question modifiedQuestion = new Question(newQuestion.getId().getSurrogate());
		// little swap between 2 elements
		modifiedQuestion.setChoices(Arrays.asList(
				choice2,
				choice1,
				choice3));
		
		questionPersister.update(modifiedQuestion, newQuestion, true);
		List<Result> persistedChoices = persistenceContext.newQuery(select(id, idx).from(choiceTable).getSelectQuery().orderBy(id), Result.class)
				.mapKey(id, Result::new)
				.map(idx, (SerializableBiConsumer<Result, Integer>) Result::setIdx)
				.execute(connectionProvider);
		// id should be left unmodified
		assertEquals(Arrays.asList(10L, 20L, 30L), Iterables.collectToList(persistedChoices, Result::getId));
		// but indexes must reflect swap done on instances
		assertEquals(Arrays.asList(1, 0, 2), Iterables.collectToList(persistedChoices, Result::getIdx));
	}
	
	@Test
	public <T extends Table<T>> void testUpdate_noChange() {
		UpdateTestData updateTestData = new UpdateTestData().build();
		Table choiceTable = updateTestData.getChoiceTable();
		Column<Table, Identifier> id = updateTestData.getId();
		Column<Table, Integer> idx = updateTestData.getIdx();
		Persister<Question, Identifier<Long>, ?> questionPersister = updateTestData.getQuestionPersister();
		Question newQuestion = updateTestData.getNewQuestion();
		Choice choice1 = updateTestData.getChoice1();
		Choice choice2 = updateTestData.getChoice2();
		Choice choice3 = updateTestData.getChoice3();
		
		// creating a clone to test instance swaping
		Question modifiedQuestion = new Question(newQuestion.getId().getSurrogate());
		// little swap between 2 elements
		modifiedQuestion.setChoices(Arrays.asList(
				choice1,
				choice2,
				choice3));
		
		IUpdateListener<Choice> updateListener = Mockito.mock(IUpdateListener.class);
		updateTestData.getChoicePersister().getPersisterListener().getUpdateListener().add(updateListener);
		
		questionPersister.update(modifiedQuestion, newQuestion, true);
		UpdatePayload<Choice, T> expectedPayload1 = new UpdatePayload<>(new Duo<>(choice1, choice1), new HashMap<>());
		UpdatePayload<Choice, T> expectedPayload2 = new UpdatePayload<>(new Duo<>(choice2, choice2), new HashMap<>());
		UpdatePayload<Choice, T> expectedPayload3 = new UpdatePayload<>(new Duo<>(choice3, choice3), new HashMap<>());
		Iterable<UpdatePayload<Choice, T>> expected = Arrays.asList(expectedPayload1, expectedPayload2, expectedPayload3);
		Predicate<Iterable<UpdatePayload<Choice, T>>> expecter = (Iterable<UpdatePayload<Choice, T>> p)
				-> Iterables.equals(expected, Iterables.copy(p), PayloadPredicate.UPDATE_PAYLOAD_TESTER::test);
		// No change on List so no call to listener
		verifyNoMoreInteractions(updateListener);
		List<Result> persistedChoices = persistenceContext.newQuery(select(id, idx).from(choiceTable).getSelectQuery().orderBy(id), Result.class)
				.mapKey(id, Result::new)
				.map(idx, (SerializableBiConsumer<Result, Integer>) Result::setIdx)
				.execute(connectionProvider);
		// id should left unmodified
		assertEquals(Arrays.asList(10L, 20L, 30L), Iterables.collectToList(persistedChoices, Result::getId));
		// but indexes must reflect swap done on instances
		assertEquals(Arrays.asList(0, 1, 2), Iterables.collectToList(persistedChoices, Result::getIdx));
	}
	
	@Test
	public void testUpdate_entityAddition() {
		UpdateTestData updateTestData = new UpdateTestData().build();
		Table choiceTable = updateTestData.getChoiceTable();
		Column<Table, Identifier> id = updateTestData.getId();
		Column<Table, Integer> idx = updateTestData.getIdx();
		Persister<Question, Identifier<Long>, ?> questionPersister = updateTestData.getQuestionPersister();
		Question newQuestion = updateTestData.getNewQuestion();
		Choice choice1 = updateTestData.getChoice1();
		Choice choice2 = updateTestData.getChoice2();
		Choice choice3 = updateTestData.getChoice3();
		Choice choice4 = new Choice(40L);
		
		// creating a clone to test instance swaping
		Question modifiedQuestion = new Question(newQuestion.getId().getSurrogate());
		// addition of an element and little swap
		modifiedQuestion.setChoices(Arrays.asList(
				choice3,
				choice4,
				choice2,
				choice1
		));
		
		questionPersister.update(modifiedQuestion, newQuestion, true);
		List<Result> persistedChoices = persistenceContext.newQuery(select(id, idx).from(choiceTable).getSelectQuery().orderBy(id), Result.class)
				.mapKey(id, Result::new)
				.map(idx, (SerializableBiConsumer<Result, Integer>) Result::setIdx)
				.execute(connectionProvider);
		// id should left unmodified
		assertEquals(Arrays.asList(10L, 20L, 30L, 40L), Iterables.collectToList(persistedChoices, Result::getId));
		// but indexes must reflect modifications
		assertEquals(Arrays.asList(3, 2, 0, 1), Iterables.collectToList(persistedChoices, Result::getIdx));
	}
	
	@Test
	public void testUpdate_entityRemoval() {
		UpdateTestData updateTestData = new UpdateTestData().build();
		Table choiceTable = updateTestData.getChoiceTable();
		Column<Table, Identifier> id = updateTestData.getId();
		Column<Table, Integer> idx = updateTestData.getIdx();
		Persister<Question, Identifier<Long>, ?> questionPersister = updateTestData.getQuestionPersister();
		Question newQuestion = updateTestData.getNewQuestion();
		Choice choice1 = updateTestData.getChoice1();
		Choice choice3 = updateTestData.getChoice3();
		
		// creating a clone to test instance swaping
		Question modifiedQuestion = new Question(newQuestion.getId().getSurrogate());
		Choice choice3Copy = new Choice();
		choice3Copy.setId(new PersistedIdentifier<>(choice3.getId().getSurrogate()));
		Choice choice1Copy = new Choice();
		choice1Copy.setId(new PersistedIdentifier<>(choice1.getId().getSurrogate()));
		// little swap between 2 elements
		modifiedQuestion.setChoices(Arrays.asList(
				choice3Copy,
				choice1Copy
		));
		
		questionPersister.update(modifiedQuestion, newQuestion, true);
		List<Result> persistedChoices = persistenceContext.newQuery(select(id, idx).from(choiceTable).getSelectQuery().orderBy(id), Result.class)
				.mapKey(id, Result::new)
				.map(idx, (SerializableBiConsumer<Result, Integer>) Result::setIdx)
				.execute(connectionProvider);
		// the removed id must be missing (entity asked for deletion)
		assertEquals(Arrays.asList(10L, 30L), Iterables.collectToList(persistedChoices, Result::getId));
		// choice 1 (10) is last, choice 3 (30) is first
		assertEquals(Arrays.asList(1, 0), Iterables.collectToList(persistedChoices, Result::getIdx));
	}
	
	/**
	 * Quite the same as {@link #testUpdate_entityRemoval()} but where swapped references are not clones but initial objects
	 * This is not an expected use case and may be removed, but as it works it is "documented" 
	 */
	@Test
	public void testUpdate_entityRemoval_entitiesAreSameObjectReference() {
		UpdateTestData updateTestData = new UpdateTestData().build();
		Table choiceTable = updateTestData.getChoiceTable();
		Column<Table, Identifier> id = updateTestData.getId();
		Column<Table, Integer> idx = updateTestData.getIdx();
		Persister<Question, Identifier<Long>, ?> questionPersister = updateTestData.getQuestionPersister();
		Question newQuestion = updateTestData.getNewQuestion();
		Choice choice1 = updateTestData.getChoice1();
		Choice choice3 = updateTestData.getChoice3();
		
		// creating a clone to test instance swaping
		Question modifiedQuestion = new Question(newQuestion.getId().getSurrogate());
		// little swap between 2 elements
		modifiedQuestion.setChoices(Arrays.asList(
				choice3,
				choice1
		));
		
		questionPersister.update(modifiedQuestion, newQuestion, true);
		List<Result> persistedChoices = persistenceContext.newQuery(select(id, idx).from(choiceTable).getSelectQuery().orderBy(id), Result.class)
				.mapKey(id, Result::new)
				.map(idx, (SerializableBiConsumer<Result, Integer>) Result::setIdx)
				.execute(connectionProvider);
		// the removed id must be missing (entity asked for deletion)
		assertEquals(Arrays.asList(10L, 30L), Iterables.collectToList(persistedChoices, Result::getId));
		// choice 1 (10) is last, choice 3 (30) is first
		assertEquals(Arrays.asList(1, 0), Iterables.collectToList(persistedChoices, Result::getIdx));
	}
	
	@Test
	public void testSelect() {
		persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
		
		Table choiceTable = new Table("Choice");
		// we declare the column that will store our List index
		Column<Table, Identifier> id = choiceTable.addColumn("id", Identifier.class).primaryKey();
		Column<Table, Integer> idx = choiceTable.addColumn("idx", int.class);
		
		Persister<Choice, Identifier<Long>, ?> choicePersister = from(Choice.class,
				LONG_TYPE, choiceTable)
				.add(Choice::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Choice::getName)
				.add(Choice::getQuestion)
				.build(persistenceContext);
		
		// We need to rebuild our cityPersister before each test because some of them alter it on country relationship.
		// So schema contains FK twice with same name, ending in duplicate FK name exception
		Persister<Question, Identifier<Long>, ?> questionPersister = from(Question.class, LONG_TYPE)
				.add(Question::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.addOneToManyList(Question::getChoices, choicePersister).mappedBy(Choice::getQuestion).indexedBy(idx).cascade(INSERT, UPDATE, SELECT)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Question newQuestion = new Question(1L);
		Choice choice1 = new Choice(10L);
		Choice choice2 = new Choice(20L);
		Choice choice3 = new Choice(30L);
		newQuestion.setChoices(Arrays.asList(
				choice1,
				choice2,
				choice3));
		questionPersister.insert(newQuestion);
		
		Question modifiedQuestion = new Question(1L);
		modifiedQuestion.setChoices(Arrays.asList(
				choice2,
				choice1,
				choice3));
		
		questionPersister.update(modifiedQuestion, newQuestion, true);
		
		Question select = questionPersister.select(new PersistedIdentifier<>(1L));
		assertEquals(Arrays.asList(20L, 10L, 30L), Iterables.collectToList(select.getChoices(), chain(Choice::getId, StatefullIdentifier::getSurrogate)));
	}
	
	@Nested
	@Disabled	// because code is not ready for this so this test fails
	public class WithDuplicates {
		@Test
		public void testInsert() {
			persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
			
			Table choiceTable = new Table("Choice");
			// we declare the column that will store our List index
			Column<Table, Identifier> id = choiceTable.addColumn("id", Identifier.class).primaryKey();
			Column<Table, Integer> idx = choiceTable.addColumn("idx", int.class);
			
			Persister<Choice, Identifier<Long>, ?> choicePersister = from(Choice.class, LONG_TYPE, choiceTable)
					.add(Choice::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
					.add(Choice::getName)
					.add(Choice::getQuestion)
					.build(persistenceContext);
			
			// We need to rebuild our cityPersister before each test because some of them alter it on country relationship.
			// So schema contains FK twice with same name, ending in duplicate FK name exception
			Persister<Question, Identifier<Long>, ?> questionPersister = from(Question.class, LONG_TYPE)
					.add(Question::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
					.addOneToManyList(Question::getChoices, choicePersister).mappedBy(Choice::getQuestion).indexedBy(idx).cascade(INSERT, UPDATE)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Question newQuestion = new Question(1L);
			questionPersister.insert(newQuestion);
			Choice choice = new Choice(20L).setQuestion(newQuestion);
			choicePersister.insert(choice);
			
			Question modifiedQuestion = new Question(new PersistedIdentifier<>(1L));
			modifiedQuestion.setChoices(Arrays.asList(
					new Choice(10L).setQuestion(modifiedQuestion),
					choice,
					choice,
					new Choice(30L).setQuestion(modifiedQuestion)));
			questionPersister.update(modifiedQuestion, newQuestion, true);
			
			List<Result> persistedChoices = persistenceContext.newQuery(select(id, idx).from(choiceTable).getSelectQuery().orderBy(id), Result.class)
					.mapKey(id, Result::new)
					.map(idx, (SerializableBiConsumer<Result, Integer>) Result::setIdx)
					.execute(connectionProvider);
			assertEquals(Arrays.asList(10L, 20L, 20L, 30L), Iterables.collectToList(persistedChoices, Result::getId));
			// stating that indexes are in same order than instances
			assertEquals(Arrays.asList(0, 1, 2, 3), Iterables.collectToList(persistedChoices, Result::getIdx));
		}
	}
	
	private class Result {
		final long id;
		int idx;
		
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
		
		private Set<Choice> nonIndexedChoices;
		
		private Question() {
		}
		
		private Question(Long id) {
			this.id = new PersistableIdentifier<>(id);
		}
		
		private Question(Identifier<Long> id) {
			this.id = id;
		}
		
		@Override
		public Identifier<Long> getId() {
			return id;
		}
		
		@Override
		public void setId(Identifier<Long> id) {
			this.id = id;
		}
		
		public List<Choice> getChoices() {
			return choices;
		}
		
		public void setChoices(List<Choice> choices) {
			this.choices = choices;
			choices.forEach(choice -> choice.setQuestion(this));
		}
		
		public Set<Choice> getNonIndexedChoices() {
			return nonIndexedChoices;
		}
		
		public void setNonIndexedChoices(Set<Choice> nonIndexedChoices) {
			this.nonIndexedChoices = nonIndexedChoices;
		}
	}
	
	private static class Choice implements Identified<Long> {
		
		private Identifier<Long> id;
		
		private Question question;
		
		private String name;
		
		public Choice() {
		}
		
		private Choice(long id) {
			this.id = new PersistableIdentifier<>(id);
		}
		
		@Override
		public Identifier<Long> getId() {
			return id;
		}
		
		@Override
		public void setId(Identifier<Long> id) {
			this.id = id;
		}
		
		public Question getQuestion() {
			return question;
		}
		
		public Choice setQuestion(Question question) {
			this.question = question;
			return this;
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
	}
	
	private class UpdateTestData {
		private Table choiceTable;
		private Column<Table, Identifier> id;
		private Column<Table, Integer> idx;
		private Persister<Question, Identifier<Long>, ?> questionPersister;
		private Persister<Choice, Identifier<Long>, ?> choicePersister;
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
		
		public Persister<Question, Identifier<Long>, ?> getQuestionPersister() {
			return questionPersister;
		}
		
		public Persister<Choice, Identifier<Long>, ?> getChoicePersister() {
			return choicePersister;
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
			
			choicePersister = from(Choice.class,
					LONG_TYPE, choiceTable)
					.add(Choice::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
					.add(Choice::getName)
					.add(Choice::getQuestion)
					.build(persistenceContext);
			
			// We need to rebuild our cityPersister before each test because some of them alter it on country relationship.
			// So schema contains FK twice with same name, ending in duplicate FK name exception
			questionPersister = from(Question.class, LONG_TYPE)
					.add(Question::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
					.addOneToManyList(Question::getChoices, choicePersister).mappedBy(Choice::getQuestion).indexedBy(idx)
					.cascade(INSERT, UPDATE)
					.deleteRemoved()
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			newQuestion = new Question(1L);
			choice1 = new Choice(10L);
			choice2 = new Choice(20L);
			choice3 = new Choice(30L);
			newQuestion.setChoices(Arrays.asList(
					choice1,
					choice2,
					choice3));
			// creating initial state
			questionPersister.insert(newQuestion);
			
			List<Result> persistedChoices = persistenceContext.newQuery(select(id, idx).from(choiceTable).getSelectQuery().orderBy(id), Result.class)
					.mapKey(id, Result::new)
					.map(idx, (SerializableBiConsumer<Result, Integer>) Result::setIdx)
					.execute(connectionProvider);
			assertEquals(Arrays.asList(10L, 20L, 30L), Iterables.collectToList(persistedChoices, Result::getId));
			// stating that indexes are in same order than instances
			assertEquals(Arrays.asList(0, 1, 2), Iterables.collectToList(persistedChoices, Result::getIdx));
			return this;
		}
	}
}
