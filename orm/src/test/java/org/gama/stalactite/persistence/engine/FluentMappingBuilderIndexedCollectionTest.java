package org.gama.stalactite.persistence.engine;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.sql.ConnectionProvider;
import org.gama.sql.binder.DefaultParameterBinders;
import org.gama.sql.test.HSQLDBInMemoryDataSource;
import org.gama.stalactite.persistence.engine.FluentMappingBuilder.IdentifierPolicy;
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
import org.junit.jupiter.api.Test;

import static org.gama.lang.function.Functions.chain;
import static org.gama.stalactite.persistence.engine.CascadeOption.CascadeType.DELETE;
import static org.gama.stalactite.persistence.engine.CascadeOption.CascadeType.INSERT;
import static org.gama.stalactite.persistence.engine.CascadeOption.CascadeType.SELECT;
import static org.gama.stalactite.persistence.engine.CascadeOption.CascadeType.UPDATE;
import static org.gama.stalactite.persistence.engine.FluentMappingBuilder.from;
import static org.gama.stalactite.persistence.id.Identifier.LONG_TYPE;
import static org.gama.stalactite.query.model.QueryEase.select;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
	public void testConfiguration_indexingColumnIsOnlySupportedOnList_elseThrowsException() {
		persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
		
		Table choiceTable = new Table("Choice");
		// we declare the column that will store our List index
		Column<Table, Identifier> id = choiceTable.addColumn("id", Identifier.class).primaryKey();
		Column<Table, Integer> idx = choiceTable.addColumn("idx", int.class);
		
		Persister<Choice, Identifier<Long>, Table> choicePersister = from(Choice.class, LONG_TYPE, choiceTable)
				.add(Choice::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Choice::getName)
				.add(Choice::getQuestion)
				.build(persistenceContext);
		
		// In folloqing tests we can't share the Question builder because cascade() is additional
		// so INSERT will always be present, then an exception is always thrown even in SELECT and DELETE 
		
		// getNonIndexedChoices() doesn't return a List, that is our test case
		
		// Test with INSERT cascade
		UnsupportedOperationException thrownException = assertThrows(UnsupportedOperationException.class, () ->
				from(Question.class, LONG_TYPE)
						.add(Question::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
						.addOneToMany(Question::getNonIndexedChoices, choicePersister).mappedBy(Choice::getQuestion).indexedBy(idx)
						.cascade(INSERT)
						.build(persistenceContext));
		assertEquals("Indexing column is only available on List, found j.u.Set", thrownException.getMessage());
		
		// Test with UPDATE cascade
		thrownException = assertThrows(UnsupportedOperationException.class, () ->
				from(Question.class, LONG_TYPE)
						.add(Question::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
						.addOneToMany(Question::getNonIndexedChoices, choicePersister).mappedBy(Choice::getQuestion).indexedBy(idx)
						.cascade(UPDATE)
						.build(persistenceContext));
		assertEquals("Indexing column is only available on List, found j.u.Set", thrownException.getMessage());
		
		// Test with SELECT cascade
		thrownException = assertThrows(UnsupportedOperationException.class, () ->
				from(Question.class, LONG_TYPE)
						.add(Question::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
						.addOneToMany(Question::getNonIndexedChoices, choicePersister).mappedBy(Choice::getQuestion).indexedBy(idx)
						.cascade(SELECT)
						.build(persistenceContext));
		assertEquals("Indexing column is only available on List, found j.u.Set", thrownException.getMessage());
		
		// Test with DELETE cascade : no exception expected because index column has no purpose on delete
		from(Question.class, LONG_TYPE)
				.add(Question::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.addOneToMany(Question::getNonIndexedChoices, choicePersister).mappedBy(Choice::getQuestion).indexedBy(idx)
				.cascade(DELETE)
				.build(persistenceContext);
	}
	
	@Test
	public void testInsert() throws SQLException {
		persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
		
		Table choiceTable = new Table("Choice");
		// we declare the column that will store our List index
		Column<Table, Identifier> id = choiceTable.addColumn("id", Identifier.class).primaryKey();
		Column<Table, Integer> idx = choiceTable.addColumn("idx", int.class);
		
		Persister<Choice, Identifier<Long>, Table> choicePersister = from(Choice.class, LONG_TYPE, choiceTable)
				.add(Choice::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Choice::getName)
				.add(Choice::getQuestion)
				.build(persistenceContext);
		
		// We need to rebuild our cityPersister before each test because some of them alter it on country relationship.
		// So schema contains FK twice with same name, ending in duplicate FK name exception
		Persister<Question, Identifier<Long>, Table> questionPersister = from(Question.class, LONG_TYPE)
				.add(Question::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.addOneToMany(Question::getChoices, choicePersister).mappedBy(Choice::getQuestion).indexedBy(idx).cascade(INSERT)
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
		assertEquals(Arrays.asList(0, 1, 2), Iterables.collectToList(persistedChoices, Result::getIdx));
	}
	
	@Test
	public void testUpdate() throws SQLException {
		persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
		
		Table choiceTable = new Table("Choice");
		// we declare the column that will store our List index
		Column<Table, Identifier> id = choiceTable.addColumn("id", Identifier.class).primaryKey();
		Column<Table, Integer> idx = choiceTable.addColumn("idx", int.class);
		
		Persister<Choice, Identifier<Long>, Table> choicePersister = from(Choice.class,
				LONG_TYPE, choiceTable)
				.add(Choice::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Choice::getName)
				.add(Choice::getQuestion)
				.build(persistenceContext);
		
		// We need to rebuild our cityPersister before each test because some of them alter it on country relationship.
		// So schema contains FK twice with same name, ending in duplicate FK name exception
		Persister<Question, Identifier<Long>, Table> questionPersister = from(Question.class, LONG_TYPE)
				.add(Question::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.addOneToMany(Question::getChoices, choicePersister).mappedBy(Choice::getQuestion).indexedBy(idx).cascade(INSERT, UPDATE)
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
		
		List<Result> persistedChoices = persistenceContext.newQuery(select(id, idx).from(choiceTable).getSelectQuery().orderBy(id), Result.class)
				.mapKey(id, Result::new)
				.map(idx, (SerializableBiConsumer<Result, Integer>) Result::setIdx)
				.execute(connectionProvider);
		assertEquals(Arrays.asList(10L, 20L, 30L), Iterables.collectToList(persistedChoices, Result::getId));
		assertEquals(Arrays.asList(0, 1, 2), Iterables.collectToList(persistedChoices, Result::getIdx));
		
		Question modifiedQuestion = new Question(1L);
		modifiedQuestion.setChoices(Arrays.asList(
				choice2,
				choice1,
				choice3));
		
		questionPersister.update(modifiedQuestion, newQuestion, true);
		persistedChoices = persistenceContext.newQuery(select(id, idx).from(choiceTable).getSelectQuery().orderBy(id), Result.class)
				.mapKey(id, Result::new)
				.map(idx, (SerializableBiConsumer<Result, Integer>) Result::setIdx)
				.execute(connectionProvider);
		assertEquals(Arrays.asList(10L, 20L, 30L), Iterables.collectToList(persistedChoices, Result::getId));
		assertEquals(Arrays.asList(1, 0, 2), Iterables.collectToList(persistedChoices, Result::getIdx));
		
		// checking that nothing is left uncleaned
		// TODO: check for delete, insert, no index modification
//		questionPersister.update(modifiedQuestion, newQuestion, true);
//		persistedChoices = persistenceContext.newQuery(select(id, idx).from(choiceTable).getSelectQuery().orderBy(id), Result.class)
//				.mapKey(id, Result::new)
//				.map(idx, (SerializableBiConsumer<Result, Integer>) Result::setIdx)
//				.execute(connectionProvider);
//		assertEquals(Arrays.asList(10L, 20L, 30L), Iterables.collectToList(persistedChoices, Result::getId));
//		assertEquals(Arrays.asList(1, 0, 2), Iterables.collectToList(persistedChoices, Result::getIdx));
	}
	
	@Test
	public void testSelect() throws SQLException {
		persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
		
		Table choiceTable = new Table("Choice");
		// we declare the column that will store our List index
		Column<Table, Identifier> id = choiceTable.addColumn("id", Identifier.class).primaryKey();
		Column<Table, Integer> idx = choiceTable.addColumn("idx", int.class);
		
		Persister<Choice, Identifier<Long>, Table> choicePersister = from(Choice.class,
				LONG_TYPE, choiceTable)
				.add(Choice::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Choice::getName)
				.add(Choice::getQuestion)
				.build(persistenceContext);
		
		// We need to rebuild our cityPersister before each test because some of them alter it on country relationship.
		// So schema contains FK twice with same name, ending in duplicate FK name exception
		Persister<Question, Identifier<Long>, Table> questionPersister = from(Question.class, LONG_TYPE)
				.add(Question::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.addOneToMany(Question::getChoices, choicePersister).mappedBy(Choice::getQuestion).indexedBy(idx).cascade(INSERT, UPDATE, SELECT)
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
		
		// TODO: test with duplicates in the list
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
		
		public Question() {
		}
		
		private Question(Long id) {
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
	}
}
