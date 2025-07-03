package org.codefilarete.stalactite.engine;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.assertj.core.groups.Tuple;
import org.codefilarete.stalactite.engine.PersistenceContext.ExecutableBeanPropertyKeyQueryMapper;
import org.codefilarete.stalactite.id.Identified;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistableIdentifier;
import org.codefilarete.stalactite.id.PersistedIdentifier;
import org.codefilarete.stalactite.id.StatefulIdentifier;
import org.codefilarete.stalactite.query.model.QueryEase;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.CurrentThreadConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.HSQLDBDialectBuilder;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.stalactite.sql.result.ResultSetIterator;
import org.codefilarete.stalactite.sql.result.Row;
import org.codefilarete.stalactite.sql.result.RowIterator;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.Maps;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.engine.CascadeOptions.RelationMode.ALL;
import static org.codefilarete.stalactite.engine.CascadeOptions.RelationMode.ALL_ORPHAN_REMOVAL;
import static org.codefilarete.stalactite.engine.CascadeOptions.RelationMode.ASSOCIATION_ONLY;
import static org.codefilarete.stalactite.engine.MappingEase.entityBuilder;
import static org.codefilarete.stalactite.id.Identifier.LONG_TYPE;
import static org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED;
import static org.codefilarete.tool.function.Functions.chain;

/**
 * @author Guillaume Mary
 */
class FluentEntityMappingConfigurationSupportOneToManyListTest {
	
	private static final Dialect DIALECT = HSQLDBDialectBuilder.defaultHSQLDBDialect();
	private final DataSource dataSource = new HSQLDBInMemoryDataSource();
	private final ConnectionProvider connectionProvider = new CurrentThreadConnectionProvider(dataSource);
	private PersistenceContext persistenceContext;
	
	@BeforeAll
	static void initBinders() {
		// binder creation for our identifier
		DIALECT.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		DIALECT.getSqlTypeRegistry().put(Identifier.class, "int");
	}
	
	@Test
	void insert_cascadeIsTriggered() {
		persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
		
		Table choiceTable = new Table("Choice");
		// we declare the column that will store our List index
		Column<Table, Identifier> id = choiceTable.addColumn("id", Identifier.class).primaryKey();
		Column<Table, Integer> idx = choiceTable.addColumn("idx", int.class);
		
		FluentEntityMappingBuilder<Choice, Identifier<Long>> choiceMappingConfiguration = entityBuilder(Choice.class, LONG_TYPE)
				.mapKey(Choice::getId, ALREADY_ASSIGNED)
				.map(Choice::getLabel);
		
		EntityPersister<Question, Identifier<Long>> questionPersister = entityBuilder(Question.class, LONG_TYPE)
				.mapKey(Question::getId, ALREADY_ASSIGNED)
				.mapOneToMany(Question::getChoices, choiceMappingConfiguration).mappedBy(Choice::getQuestion).indexedBy(idx).cascading(ALL)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Question newQuestion = new Question(1L);
		newQuestion.setChoices(Arrays.asList(
				new Choice(10L),
				new Choice(20L),
				new Choice(30L)));
		questionPersister.insert(newQuestion);
		
		ExecutableQuery<Result> resultExecutableQuery = persistenceContext.newQuery(QueryEase.select(id, idx).from(choiceTable).orderBy(id), Result.class)
				.mapKey(Result::new, id)
				.map(idx, (SerializableBiConsumer<Result, Integer>) Result::setIdx);
		Set<Result> persistedChoices = resultExecutableQuery.execute(Accumulators.toKeepingOrderSet());
		assertThat(persistedChoices).extracting(Result::getId).containsExactly(10L, 20L, 30L);
		// stating that indexes are in same order than instances
		assertThat(persistedChoices).extracting(Result::getIdx).containsExactly(1, 2, 3);
	}
	
	@Test
	void insert_indexedByColumnName() {
		persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
		
		Table choiceTable = new Table("Choice");
		// we declare the column that will store our List index
		Column<Table, Identifier> id = choiceTable.addColumn("id", Identifier.class).primaryKey();
		Column<Table, Integer> idx = choiceTable.addColumn("myIdx", int.class);
		
		FluentEntityMappingBuilder<Choice, Identifier<Long>> choiceMappingConfiguration = entityBuilder(Choice.class, LONG_TYPE)
				.mapKey(Choice::getId, ALREADY_ASSIGNED)
				.map(Choice::getLabel);
		
		EntityPersister<Question, Identifier<Long>> questionPersister = entityBuilder(Question.class, LONG_TYPE)
				.mapKey(Question::getId, ALREADY_ASSIGNED)
				.mapOneToMany(Question::getChoices, choiceMappingConfiguration).mappedBy(Choice::getQuestion).indexedBy(idx.getName()).cascading(ALL)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Question newQuestion = new Question(1L);
		newQuestion.setChoices(Arrays.asList(
				new Choice(10L),
				new Choice(20L),
				new Choice(30L)));
		questionPersister.insert(newQuestion);
		
		ExecutableQuery<Result> resultExecutableQuery = persistenceContext.newQuery(QueryEase.select(id, idx).from(choiceTable).orderBy(id), Result.class)
				.mapKey(Result::new, id)
				.map(idx, (SerializableBiConsumer<Result, Integer>) Result::setIdx);
		Set<Result> persistedChoices = resultExecutableQuery.execute(Accumulators.toKeepingOrderSet());
		assertThat(persistedChoices).extracting(Result::getId).containsExactly(10L, 20L, 30L);
		// stating that indexes are in same order than instances
		assertThat(persistedChoices).extracting(Result::getIdx).containsExactly(1, 2, 3);
	}
	
	@Test
	void crud_mappedBy() {
		
		PersistenceContext persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
		
		Table choiceTable = new Table("Choice");
		// we declare the column that will store our List index
		Column<Table, Identifier> id = choiceTable.addColumn("id", Identifier.class).primaryKey();
		// since we won't ask for orphan deletion, index column will be set to null so it can't be a primitive type
		Column<Table, Integer> idx = choiceTable.addColumn("idx", Integer.class);
		
		FluentEntityMappingBuilder<Choice, Identifier<Long>> choiceMappingConfiguration = entityBuilder(Choice.class, LONG_TYPE)
				.mapKey(Choice::getId, ALREADY_ASSIGNED)
				.map(Choice::getLabel);
		
		EntityPersister<Question, Identifier<Long>> questionPersister = entityBuilder(Question.class, LONG_TYPE)
				.mapKey(Question::getId, ALREADY_ASSIGNED)
				.mapOneToMany(Question::getChoices, choiceMappingConfiguration).mappedBy(Choice::getQuestion).indexedBy(idx).cascading(ALL)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Question question = new Question(new PersistableIdentifier<>(1L));
		Choice choice1 = new Choice(new PersistableIdentifier<>(13L));
		choice1.setLabel("Grenoble");
		question.addChoice(choice1);
		Choice choice2 = new Choice(new PersistableIdentifier<>(17L));
		choice2.setLabel("Lyon");
		question.addChoice(choice2);
		questionPersister.insert(question);
		
		ExecutableQuery<Long> longExecutableQuery2 = persistenceContext.newQuery("select questionId from Choice", Long.class)
				.mapKey(i -> i, "questionId", Long.class);
		Set<Long> choiceQuestionIds = longExecutableQuery2.execute(Accumulators.toSet());
		
		assertThat(choiceQuestionIds).containsExactlyInAnyOrder(question.getId().getDelegate());
		
		// testing select
		Question loadedQuestion = questionPersister.select(question.getId());
		assertThat(loadedQuestion.getChoices()).extracting(Choice::getLabel).containsExactlyInAnyOrder("Grenoble", "Lyon");
		// ensuring that source is set on reverse side too
		assertThat(Iterables.first(loadedQuestion.getChoices()).getQuestion()).isEqualTo(loadedQuestion);
		
		// testing update : removal of a choice, reversed column must be set to null
		Question modifiedQuestion = new Question(question.getId());
		modifiedQuestion.addChoice(Iterables.first(question.getChoices()));
		
		questionPersister.update(modifiedQuestion, question, false);
		
		ExecutableQuery<Long> longExecutableQuery1 = persistenceContext.newQuery("select questionId from Choice", Long.class)
				.mapKey(i -> i, "questionId", Long.class);
		choiceQuestionIds = longExecutableQuery1.execute(Accumulators.toSet());
		assertThat(choiceQuestionIds).containsExactlyInAnyOrder(question.getId().getDelegate(), null);
		
		// testing delete
		questionPersister.delete(modifiedQuestion);
		// referencing columns must be set to null (we didn't ask for delete orphan)
		ExecutableQuery<Long> longExecutableQuery = persistenceContext.newQuery("select questionId from Choice", Long.class)
				.mapKey(i -> i, "questionId", Long.class);
		choiceQuestionIds = longExecutableQuery.execute(Accumulators.toSet());
		ArrayList<Long> expectedResult = new ArrayList<>();
		expectedResult.add(null);
		assertThat(choiceQuestionIds).containsExactlyElementsOf(expectedResult);
	}
	
	@Nested
	class Update {
		
		@Test
		void update_entitySwapping() {
			UpdateTestData updateTestData = new UpdateTestData().build();
			Table choiceTable = updateTestData.getChoiceTable();
			Column<Table, Identifier> id = updateTestData.getId();
			Column<Table, Integer> idx = updateTestData.getIdx();
			EntityPersister<Question, Identifier<Long>> questionPersister = updateTestData.getQuestionPersister();
			Question newQuestion = updateTestData.getNewQuestion();
			Choice choice1Clone = new Choice(updateTestData.getChoice1().getId());
			Choice choice2Clone = new Choice(updateTestData.getChoice2().getId());
			Choice choice3Clone = new Choice(updateTestData.getChoice3().getId());
			
			// creating a clone to test instance swapping
			Question modifiedQuestion = new Question(newQuestion.getId().getDelegate());
			// little swap between 2 elements
			modifiedQuestion.setChoices(Arrays.asList(choice2Clone, choice1Clone, choice3Clone));
			
			questionPersister.update(modifiedQuestion, newQuestion, true);
			ExecutableQuery<Result> resultExecutableQuery = persistenceContext.newQuery(QueryEase.select(id, idx).from(choiceTable).orderBy(id), Result.class)
					.mapKey(Result::new, id)
					.map(idx, (SerializableBiConsumer<Result, Integer>) Result::setIdx);
			Set<Result> persistedChoices = resultExecutableQuery.execute(Accumulators.toKeepingOrderSet());
			// id should be left unmodified
			assertThat(persistedChoices).extracting(Result::getId).containsExactly(10L, 20L, 30L);
			// but indexes must reflect swap done on instances
			assertThat(persistedChoices).extracting(Result::getIdx).containsExactly(2, 1, 3);
		}
		
		@Test
		void update_noChange() {
			UpdateTestData updateTestData = new UpdateTestData().build();
			Table choiceTable = updateTestData.getChoiceTable();
			Column<Table, Identifier> id = updateTestData.getId();
			Column<Table, Integer> idx = updateTestData.getIdx();
			EntityPersister<Question, Identifier<Long>> questionPersister = updateTestData.getQuestionPersister();
			Question newQuestion = updateTestData.getNewQuestion();
			
			// creating a clone to test for no change
			Question modifiedQuestion = new Question(newQuestion.getId().getDelegate());
			// no modifications
			modifiedQuestion.setChoices(newQuestion.getChoices());
			
			questionPersister.update(modifiedQuestion, newQuestion, true);
			ExecutableQuery<Result> resultExecutableQuery = persistenceContext.newQuery(QueryEase.select(id, idx).from(choiceTable).orderBy(id), Result.class)
					.mapKey(Result::new, id)
					.map(idx, (SerializableBiConsumer<Result, Integer>) Result::setIdx);
			Set<Result> persistedChoices = resultExecutableQuery.execute(Accumulators.toKeepingOrderSet());
			// nothing should have changed
			assertThat(persistedChoices).extracting(Result::getId).containsExactly(10L, 20L, 30L);
			assertThat(persistedChoices).extracting(Result::getIdx).containsExactly(1, 2, 3);
		}
		
		@Test
		void update_entityAddition() {
			UpdateTestData updateTestData = new UpdateTestData().build();
			Table choiceTable = updateTestData.getChoiceTable();
			Column<Table, Identifier> id = updateTestData.getId();
			Column<Table, Integer> idx = updateTestData.getIdx();
			EntityPersister<Question, Identifier<Long>> questionPersister = updateTestData.getQuestionPersister();
			Question newQuestion = updateTestData.getNewQuestion();
			Choice choice1Clone = new Choice(updateTestData.getChoice1().getId());
			Choice choice2Clone = new Choice(updateTestData.getChoice2().getId());
			Choice choice3Clone = new Choice(updateTestData.getChoice3().getId());
			Choice choice4 = new Choice(40L);
			
			// creating a clone to test instance swapping
			Question modifiedQuestion = new Question(newQuestion.getId().getDelegate());
			// addition of an element and little swap
			modifiedQuestion.setChoices(Arrays.asList(choice3Clone, choice4, choice2Clone, choice1Clone));
			
			questionPersister.update(modifiedQuestion, newQuestion, true);
			ExecutableQuery<Result> resultExecutableQuery = persistenceContext.newQuery(QueryEase.select(id, idx).from(choiceTable).orderBy(id), Result.class)
					.mapKey(Result::new, id)
					.map(idx, (SerializableBiConsumer<Result, Integer>) Result::setIdx);
			Set<Result> persistedChoices = resultExecutableQuery.execute(Accumulators.toKeepingOrderSet());
			// id should be left unmodified
			assertThat(persistedChoices).extracting(Result::getId).containsExactly(10L, 20L, 30L, 40L);
			// but indexes must reflect modifications
			assertThat(persistedChoices).extracting(Result::getIdx).containsExactly(4, 3, 1, 2);
		}
		
		@Test
		void update_entityRemoval() {
			UpdateTestData updateTestData = new UpdateTestData().build();
			Table choiceTable = updateTestData.getChoiceTable();
			Column<Table, Identifier> id = updateTestData.getId();
			Column<Table, Integer> idx = updateTestData.getIdx();
			EntityPersister<Question, Identifier<Long>> questionPersister = updateTestData.getQuestionPersister();
			Question newQuestion = updateTestData.getNewQuestion();
			Choice choice1 = updateTestData.getChoice1();
			Choice choice3 = updateTestData.getChoice3();
			
			// creating a clone to test instance swapping
			Question modifiedQuestion = new Question(newQuestion.getId().getDelegate());
			Choice choice3Copy = new Choice(new PersistedIdentifier<>(choice3.getId().getDelegate()));
			Choice choice1Copy = new Choice(new PersistedIdentifier<>(choice1.getId().getDelegate()));
			// little swap between 2 elements
			modifiedQuestion.setChoices(Arrays.asList(choice3Copy, choice1Copy));
			
			questionPersister.update(modifiedQuestion, newQuestion, true);
			ExecutableQuery<Result> resultExecutableQuery = persistenceContext.newQuery(QueryEase.select(id, idx).from(choiceTable).orderBy(id), Result.class)
					.mapKey(Result::new, id)
					.map(idx, (SerializableBiConsumer<Result, Integer>) Result::setIdx);
			Set<Result> persistedChoices = resultExecutableQuery.execute(Accumulators.toKeepingOrderSet());
			// the removed id must be missing (entity asked for deletion)
			assertThat(persistedChoices).extracting(Result::getId).containsExactly(10L, 30L);
			// choice 1 (10) is last, choice 3 (30) is first
			assertThat(persistedChoices).extracting(Result::getIdx).containsExactly(2, 1);
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
			EntityPersister<Question, Identifier<Long>> questionPersister = updateTestData.getQuestionPersister();
			Question newQuestion = updateTestData.getNewQuestion();
			Choice choice1Clone = new Choice(updateTestData.getChoice1().getId());
			Choice choice3Clone = new Choice(updateTestData.getChoice3().getId());
			
			// creating a clone to test instance swaping
			Question modifiedQuestion = new Question(newQuestion.getId().getDelegate());
			// little swap between 2 elements
			modifiedQuestion.setChoices(Arrays.asList(choice3Clone, choice1Clone));
			
			questionPersister.update(modifiedQuestion, newQuestion, true);
			ExecutableQuery<Result> resultExecutableQuery = persistenceContext.newQuery(QueryEase.select(id, idx).from(choiceTable).orderBy(id), Result.class)
					.mapKey(Result::new, id)
					.map(idx, (SerializableBiConsumer<Result, Integer>) Result::setIdx);
			Set<Result> persistedChoices = resultExecutableQuery.execute(Accumulators.toKeepingOrderSet());
			// the removed id must be missing (entity asked for deletion)
			assertThat(persistedChoices).extracting(Result::getId).containsExactly(10L, 30L);
			// choice 1 (10) is last, choice 3 (30) is first
			assertThat(persistedChoices).extracting(Result::getIdx).containsExactly(2, 1);
		}
		
		@Test
		void update_withAssociationTable_associationRecordsMustBeUpdated_butNotTargetEntities_list() throws SQLException {
			PersistenceContext persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
			
			FluentEntityMappingBuilder<Choice, Identifier<Long>> choiceMappingConfiguration = entityBuilder(Choice.class, LONG_TYPE)
					.mapKey(Choice::getId, ALREADY_ASSIGNED)
					.map(Choice::getLabel);
			
			EntityPersister<Question, Identifier<Long>> questionPersister = entityBuilder(Question.class, LONG_TYPE)
					.mapKey(Question::getId, ALREADY_ASSIGNED)
					.map(Question::getLabel)
					.mapOneToMany(Question::getChoices, choiceMappingConfiguration).cascading(ASSOCIATION_ONLY)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			// we need to insert target choices because they won't be inserted by ASSOCIATION_ONLY cascade
			persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into Choice(id) values (100), (200)");
			
			Question question = new Question(new PersistableIdentifier<>(42L));
			Choice choice1 = new Choice(new PersistableIdentifier<>(100L));
			Choice choice2 = new Choice(new PersistableIdentifier<>(200L));
			question.addChoice(choice1);
			question.addChoice(choice2);
			
			questionPersister.insert(question);
			
			// changing values before update
			question.setLabel("What's your name ?");
			choice1.setLabel("John");
			questionPersister.update(question, questionPersister.select(question.getId()), true);
			
			ResultSet resultSet;
			// Checking that country name was updated
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select label from Question where id = 42");
			ResultSetIterator<Row> questionIterator = new RowIterator(resultSet, Maps.asMap("label", DefaultParameterBinders.STRING_BINDER));
			assertThat(Iterables.collectToList(() -> questionIterator, row -> row.get("label"))).isEqualTo(Arrays.asList("What's your name ?"));
			// .. but not its city name
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select label from Choice where id = 100");
			ResultSetIterator<Row> choiceIterator = new RowIterator(resultSet, Maps.asMap("label", DefaultParameterBinders.STRING_BINDER));
			assertThat(Iterables.collectToList(() -> choiceIterator, row -> row.get("label"))).isEqualTo(Arrays.asList((Object) null));
			
			// removing choice doesn't have any effect either
			assertThat(question.getChoices().size()).isEqualTo(2);	// safeguard for unwanted regression on choice removal, because it would 
			// totally corrupt this test
			question.getChoices().remove(choice1);
			assertThat(question.getChoices().size()).isEqualTo(1);	// safeguard for unwanted regression on choice removal, because it would totally corrupt this test
			questionPersister.update(question, questionPersister.select(question.getId()), true);
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select label from Choice where id = 100");
			ResultSetIterator<Row> choiceIterator2 = new RowIterator(resultSet, Maps.asMap("label", DefaultParameterBinders.STRING_BINDER));
			assertThat(Iterables.collectToList(() -> choiceIterator2, row -> row.get("label"))).isEqualTo(Arrays.asList((Object) null));
		}
	}
	
	@Test
	void select() throws SQLException {
		persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
		
		Table choiceTable = new Table("Choice");
		// we declare the column that will store our List index
		Column<Table, Identifier> id = choiceTable.addColumn("id", Identifier.class).primaryKey();
		Column<Table, Integer> idx = choiceTable.addColumn("idx", int.class);
		
		FluentEntityMappingBuilder<Choice, Identifier<Long>> choiceMappingConfiguration = entityBuilder(Choice.class, LONG_TYPE)
				.mapKey(Choice::getId, ALREADY_ASSIGNED)
				.map(Choice::getLabel);
		
		EntityPersister<Question, Identifier<Long>> questionPersister = entityBuilder(Question.class, LONG_TYPE)
				.mapKey(Question::getId, ALREADY_ASSIGNED)
				.mapOneToMany(Question::getChoices, choiceMappingConfiguration).mappedBy(Choice::getQuestion).indexedBy(idx).cascading(ALL)
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
		connectionProvider.giveConnection().commit();
		assertThat(select.getChoices()).extracting(chain(Choice::getId, StatefulIdentifier::getDelegate)).containsExactlyInAnyOrder(10L, 20L, 30L);
	}
	
	@Test
	void delete_reverseSideIsNotMapped_relationRecordsMustBeDeleted() {
		persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
		
		DuplicatesTestData duplicatesTestData = new DuplicatesTestData().build();
		
		EntityPersister<Question, Identifier<Long>> questionPersister = duplicatesTestData.getQuestionPersister();
		
		Question newQuestion = new Question(1L);
		Choice choice1 = new Choice(10L);
		Choice choice2 = new Choice(20L);
		Choice choice3 = new Choice(30L);
		newQuestion.setChoices(Arrays.asList(choice1, choice2, choice3));
		questionPersister.insert(newQuestion);
		
		assertThat(choice1.getId().isPersisted()).isTrue();
		
		EntityPersister<Answer, Identifier<Long>> answerPersister = duplicatesTestData.getAnswerPersister();
		Answer answer = new Answer(1L);
		answer.takeChoices(Arrays.asList(choice1, choice2, choice3));
		answerPersister.persist(answer);
		
		answerPersister.delete(answer);
		
		ResultSet resultSet;
		long answerCount = persistenceContext.newQuery("select count(id) as answerId from Answer", long.class)
				.mapKey("answerId", long.class).execute(Accumulators.getFirst());
		assertThat(answerCount).isEqualTo(0);
		long relationCount = persistenceContext.newQuery("select count(*) as relationCount from Answer_Choices", long.class)
				.mapKey("relationCount", long.class).execute(Accumulators.getFirst());
		assertThat(relationCount).isEqualTo(0);
		long choiceCount = persistenceContext.newQuery("select count(id) as choiceCount from Choice", long.class)
				.mapKey("choiceCount", long.class).execute(Accumulators.getFirst());
		assertThat(choiceCount).isEqualTo(3);
	}
	
	@Nested
	class WithDuplicates {
		
		@Test
		void insert() {
			persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
			
			DuplicatesTestData duplicatesTestData = new DuplicatesTestData().build();
			
			EntityPersister<Question, Identifier<Long>> questionPersister = duplicatesTestData.getQuestionPersister();
			EntityPersister<Answer, Identifier<Long>> answerPersister = duplicatesTestData.getAnswerPersister();
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
			
			ExecutableBeanPropertyKeyQueryMapper<RawAnswer> query = persistenceContext.newQuery(
					QueryEase.select(answerChoicesTableId, answerChoicesTableIdx, answerChoicesTableChoiceId)
							.from(answerChoicesTable).orderBy(answerChoicesTableIdx), RawAnswer.class);
			ExecutableQuery<RawAnswer> rawAnswerExecutableQuery = query
					.mapKey(RawAnswer::new, answerChoicesTableId, answerChoicesTableIdx, answerChoicesTableChoiceId);
			Set<RawAnswer> persistedChoices = rawAnswerExecutableQuery.execute(Accumulators.toKeepingOrderSet());
			assertThat(persistedChoices).extracting(RawAnswer::getChoiceId).containsExactly(10L, 20L, 20L, 30L);
			// stating that indexes are in same order than instances
			assertThat(persistedChoices).extracting(RawAnswer::getChoiceIdx).containsExactly(1, 2, 3, 4);
		}
		
		@Test
		void select_default() {
			persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
			
			DuplicatesTestData duplicatesTestData = new DuplicatesTestData().build();
			
			EntityPersister<Question, Identifier<Long>> questionPersister = duplicatesTestData.getQuestionPersister();
			EntityPersister<Answer, Identifier<Long>> answerPersister = duplicatesTestData.getAnswerPersister();
			
			Question newQuestion = new Question(1L);
			Choice choice1 = new Choice(10L);
			choice1.setLabel("toto");
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
			
			assertThat(selectedAnswer.getId().getDelegate()).isEqualTo((Long) 1L);
			assertThat(selectedAnswer.getChoices()).extracting(AnswerChoice::getId, AnswerChoice::getLabel).containsExactly(
					new Tuple(choice1.getId(), choice1.getLabel()),
					new Tuple(choice2.getId(), choice2.getLabel()),
					new Tuple(choice2.getId(), choice2.getLabel()),
					new Tuple(choice3.getId(), choice3.getLabel()));
		}
		
		/** Test to check that loading a target entity from its persister still work (no use of the aggregate persister) */
		@Test
		void select_targets() {
			persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
			
			DuplicatesTestData duplicatesTestData = new DuplicatesTestData().build();
			
			EntityPersister<Question, Identifier<Long>> questionPersister = duplicatesTestData.getQuestionPersister();
			
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
			Long loadedId= persistenceContext.newQuery("select id from CHOICE where id = :id", Long.class)
					.mapKey("id", Long.class)
					.set("id", 10L)
					.execute(Accumulators.getFirstUnique());
			assertThat(loadedId).isEqualTo(choice1.getId().getDelegate());
			
			// loading the target entity from its aggregate persister
			Question loadedQuestion = duplicatesTestData.questionPersister.select(new PersistableIdentifier<>(1L));
			assertThat(loadedQuestion.getChoices()).isEqualTo(newQuestion.getChoices());
		}
		
		@Test
		void delete() {
			persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
			
			DuplicatesTestData duplicatesTestData = new DuplicatesTestData().build();
			
			EntityPersister<Question, Identifier<Long>> questionPersister = duplicatesTestData.getQuestionPersister();
			EntityPersister<Answer, Identifier<Long>> answerPersister = duplicatesTestData.getAnswerPersister();
			
			Question newQuestion = new Question(1L);
			Choice choice1 = new Choice(10L);
			Choice choice2 = new Choice(20L);
			Choice choice3 = new Choice(30L);
			newQuestion.setChoices(Arrays.asList(choice1, choice2, choice3));
			questionPersister.insert(newQuestion);
			
			Answer answer = new Answer(1L);
			answer.takeChoices(Arrays.asList(choice1, choice2, choice2, choice3));
			answerPersister.insert(answer);
			
			Answer loadedAnswer = answerPersister.select(answer.getId());
			answer.getChoices().remove(1);
			answerPersister.update(answer, loadedAnswer, true);
			
			Answer loadedAnswer2 = answerPersister.select(answer.getId());
			org.assertj.core.api.Assertions.assertThat(loadedAnswer2.getChoices())
					.usingRecursiveComparison()
					.isEqualTo(Arrays.asList(new AnswerChoice(choice1), new AnswerChoice(choice2), new AnswerChoice(choice3)));
			
			answerPersister.delete(answer);
			
			Table answerChoicesTable = duplicatesTestData.getAnswerChoicesTable();
			ExecutableQuery<Long> longExecutableQuery1 = persistenceContext.newQuery(QueryEase.select("count(*) as c", long.class).from(answerChoicesTable), Long.class)
					.mapKey("c", long.class);
			Set<Long> persistedChoices = longExecutableQuery1.execute(Accumulators.toSet());
			assertThat(Iterables.first(persistedChoices)).isEqualTo((Long) 0L);
			
			// No choice must be deleted
			// NB : we use choiceReader instead of choicePersister because the latter needs the idx column which is not mapped for Answer -> Choice
			ExecutableQuery<Long> longExecutableQuery = persistenceContext.newQuery("select id from Choice", Long.class)
					.mapKey("id", long.class);
			Set<Long> remainingChoices = longExecutableQuery.execute(Accumulators.toSet());
			assertThat(new HashSet<>(remainingChoices)).isEqualTo(Arrays.asSet(choice1.getId().getDelegate(), choice2.getId().getDelegate(),
					choice3.getId().getDelegate()));
		}
		
		@Test
		void deleteById() {
			persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
			
			DuplicatesTestData duplicatesTestData = new DuplicatesTestData().build();
			
			EntityPersister<Question, Identifier<Long>> questionPersister = duplicatesTestData.getQuestionPersister();
			EntityPersister<Answer, Identifier<Long>> answerPersister = duplicatesTestData.getAnswerPersister();
			
			Question newQuestion = new Question(1L);
			Choice choice1 = new Choice(10L);
			Choice choice2 = new Choice(20L);
			Choice choice3 = new Choice(30L);
			newQuestion.setChoices(Arrays.asList(choice1, choice2, choice3));
			questionPersister.insert(newQuestion);
			
			Answer answer = new Answer(1L);
			answer.takeChoices(Arrays.asList(choice1, choice2, choice2, choice3));
			answerPersister.insert(answer);
			
			answerPersister.deleteById(answer);
			
			Table answerChoicesTable = duplicatesTestData.getAnswerChoicesTable();
			ExecutableQuery<Long> longExecutableQuery1 = persistenceContext.newQuery(QueryEase.select("count(*) as c", long.class).from(answerChoicesTable).getQuery(), Long.class)
					.mapKey("c", long.class);
			Set<Long> persistedChoices = longExecutableQuery1.execute(Accumulators.toSet());
			assertThat(Iterables.first(persistedChoices)).isEqualTo((Long) 0L);
			
			// No choice must be deleted
			// NB : we use choiceReader instead of choicePersister because the latter needs the idx column which is not mapped for Answer -> Choice
			ExecutableQuery<Long> longExecutableQuery = persistenceContext.newQuery("select id from Choice", Long.class)
					.mapKey("id", long.class);
			Set<Long> remainingChoices = longExecutableQuery.execute(Accumulators.toSet());
			assertThat(new HashSet<>(remainingChoices)).isEqualTo(Arrays.asSet(choice1.getId().getDelegate(), choice2.getId().getDelegate(),
					choice3.getId().getDelegate()));
		}

		@Test
		void update() {
			persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
			
			DuplicatesTestData duplicatesTestData = new DuplicatesTestData().build();
			
			EntityPersister<Question, Identifier<Long>> questionPersister = duplicatesTestData.getQuestionPersister();
			EntityPersister<Answer, Identifier<Long>> answerPersister = duplicatesTestData.getAnswerPersister();
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
			newQuestion.setChoices(Arrays.asList(choice1, choice2, choice3, choice4, newChoice));
			questionPersister.update(newQuestion);
			Answer selectedAnswer = answerPersister.select(new PersistableIdentifier<>(1L));
			// NB: difference with previous state is addition on newChoice (in the middle), choice1 & choice4 to the end
			selectedAnswer.takeChoices(Arrays.asList(choice1, choice2, newChoice, choice2, choice3, choice1, choice4));
			answerPersister.update(selectedAnswer, answer, true);
			
			ExecutableBeanPropertyKeyQueryMapper<RawAnswer> query = persistenceContext.newQuery(
					QueryEase.select(answerChoicesTableId, answerChoicesTableIdx, answerChoicesTableChoiceId)
							.from(answerChoicesTable).orderBy(answerChoicesTableIdx), RawAnswer.class);
			ExecutableQuery<RawAnswer> rawAnswerExecutableQuery1 = query
					.mapKey(RawAnswer::new, answerChoicesTableId, answerChoicesTableIdx, answerChoicesTableChoiceId);
			Set<RawAnswer> persistedChoices = rawAnswerExecutableQuery1.execute(Accumulators.toKeepingOrderSet());
			assertThat(persistedChoices).extracting(RawAnswer::getChoiceId).containsExactly(10L, 20L, 50L, 20L, 30L, 10L, 40L);
			// stating that indexes are in same order than instances
			assertThat(persistedChoices).extracting(RawAnswer::getChoiceIdx).containsExactly(1, 2, 3, 4, 5, 6, 7);
			
			// test with entity removal
			Answer selectedAnswer1 = answerPersister.select(new PersistableIdentifier<>(1L));
			selectedAnswer1.takeChoices(Arrays.asList(choice1, choice2, choice1));
			answerPersister.update(selectedAnswer1, selectedAnswer, true);
			
			query = persistenceContext.newQuery(
					QueryEase.select(answerChoicesTableId, answerChoicesTableIdx, answerChoicesTableChoiceId)
							.from(answerChoicesTable).orderBy(answerChoicesTableIdx), RawAnswer.class);
			ExecutableQuery<RawAnswer> rawAnswerExecutableQuery = query
					.mapKey(RawAnswer::new, answerChoicesTableId, answerChoicesTableIdx, answerChoicesTableChoiceId);
			persistedChoices = rawAnswerExecutableQuery.execute(Accumulators.toKeepingOrderSet());
			assertThat(persistedChoices).extracting(RawAnswer::getChoiceId).containsExactly(10L, 20L, 10L);
			// stating that indexes are in same order than instances
			assertThat(persistedChoices).extracting(RawAnswer::getChoiceIdx).containsExactly(1, 2, 3);
		}
	}
	
	/**
	 * Simple class to store index of an entity. Very general implementation. 
	 */
	private static class Result {
		private final long id;
		private int idx;
		
		private Result(Identifier<Long> id) {
			this.id = id.getDelegate();
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
		
		private String label;
		
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
		
		public String getLabel() {
			return label;
		}
		
		public void setLabel(String label) {
			this.label = label;
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
		
		private String label;
		
		public AnswerChoice() {
		}
		
		private AnswerChoice(Choice choice) {
			this(choice.getId());
			setLabel(choice.getLabel());
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
		
		public String getLabel() {
			return label;
		}
		
		public void setLabel(String label) {
			this.label = label;
		}
		
		@Override
		public String toString() {
			return "AnswerChoice{id=" + id.getDelegate() + ", label='" + label + '\'' + '}';
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
			this(answerId.getDelegate(), choiceIdx, choiceId.getDelegate());
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
		
		private String label;
		
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
		
		public String getLabel() {
			return label;
		}
		
		public void setLabel(String label) {
			this.label = label;
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
			return "Choice{id=" + id.getDelegate() + ", question=" + question + ", label='" + label + '\'' + '}';
		}
	}
	
	private class UpdateTestData {
		private Table choiceTable;
		private Column<Table, Identifier> id;
		private Column<Table, Integer> idx;
		private EntityPersister<Question, Identifier<Long>> questionPersister;
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
		
		public EntityPersister<Question, Identifier<Long>> getQuestionPersister() {
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
			
			FluentEntityMappingBuilder<Choice, Identifier<Long>> choiceMappingConfiguration = entityBuilder(Choice.class, LONG_TYPE)
					.mapKey(Choice::getId, ALREADY_ASSIGNED)
					.map(Choice::getLabel);
			
			questionPersister = entityBuilder(Question.class, LONG_TYPE)
					.mapKey(Question::getId, ALREADY_ASSIGNED)
					.mapOneToMany(Question::getChoices, choiceMappingConfiguration).mappedBy(Choice::getQuestion).indexedBy(idx)
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
			
			ExecutableQuery<Result> resultExecutableQuery = persistenceContext.newQuery(QueryEase.select(id, idx).from(choiceTable).orderBy(id), Result.class)
					.mapKey(Result::new, id)
					.map(idx, (SerializableBiConsumer<Result, Integer>) Result::setIdx);
			Set<Result> persistedChoices = resultExecutableQuery.execute(Accumulators.toKeepingOrderSet());
			assertThat(persistedChoices).extracting(Result::getId).containsExactly(10L, 20L, 30L);
			// stating that indexes are in same order than instances
			assertThat(persistedChoices).extracting(Result::getIdx).containsExactly(1, 2, 3);
			return this;
		}
	}
	
	private class DuplicatesTestData {
		
		private EntityPersister<Question, Identifier<Long>> questionPersister;
		private EntityPersister<Answer, Identifier<Long>> answerPersister;
		private Table answerChoicesTable;
		private Column<Table, Identifier> answerChoicesTableId;
		private Column<Table, Integer> answerChoicesTableIdx;
		private Column<Table, Identifier> answerChoicesTableChoiceId;
		
		public EntityPersister<Question, Identifier<Long>> getQuestionPersister() {
			return questionPersister;
		}
		
		public EntityPersister<Answer, Identifier<Long>> getAnswerPersister() {
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
			
			FluentEntityMappingBuilder<Choice, Identifier<Long>> choiceMappingConfiguration = entityBuilder(Choice.class, LONG_TYPE)
					.mapKey(Choice::getId, ALREADY_ASSIGNED)
					.map(Choice::getLabel);
			
			questionPersister = entityBuilder(Question.class, LONG_TYPE)
					.mapKey(Question::getId, ALREADY_ASSIGNED)
					.mapOneToMany(Question::getChoices, choiceMappingConfiguration).mappedBy(Choice::getQuestion).indexedBy(idx)
					.cascading(ALL)
					.build(persistenceContext);
			
			// We create another choices persister dedicated to Answer association because usages are not the same :
			// like Aggregate (in Domain Driven Design) or CQRS, Answers are not in the same context than Questions so it requires a different
			// mapping. For instance there's no need of Question relationship mapping.
			// BE AWARE THAT mapping Choice a second time is a bad practise
			FluentEntityMappingBuilder<AnswerChoice, Identifier<Long>> answerChoiceMappingConfiguration = entityBuilder(AnswerChoice.class, LONG_TYPE)
					.mapKey(AnswerChoice::getId, ALREADY_ASSIGNED)
					.map(AnswerChoice::getLabel);
			
			answerPersister = entityBuilder(Answer.class, LONG_TYPE)
					.mapKey(Answer::getId, ALREADY_ASSIGNED)
					.mapOneToMany(Answer::getChoices, answerChoiceMappingConfiguration, choiceTable)
						.cascading(ALL).indexed()
					.build(persistenceContext);
			
			// We declare the table that will store our relationship, and overall our List index
			// NB: names are hardcoded here because they are hardly accessible from outside of CascadeManyConfigurer
			answerChoicesTable = new Table("Answer_Choices");
			answerChoicesTableId = answerChoicesTable.addColumn("answer_Id", Identifier.class).primaryKey();
			answerChoicesTableIdx = answerChoicesTable.addColumn("idx", Integer.class).primaryKey();
			answerChoicesTableChoiceId = answerChoicesTable.addColumn("choices_Id", Identifier.class).primaryKey();
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			return this;
		}
	}
}
