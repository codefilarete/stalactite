package org.codefilarete.stalactite.engine;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import org.codefilarete.stalactite.engine.FluentEntityMappingBuilder.FluentMappingBuilderPropertyOptions;
import org.codefilarete.stalactite.engine.idprovider.LongProvider;
import org.codefilarete.stalactite.engine.runtime.OptimizedUpdatePersister;
import org.codefilarete.stalactite.id.Identified;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistableIdentifier;
import org.codefilarete.stalactite.id.PersistedIdentifier;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.CurrentThreadConnectionProvider;
import org.codefilarete.stalactite.sql.HSQLDBDialect;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ResultSetIterator;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.engine.CascadeOptions.RelationMode.*;
import static org.codefilarete.stalactite.engine.MappingEase.entityBuilder;
import static org.codefilarete.stalactite.id.Identifier.LONG_TYPE;
import static org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED;

/**
 * @author Guillaume Mary
 */
class FluentEntityMappingConfigurationSupportManyToManySetTest {
	
	private static final HSQLDBDialect DIALECT = new HSQLDBDialect();
	private static FluentMappingBuilderPropertyOptions<Choice, Identifier<Long>> CHOICE_MAPPING_CONFIGURATION;
	private final DataSource dataSource = new HSQLDBInMemoryDataSource();
	private final ConnectionProvider connectionProvider = new CurrentThreadConnectionProvider(dataSource);
	private PersistenceContext persistenceContext;
	
	@BeforeAll
	static void initBinders() {
		// binder creation for our identifier
		DIALECT.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		DIALECT.getSqlTypeRegistry().put(Identifier.class, "int");
	}

	@BeforeEach
	void beforeTest() {
		persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
		
		
		// We need to rebuild our choicePersister before each test because some of them alter it on answer relationship.
		// So schema contains FK twice with same name, ending in duplicate FK name exception
		CHOICE_MAPPING_CONFIGURATION = entityBuilder(Choice.class, LONG_TYPE)
				.mapKey(Choice::getId, ALREADY_ASSIGNED)
				.map(Choice::getLabel);
		

	}

	@Test
	void foreignKeysAreCreated() throws SQLException {
		EntityPersister<Answer, Identifier<Long>> answerPersister = entityBuilder(Answer.class, LONG_TYPE)
				.mapKey(Answer::getId, ALREADY_ASSIGNED)
				.mapManyToManySet(Answer::getChoices, CHOICE_MAPPING_CONFIGURATION)
				.cascading(READ_ONLY)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Connection currentConnection = persistenceContext.getConnectionProvider().giveConnection();
		
		ResultSetIterator<JdbcForeignKey> fkChoiceIterator = new ResultSetIterator<JdbcForeignKey>(currentConnection.getMetaData().getImportedKeys(null, null,
				"ANSWER_CHOICES")) {
			@Override
			public JdbcForeignKey convert(ResultSet rs) throws SQLException {
				return new JdbcForeignKey(
						rs.getString("FK_NAME"),
						rs.getString("FKTABLE_NAME"), rs.getString("FKCOLUMN_NAME"),
						rs.getString("PKTABLE_NAME"), rs.getString("PKCOLUMN_NAME")
				);
			}
		};
		Set<String> foundForeignKey = Iterables.collect(() -> fkChoiceIterator, JdbcForeignKey::getSignature, HashSet::new);
		assertThat(foundForeignKey).containsExactlyInAnyOrder(
				new JdbcForeignKey("FK_ANSWER_CHOICES_ANSWER_ID_ANSWER_ID", "ANSWER_CHOICES", "ANSWER_ID", "ANSWER", "ID").getSignature(),
				new JdbcForeignKey("FK_ANSWER_CHOICES_CHOICES_ID_CHOICE_ID", "ANSWER_CHOICES", "CHOICES_ID", "CHOICE", "ID").getSignature()
		);
	}
	
	@Test
	void foreignKeysAreCreated_targetTableSpecified() throws SQLException {
		Table targetTable = new Table("MyChoice");
		
		EntityPersister<Answer, Identifier<Long>> answerPersister = entityBuilder(Answer.class, LONG_TYPE)
				.mapKey(Answer::getId, ALREADY_ASSIGNED)
				.mapManyToManySet(Answer::getChoices, CHOICE_MAPPING_CONFIGURATION, targetTable)
				.cascading(READ_ONLY)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Connection currentConnection = persistenceContext.getConnectionProvider().giveConnection();
		
		ResultSetIterator<JdbcForeignKey> fkChoiceIterator = new ResultSetIterator<JdbcForeignKey>(currentConnection.getMetaData().getImportedKeys(null, null,
				"ANSWER_CHOICES")) {
			@Override
			public JdbcForeignKey convert(ResultSet rs) throws SQLException {
				return new JdbcForeignKey(
						rs.getString("FK_NAME"),
						rs.getString("FKTABLE_NAME"), rs.getString("FKCOLUMN_NAME"),
						rs.getString("PKTABLE_NAME"), rs.getString("PKCOLUMN_NAME")
				);
			}
		};
		Set<String> foundForeignKey = Iterables.collect(() -> fkChoiceIterator, JdbcForeignKey::getSignature, HashSet::new);
		assertThat(foundForeignKey).containsExactlyInAnyOrder(
				new JdbcForeignKey("FK_ANSWER_CHOICES_ANSWER_ID_ANSWER_ID", "ANSWER_CHOICES", "ANSWER_ID", "ANSWER", "ID").getSignature(),
				new JdbcForeignKey("FK_ANSWER_CHOICES_CHOICES_ID_MYCHOICE_ID", "ANSWER_CHOICES", "CHOICES_ID", "MYCHOICE", "ID").getSignature()
		);
	}
	
	@Test
	void crud_relationContainsOneToMany() {
		EntityPersister<Answer, Identifier<Long>> persister = entityBuilder(Answer.class, LONG_TYPE)
				.mapKey(Answer::getId, ALREADY_ASSIGNED)
				.mapManyToManySet(Answer::getChoices, CHOICE_MAPPING_CONFIGURATION)
				.build(persistenceContext);

		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();

		Answer answer = new Answer(new PersistableIdentifier<>(1L));
		Choice grenoble = new Choice(new PersistableIdentifier<>(13L));
		grenoble.setLabel("Grenoble");
		Choice lyon = new Choice(new PersistableIdentifier<>(17L));
		lyon.setLabel("Lyon");
		answer.addChoices(grenoble, lyon);
		persister.insert(answer);
		
		Set<Long> choiceAnswerIds = persistenceContext.newQuery("select answer_id from answer_choices", Long.class)
				.mapKey("answer_id", Long.class)
				.execute();

		assertThat(choiceAnswerIds).containsExactlyInAnyOrder(answer.getId().getSurrogate());

		// testing select
		Answer loadedAnswer = persister.select(answer.getId());
		assertThat(loadedAnswer.getChoices()).extracting(Choice::getLabel).containsExactlyInAnyOrder("Grenoble", "Lyon");
		
		// testing update : removal of a city, reversed column must be set to null
		Answer modifiedAnswer = new Answer(answer.getId());
		modifiedAnswer.addChoices(Iterables.first(answer.getChoices()));

		persister.update(modifiedAnswer, answer, false);

		choiceAnswerIds = persistenceContext.newQuery("select answer_id from answer_choices", Long.class)
				.mapKey("answer_id", Long.class)
				.execute();
		assertThat(choiceAnswerIds).containsExactlyInAnyOrder(answer.getId().getSurrogate());
		
		// referenced Choices must not be deleted (we didn't ask for delete orphan)
		Set<Long> choiceIds = persistenceContext.newQuery("select id from choice", Long.class)
				.mapKey("id", Long.class)
				.execute();
		assertThat(choiceIds).containsExactlyInAnyOrder(grenoble.getId().getSurrogate(), lyon.getId().getSurrogate());

		// testing delete
		persister.delete(modifiedAnswer);
		choiceAnswerIds = persistenceContext.newQuery("select answer_id from answer_choices", Long.class)
				.mapKey("answer_id", Long.class)
				.execute();
		assertThat(choiceAnswerIds).isEmpty();
	}
	
	@Test
	void crud_relationContainsManyToMany() {
		EntityPersister<Answer, Identifier<Long>> persister = entityBuilder(Answer.class, LONG_TYPE)
				.mapKey(Answer::getId, ALREADY_ASSIGNED)
				.mapManyToManySet(Answer::getChoices, CHOICE_MAPPING_CONFIGURATION).cascading(ALL)
				.build(persistenceContext);

		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();

		Answer answer1 = new Answer(new PersistableIdentifier<>(1L));
		Answer answer2 = new Answer(new PersistableIdentifier<>(2L));
		Choice grenoble = new Choice(new PersistableIdentifier<>(13L));
		grenoble.setLabel("Grenoble");
		Choice lyon = new Choice(new PersistableIdentifier<>(17L));
		lyon.setLabel("Lyon");
		answer1.addChoices(grenoble, lyon);
		answer2.addChoices(grenoble, lyon);
		persister.insert(Arrays.asList(answer1, answer2));
		
		Set<Long> choiceAnswerIds = persistenceContext.newQuery("select answer_id from answer_choices", Long.class)
				.mapKey("answer_id", Long.class)
				.execute();

		assertThat(choiceAnswerIds).containsExactlyInAnyOrder(answer1.getId().getSurrogate(), answer2.getId().getSurrogate());

		// testing select
		Answer loadedAnswer = persister.select(answer1.getId());
		assertThat(loadedAnswer.getChoices()).extracting(Choice::getLabel).containsExactlyInAnyOrder("Grenoble", "Lyon");
		
		// testing update : removal of a city, reversed column must be set to null
		Answer modifiedAnswer = new Answer(answer1.getId());
		modifiedAnswer.addChoices(Iterables.first(answer1.getChoices()));

		persister.update(modifiedAnswer, answer1, false);

		choiceAnswerIds = persistenceContext.newQuery("select answer_id from answer_choices", Long.class)
				.mapKey("answer_id", Long.class)
				.execute();
		assertThat(choiceAnswerIds).containsExactlyInAnyOrder(answer1.getId().getSurrogate(), answer2.getId().getSurrogate());
		
		// referenced Choices must not be deleted (we didn't ask for orphan deletion)
		Set<Long> choiceIds = persistenceContext.newQuery("select id from choice", Long.class)
				.mapKey("id", Long.class)
				.execute();
		assertThat(choiceIds).containsExactlyInAnyOrder(grenoble.getId().getSurrogate(), lyon.getId().getSurrogate());

		// testing delete
		persister.delete(modifiedAnswer);
		choiceAnswerIds = persistenceContext.newQuery("select answer_id from answer_choices", Long.class)
				.mapKey("answer_id", Long.class)
				.execute();
		assertThat(choiceAnswerIds).containsExactlyInAnyOrder(answer2.getId().getSurrogate());
	}
	
	@Test
	void select_collectionFactory() throws SQLException {
		// mapping building thanks to fluent API
		EntityPersister<Answer, Identifier<Long>> answerPersister = MappingEase.entityBuilder(Answer.class, Identifier.LONG_TYPE)
				.mapKey(Answer::getId, ALREADY_ASSIGNED)
				.mapManyToManySet(Answer::getChoices, CHOICE_MAPPING_CONFIGURATION)
					// applying a Set that will sort cities by their name
					.initializeWith(() -> new TreeSet<>(Comparator.comparing(Choice::getLabel)))
				.build(persistenceContext);

		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();

		// preparing data
		persistenceContext.getConnectionProvider().giveConnection().prepareStatement("insert into Answer(id) values (42)").execute();
		persistenceContext.getConnectionProvider().giveConnection().prepareStatement("insert into Choice(id, label) values (1, 'Paris')").execute();
		persistenceContext.getConnectionProvider().giveConnection().prepareStatement("insert into Choice(id, label) values (2, 'Lyon')").execute();
		persistenceContext.getConnectionProvider().giveConnection().prepareStatement("insert into Answer_Choices(answer_id, choices_id) values (42, 1), (42, 2)").execute();

		Answer loadedAnswer = answerPersister.select(new PersistedIdentifier<>(42L));
		assertThat(loadedAnswer.getChoices().getClass()).isEqualTo(TreeSet.class);
		assertThat(loadedAnswer.getChoices()).extracting(Choice::getLabel).containsExactly("Lyon", "Paris");
	}
	
	@Nested
	class CascadeReadOnly {
		
		@Test
		void insert_onlySourceEntitiesArePersisted() {
			// mapping building thanks to fluent API
			EntityPersister<Answer, Identifier<Long>> answerPersister = MappingEase.entityBuilder(Answer.class, Identifier.LONG_TYPE)
					.mapKey(Answer::getId, ALREADY_ASSIGNED)
					// no cascade, nor reverse side
					.mapManyToManySet(Answer::getChoices, CHOICE_MAPPING_CONFIGURATION).cascading(READ_ONLY)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Answer answer = new Answer(new PersistableIdentifier<>(1L));
			Choice grenoble = new Choice(new PersistableIdentifier<>(13L));
			grenoble.setLabel("Grenoble");
			Choice lyon = new Choice(new PersistableIdentifier<>(17L));
			lyon.setLabel("Lyon");
			answer.addChoices(grenoble, lyon);
			answerPersister.insert(answer);
			
			Set<Long> answerIds = persistenceContext.newQuery("select id from answer", Long.class)
					.mapKey("id", Long.class)
					.execute();
			assertThat(answerIds).containsExactlyInAnyOrder(answer.getId().getSurrogate());
			
			Long choiceAnswerCount = persistenceContext.newQuery("select count(*) as relationCount from answer_choices", Long.class)
					.mapKey("relationCount", Long.class)
					.singleResult()
					.execute();
			assertThat(choiceAnswerCount).isEqualTo(0);
			
			Long choiceCount = persistenceContext.newQuery("select count(*) as choiceCount from choice", Long.class)
					.mapKey("choiceCount", Long.class)
					.singleResult()
					.execute();
			assertThat(choiceCount).isEqualTo(0);
		}
	}
	
	@Nested
	class CascadeAll {

		@Test
		void update_associationTableIsMaintained() {
			// mapping building thanks to fluent API
			EntityPersister<Answer, Identifier<Long>> answerPersister = MappingEase.entityBuilder(Answer.class, Identifier.LONG_TYPE)
					.mapKey(Answer::getId, ALREADY_ASSIGNED)
					.mapOneToManySet(Answer::getChoices, CHOICE_MAPPING_CONFIGURATION).cascading(ALL)
					.build(persistenceContext);

			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();

			LongProvider answerIdProvider = new LongProvider();
			Answer dummyAnswer = new Answer(answerIdProvider.giveNewIdentifier());
			LongProvider cityIdProvider = new LongProvider();
			Choice paris = new Choice(cityIdProvider.giveNewIdentifier());
			paris.setLabel("Paris");
			dummyAnswer.addChoices(paris);
			Choice lyon = new Choice(cityIdProvider.giveNewIdentifier());
			lyon.setLabel("Lyon");
			dummyAnswer.addChoices(lyon);
			answerPersister.insert(dummyAnswer);

			// Changing answer cities to see what happens when we save it to the database
			// removing Paris
			// adding Grenoble
			// renaming Lyon
			Answer persistedAnswer = answerPersister.select(dummyAnswer.getId());
			persistedAnswer.getChoices().remove(paris);
			Choice grenoble = new Choice(cityIdProvider.giveNewIdentifier());
			grenoble.setLabel("Grenoble");
			persistedAnswer.addChoices(grenoble);
			Iterables.first(persistedAnswer.getChoices()).setLabel("changed");

			answerPersister.update(persistedAnswer, dummyAnswer, true);

			Answer persistedAnswer2 = answerPersister.select(dummyAnswer.getId());
			assertThat(persistedAnswer2.getChoices()).extracting(Choice::getLabel).containsExactlyInAnyOrder("changed", "Grenoble");
		}
		
		@Test
		void delete_associationRecordsMustBeDeleted() throws SQLException {
			EntityPersister<Answer, Identifier<Long>> answerPersister = MappingEase.entityBuilder(Answer.class, Identifier.LONG_TYPE)
					.mapKey(Answer::getId, ALREADY_ASSIGNED)
					.mapOneToManySet(Answer::getChoices, CHOICE_MAPPING_CONFIGURATION).cascading(ALL)
					.build(persistenceContext);

			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();

			persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into Answer(id) values (42, 666)");
			persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into Choice(id) values (100), (200), (300)");
			persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into Answer_choices(answer_Id, choices_Id)" +
					" values (42, 100), (42, 200), (666, 300)");

			Answer answer1 = new Answer(new PersistedIdentifier<>(42L));
			Choice city1 = new Choice(new PersistedIdentifier<>(100L));
			Choice city2 = new Choice(new PersistedIdentifier<>(200L));
			answer1.addChoices(city1, city2);
			Answer answer2 = new Answer(new PersistedIdentifier<>(666L));
			Choice city3 = new Choice(new PersistedIdentifier<>(300L));
			answer2.addChoices(city3);

			// testing deletion
			answerPersister.delete(answer1);

			// Checking that we deleted what we wanted
			Long answerCount = persistenceContext.newQuery("select count(*) as answerCount from Answer where id = 42", Long.class)
					.mapKey("answerCount", Long.class)
					.singleResult()
					.execute();
			assertThat(answerCount).isEqualTo(0);
			// this test is unnecessary because foreign keys should have been violated, left for more insurance
			Long relationCount = persistenceContext.newQuery("select count(*) as relationCount from Answer_choices where answer_Id = 42", Long.class)
					.mapKey("relationCount", Long.class)
					.singleResult()
					.execute();
			assertThat(relationCount).isEqualTo(0);
			// target entities are not deleted with cascade All
			Set<Long> choiceIds = persistenceContext.newQuery("select id from Choice where id in (100, 200)", Long.class)
					.mapKey("id", Long.class)
					.execute();
			assertThat(choiceIds).containsExactlyInAnyOrder(100L, 200L);

			// but we didn't delete everything !
			Set<Long> answerIds = persistenceContext.newQuery("select id from Answer where id = 666", Long.class)
					.mapKey("id", Long.class)
					.execute();
			assertThat(answerIds).containsExactlyInAnyOrder(666L);
			choiceIds = persistenceContext.newQuery("select id from Choice where id = 300", Long.class)
					.mapKey("id", Long.class)
					.execute();
			assertThat(choiceIds).containsExactlyInAnyOrder(300L);

			// testing deletion of the last one
			answerPersister.delete(answer2);
			answerCount = persistenceContext.newQuery("select count(*) as answerCount from Answer where id = 666", Long.class)
					.mapKey("answerCount", Long.class)
					.singleResult()
					.execute();
			assertThat(answerCount).isEqualTo(0);
			// this test is unnecessary because foreign keys should have been violated, left for more insurance
			relationCount = persistenceContext.newQuery("select count(*) as relationCount from Answer_choices where answer_Id = 666", Long.class)
					.mapKey("relationCount", Long.class)
					.singleResult()
					.execute();
			assertThat(relationCount).isEqualTo(0);
			// target entities are not deleted with cascade All
			choiceIds = persistenceContext.newQuery("select id from Choice where id = 300", Long.class)
					.mapKey("id", Long.class)
					.execute();
			assertThat(choiceIds).containsExactlyInAnyOrder(300L);
		}
	}
	
	@Nested
	class CascadeAllOrphanRemoval {

		@Test
		void update_removedElementsAreDeleted() {
			EntityPersister<Answer, Identifier<Long>> answerPersister = MappingEase.entityBuilder(Answer.class, Identifier.LONG_TYPE)
					.mapKey(Answer::getId, ALREADY_ASSIGNED)
					.mapOneToManySet(Answer::getChoices, CHOICE_MAPPING_CONFIGURATION).cascading(ALL_ORPHAN_REMOVAL)
					.build(persistenceContext);

			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();

			LongProvider answerIdProvider = new LongProvider();
			Answer dummyAnswer = new Answer(answerIdProvider.giveNewIdentifier());
			LongProvider cityIdProvider = new LongProvider();
			Choice paris = new Choice(cityIdProvider.giveNewIdentifier());
			paris.setLabel("Paris");
			dummyAnswer.addChoices(paris);
			Choice lyon = new Choice(cityIdProvider.giveNewIdentifier());
			lyon.setLabel("Lyon");
			dummyAnswer.addChoices(lyon);
			answerPersister.insert(dummyAnswer);

			// Changing answer choices to see what happens when we save it to the database
			Answer persistedAnswer = answerPersister.select(dummyAnswer.getId());
			persistedAnswer.getChoices().remove(paris);
			Choice grenoble = new Choice(cityIdProvider.giveNewIdentifier());
			grenoble.setLabel("Grenoble");
			persistedAnswer.addChoices(grenoble);
			Iterables.first(persistedAnswer.getChoices()).setLabel("changed");

			answerPersister.update(persistedAnswer, dummyAnswer, true);

			Answer persistedAnswer2 = answerPersister.select(dummyAnswer.getId());
			// Checking deletion has been taken into account : the reloaded instance contains choices that are the same as the memory one
			// (comparison are done on equals/hashCode => id)
			assertThat(persistedAnswer2.getChoices()).isEqualTo(Arrays.asHashSet(lyon, grenoble));
			// Checking update is done too
			assertThat(persistedAnswer2.getChoices()).extracting(Choice::getLabel).containsExactlyInAnyOrder("changed", "Grenoble");
		}
		
		@Test
		void delete_associationRecordsMustBeDeleted() throws SQLException {
			EntityPersister<Answer, Identifier<Long>> answerPersister = MappingEase.entityBuilder(Answer.class, Identifier.LONG_TYPE)
					.mapKey(Answer::getId, ALREADY_ASSIGNED)
					.mapOneToManySet(Answer::getChoices, CHOICE_MAPPING_CONFIGURATION).cascading(ALL_ORPHAN_REMOVAL)
					.build(persistenceContext);

			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();

			persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into Answer(id) values (42, 666)");
			persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into Choice(id) values (100), (200), (300)");
			persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into Answer_choices(answer_Id, choices_Id)" +
					" values (42, 100), (42, 200), (666, 300)");

			Answer answer1 = new Answer(new PersistedIdentifier<>(42L));
			Choice city1 = new Choice(new PersistedIdentifier<>(100L));
			Choice city2 = new Choice(new PersistedIdentifier<>(200L));
			answer1.addChoices(city1, city2);
			Answer answer2 = new Answer(new PersistedIdentifier<>(666L));
			Choice city3 = new Choice(new PersistedIdentifier<>(300L));
			answer2.addChoices(city3);

			// testing deletion
			answerPersister.delete(answer1);

			// Checking that we deleted what we wanted
			Long answerCount = persistenceContext.newQuery("select count(*) as answerCount from Answer where id = 42", Long.class)
					.mapKey("answerCount", Long.class)
					.singleResult()
					.execute();
			assertThat(answerCount).isEqualTo(0);
			// this test is unnecessary because foreign keys should have been violated, left for more insurance
			Long relationCount = persistenceContext.newQuery("select count(*) as relationCount from Answer_choices where answer_Id = 42", Long.class)
					.mapKey("relationCount", Long.class)
					.singleResult()
					.execute();
			assertThat(relationCount).isEqualTo(0);
			// target entities are not deleted with cascade All
			Set<Long> choiceIds = persistenceContext.newQuery("select id from Choice where id in (100, 200)", Long.class)
					.mapKey("id", Long.class)
					.execute();
			assertThat(choiceIds).isEmpty();
			
			// but we didn't delete everything !
			Set<Long> answerIds = persistenceContext.newQuery("select id from Answer where id = 666", Long.class)
					.mapKey("id", Long.class)
					.execute();
			assertThat(answerIds).containsExactlyInAnyOrder(666L);
			choiceIds = persistenceContext.newQuery("select id from Choice where id = 300", Long.class)
					.mapKey("id", Long.class)
					.execute();
			assertThat(choiceIds).containsExactlyInAnyOrder(300L);
			
			// testing deletion of the last one
			answerPersister.delete(answer2);
			answerCount = persistenceContext.newQuery("select count(*) as answerCount from Answer where id = 666", Long.class)
					.mapKey("answerCount", Long.class)
					.singleResult()
					.execute();
			assertThat(answerCount).isEqualTo(0);
			// this test is unnecessary because foreign keys should have been violated, left for more insurance
			relationCount = persistenceContext.newQuery("select count(*) as relationCount from Answer_choices where answer_Id = 666", Long.class)
					.mapKey("relationCount", Long.class)
					.singleResult()
					.execute();
			assertThat(relationCount).isEqualTo(0);
			// target entities are not deleted with cascade All
			choiceIds = persistenceContext.newQuery("select id from Choice where id = 300", Long.class)
					.mapKey("id", Long.class)
					.execute();
			assertThat(choiceIds).isEmpty();
		}
	}

	@Nested
	class CascadeAssociationOnly {

		@Test
		void insert_associationRecordsMustBeInserted_butNotTargetEntities() throws SQLException {
			EntityPersister<Answer, Identifier<Long>> answerPersister = MappingEase.entityBuilder(Answer.class, Identifier.LONG_TYPE)
					.mapKey(Answer::getId, ALREADY_ASSIGNED)
					.mapOneToManySet(Answer::getChoices, CHOICE_MAPPING_CONFIGURATION).cascading(ASSOCIATION_ONLY)
					.build(persistenceContext);

			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			// We need to insert target choices because they won't be inserted by ASSOCIATION_ONLY cascade
			// If they were inserted by cascade an constraint violation error will be thrown 
			persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into Choice(id) values (100), (200), (300)");
			
			Answer answer1 = new Answer(new PersistableIdentifier<>(42L));
			Choice choice1 = new Choice(new PersistableIdentifier<>(100L));
			Choice choice2 = new Choice(new PersistableIdentifier<>(200L));
			answer1.addChoices(choice1, choice2);
			Answer answer2 = new Answer(new PersistableIdentifier<>(666L));
			Choice choice3 = new Choice(new PersistableIdentifier<>(300L));
			answer2.addChoices(choice3);
			
			// testing insertion
			answerPersister.insert(Arrays.asList(answer1, answer2));
			
			// Checking that we inserted what we wanted
			Set<Long> answerIds = persistenceContext.newQuery("select id from Answer where id in (42, 666)", Long.class)
					.mapKey("id", Long.class)
					.execute();
			assertThat(answerIds).containsExactlyInAnyOrder(answer1.getId().getSurrogate(), answer2.getId().getSurrogate());
			// this test is unnecessary because foreign keys should have been violated, left for more insurance
			Set<Long> choicesInRelationIds = persistenceContext.newQuery("select choices_Id from Answer_choices where answer_id in (42, 666)", Long.class)
					.mapKey("choices_id", Long.class)
					.execute();
			assertThat(choicesInRelationIds).containsExactlyInAnyOrder(choice1.getId().getSurrogate(), choice2.getId().getSurrogate(), choice3.getId().getSurrogate());
		}
		
		@Test
		void update_associationRecordsMustBeUpdated_butNotTargetEntities() throws SQLException {
			EntityPersister<Answer, Identifier<Long>> answerPersister = MappingEase.entityBuilder(Answer.class, Identifier.LONG_TYPE)
					.mapKey(Answer::getId, ALREADY_ASSIGNED)
					.map(Answer::getComment)
					.mapOneToManySet(Answer::getChoices, CHOICE_MAPPING_CONFIGURATION).cascading(ASSOCIATION_ONLY)
					.build(persistenceContext);

			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();

			// we need to insert target choices because they won't be inserted by ASSOCIATION_ONLY cascade
			persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into Choice(id) values (100), (200)");

			Answer answer1 = new Answer(new PersistableIdentifier<>(42L));
			answer1.setComment("Hello world !");
			Choice choice1 = new Choice(new PersistableIdentifier<>(100L));
			Choice choice2 = new Choice(new PersistableIdentifier<>(200L));
			answer1.addChoices(choice1, choice2);

			answerPersister.insert(answer1);

			// changing values before update
			choice1.setLabel("Grenoble");
			answerPersister.update(answer1, answerPersister.select(answer1.getId()), true);

			ResultSet resultSet;
			// Checking that answer name was updated
			String answerComment = persistenceContext.newQuery("select comment from Answer where id = 42", String.class)
					.mapKey("comment", String.class)
					.singleResult()
					.execute();
			assertThat(answerComment).isEqualTo(answer1.getComment());
			// .. but not its city name
			Set<String> choiceLabels = persistenceContext.newQuery("select label from Choice where id = 100", String.class)
					.mapKey("label", String.class)
					.execute();
			assertThat(choiceLabels).containsExactlyInAnyOrder((String) null);
			
			// removing city doesn't have any effect either
			assertThat(answer1.getChoices().size()).isEqualTo(2);	// safeguard for unwanted regression on city removal, because it would totally corrupt this test
			answer1.getChoices().remove(choice1);
			assertThat(answer1.getChoices().size()).isEqualTo(1);	// safeguard for unwanted regression on city removal, because it would totally corrupt this test
			answerPersister.update(answer1, answerPersister.select(answer1.getId()), true);
			choiceLabels = persistenceContext.newQuery("select label from Choice where id = 100", String.class)
					.mapKey("label", String.class)
					.execute();
			assertThat(choiceLabels).containsExactlyInAnyOrder((String) null);
		}

		@Test
		void delete_associationRecordsMustBeDeleted_butNotTargetEntities() throws SQLException {
			EntityPersister<Answer, Identifier<Long>> answerPersister = MappingEase.entityBuilder(Answer.class, Identifier.LONG_TYPE)
					.mapKey(Answer::getId, ALREADY_ASSIGNED)
					.mapManyToManySet(Answer::getChoices, CHOICE_MAPPING_CONFIGURATION).cascading(ASSOCIATION_ONLY)
					.build(persistenceContext);

			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();

			persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into Answer(id) values (42, 666)");
			persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into Choice(id) values (100), (200), (300)");
			persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into Answer_choices(answer_Id, choices_Id)" +
					" values (42, 100), (42, 200), (666, 300)");

			Answer answer1 = new Answer(new PersistedIdentifier<>(42L));
			Choice choice1 = new Choice(new PersistedIdentifier<>(100L));
			Choice choice2 = new Choice(new PersistedIdentifier<>(200L));
			answer1.addChoices(choice1, choice2);
			Answer answer2 = new Answer(new PersistedIdentifier<>(666L));
			Choice choice3 = new Choice(new PersistedIdentifier<>(300L));
			answer2.addChoices(choice3);

			// testing deletion
			answerPersister.delete(answer1);

			// Checking that we deleted what we wanted
			Long answerCount = persistenceContext.newQuery("select count(*) as answerCount from Answer where id = 42", Long.class)
					.mapKey("answerCount", Long.class)
					.singleResult()
					.execute();
			assertThat(answerCount).isEqualTo(0);
			// with ASSOCIATION_ONLY, association table records must be deleted
			Long relationCount = persistenceContext.newQuery("select count(*) as relationCount from Answer_choices where answer_Id = 42", Long.class)
					.mapKey("relationCount", Long.class)
					.singleResult()
					.execute();
			assertThat(relationCount).isEqualTo(0);
			// ... but target entities are not deleted with ASSOCIATION_ONLY
			Set<Long> choiceIds = persistenceContext.newQuery("select id from Choice where id in (100, 200)", Long.class)
					.mapKey("id", Long.class)
					.execute();
			assertThat(choiceIds).containsExactlyInAnyOrder(100L, 200L);
			
			// but we didn't delete everything !
			Set<Long> answerIds = persistenceContext.newQuery("select id from Answer where id = 666", Long.class)
					.mapKey("id", Long.class)
					.execute();
			assertThat(answerIds).containsExactlyInAnyOrder(666L);
			choiceIds = persistenceContext.newQuery("select id from Choice where id = 300", Long.class)
					.mapKey("id", Long.class)
					.execute();
			assertThat(choiceIds).containsExactlyInAnyOrder(300L);
			
			// testing deletion of the last one
			answerPersister.delete(answer2);
			answerCount = persistenceContext.newQuery("select count(*) as answerCount from Answer where id = 666", Long.class)
					.mapKey("answerCount", Long.class)
					.singleResult()
					.execute();
			assertThat(answerCount).isEqualTo(0);
			// this test is unnecessary because foreign keys should have been violated, left for more insurance
			relationCount = persistenceContext.newQuery("select count(*) as relationCount from Answer_choices where answer_Id = 666", Long.class)
					.mapKey("relationCount", Long.class)
					.singleResult()
					.execute();
			assertThat(relationCount).isEqualTo(0);
			// target entities are not deleted with cascade All
			choiceIds = persistenceContext.newQuery("select id from Choice where id = 300", Long.class)
					.mapKey("id", Long.class)
					.execute();
			assertThat(choiceIds).containsExactlyInAnyOrder(300L);
			
		}
	}

	@Test
	void select_noRecordInAssociationTable_mustReturnEmptyCollection() throws SQLException {
		EntityPersister<Answer, Identifier<Long>> answerPersister = MappingEase.entityBuilder(Answer.class, Identifier.LONG_TYPE)
				.mapKey(Answer::getId, ALREADY_ASSIGNED)
				.mapManyToManySet(Answer::getChoices, CHOICE_MAPPING_CONFIGURATION).cascading(READ_ONLY)
				.build(persistenceContext);

		// this is a configuration safeguard, thus we ensure that configuration matches test below
		assertThat(((OptimizedUpdatePersister<Answer, Identifier<Long>>) answerPersister).getSurrogate()
				.getEntityJoinTree().getJoin("Answer_Choices0")).isNull();

		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();

		// we only register one answer without any city
		persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into Answer(id) values (42)");

		// Then : Answer must exist and have an empty city collection
		Answer loadedAnswer = answerPersister.select(new PersistedIdentifier<>(42L));
		assertThat(loadedAnswer.getChoices()).isEqualTo(null);

	}
	
	public static class Answer implements Identified<Long> {
		
		private Identifier<Long> id;
		
		private String comment;
		
		private Set<Choice> choices;
		
		private Answer() {
		}
		
		private Answer(Long id) {
			this(new PersistableIdentifier<>(id));
		}
		
		private Answer(Identifier<Long> id) {
			this.id = id;
		}
		
		public Answer(Choice choice) {
			this.choices.add(choice);
		}
		
		@Override
		public Identifier<Long> getId() {
			return id;
		}
		
		public String getComment() {
			return comment;
		}
		
		public void setComment(String comment) {
			this.comment = comment;
		}
		
		public Set<Choice> getChoices() {
			return choices;
		}
		
		public void addChoices(Collection<Choice> choices) {
			this.choices.addAll(choices);
		}
		
		public void addChoices(Choice... choices) {
			if (this.choices == null) {
				this.choices = new HashSet<>();
			}
			this.choices.addAll(Arrays.asList(choices));
		}
		
		public void setChoices(Set<Choice> choices) {
			this.choices = choices;
		}
		
	}
	
	private static class Choice implements Identified<Long> {
		
		private Identifier<Long> id;
		
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
			return "Choice{id=" + id.getSurrogate() + ", label='" + label + '\'' + '}';
		}
	}
}
