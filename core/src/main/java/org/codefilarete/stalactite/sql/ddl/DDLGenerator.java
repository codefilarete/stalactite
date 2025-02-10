package org.codefilarete.stalactite.sql.ddl;

import java.util.*;

import org.codefilarete.stalactite.sql.DMLNameProviderFactory;
import org.codefilarete.stalactite.sql.ddl.structure.Sequence;
import org.codefilarete.stalactite.sql.ddl.structure.UniqueConstraint;
import org.codefilarete.tool.collection.Collections;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.stalactite.sql.ddl.structure.ForeignKey;
import org.codefilarete.stalactite.sql.ddl.structure.Index;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * A class to collect multiple DDL sources. Main purpose is multiple table creation scripts so they can be played against a database to deploy
 * a schema. Use cases are tests and installation of a new application.
 * 
 * @author Guillaume Mary
 * @see DDLDeployer
 */
public class DDLGenerator implements DDLProvider {
	
	private Set<Table<?>> tables = new KeepOrderSet<>();
	
	private Set<Sequence> sequences = new KeepOrderSet<>();
	
	private Set<DDLProvider> ddlProviders = new KeepOrderSet<>();
	
	private final DDLTableGenerator ddlTableGenerator;
	
	private final DDLSequenceGenerator ddlSequenceGenerator;
	
	/**
	 * Simple generator where column SQL types are took from given Java-SQL type mapping.
	 * Name of tables and columns are given by a default {@link org.codefilarete.stalactite.query.builder.DMLNameProvider}
	 * 
	 * @param sqlTypeRegistry the mapping to be used to get SQL type of columns
	 */
	public DDLGenerator(SqlTypeRegistry sqlTypeRegistry, DMLNameProviderFactory dmlNameProviderFactory) {
		this.ddlTableGenerator = new DDLTableGenerator(sqlTypeRegistry, dmlNameProviderFactory);
		this.ddlSequenceGenerator = new DDLSequenceGenerator(dmlNameProviderFactory);
	}
	
	public DDLGenerator(DDLTableGenerator ddlTableGenerator, DDLSequenceGenerator ddlSequenceGenerator) {
		this.ddlTableGenerator = ddlTableGenerator;
		this.ddlSequenceGenerator = ddlSequenceGenerator;
	}
	
	public DDLTableGenerator getDdlTableGenerator() {
		return ddlTableGenerator;
	}
	
	public void setTables(Set<? extends Table<?>> tables) {
		this.tables = (Set<Table<?>>) tables;
	}
	
	public void addTables(Collection<? extends Table<?>> tables) {
		this.tables.addAll(tables);
	}
	
	public void addTables(Table<?> table, Table<?>... tables) {
		this.tables.add(table);
		this.tables.addAll(Arrays.asList(tables));
	}
	
	public void setSequences(Set<Sequence> sequences) {
		this.sequences = sequences;
	}
	
	public void addSequences(Sequence sequence, Sequence... sequences) {
		this.sequences.add(sequence);
		this.sequences.addAll(Arrays.asList(sequences));
	}
	
	public void addSequences(Collection<? extends Sequence> sequences) {
		this.sequences.addAll(sequences);
	}
	
	public void setDDLProviders(Set<DDLProvider> ddlParticipants) {
		this.ddlProviders = ddlParticipants;
	}
	
	public void addDDLProviders(DDLProvider ddlParticipant, DDLProvider... ddlParticipants) {
		this.ddlProviders.add(ddlParticipant);
		this.ddlProviders.addAll(Arrays.asList(ddlParticipants));
	}
	
	public void addDDLProviders(Collection<? extends DDLProvider> ddlParticipants) {
		this.ddlProviders.addAll(ddlParticipants);
	}
	
	@Override
	public List<String> getCreationScripts() {
		// DDLParticipants is supposed to be post treatments (creation of sequences, triggers, ...)
		return Collections.cat(generateTableCreationScripts(), generateSequencesCreationScripts(), generateDDLProvidersCreationScripts());
	}
	
	protected List<String> generateDDLProvidersCreationScripts() {
		List<String> participantsScripts = new ArrayList<>();
		for (DDLProvider ddlProvider : ddlProviders) {
			participantsScripts.addAll(ddlProvider.getCreationScripts());
		}
		return participantsScripts;
	}
	
	protected List<String> generateTableCreationScripts() {
		List<String> tableCreationScripts = new ArrayList<>();
		List<String> foreignKeysCreationScripts = new ArrayList<>();
		List<String> indexesCreationScripts = new ArrayList<>();
		List<String> uniqueConstraintsCreationScripts = new ArrayList<>();
		
		for (Table table : tables) {
			tableCreationScripts.add(generateCreationScript(table));
			foreignKeysCreationScripts.addAll(getForeignKeyCreationScripts(table));
			indexesCreationScripts.addAll(generateIndexCreationScripts(table));
			uniqueConstraintsCreationScripts.addAll(generateUniqueConstraintCreationScripts(table));
		}
		
		// Foreign keys must be after constraints and indexes because some databases require foreign keys to have index on referenced column (MariaDB)
		// Since that's only a matter for some databases and other don't care, we set it here, not in a vendor-dedicated DDLGenerator
		return Collections.cat(tableCreationScripts, uniqueConstraintsCreationScripts, indexesCreationScripts, foreignKeysCreationScripts);
	}
	
	protected String generateCreationScript(Table table) {
		return this.ddlTableGenerator.generateCreateTable(table);
	}
	
	protected List<String> generateUniqueConstraintCreationScripts(Table<?> table) {
		List<String> uniqueConstraintCreationScripts = new ArrayList<>();
		for (UniqueConstraint uniqueConstraint : table.getUniqueConstraints()) {
			uniqueConstraintCreationScripts.add(generateCreationScript(uniqueConstraint));
		}
		return uniqueConstraintCreationScripts;
	}
	
	protected List<String> generateIndexCreationScripts(Table<?> table) {
		List<String> indexesCreationScripts = new ArrayList<>();
		for (Index index : table.getIndexes()) {
			indexesCreationScripts.add(generateCreationScript(index));
		}
		return indexesCreationScripts;
	}
	
	protected String generateCreationScript(UniqueConstraint uniqueConstraint) {
		return this.ddlTableGenerator.generateCreateUniqueConstraint(uniqueConstraint);
	}
	
	protected String generateCreationScript(Index index) {
		return this.ddlTableGenerator.generateCreateIndex(index);
	}
	
	protected List<String> getForeignKeyCreationScripts(Table<?> table) {
		List<String> foreignKeysCreationScripts = new ArrayList<>();
		for (ForeignKey foreignKey : table.getForeignKeys()) {
			foreignKeysCreationScripts.add(generateCreationScript(foreignKey));
		}
		return foreignKeysCreationScripts;
	}
	
	protected String generateCreationScript(ForeignKey foreignKey) {
		return this.ddlTableGenerator.generateCreateForeignKey(foreignKey);
	}
	
	protected List<String> generateSequencesCreationScripts() {
		List<String> sequenceCreationScripts = new ArrayList<>();
		for (Sequence sequence : sequences) {
			sequenceCreationScripts.add(ddlSequenceGenerator.generateCreateSequence(sequence));
		}
		return sequenceCreationScripts;
	}
	
	@Override
	public List<String> getDropScripts() {
		// DDLParticipants is supposed to be post treatments (creation of sequences, triggers, ...)
		return Collections.cat(generateTableDropScripts(), generateSequencesDropScripts(), generateDDLParticipantsDropScripts());
	}
	
	protected List<String> generateTableDropScripts() {
		List<String> tableCreationScripts = new ArrayList<>();
		
		for (Table table : tables) {
			tableCreationScripts.add(this.ddlTableGenerator.generateDropTable(table));
		}
		
		// foreign keys must be after table scripts, index is fine tuning
		return tableCreationScripts;
	}
	
	protected List<String> generateDDLParticipantsDropScripts() {
		List<String> participantsScripts = new ArrayList<>();
		for (DDLProvider ddlProvider : ddlProviders) {
			List<String> dropScripts = ddlProvider.getDropScripts();
			if (!Collections.isEmpty(dropScripts)) {
				participantsScripts.addAll(dropScripts);
			}
		}
		return participantsScripts;
	}
	
	protected List<String> generateSequencesDropScripts() {
		List<String> sequenceCreationScripts = new ArrayList<>();
		for (Sequence sequence : sequences) {
			sequenceCreationScripts.add(ddlSequenceGenerator.generateDropSequence(sequence));
		}
		return sequenceCreationScripts;
	}
}
