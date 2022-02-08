package org.gama.stalactite.persistence.sql.ddl;

import java.util.*;

import org.codefilarete.tool.collection.Collections;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.gama.stalactite.persistence.structure.ForeignKey;
import org.gama.stalactite.persistence.structure.Index;
import org.gama.stalactite.persistence.structure.Table;

/**
 * A class to collect multiple DDL sources. Main purpose is multiple table creation scripts so they can be played against a database to deploy
 * a schema. Use cases are tests and installation of a new application.
 * 
 * @author Guillaume Mary
 * @see org.gama.stalactite.persistence.engine.DDLDeployer
 */
public class DDLGenerator implements DDLProvider {
	
	private Set<Table> tables = new KeepOrderSet<>();
	
	private Set<DDLProvider> ddlProviders = new KeepOrderSet<>();
	
	private DDLTableGenerator ddlTableGenerator;
	
	/**
	 * Simple generator where column SQL types are took from given Java-SQL type mapping.
	 * Name of tables and columns are given by a default {@link org.gama.stalactite.query.builder.DMLNameProvider}
	 * 
	 * @param sqlTypeRegistry the mapping to be used to get SQL type of columns
	 */
	public DDLGenerator(SqlTypeRegistry sqlTypeRegistry) {
		this.ddlTableGenerator = new DDLTableGenerator(sqlTypeRegistry);
	}
	
	public DDLGenerator(DDLTableGenerator ddlTableGenerator) {
		this.ddlTableGenerator = ddlTableGenerator;
	}
	
	public DDLTableGenerator getDdlTableGenerator() {
		return ddlTableGenerator;
	}
	
	public void setDdlTableGenerator(DDLTableGenerator ddlTableGenerator) {
		this.ddlTableGenerator = ddlTableGenerator;
	}
	
	public void setTables(Set<Table> tables) {
		this.tables = tables;
	}
	
	public void addTables(Collection<Table> tables) {
		this.tables.addAll(tables);
	}
	
	public void addTables(Table table, Table ... tables) {
		this.tables.add(table);
		this.tables.addAll(Arrays.asList(tables));
	}
	
	public void setDDLGenerators(Set<DDLProvider> ddlParticipants) {
		this.ddlProviders = ddlParticipants;
	}
	
	public void addDDLGenerators(DDLProvider ddlGenerator, DDLProvider... ddlGenerators) {
		this.ddlProviders.add(ddlGenerator);
		this.ddlProviders.addAll(Arrays.asList(ddlGenerators));
	}
	
	@Override
	public List<String> getCreationScripts() {
		// DDLParticipants is supposed to be post treatments (creation of sequences, triggers, ...)
		return Collections.cat(generateTableCreationScripts(), generateDDLParticipantsCreationScripts());
	}
	
	protected List<String> generateDDLParticipantsCreationScripts() {
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
		
		for (Table table : tables) {
			tableCreationScripts.add(generateCreationScript(table));
			foreignKeysCreationScripts.addAll(getForeignKeyCreationScripts(table));
			indexesCreationScripts.addAll(generateIndexCreationScripts(table));
		}
		
		// foreign keys must be after table scripts, index is fine tuning
		return Collections.cat(tableCreationScripts, foreignKeysCreationScripts, indexesCreationScripts);
	}
	
	protected String generateCreationScript(Table table) {
		return this.ddlTableGenerator.generateCreateTable(table);
	}
	
	protected List<String> generateIndexCreationScripts(Table<?> table) {
		List<String> indexesCreationScripts = new ArrayList<>();
		for (Index index : table.getIndexes()) {
			indexesCreationScripts.add(generateCreationScript(index));
		}
		return indexesCreationScripts;
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
	
	@Override
	public List<String> getDropScripts() {
		// DDLParticipants is supposed to be post treatments (creation of sequences, triggers, ...)
		return Collections.cat(generateTableDropScripts(), generateDDLParticipantsDropScripts());
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
}
