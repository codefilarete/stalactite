package org.stalactite.persistence.sql.ddl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.stalactite.lang.collection.Collections;
import org.stalactite.persistence.sql.Dialect;
import org.stalactite.persistence.structure.Table;
import org.stalactite.persistence.structure.Table.ForeignKey;
import org.stalactite.persistence.structure.Table.Index;

/**
 * @author mary
 */
public class DDLGenerator implements DDLParticipant {
	
	private final Collection<Table> tables;
	
	private final DDLTableGenerator ddlTableGenerator;
	
	private final Set<DDLParticipant> ddlParticipants = new LinkedHashSet<>();
	
	public DDLGenerator(List<Table> tablesToCreate, Dialect dialect) {
		this(tablesToCreate, dialect.getJavaTypeToSqlTypeMapping());
	}
	
	public DDLGenerator(Collection<Table> tablesToCreate, JavaTypeToSqlTypeMapping typeMapping) {
		this.tables = tablesToCreate;
		this.ddlTableGenerator = newDDLTableGenerator(typeMapping);
	}
	
	protected DDLTableGenerator newDDLTableGenerator(JavaTypeToSqlTypeMapping typeMapping) {
		return new DDLTableGenerator(typeMapping);
	}
	
	public void add(DDLParticipant ddlParticipant) {
		this.ddlParticipants.add(ddlParticipant);
	}
	
	@Override
	public List<String> getCreationScripts() {
		// DDLParticipants is supposed to be post treatments (creation of sequences, triggers, ...)
		return Collections.cat(generateTableScripts(), generateDDLParticipantsScripts());
	}
	
	protected List<String> generateDDLParticipantsScripts() {
		List<String> participantsScripts = new ArrayList<>();
		for (DDLParticipant ddlParticipant : ddlParticipants) {
			participantsScripts.addAll(ddlParticipant.getCreationScripts());
		}
		return participantsScripts;
	}
	
	protected List<String> generateTableScripts() {
		List<String> tableCreationScripts = new ArrayList<>();
		List<String> foreignKeysCreationScripts = new ArrayList<>();
		List<String> indexesCreationScripts = new ArrayList<>();
		
		for (Table table : tables) {
			tableCreationScripts.add(this.ddlTableGenerator.generateCreateTable(table));
			foreignKeysCreationScripts.addAll(getForeignKeyScripts(table));
			indexesCreationScripts.addAll(generateIndexScripts(table));
		}
		
		// foreign keys must be after table scripts, index is fine tuning
		return Collections.cat(tableCreationScripts, foreignKeysCreationScripts, indexesCreationScripts);
	}
	
	protected List<String> generateIndexScripts(Table table) {
		List<String> indexesCreationScripts = new ArrayList<>();
		for (Index index : table.getIndexes()) {
			indexesCreationScripts.add(this.ddlTableGenerator.generateCreateIndex(index));
		}
		return indexesCreationScripts;
	}
	
	protected List<String> getForeignKeyScripts(Table table) {
		List<String> foreignKeysCreationScripts = new ArrayList<>();
		for (ForeignKey foreignKey : table.getForeignKeys()) {
			foreignKeysCreationScripts.add(this.ddlTableGenerator.generateCreateForeignKey(foreignKey));
		}
		return foreignKeysCreationScripts;
	}
}
