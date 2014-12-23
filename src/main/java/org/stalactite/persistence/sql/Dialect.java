package org.stalactite.persistence.sql;

import org.stalactite.persistence.sql.ddl.DDLGenerator;
import org.stalactite.persistence.structure.Table.Column;

/**
 * @author mary
 */
public abstract class Dialect {
	
	public abstract String getTypeName(Column column);
	
	public abstract DDLGenerator getDDlGenerator();
	
}
