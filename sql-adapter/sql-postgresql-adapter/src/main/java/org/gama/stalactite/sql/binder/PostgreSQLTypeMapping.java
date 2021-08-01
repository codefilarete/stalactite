package org.gama.stalactite.sql.binder;

import java.io.InputStream;
import java.sql.Blob;

import org.gama.stalactite.sql.ddl.DefaultTypeMapping;

public class PostgreSQLTypeMapping extends DefaultTypeMapping {

    public PostgreSQLTypeMapping() {
        super();
        // because PostgreSQL doesn't support "blob" type
        put(InputStream.class, "bytea");
        put(byte[].class, "bytea");
        // because Blob are supposed to be store outside table we used the PostgreSQL type for it : OID refers to an external LargeObject, taken into account by PostgreSQL binders
        // see https://jdbc.postgresql.org/documentation/head/binary-data.html#binary-data-example
        put(Blob.class, "OID");
        put(double.class, "float8");
        put(Double.class, "float8");
        put(float.class, "float4");
        put(Float.class, "float4");
    }
}