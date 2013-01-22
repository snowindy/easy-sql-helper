package com.github.snowindy.sql;

import java.sql.Connection;

/**
 * Inits connection (E.g. fills tmp list and such) and returns it.
 * @author esapozhnikov
 *
 */
public interface ConnInitter {
    public Connection initConnection() throws Exception;
}
