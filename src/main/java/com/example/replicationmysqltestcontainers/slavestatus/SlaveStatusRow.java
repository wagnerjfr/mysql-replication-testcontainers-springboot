package com.example.replicationmysqltestcontainers.slavestatus;

import java.sql.ResultSet;
import java.sql.SQLException;

public record SlaveStatusRow(String masterHost, String masterLogFile, String slaveIORunning, String slaveSQLRunning,
                             String lastIOErrno, String lastIOError, String lastSQLErrno, String lastSQLError) {

    public static SlaveStatusRow create(ResultSet rs) throws SQLException {
        String masterHost = rs.getString(Variable.Master_Host.name());
        String masterLogFile = rs.getString(Variable.Master_Log_File.name());
        String slaveIORunning = rs.getString(Variable.Slave_IO_Running.name());
        String slaveSQLRunning = rs.getString(Variable.Slave_SQL_Running.name());
        String lastIOErrno = rs.getString(Variable.Last_IO_Errno.name());
        String lastIOError = rs.getString(Variable.Last_IO_Error.name());
        String lastSQLErrno = rs.getString(Variable.Last_SQL_Errno.name());
        String lastSQLError = rs.getString(Variable.Last_SQL_Error.name());

        return new SlaveStatusRow(masterHost, masterLogFile, slaveIORunning, slaveSQLRunning, lastIOErrno, lastIOError, lastSQLErrno, lastSQLError);
    }
}
