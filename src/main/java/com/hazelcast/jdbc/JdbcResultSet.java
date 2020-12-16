/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jdbc;

import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

public class JdbcResultSet implements ResultSet {

    static final JdbcResultSet EMPTY = new EmptyJdbcResultSet();

    private final SqlResult sqlResult;
    private final Iterator<SqlRow> iterator;
    private SqlRow currentCursorPosition;

    /** Whether the last read column was null. */
    private boolean wasNull;

    /** Whether the result set is closed. */
    private boolean closed;

    /**
     * Whether the result set is in the closing process.
     * Needed to break the loop (ResultSet -> Statement -> ResultSet)
     * that may occur when {@link Statement#isCloseOnCompletion()} is set to true.
     */
    private boolean closing;

    /** Parent statement */
    private final JdbcStatement statement;

    /** Fetch direction. */
    private int fetchDirection;
    /** Fetch size */
    private int fetchSize;


    JdbcResultSet(SqlResult sqlResult, JdbcStatement statement) {
        this.sqlResult = sqlResult;
        iterator = sqlResult.iterator();
        this.statement = statement;
    }

    @Override
    public boolean next() throws SQLException {
        checkClosed();
        if (iterator.hasNext()) {
            currentCursorPosition = iterator.next();
            return true;
        }
        return false;
    }

    @Override
    public void close() throws SQLException {
        if (!isClosed()) {
            if (closing) {
                return;
            }
            closing = true;
            sqlResult.close();
            statement.tryCloseOnCompletion();
            closed = true;
        }
    }

    @Override
    public boolean wasNull() throws SQLException {
        checkClosed();
        return wasNull;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        return get(columnIndex);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return get(columnIndex);
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return get(columnIndex);
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return get(columnIndex);
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return get(columnIndex);
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return get(columnIndex);
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return get(columnIndex);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return get(columnIndex);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        throw JdbcUtils.unsupported("BigDecimal with scale is not supported");
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        return get(columnIndex);
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return get(columnIndex);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return get(columnIndex);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return get(columnIndex);
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw JdbcUtils.unsupported("Ascii Stream is not supported");
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw JdbcUtils.unsupported("Unicode Stream is not supported");
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw JdbcUtils.unsupported("Binary Stream is not supported");
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return get(columnLabel);
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return get(columnLabel);
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return get(columnLabel);
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return get(columnLabel);
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return get(columnLabel);
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return get(columnLabel);
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return get(columnLabel);
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return get(columnLabel);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        throw JdbcUtils.unsupported("BigDecimal with scale is not supported");
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return get(columnLabel);
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return get(columnLabel);
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return get(columnLabel);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return get(columnLabel);
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw JdbcUtils.unsupported("Ascii Stream is not supported");
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw JdbcUtils.unsupported("Unicode Stream is not supported");
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        throw JdbcUtils.unsupported("Binary Stream is not supported");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkClosed();
    }

    @Override
    public String getCursorName() throws SQLException {
        throw JdbcUtils.unsupported("Cursor Name is not supported");
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        throw JdbcUtils.unsupported("ResultSetMetaData not supported");
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return get(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return get(columnLabel);
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        checkClosed();
        int index = sqlResult.getRowMetadata().findColumn(columnLabel);
        if (index == -1) {
            throw new SQLException("ResultSet does not contain column \"" + columnLabel + "\"");
        }
        return index;
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw JdbcUtils.unsupported("Character Stream is not supported");
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        throw JdbcUtils.unsupported("Character Stream is not supported");
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return get(columnIndex);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return get(columnLabel);
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        throw JdbcUtils.unsupported("Method not supported");
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        throw JdbcUtils.unsupported("Method not supported");
    }

    @Override
    public boolean isFirst() throws SQLException {
        throw JdbcUtils.unsupported("Method not supported");
    }

    @Override
    public boolean isLast() throws SQLException {
        throw JdbcUtils.unsupported("Method not supported");
    }

    @Override
    public void beforeFirst() throws SQLException {
        throw JdbcUtils.unsupported("Method not supported");
    }

    @Override
    public void afterLast() throws SQLException {
        throw JdbcUtils.unsupported("Method not supported");
    }

    @Override
    public boolean first() throws SQLException {
        throw JdbcUtils.unsupported("Method not supported");
    }

    @Override
    public boolean last() throws SQLException {
        throw JdbcUtils.unsupported("Method not supported");
    }

    @Override
    public int getRow() throws SQLException {
        throw JdbcUtils.unsupported("Method not supported");
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        throw JdbcUtils.unsupported("Method not supported");
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        throw JdbcUtils.unsupported("Method not supported");
    }

    @Override
    public boolean previous() throws SQLException {
        throw JdbcUtils.unsupported("Method not supported");
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkClosed();
        switch (direction) {
            case ResultSet.FETCH_FORWARD:
            case ResultSet.FETCH_REVERSE:
            case ResultSet.FETCH_UNKNOWN:
                this.fetchDirection = direction;
                break;
            default:
                throw new SQLException("Invalid fetch direction value: " + direction);
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkClosed();
        return fetchDirection;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        checkClosed();
        fetchSize = rows;
    }

    @Override
    public int getFetchSize() throws SQLException {
        checkClosed();
        return fetchSize;
    }

    @Override
    public int getType() throws SQLException {
        checkClosed();
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() throws SQLException {
        checkClosed();
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        throw JdbcUtils.unsupported("Method not supported");
    }

    @Override
    public boolean rowInserted() throws SQLException {
        throw JdbcUtils.unsupported("Method not supported");
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        throw JdbcUtils.unsupported("Method not supported");
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void insertRow() throws SQLException {
        throw JdbcUtils.unsupported("Method not supported");
    }

    @Override
    public void updateRow() throws SQLException {
        throw JdbcUtils.unsupported("Method not supported");
    }

    @Override
    public void deleteRow() throws SQLException {
        throw JdbcUtils.unsupported("Method not supported");
    }

    @Override
    public void refreshRow() throws SQLException {
        throw JdbcUtils.unsupported("Method not supported");
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw JdbcUtils.unsupported("Method not supported");
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw JdbcUtils.unsupported("Method not supported");
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw JdbcUtils.unsupported("Method not supported");
    }

    @Override
    public Statement getStatement() throws SQLException {
        checkClosed();
        return statement;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return get(columnIndex);
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw JdbcUtils.unsupported("Ref is not supported");
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        throw JdbcUtils.unsupported("Blob is not supported");
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        throw JdbcUtils.unsupported("Clob is not supported");
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        throw JdbcUtils.unsupported("Array is not supported");
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return get(columnLabel);
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw JdbcUtils.unsupported("Ref is not supported");
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        throw JdbcUtils.unsupported("Blob is not supported");
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        throw JdbcUtils.unsupported("Clob is not supported");
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        throw JdbcUtils.unsupported("Array is not supported");
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        throw JdbcUtils.unsupported("Date is not supported");
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        throw JdbcUtils.unsupported("Date is not supported");
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        throw JdbcUtils.unsupported("Time is not supported");
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        throw JdbcUtils.unsupported("Time is not supported");
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        throw JdbcUtils.unsupported("Timestamp is not supported");
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        throw JdbcUtils.unsupported("Timestamp is not supported");
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        throw JdbcUtils.unsupported("URL is not supported");
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        throw JdbcUtils.unsupported("URL is not supported");
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw JdbcUtils.unsupported("RowId is not supported");
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw JdbcUtils.unsupported("RowId is not supported");
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public int getHoldability() throws SQLException {
        checkClosed();
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return get(columnIndex);
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return get(columnLabel);
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw JdbcUtils.unsupported("NCharacter stream is not supported");
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw JdbcUtils.unsupported("NCharacter stream is not supported");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");

    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw JdbcUtils.unsupported("Update ResultSet not supported");
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return get(columnIndex);
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return get(columnLabel);
    }

    @Override
    public <T> T unwrap(Class<T> iface) {
        return JdbcUtils.unwrap(this, iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return JdbcUtils.isWrapperFor(this, iface);
    }

    private <T> T get(String columnLabel) throws SQLException {
        checkClosed();
        return getByIndex(findColumn(columnLabel));
    }

    private <T> T get(int columnIndex) throws SQLException {
        checkClosed();
        if (sqlResult.getRowMetadata().getColumnCount() < columnIndex) {
            throw new SQLException("ResultSet does not contain column with index " + columnIndex);
        }
        return getByIndex(columnIndex - 1);
    }

    private <T> T getByIndex(int columnIndex) {
        T result = currentCursorPosition.getObject(columnIndex);
        if (result == null) {
            wasNull = true;
        }
        return result;
    }

    private void checkClosed() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Result set is closed");
        }
    }

    /**
     * ResultSet with empty results
     */
    private static class EmptyJdbcResultSet extends JdbcResultSet {
        EmptyJdbcResultSet() {
            super(new SqlResult() {
                @SuppressWarnings("ConstantConditions")
                @Override
                public SqlRowMetadata getRowMetadata() {
                    return null;
                }
                @Override
                public Iterator<SqlRow> iterator() {
                    return Collections.emptyIterator();
                }
                @Override
                public long updateCount() {
                    return -1;
                }
                @Override
                public void close() {
                }
            }, null);
        }
    }
}
