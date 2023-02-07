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

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.impl.AbstractSqlResult;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.ResultIterator;

/**
 * Helper class for returning ResultSets with in-driver generated rows. Doesn't require to be explicitly closed as
 * it holds no resources.
 */
public class FixedRowsSqlResult extends AbstractSqlResult {

    private final List<SqlRow> rows;
    private final QueryId queryId = new QueryId();
    private final SqlRowMetadata sqlRowMetadata;

    FixedRowsSqlResult(SqlRowMetadata sqlRowMetadata, List<SqlRow> rows) {
        this.rows = rows;
        this.sqlRowMetadata = sqlRowMetadata;
    }

    @Override
    public SqlRowMetadata getRowMetadata() {
        return this.sqlRowMetadata;
    }

    @Override
    public long updateCount() {
        return 0;
    }

    @Override
    public QueryId getQueryId() {
        return this.queryId;
    }

    @Override
    public boolean isInfiniteRows() {
        return false;
    }

    @Override
    public ResultIterator<SqlRow> iterator() {
        return new FixedRowsSqlResultIterator(this.rows.iterator());
    }

    @Override
    public void close(QueryException exception) { }

    private static final class FixedRowsSqlResultIterator implements ResultIterator<SqlRow> {

        private final Iterator<SqlRow> iterator;

        FixedRowsSqlResultIterator(Iterator<SqlRow> arg0) {
            this.iterator = arg0;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public SqlRow next() {
            return iterator.next();
        }

        @Override
        public HasNextResult hasNext(long timeout, TimeUnit timeUnit) {
            return iterator.hasNext() ? HasNextResult.YES : HasNextResult.DONE;
        }
    }
}
