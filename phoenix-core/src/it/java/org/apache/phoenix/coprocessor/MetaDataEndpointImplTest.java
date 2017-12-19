/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.coprocessor;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MetaDataEndpointImplTest {

    @Test
    public void testExceededIndexQuota() throws Exception {
        PTable parentTable = mock(PTable.class);
        List<PTable> indexes = Lists.newArrayList(mock(PTable.class), mock(PTable.class));
        when(parentTable.getIndexes()).thenReturn(indexes);
        Configuration configuration = new Configuration();
        assertFalse(MetaDataEndpointImpl.execeededIndexQuota(PTableType.INDEX, parentTable, configuration));
        configuration.setInt(QueryServices.MAX_INDEXES_PER_TABLE, 1);
        assertTrue(MetaDataEndpointImpl.execeededIndexQuota(PTableType.INDEX, parentTable, configuration));
    }
}