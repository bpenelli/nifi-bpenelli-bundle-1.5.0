/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.bpenelli.nifi.processors.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos;
import org.apache.nifi.hbase.HBaseClientService;
import org.apache.nifi.hbase.HBase_1_1_2_ClientService;
import org.apache.nifi.hbase.scan.ResultCell;
import org.apache.nifi.hbase.scan.ResultHandler;

import java.io.IOException;
import java.util.Arrays;

public class HBaseDeleteRowHandler implements ResultHandler {

    private final HBaseResults results = new HBaseResults();
    private HBaseClientService hbaseService;
    private String tablename;
    private int rowCount = 0;
    public IOException ioException;
    public Configuration config;

    private HBaseDeleteRowHandler() {}

    public HBaseDeleteRowHandler(HBaseClientService hbaseService, String tablename) {
        this.hbaseService = hbaseService;
        this.tablename = tablename;
    }

    @Override
    public void handle(byte[] resultRow, ResultCell[] resultCells) {
        try {
            this.hbaseService.delete(tablename, resultRow);
        } catch (IOException e) {
            this.ioException = e;
        }
    }
}