/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.tools.nodetool;

import io.airlift.command.Arguments;
import io.airlift.command.Command;
import io.airlift.command.Option;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "scrub", description = "Scrub (rebuild sstables for) one or more tables")
public class Scrub extends NodeToolCmd
{
    @Arguments(usage = "[<keyspace> <tables>...]", description = "The keyspace followed by one or many tables")
    private List<String> args = new ArrayList<>();

    @Option(title = "disable_snapshot",
            name = {"-ns", "--no-snapshot"},
            description = "Scrubbed CFs will be snapshotted first, if disableSnapshot is false. (default false)")
    private boolean disableSnapshot = false;

    @Option(title = "skip_corrupted",
            name = {"-s", "--skip-corrupted"},
            description = "Skip corrupted partitions even when scrubbing counter tables. (default false)")
    private boolean skipCorrupted = false;

    @Option(title = "no_validate",
                   name = {"-n", "--no-validate"},
                   description = "Do not validate columns using column validator")
    private boolean noValidation = false;

    @Option(title = "sstables",
                    name = {"-f", "--sstables"},
                    description = "A comma delimited list of sstables to scrub")
    private String sstables = null;

    @Option(title = "ignore_overlap",
                    name = {"-i", "--ignore-overlap"},
                    description = "ignore sstable timetamp overlap when purging tombstones (WARNING: Could bring columns back from the dead)")
    private boolean ignoreOverlap = false;

    @Override
    public void execute(NodeProbe probe)
    {
        // NOTE: currently this requires us to include both the keyspace and the columnfamily names, even though they're
        // this information can be obtained from the sstable name. It would requires more code down stream to change.
        List<String> keyspaces = parseOptionalKeyspace(args, probe);
        String[] cfnames = parseOptionalColumnFamilies(args);

        for (String keyspace : keyspaces)
        {
            try
            {
                probe.scrub(System.out, disableSnapshot, skipCorrupted, !noValidation, sstables, keyspace, ignoreOverlap, cfnames);
            } catch (IllegalArgumentException e)
            {
                throw e;
            } catch (Exception e)
            {
                throw new RuntimeException("Error occurred during scrubbing", e);
            }
        }
    }
}
