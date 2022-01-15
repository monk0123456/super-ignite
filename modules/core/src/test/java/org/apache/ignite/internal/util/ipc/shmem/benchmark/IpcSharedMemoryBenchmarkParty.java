/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util.ipc.shmem.benchmark;

import java.io.File;

/**
 *
 */
interface IpcSharedMemoryBenchmarkParty {
    /** */
    public static final int DFLT_SPACE_SIZE = 512 * 1024;

    /** */
    public static final int DFLT_BUF_SIZE = 8 * 1024;

    /** */
    public static final String DFLT_TOKEN =
        new File(System.getProperty("java.io.tmpdir"), "benchmark").getAbsolutePath();
}