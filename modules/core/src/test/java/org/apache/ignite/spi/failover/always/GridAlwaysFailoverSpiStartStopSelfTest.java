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

package org.apache.ignite.spi.failover.always;

import org.apache.ignite.spi.GridSpiStartStopAbstractTest;
import org.apache.ignite.spi.failover.FailoverSpi;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;

/**
 * Always-failover SPI start-stop test.
 */
@GridSpiTest(spi = AlwaysFailoverSpi.class, group = "Failover SPI")
public class GridAlwaysFailoverSpiStartStopSelfTest extends GridSpiStartStopAbstractTest<FailoverSpi> {
    // No-op.
}