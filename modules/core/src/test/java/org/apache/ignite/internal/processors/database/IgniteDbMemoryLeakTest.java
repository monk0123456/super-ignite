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

package org.apache.ignite.internal.processors.database;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.IgniteEx;

/**
 *
 */
public class IgniteDbMemoryLeakTest extends IgniteDbMemoryLeakAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteCache<Object, Object> cache(IgniteEx ig) {
        return ig.cache("non-primitive");
    }

    /** {@inheritDoc} */
    @Override protected Object key() {
        return new DbKey(nextInt(200_000));
    }

    /** {@inheritDoc} */
    @Override protected Object value(Object key) {
        return new DbValue(((DbKey)key).val, "test-value-" + nextInt(200), nextInt(500));
    }

    /** {@inheritDoc} */
    @Override protected long pagesMax() {
        return 20_000;
    }
}