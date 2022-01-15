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

package org.apache.ignite.cache.query;

import javax.cache.Cache;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.EventType;

/**
 * A Cache continuous query entry event.
 *
 * @param <K> The type of key.
 * @param <V> The type of value.
 */
public abstract class CacheQueryEntryEvent<K, V> extends CacheEntryEvent<K, V> {
    /**
     * Constructs a cache entry event from a given cache as source.
     *
     * @param src The cache that originated the event.
     * @param evtType Event type.
     */
    public CacheQueryEntryEvent(Cache src, EventType evtType) {
        super(src, evtType);
    }

    /**
     * Each cache update increases partition counter. The same cache updates have on the same value of counter
     * on primary and backup nodes. This value can be useful to communicate with external applications.
     *
     * @return Value of counter for this event.
     */
    public abstract long getPartitionUpdateCounter();
}