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

package org.apache.ignite.resources;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates a field or a setter method for injection of {@link org.apache.ignite.compute.ComputeJobContext} instance.
 * It can be injected into grid jobs only.
 * <p>
 * Job context can be injected into instances of following classes:
 * <ul>
 * <li>{@link org.apache.ignite.compute.ComputeJob}</li>
 * </ul>
 * <p>
 * Here is how injection would typically happen:
 * <pre name="code" class="java">
 * public class MyGridJob implements ComputeJob {
 *      ...
 *      &#64;IgniteJobContextResource
 *      private ComputeJobContext jobCtx;
 *      ...
 *  }
 * </pre>
 * or
 * <pre name="code" class="java">
 * public class MyGridJob implements ComputeJob {
 *     ...
 *     private ComputeJobContext jobCtx;
 *     ...
 *     &#64;JobContextResource
 *     public void setJobContext(ComputeJobContext jobCtx) {
 *          this.jobCtx = jobCtx;
 *     }
 *     ...
 * }
 * </pre>
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD})
public @interface JobContextResource {
    // No-op.
}
