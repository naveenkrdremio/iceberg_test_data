/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dremio.iceberg.s3;

import java.time.Duration;

import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;

/**
 * Apache HTTP Connection Utility that supports aws sdk 2.X
 */
public final class ApacheHttpConnectionUtil {

  private ApacheHttpConnectionUtil() {
  }

  public static SdkHttpClient.Builder<?> initConnectionSettings() {
    final ApacheHttpClient.Builder httpBuilder = ApacheHttpClient.builder();
    httpBuilder.maxConnections(15000);
    httpBuilder.connectionTimeout(
            Duration.ofSeconds(10000));
    httpBuilder.socketTimeout(
            Duration.ofSeconds(10000));

    return httpBuilder;
  }
}
