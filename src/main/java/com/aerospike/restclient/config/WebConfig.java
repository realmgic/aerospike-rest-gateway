/*
 * Copyright 2022 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

/*
 * Copyright 2022 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.restclient.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.ByteArrayHttpMessageConverter;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.util.List;

@Configuration
public class WebConfig implements WebMvcConfigurer {

    @Value("${aerospike.restclient.cors.allowed-origins:*}")
    private String corsAllowedOrigins;

    @Value("${aerospike.restclient.cors.allowed-methods:*}")
    private String corsAllowedMethods;

    @Value("${aerospike.restclient.cors.allowed-headers:*}")
    private String corsAllowedHeaders;

    @Override
    public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
		/*
			Put our converters first in line.
			StringHttpMessageConverter must be first meaning it should be the last to add at index 0.
		 */
        converters.add(0, new MsgPackConverter());
        converters.add(0, new JSONMessageConverter());
        converters.add(0, new StringHttpMessageConverter());
        converters.add(0, new ByteArrayHttpMessageConverter());
    }

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/**")
                .allowedOriginPatterns(splitCors(corsAllowedOrigins))
                .allowedMethods(splitCors(corsAllowedMethods))
                .allowedHeaders(splitCors(corsAllowedHeaders));
    }

    private static String[] splitCors(String value) {
        if (value == null || value.trim().isEmpty()) {
            return new String[]{};
        }
        return value.split("\\s*,\\s*");
    }
}


