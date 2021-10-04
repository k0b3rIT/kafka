/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Copyright (c) 2021 Cloudera, Inc. All rights reserved.
package com.cloudera.kafka.connect.trustedproxy;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class PrincipalValidatorTest {

    @Test
    public void testValidPrincipals() {
        List<String> primaries = Arrays.asList(
                "primary",
                "primary-with-dash",
                "primary_with_underscore",
                "primary.with.dot",
                "primary\\with\\backslash",
                "primary-with_all.\\3"
        );
        List<String> instances = Arrays.asList(
                "", // no instance
                "instance",
                "instance-with-dash",
                "instance_with_underscore",
                "instance.with.dot",
                "instance-with_all.3"
        );
        List<String> realms = Arrays.asList(
                "", // no realm
                "realm",
                "realm-with-dash",
                "realm_with_underscore",
                "realm.with.dot",
                "realm\\with\\backslash",
                "realm-with_all.weird3@chars$WE|can\"think'of\\"
        );
        //Cross product of primaries, instances and realms
        Map<String, PrincipalName> cases = primaries
                .stream()
                .flatMap(
                        primary -> instances.stream().flatMap(instance ->
                                realms.stream().map(realm -> new AbstractMap.SimpleImmutableEntry<>(
                                        primary + (instance.isEmpty() ? "" : "/" + instance)
                                                + (realm.isEmpty() ? "" : "@" + realm),
                                        new PrincipalName(
                                                primary,
                                                instance.isEmpty() ? null : instance,
                                                realm.isEmpty() ? null : realm
                                        )
                                ))
                        )
                )
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (v1, v2) -> {
                            throw new IllegalArgumentException("Duplicate values");
                        },
                        LinkedHashMap::new
                ));

        cases.forEach((value, expected) -> {
            PrincipalName actual = PrincipalValidator.parsePrincipal("test", value);
            assertEquals(expected, actual);
        });
    }

    @Test
    public void testInvalidPrincipals() {
        List<String> principals = Arrays.asList(
                "/kafka/host-1.some.domain@.SOME.REALM",
                "kafka//host-1.some.domain@.SOME.REALM",
                "kafka/host-/1.some.domain@.SOME.REALM",
                "/host-1.some.domain@.SOME.REALM",
                "kafka/host-1.some.domain@.SOME  .  REALM"
        );
        for (String p : principals) {
            assertThrows(ConfigException.class,
                () -> PrincipalValidator.parsePrincipal("test", p),
                "Invalid principal should cause throw: " + p);
        }
    }

    @Test
    public void testValidatorWithoutRequired() {
        PrincipalValidator validator = new PrincipalValidator(false, false);
        validator.ensureValid("test", "kafka");
        validator.ensureValid("test", "kafka/host-1.some.domain");
        validator.ensureValid("test", "kafka@SOME.REALM");
        validator.ensureValid("test", "kafka/host-1.some.domain@SOME.REALM");
    }

    @Test
    public void testValidatorWithInstanceRequired() {
        PrincipalValidator validator = new PrincipalValidator(true, false);
        assertThrows(ConfigException.class, () -> validator.ensureValid("test", "kafka"));
        validator.ensureValid("test", "kafka/host-1.some.domain");
        assertThrows(ConfigException.class, () -> validator.ensureValid("test", "kafka@SOME.REALM"));
        validator.ensureValid("test", "kafka/host-1.some.domain@SOME.REALM");
    }

    @Test
    public void testValidatorWithRealmRequired() {
        PrincipalValidator validator = new PrincipalValidator(false, true);
        assertThrows(ConfigException.class, () -> validator.ensureValid("test", "kafka"));
        assertThrows(ConfigException.class, () -> validator.ensureValid("test", "kafka/host-1.some.domain"));
        validator.ensureValid("test", "kafka@SOME.REALM");
        validator.ensureValid("test", "kafka/host-1.some.domain@SOME.REALM");
    }

    @Test
    public void testValidatorWithInstanceAndRealmRequired() {
        PrincipalValidator validator = new PrincipalValidator(true, true);
        assertThrows(ConfigException.class, () -> validator.ensureValid("test", "kafka"));
        assertThrows(ConfigException.class, () -> validator.ensureValid("test", "kafka/host-1.some.domain"));
        assertThrows(ConfigException.class, () -> validator.ensureValid("test", "kafka@SOME.REALM"));
        validator.ensureValid("test", "kafka/host-1.some.domain@SOME.REALM");
    }
}
