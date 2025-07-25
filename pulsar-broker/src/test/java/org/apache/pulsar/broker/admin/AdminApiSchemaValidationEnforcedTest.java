/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.admin;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.protocol.schema.PostSchemaPayload;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-admin")
public class AdminApiSchemaValidationEnforcedTest extends MockedPulsarServiceBaseTest {

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.internalSetup();

        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Set.of("role1", "role2"), Set.of("test"));
        admin.tenants().createTenant("schema-validation-enforced", tenantInfo);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testGetSchemaValidationEnforcedApplied() throws Exception {
        String namespace = "schema-validation-enforced/testApplied";
        admin.namespaces().createNamespace(namespace);
        this.conf.setSchemaValidationEnforced(true);
        assertTrue(admin.namespaces().getSchemaValidationEnforced(namespace, true));
        assertFalse(admin.namespaces().getSchemaValidationEnforced(namespace, false));
    }

    @Test
    public void testDisableSchemaValidationEnforcedNoSchema() throws Exception {
        admin.namespaces().createNamespace("schema-validation-enforced/default-no-schema");
        String namespace = "schema-validation-enforced/default-no-schema";
        String topicName = "persistent://schema-validation-enforced/default-no-schema/test";
        assertFalse(admin.namespaces().getSchemaValidationEnforced(namespace));
        admin.namespaces().setSchemaValidationEnforced(namespace, false);
        try {
            admin.schemas().getSchemaInfo(topicName);
            fail();
        } catch (PulsarAdminException.NotFoundException e) {
            assertEquals(e.getMessage(), "Schema not found");
        }
        try (Producer p = pulsarClient.newProducer().topic(topicName).create()) {
            p.send("test schemaValidationEnforced".getBytes());
        }
    }

    @Test
    public void testDisableSchemaValidationEnforcedHasSchema() throws Exception {
        admin.namespaces().createNamespace("schema-validation-enforced/default-has-schema");
        String namespace = "schema-validation-enforced/default-has-schema";
        String topicName = "persistent://schema-validation-enforced/default-has-schema/test";
        assertFalse(admin.namespaces().getSchemaValidationEnforced(namespace));
        admin.namespaces().setSchemaValidationEnforced(namespace, false);
        try {
            admin.schemas().getSchemaInfo(topicName);
            fail();
        } catch (PulsarAdminException.NotFoundException e) {
            assertEquals(e.getMessage(), "Schema not found");
        }
        Map<String, String> properties = new HashMap<>();
        SchemaInfo schemaInfo = SchemaInfo.builder()
                .type(SchemaType.STRING)
                .properties(properties)
                .name("test")
                .timestamp(1L)
                .schema("".getBytes())
                .build();
        PostSchemaPayload postSchemaPayload = new PostSchemaPayload("STRING", "", properties);
        admin.schemas().createSchema(topicName, postSchemaPayload);
        try (Producer p = pulsarClient.newProducer().topic(topicName).create()) {
            p.send("test schemaValidationEnforced".getBytes());
        }
        assertSchemaInfoEquals(admin.schemas().getSchemaInfo(topicName), schemaInfo);
    }

    private static void assertSchemaInfoEquals(SchemaInfo actual, SchemaInfo expected) {
        assertEquals(actual.getName(), expected.getName());
        assertEquals(actual.getSchema(), expected.getSchema());
        assertEquals(actual.getType(), expected.getType());
        assertEquals(actual.getProperties(), expected.getProperties());
        assertEquals(actual.getSchemaDefinition(), expected.getSchemaDefinition());
    }


    @Test
    public void testEnableSchemaValidationEnforcedNoSchema() throws Exception {
        admin.namespaces().createNamespace("schema-validation-enforced/enable-no-schema");
        String namespace = "schema-validation-enforced/enable-no-schema";
        String topicName = "persistent://schema-validation-enforced/enable-no-schema/test";
        assertFalse(admin.namespaces().getSchemaValidationEnforced(namespace));
        admin.namespaces().setSchemaValidationEnforced(namespace, true);
        try {
            admin.schemas().getSchemaInfo(topicName);
            fail();
        } catch (PulsarAdminException.NotFoundException e) {
            assertEquals(e.getMessage(), "Schema not found");
        }
        try (Producer p = pulsarClient.newProducer().topic(topicName).create()) {
            p.send("test schemaValidationEnforced".getBytes());
        }
    }

    @Test
    public void testEnableSchemaValidationEnforcedHasSchemaMismatch() throws Exception {
        admin.namespaces().createNamespace("schema-validation-enforced/enable-has-schema-mismatch");
        String namespace = "schema-validation-enforced/enable-has-schema-mismatch";
        String topicName = "persistent://schema-validation-enforced/enable-has-schema-mismatch/test";
        assertFalse(admin.namespaces().getSchemaValidationEnforced(namespace));
        admin.namespaces().setSchemaValidationEnforced(namespace, true);
        assertTrue(admin.namespaces().getSchemaValidationEnforced(namespace));
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().getStats(topicName);
        try {
            admin.schemas().getSchemaInfo(topicName);
            fail();
        } catch (PulsarAdminException.NotFoundException e) {
            assertEquals(e.getMessage(), "Schema not found");
        }
        Map<String, String> properties = new HashMap<>();
        properties.put("key1", "value1");
        SchemaInfo schemaInfo = SchemaInfo.builder()
                .type(SchemaType.STRING)
                .properties(properties)
                .name("test")
                .schema("".getBytes())
                .build();
        PostSchemaPayload postSchemaPayload = new PostSchemaPayload("STRING", "", properties);
        admin.schemas().createSchema(topicName, postSchemaPayload);
        try (Producer p = pulsarClient.newProducer().topic(topicName).create()) {
            fail("Client no schema, but topic has schema, should fail");
        }  catch (PulsarClientException e) {
            assertTrue(e.getMessage().contains("IncompatibleSchemaException"));
        }
        assertEquals(admin.schemas().getSchemaInfo(topicName).getName(), schemaInfo.getName());
        assertEquals(admin.schemas().getSchemaInfo(topicName).getType(), schemaInfo.getType());
    }

    @Test
    public void testEnableSchemaValidationEnforcedHasSchemaMatch() throws Exception {
        admin.namespaces().createNamespace("schema-validation-enforced/enable-has-schema-match");
        String namespace = "schema-validation-enforced/enable-has-schema-match";
        String topicName = "persistent://schema-validation-enforced/enable-has-schema-match/test";
        assertFalse(admin.namespaces().getSchemaValidationEnforced(namespace));
        try {
            admin.schemas().getSchemaInfo(topicName);
            fail();
        } catch (PulsarAdminException.NotFoundException e) {
            assertEquals(e.getMessage(), "Schema not found");
        }
        admin.namespaces().setSchemaValidationEnforced(namespace, true);
        Map<String, String> properties = new HashMap<>();
        SchemaInfo schemaInfo = SchemaInfo.builder()
                .type(SchemaType.STRING)
                .properties(properties)
                .name("test")
                .schema("".getBytes())
                .build();
        PostSchemaPayload postSchemaPayload = new PostSchemaPayload("STRING", "", properties);
        admin.schemas().createSchema(topicName, postSchemaPayload);
        try (Producer<String> p = pulsarClient.newProducer(Schema.STRING).topic(topicName).create()) {
            p.send("test schemaValidationEnforced");
        }
        assertEquals(admin.schemas().getSchemaInfo(topicName).getName(), schemaInfo.getName());
        assertEquals(admin.schemas().getSchemaInfo(topicName).getType(), schemaInfo.getType());
    }

}
