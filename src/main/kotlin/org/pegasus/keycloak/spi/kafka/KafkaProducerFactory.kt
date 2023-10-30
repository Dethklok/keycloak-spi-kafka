package org.pegasus.keycloak.spi.kafka

import org.apache.kafka.clients.producer.Producer

interface KafkaProducerFactory {
    fun createProducer(
        bootstrapServer: String,
        clientId: String,
    ): Producer<String, String>
}
