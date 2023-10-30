package org.pegasus.keycloak.spi.kafka

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*

class KafkaProducerFactoryImpl : KafkaProducerFactory {
    override fun createProducer(bootstrapServer: String, clientId: String): Producer<String, String> {
        val properties = Properties()
        properties[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServer
        properties[ProducerConfig.CLIENT_ID_CONFIG] = clientId
        properties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
        properties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name

        return KafkaProducer(properties)
    }
}
