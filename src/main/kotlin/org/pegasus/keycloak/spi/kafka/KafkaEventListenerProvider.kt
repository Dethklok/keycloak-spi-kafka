package org.pegasus.keycloak.spi.kafka

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.sun.org.slf4j.internal.LoggerFactory
import org.apache.kafka.clients.producer.ProducerRecord
import org.keycloak.events.Event
import org.keycloak.events.EventListenerProvider
import org.keycloak.events.EventType
import org.keycloak.events.admin.AdminEvent
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

class KafkaEventListenerProvider(
    bootstrapServers: String,
    clientId: String,
    private val topicEvents: String,
    events: Array<String>,
    producerFactory: KafkaProducerFactory
) : EventListenerProvider {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val mapper = ObjectMapper()

    private val events = events.mapNotNull { event ->
        try {
            EventType.valueOf(event)
        } catch (e: IllegalArgumentException) {
            logger.debug("Event $event is not supported. Skipping.")
            null
        }
    }

    private val producer = producerFactory.createProducer(
        bootstrapServers,
        clientId,
    )

    override fun onEvent(event: Event) {
        if (events.contains(event.type)) {
            try {
                produceEvent(mapper.writeValueAsString(event), topicEvents)
            } catch (e: Exception) {
                when (e) {
                    is JsonProcessingException, is ExecutionException, is TimeoutException -> logger.error(e.message)
                    is InterruptedException -> {
                        logger.error(e.message)
                        Thread.currentThread().interrupt()
                    }
                }
            }
        }
    }

    override fun onEvent(event: AdminEvent?, includeRepresentation: Boolean) {
        // ignore
    }

    private fun produceEvent(eventAsString: String, topic: String) {
        logger.debug("Producing event to topic $topic...")
        val record = ProducerRecord<String, String>(topic, eventAsString)
        val metadata = producer.send(record).get(30, TimeUnit.SECONDS)
        logger.debug("Produced event to topic ${metadata.topic()}")
    }

    override fun close() {
        // ignore
    }
}
