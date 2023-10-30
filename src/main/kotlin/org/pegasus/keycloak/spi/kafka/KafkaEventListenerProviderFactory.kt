package org.pegasus.keycloak.spi.kafka

import org.keycloak.Config
import org.keycloak.events.EventListenerProvider
import org.keycloak.events.EventListenerProviderFactory
import org.keycloak.models.KeycloakSession

class KafkaEventListenerProviderFactory : EventListenerProviderFactory {
    private lateinit var bootstrapServers: String
    private lateinit var clientId: String
    private lateinit var topicEvents: String
    private lateinit var events: Array<String>
    private var instance: KafkaEventListenerProvider? = null

    override fun init(config: Config.Scope) {
        bootstrapServers = config.get("bootstrapServers", System.getenv("KAFKA_BOOTSTRAP_SERVERS"))
        clientId = config.get("clientId", "KAFKA_CLIENT_ID")
        topicEvents = config.get("topicEvents", "KAFKA_TOPIC_EVENTS")
        events = config.get("events", "KAFKA_EVENTS").orEmpty().split(',').toTypedArray()
    }

    override fun getId() = "kafka"

    override fun create(session: KeycloakSession?): EventListenerProvider {
        if (instance == null) {
            instance = KafkaEventListenerProvider(
                bootstrapServers,
                clientId,
                topicEvents,
                events,
                KafkaProducerFactoryImpl()
            )
        }

        return instance!!
    }

    override fun postInit(factory: org.keycloak.models.KeycloakSessionFactory?) {
        // ignore
    }

    override fun close() {
        // ignore
    }
}
