package com.example.order

import order.OrderStatus
import order.OrderToProcess
import order.ProcessedOrder
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.Headers
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import java.nio.charset.StandardCharsets

@Service
class ProcessOrderService(private val kafkaTemplate: KafkaTemplate<String, ProcessedOrder>) {
    companion object {
        private const val PROCESSED_ORDERS_TOPIC = "processed-orders"
        private const val WIP_ORDERS_TOPIC = "wip-orders"
        private const val CORRELATION_ID = "orderRequestId"
    }

    private val serviceName = this::class.simpleName

    init {
        println("$serviceName started running..")
    }

    @KafkaListener(topics = [WIP_ORDERS_TOPIC])
    fun processOrder(record: ConsumerRecord<String, OrderToProcess>) {
        val orderRequest = record.value()
        println("[$serviceName] Received message on topic $WIP_ORDERS_TOPIC - $orderRequest")

        val processedOrder = ProcessedOrder.newBuilder()
            .setId(orderRequest.id)
            .setStatus(OrderStatus.PROCESSED)
            .setAmount(1000)
            .build()

        val correlationId = extractCorrelationId(record.headers())

        val message = ProducerRecord<String, ProcessedOrder>(
            PROCESSED_ORDERS_TOPIC,
            processedOrder
        ).also {
            it.headers().add(CORRELATION_ID, correlationId.toString().toByteArray())
        }

        kafkaTemplate.send(message)
        println("[$serviceName] Sent message to topic $PROCESSED_ORDERS_TOPIC - $processedOrder (orderRequestId=${correlationId ?: orderRequest.id})")
    }

    private fun extractCorrelationId(headers: Headers): Int? {
        return headers.lastHeader(CORRELATION_ID)?.let {
            String(it.value(), StandardCharsets.UTF_8).toInt()
        }
    }
}