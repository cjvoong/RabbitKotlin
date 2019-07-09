package com.tsg.sportsplatform.gauge.steps

import com.rabbitmq.client.AMQP
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DefaultConsumer
import com.rabbitmq.client.Envelope
import org.assertj.core.api.Assertions.assertThat
import java.io.IOException
import java.nio.charset.Charset


fun main(args:Array<String>){
        startConsumer()
    }

    fun read(){
        val QUEUE_NAME="sports.platform.betslip"
        val ROUTING_KEY="betslip.opportunities"

        val factory = ConnectionFactory()
        factory.host = "localhost"

        val connection = factory.newConnection()
        val channel = connection.createChannel()
        channel.queueDeclare(QUEUE_NAME,false,false,false,null)

        while (true) {
            val response = channel.basicGet(QUEUE_NAME,true) ?: break
            val env = response.envelope
            assertThat(env.routingKey).isEqualTo(ROUTING_KEY)
            var msg = String(response.body, Charset.defaultCharset())
            println(msg)
        }
    }

fun startConsumer(){
    val QUEUE_NAME="sports.platform.betslip"
    val ROUTING_KEY="betslip.opportunities"

    val factory = ConnectionFactory()
    factory.host = "localhost"

    val connection = factory.newConnection()
    val channel = connection.createChannel()
    val autoAck = false
    channel.basicConsume(QUEUE_NAME, autoAck, "myConsumerTag",
        object : DefaultConsumer(channel) {
            @Throws(IOException::class)
            override fun handleDelivery(
                consumerTag: String,
                envelope: Envelope,
                properties: AMQP.BasicProperties,
                body: ByteArray
            ) {
                val routingKey = envelope.getRoutingKey()
                val contentType = properties.getContentType()
                val deliveryTag = envelope.getDeliveryTag()
                // (process the message components here ...)
                var msg = String(body, Charset.defaultCharset())
                print("got message: $msg")
                channel.basicAck(deliveryTag, false)
            }
        })

}
