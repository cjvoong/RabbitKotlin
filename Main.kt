import com.rabbitmq.client.ConnectionFactory
import org.assertj.core.api.Assertions.assertThat

    fun main(args: Array<String>){
        val QUEUE_NAME="sports.platform.betslip"
        val ROUTING_KEY="betslip.opportunities"

        val factory = ConnectionFactory()
        factory.host = "localhost"

        val connection = factory.newConnection()
        val channel = connection.createChannel()
        channel.queueDeclare(QUEUE_NAME,false,false,false,null)

        while (true) {
            val response = channel.basicGet(QUEUE_NAME,true)
            if (response == null)
                break
            val env = response.envelope
            assertThat(env.routingKey).isEqualTo(ROUTING_KEY)
            println(response.body)
        }
    }
