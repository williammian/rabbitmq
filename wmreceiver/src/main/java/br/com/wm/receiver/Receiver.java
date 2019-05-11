package br.com.wm.receiver;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;


public class Receiver {
	
	private static final String QUEUE_NAME = "wm-queue";

	public static void main(String[] args) {

		System.out.println("Starting the receiver program!");

		try {
			// Create the connection.
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost("localhost");
			Connection connection = factory.newConnection();

			// Create the channel and the queue.
			Channel channel = connection.createChannel();
			channel.queueDeclare(QUEUE_NAME, true, false, false, null);

			DeliverCallback deliverCallback = (consumerTag, delivery) -> {
			    String message = new String(delivery.getBody(), "UTF-8");
			    System.out.println(" [x] Received '" + message + "'");
			};
			channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> { });
			
		} catch (Exception e) {
			System.out.println(String.format("An error occurs [%s]. [%s]", e.getMessage(), e));
		}
	}

}
