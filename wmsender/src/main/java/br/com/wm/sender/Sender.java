package br.com.wm.sender;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;



public class Sender {
	
	private static final String QUEUE_NAME = "wm-queue";

	public static void main(String[] args) {

		System.out.println("Starting the sender program!");

		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));

		try {
			// Create the connection with the "Rabbit Server" and get the channel.
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost("localhost");
			Connection connection = factory.newConnection();
			Channel channel = connection.createChannel();

			// Declare the queue.
			channel.queueDeclare(QUEUE_NAME, true, false, false, null);

			System.out.println("Message to be sent ('exit' to close the program): ");
			String message = in.readLine();

			// Send the message to the queue.
			while (!message.equals("exit")) {
				if (!message.trim().isEmpty()) {
					channel.basicPublish("", QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
				}

				message = in.readLine();
			}

			// Closing the channel and connection.
			channel.close();
			connection.close();
		} catch (Exception e) {
			System.out.println(String.format("An error occurs [%s]. [%s]", e.getMessage(), e));
		}
	}

}
