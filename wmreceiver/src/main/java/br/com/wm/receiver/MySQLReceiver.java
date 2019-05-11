package br.com.wm.receiver;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class MySQLReceiver {
	
	private static final String QUEUE_NAME = "wm-queue";

	public static void main(String[] args) {

		System.out.println("Starting the MySQL receiver program!");

		try {
			// Get the connection with MySQL.
			Statement mySqlConnection = connectToMySQL();

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
			    
			    try {
				    // Send the message to MySQL.
					mySqlConnection.execute(String.format(QUERY, message));
			    }catch (SQLException e) {
					e.printStackTrace();
				}
			};
			channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> { });

			
		} catch (Exception e) {
			System.out.println(String.format("An error occurs [%s]. [%s]", e.getMessage(), e));
		}
	}

	// Database query.
	private static String QUERY = "INSERT INTO TB_MESSAGES (message) VALUES ('%s')";

	// Driver used to connect with the database.
	private static String DRIVER = "com.mysql.jdbc.Driver";

	// Database URL.
	private static String URL = "jdbc:mysql://localhost/rabbit";

	// Database username.
	private static String USERNAME = "root";

	// Database password.
	private static String PASSWORD = "root";

	public static Statement connectToMySQL() {

		try {
			Class.forName(DRIVER);
			java.sql.Connection con = DriverManager.getConnection(URL, USERNAME, PASSWORD);
			Statement stmt = con.createStatement();

			return stmt;
		} catch (Exception e) {
			System.out.println(String.format("Error while connecting to the MySQL [%s]. [%s]", e.getMessage(), e));
		}

		return null;
	}

}
