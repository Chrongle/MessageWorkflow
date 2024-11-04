using System;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Newtonsoft.Json;


var factory = new ConnectionFactory() { HostName = "localhost" };
using var connection = factory.CreateConnection();
using var channel = connection.CreateModel();

channel.QueueDeclare("lego_finisher_queue", true, false, false, null);
channel.QueueDeclare("lego_finisher_response", true, false, false, null);

bool shouldFail = true; // Set to false to simulate success

var consumer = new EventingBasicConsumer(channel);
consumer.Received += (model, ea) =>
{
    var message = Encoding.UTF8.GetString(ea.Body.ToArray());
    Console.WriteLine($"LegoFinisher: Received message - {message}");
    channel.BasicAck(ea.DeliveryTag, false); // Acknowledge the receipt

    // Simulate processing
    var responseMessage = "Processing successful.";
    var responseBody = Encoding.UTF8.GetBytes(responseMessage);

    try
    {
        if (shouldFail)
        {
            throw new Exception("Simulated failure.");
        }

        channel.TxSelect(); // Start transaction
        channel.BasicPublish("", "lego_finisher_response", null, responseBody);
        Console.WriteLine("LegoFinisher: Sent response back to LegoAssembler.");
        channel.TxCommit(); // Commit the transaction
    }
    catch (Exception ex)
    {
        Console.WriteLine($"LegoFinisher: Error - {ex.Message}. Rolling back.");
        channel.TxRollback(); // Rollback on error
    }
};

channel.BasicConsume("lego_finisher_queue", false, consumer);
Console.WriteLine("LegoFinisher: Listening for messages...");
Console.ReadLine();