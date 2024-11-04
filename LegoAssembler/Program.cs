using System;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Newtonsoft.Json;


var factory = new ConnectionFactory() { HostName = "localhost" };
using var connection = factory.CreateConnection();
using var channel = connection.CreateModel();

channel.QueueDeclare("lego_assembler_queue", true, false, false, null);
channel.QueueDeclare("lego_finisher_queue", true, false, false, null);
channel.QueueDeclare("lego_finisher_response", true, false, false, null);
channel.QueueDeclare("lego_producer_response", true, false, false, null); // Ensure this is declared

bool shouldFail = false; // Set to false to simulate success

var producerConsumer = new EventingBasicConsumer(channel);
producerConsumer.Received += (model, ea) =>
{
    var message = Encoding.UTF8.GetString(ea.Body.ToArray());
    Console.WriteLine($"LegoAssembler: Received message - {message}");
    channel.BasicAck(ea.DeliveryTag, false); // Acknowledge the receipt

    // Process and send to LegoFinisher
    var finisherMessage = new { Action = "Finish", Message = message };
    var finisherBody = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(finisherMessage));

    try
    {
        if (shouldFail)
        {
            throw new Exception("Simulated failure.");
        }

        channel.TxSelect(); // Start transaction
        channel.BasicPublish("", "lego_finisher_queue", null, finisherBody);
        Console.WriteLine("LegoAssembler: Sent message to LegoFinisher.");
        channel.TxCommit(); // Commit the transaction
    }
    catch (Exception ex)
    {
        Console.WriteLine($"LegoAssembler: Error - {ex.Message}. Rolling back.");
        channel.TxRollback(); // Rollback on error
    }
};

// Consumer for receiving messages from LegoFinisher
var finisherConsumer = new EventingBasicConsumer(channel);
finisherConsumer.Received += (model, ea) =>
{
    var responseMessage = Encoding.UTF8.GetString(ea.Body.ToArray());
    Console.WriteLine($"LegoAssembler: Received response from Finisher - {responseMessage}");
    channel.BasicAck(ea.DeliveryTag, false); // Acknowledge the response

    // Send response back to the LegoProducer
    var producerResponseMessage = "LegoAssembler processed successfully.";
    var producerResponseBody = Encoding.UTF8.GetBytes(producerResponseMessage);
    try
    {
        channel.TxSelect(); // Start transaction
        channel.BasicPublish("", "lego_producer_response", null, producerResponseBody);
        Console.WriteLine("LegoAssembler: Sent response back to Producer.");
        channel.TxCommit(); // Commit the transaction
    }
    catch (Exception ex)
    {
        Console.WriteLine($"LegoAssembler: Error sending to Producer - {ex.Message}. Rolling back.");
        channel.TxRollback(); // Rollback on error
    }
};

// Consume messages from the LegoFinisher response queue
channel.BasicConsume("lego_finisher_response", false, finisherConsumer);
// Consume messages from the LegoAssembler queue
channel.BasicConsume("lego_assembler_queue", false, producerConsumer);

Console.WriteLine("LegoAssembler: Listening for messages...");
Console.ReadLine();