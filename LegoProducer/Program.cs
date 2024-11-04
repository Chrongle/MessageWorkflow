using System;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Newtonsoft.Json;


var factory = new ConnectionFactory() { HostName = "localhost" };
using var connection = factory.CreateConnection();
using var channel = connection.CreateModel();

channel.QueueDeclare("lego_assembler_queue", true, false, false, null);
channel.QueueDeclare("lego_producer_response", true, false, false, null);

var legoData = new { Id = Guid.NewGuid(), Action = "Assemble" };
var messageBody = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(legoData));

try
{
    channel.TxSelect(); // Start transaction
    channel.BasicPublish("", "lego_assembler_queue", null, messageBody);
    Console.WriteLine("LegoProducer: Sent message to LegoAssembler.");
    channel.TxCommit(); // Commit the transaction
}
catch (Exception ex)
{
    Console.WriteLine($"LegoProducer: Error - {ex.Message}. Rolling back.");
    channel.TxRollback(); // Rollback on error
}

var consumer = new EventingBasicConsumer(channel);
consumer.Received += (model, ea) =>
{
    var responseMessage = Encoding.UTF8.GetString(ea.Body.ToArray());
    Console.WriteLine($"LegoProducer: Received response - {responseMessage}");

    // Acknowledge the response only after processing
    try
    {
        channel.BasicAck(ea.DeliveryTag, false); // Acknowledge the response
        Console.WriteLine("LegoProducer: Acknowledged the response.");
    }
    catch (Exception ackEx)
    {
        Console.WriteLine($"LegoProducer: Acknowledgment failed - {ackEx.Message}");
    }
};

channel.BasicConsume("lego_producer_response", false, consumer);
Console.WriteLine("LegoProducer: Listening for responses...");
Console.ReadLine();