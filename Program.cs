using System;
using System.Text;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;
using Azure.Messaging.EventHubs.Producer;

namespace EventHubLearning
{
    public class Program
    {
        private const string connectionString = "YOUR EVENT HUB CONNECTION STRING";
        private const string blobStorageConnectionString = "YOUR BLOB STORAGE CONNECTION STRING";

        private const string eventHubName = "learning-hub";
        private const string blobContainerName = "event-hub-processing-container";

        private static EventHubProducerClient producerClient;
        private static EventProcessorClient processor;
        private static BlobContainerClient storageClient;

        public static async Task Main(string[] args)
        {
            await WriteMessages();
            // await ReadMessages();
        }

        private static async Task WriteMessages()
        {
            producerClient = new EventHubProducerClient(connectionString, eventHubName);
            using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();

            for (int i = 1; i <= 100; i++)
            {
                if (!eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes($"Event {i}"))))
                {
                    // if it is too large for the batch
                    throw new Exception($"Event {i} is too large for the batch and cannot be sent.");
                }
            }

            try
            {
                await producerClient.SendAsync(eventBatch);
                Console.WriteLine($"A batch of {100} events has been published.");
            }
            finally
            {
                await producerClient.DisposeAsync();
            }
        }

        private static async Task ReadMessages()
        {
            string consumerGroup = EventHubConsumerClient.DefaultConsumerGroupName;

            storageClient = new BlobContainerClient(blobStorageConnectionString, blobContainerName);

            processor = new EventProcessorClient(storageClient, consumerGroup, connectionString, eventHubName);

            processor.ProcessEventAsync += ProcessEventHandler;
            processor.ProcessErrorAsync += ProcessErrorHandler;

            await processor.StartProcessingAsync();
            await Task.Delay(TimeSpan.FromSeconds(30));
            await processor.StopProcessingAsync();
        }

        static async Task ProcessEventHandler(ProcessEventArgs eventArgs)
        {
            Console.WriteLine("\tReceived event: {0}", Encoding.UTF8.GetString(eventArgs.Data.Body.ToArray()));
            await eventArgs.UpdateCheckpointAsync(eventArgs.CancellationToken);
        }

        static Task ProcessErrorHandler(ProcessErrorEventArgs eventArgs)
        {
            // Write details about the error to the console window
            Console.WriteLine($"\tPartition '{ eventArgs.PartitionId}': an unhandled exception was encountered. This was not expected to happen.");
            Console.WriteLine(eventArgs.Exception.Message);
            return Task.CompletedTask;
        }
    }
}
