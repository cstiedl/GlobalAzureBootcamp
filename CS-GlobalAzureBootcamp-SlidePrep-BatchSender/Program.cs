using System;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Nito.AsyncEx;

namespace CS_GlobalAzureBootcamp_SlidePrep_BatchSender
{
    class Program
    {
        static string connectionString = "Endpoint=sb://cs-globalazurebootcamp-slideprep.servicebus.windows.net/;SharedAccessKeyName=accountinternal_key;SharedAccessKey=QgMK+lNMiA5goQmD/xsYZZLnQzZk4nTRlCRfSVeo/k4=";
        const string topicName = "accountInternal";
        private static TopicClient topicClient = new TopicClient(connectionString, topicName);

        const bool useBatching = true;
        const bool useSampleJson = true;
        const int messages = 10000;
        const int batchSize = 1000;

        static async Task Main(string[] args)
        {
            Console.WriteLine("Starting batch sending ...");

            // V1: Send batches of 1000 after each other until 10.000 reached.
            await SendBatch();

            // V2: Using groups where each group sends batches of 1000 until 10.000 messages reached, but
            // groups are processed in parallel. Below thus, up to 5 baches of 1000 messages each, are sent in parallel.
            /*
            int parallelBatches = 5;
            Parallel.ForEach(Enumerable.Range(1, parallelBatches).ToList(), i => 
            {
                AsyncContext.Run(SendBatch);
                Console.WriteLine("Group {0} of batches has been sent", i);
            });
            */
        }

        private static async Task SendBatch()
        {
            var stopwatch = new Stopwatch();
            stopwatch.Start();

            for (int i = 0; i < messages; i += batchSize)
            {
                // For sending in batch, we can pass a list of messages here.
                await topicClient.SendAsync(Enumerable.Range(0, batchSize).Select(n =>
                {
                    return new Message
                    {
                        // Internal Format of Dynamics 365. We use that to simulate some internal message that we
                        // translate in our Azure Function into the Common Data Model.
                        Body = Encoding.UTF8.GetBytes("{ \"InputParameters\": [ { \"key\": \"Target\", \"value\": { \"Attributes\": [ { \"key\": \"name\", \"value\": \"Global Azure Bootcamp - Batch\" }, ], } } ] }")
                    };
                }).ToList());
            }

            Console.WriteLine($"{stopwatch.ElapsedMilliseconds}ms to send {messages} messages");
            stopwatch.Reset();
        }
    }
}
