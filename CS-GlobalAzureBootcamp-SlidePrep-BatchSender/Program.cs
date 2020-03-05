using System;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;

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
