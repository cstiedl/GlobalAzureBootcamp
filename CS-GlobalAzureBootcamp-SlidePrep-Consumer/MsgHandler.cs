using System;
using System.Text;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.ServiceBus;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

namespace CS_GlobalAzureBootcamp_SlidePrep_Consumer
{
    public static class MsgHandler
    {

        [FunctionName("ProcessInternalCRMMsg")]
        [return: ServiceBus("account", Connection = "cs-globalazurebootcamp-slideprep-account", EntityType = EntityType.Topic)]
        public static Account Run([ServiceBusTrigger("accountinternal", "accountinternal-func", Connection = "cs-globalazurebootcamp-slideprep")] Message message, ILogger log)
        {
            var body = Encoding.Default.GetString(message.Body);
            var jObject = JObject.Parse(body);
            var name = string.Empty;
            var inputParams = jObject["InputParameters"];
            var attributes = inputParams.First["value"]["Attributes"];
            foreach (var attr in attributes)
            {
                if ((string) attr["key"] != "name") continue;
                name = (string) attr["value"];
            }
            return new Account { Name = name };
        }

        public class Account
        {
            public string Name { get; set; }
        }
    }
}
