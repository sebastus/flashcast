using Microsoft.Extensions.Configuration;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Queue;
using Newtonsoft.Json.Linq;
using System;
using System.IO;
using System.Linq;
using System.Threading;

namespace DownloadBlobConsole
{
    class Program
    {
        public static IConfiguration Configuration { get; set; }

        static void Main(string[] args)
        {
            var builder = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json");

            Configuration = builder.Build();

            CloudStorageAccount account = CloudStorageAccount.Parse(Configuration["AppSettings:storageConnection"]);
            CloudQueueClient queueClient = account.CreateCloudQueueClient();
            CloudQueue queue = queueClient.GetQueueReference("blobcreateevents");
            queue.CreateIfNotExistsAsync();

            CloudBlobClient blobClient = account.CreateCloudBlobClient();

            bool done = false;
            ListBlockItem lastBlockDownloaded = null;
            int currentBackOff = 0;
            int maxBackOff = 5;
            byte[] bytesFromBlob = new byte[4 * 1024 * 1024];

            do
            {
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine("Getting messages from the queue.");

                queue.FetchAttributesAsync().Wait();

                int countOfMessages = Convert.ToInt32(queue.ApproximateMessageCount);
                if (countOfMessages > 0)
                {
                    currentBackOff = 1;
                    var msgs = queue.GetMessagesAsync(countOfMessages>32 ? 32 : countOfMessages).Result;
                    var numberOfMessagesLeftInBatch = msgs.Count();
                    foreach (var msg in msgs)
                    {
                        if (numberOfMessagesLeftInBatch == 1)
                        // process only the last message
                        // all messages are equivalent in meaning
                        // sender usually gets ahead of receiver
                        {
                            JToken token = JObject.Parse(msg.AsString);

                            string resourceId = (string)token.SelectToken("topic");
                            string subject = (string)token.SelectToken("subject");
                            string eventType = (string)token.SelectToken("eventType");

                            if (eventType == "Microsoft.Storage.BlobCreated")
                            {
                                var storageAccountName = resourceId.Split('/')[8];
                                var storageContainerName = subject.Split('/')[4];
                                var blobName = subject.Split('/')[6];
                                Uri uri = new Uri($"https://{storageAccountName}.blob.core.windows.net/{storageContainerName}/{blobName}");

                                CloudBlockBlob blob = new CloudBlockBlob(uri, blobClient);

                                var blockList = blob.DownloadBlockListAsync().Result;

                                long blobOffset = 0;
                                var blocksToCopy = blockList;

                                if (lastBlockDownloaded != null)
                                {
                                    // count the blocks already written
                                    var countOfblocksAlreadyWritten = blockList.TakeWhile(item => (item.Name != lastBlockDownloaded.Name)).Count() + 1;

                                    // get an enumerable of those block list items
                                    var blocksAlreadyWritten = blockList.Take(countOfblocksAlreadyWritten);

                                    // add up the bytes already written
                                    foreach (var block in blocksAlreadyWritten)
                                    {
                                        blobOffset += block.Length;
                                    }

                                    // skip over blocks already written
                                    blocksToCopy = blockList.SkipWhile(item => (item.Name != lastBlockDownloaded.Name)).Skip(1);
                                }

                                if (blocksToCopy.Count() > 0)
                                {
                                    var fs = File.OpenWrite(@"c:\temp\abigfile.dat");
                                    using (BinaryWriter writer = new BinaryWriter(fs))
                                    {
                                        foreach (var block in blocksToCopy)
                                        {
                                            blob.DownloadRangeToByteArrayAsync(bytesFromBlob, 0, blobOffset, block.Length).Wait();

                                            writer.Seek(0, SeekOrigin.End);
                                            writer.Write(bytesFromBlob, 0, (int)block.Length);

                                            blobOffset += block.Length;

                                            Console.ForegroundColor = ConsoleColor.White;
                                            Console.WriteLine($"{block.Name}");
                                        }
                                    };
                                    fs.Close();
                                }

                                lastBlockDownloaded = blockList.Last();
                            }

                        }

                        Console.ForegroundColor = ConsoleColor.Green;
                        Console.WriteLine("Deleting a message from the queue");

                        queue.DeleteMessageAsync(msg);
                        numberOfMessagesLeftInBatch--;
                    }
                }
                else
                {
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine($"Waiting for {currentBackOff} seconds for next message");

                    Thread.Sleep(currentBackOff * 1000);

                    currentBackOff += (currentBackOff < maxBackOff ? 1 : 0);

                }

                // set done flag in some way, or loop forever.
            } while (!done);

            Console.ReadKey();
        }
    }
}
