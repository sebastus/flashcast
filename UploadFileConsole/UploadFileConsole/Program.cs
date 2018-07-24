using Microsoft.Extensions.Configuration;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace StorageTests
{
    class Program
    {
        static string AcctName;
        static string AcctKey;
        static Uri uri;
        static string Filename;

        // pick block size depending on speed of network connection
        // target about 1 block per second
        const int ONEMB = 1 * 1024 * 1024;
        const int FOURMB = 4 * ONEMB;
        const int HALFMB = ONEMB / 2;
        const int EIGHTHMB = ONEMB / 8;
        const int SIXTEENTHMB = ONEMB / 16;

        class NewBlockId
        {
            Guid guid { get; set; }
            public string BlockId { get; set; }
            public string Base64BlockId { get; set; }
            public NewBlockId()
            {
                guid = Guid.NewGuid();
                BlockId = guid.ToString();
                Base64BlockId = Convert.ToBase64String(guid.ToByteArray());
            }
        }

        public static IConfiguration Configuration { get; set; }

        static void Main(string[] args)
        {

            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appSettings.json");

            Configuration = builder.Build();

            AcctName = Configuration["AppSettings:storageAcct"];
            AcctKey = Configuration["AppSettings:storageKey"];
            uri = new Uri(Configuration["AppSettings:blobUri"]);
            Filename = Configuration["AppSettings:fileAbsolutePath"];

            UploadBigFileWithCommitsAsync().Wait();

            Console.ReadKey();
            return;
        }

        public static async Task UploadBigFileWithCommitsAsync()
        {
            CloudBlockBlob blob = new CloudBlockBlob(uri,
                new CloudStorageAccount(
                    new StorageCredentials(AcctName, AcctKey), 
                    useHttps: true)
                .CreateCloudBlobClient());

            int blocksize = FOURMB;

            var blockList = new List<string>();
            byte[] bytesRead = new byte[blocksize];
            int numBytesRead = 0;

            MemoryStream ms = new MemoryStream();
            ms.Capacity = blocksize;
            Boolean DoneSending = false;

            using (FileStream fs = File.OpenRead(Filename))
            {

                do
                {

                    Console.Write(".");
                    NewBlockId blockId = new NewBlockId();

                    try
                    {
                        ms.Position = 0;
                        numBytesRead = await fs.ReadAsync(bytesRead, 0, blocksize);

                        if (numBytesRead == blocksize)
                        {
                            ms.Write(bytesRead, 0, blocksize);
                            await Send4mbBlockAsync(blob, ms, blockId);
                        }
                        else
                        {
                            MemoryStream msLast = new MemoryStream();
                            msLast.Capacity = numBytesRead;

                            await msLast.WriteAsync(bytesRead, 0, numBytesRead);
                            await Send4mbBlockAsync(blob, msLast, blockId);

                            DoneSending = true;
                        }

                    }
                    catch (Exception ex)
                    {
                        ;
                    }

                    blockList.Add(blockId.Base64BlockId);
                    await blob.PutBlockListAsync(blockList);

                } while (!DoneSending);

            }

        }

        private static async Task Send4mbBlockAsync(CloudBlockBlob blob, MemoryStream ms, NewBlockId blockId)
        {
            ms.Seek(0, SeekOrigin.Begin);
            await blob.PutBlockAsync(
                blockId: blockId.Base64BlockId,
                blockData: ms,
                contentMD5: null);
        }

    }
}
