using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;

namespace utils
{
    class ReturnFrom4mbSend
    {
        public Boolean DoneSending { get; set; }
        public string BlockId { get; set; }

        public ReturnFrom4mbSend(Boolean b, string id)
        {
            DoneSending = b;
            BlockId = id;
        }
    }

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

    public class BigFile
    {
        const int MAX32KBLOCKS = 128;
        const int ONEMB = 1 * 1024 * 1024;
        const int FOURMB = 4 * ONEMB;
        const int HALFMB = ONEMB / 2;
        const int EIGHTHMB = ONEMB / 8;
        const int SIXTEENTHMB = ONEMB / 16;

        public string Filename { get; set; }
        public string AcctName { get; set; }
        public string AcctKey { get; set; }
        public Uri Uri { get; set; }

        public BigFile(string fileName, Uri uri)
        {
            Filename = fileName;
            Uri = uri;
        }

        public void StorageAccount(string acctName, string acctKey)
        {
            AcctName = acctName;
            AcctKey = acctKey;
        }

        public async Task<int> GetBlockListAsync()
        {
            StorageCredentials creds = new StorageCredentials(AcctName, AcctKey);
            CloudStorageAccount account = new CloudStorageAccount(creds, useHttps: true);
            CloudBlobClient client = account.CreateCloudBlobClient();
            CloudBlockBlob blob = new CloudBlockBlob(Uri, client);

            System.Collections.Generic.IEnumerable<ListBlockItem> list = await blob.DownloadBlockListAsync();

            int count = 0;
            foreach (var item in list) count++;

            return count;
        }

        public async Task UploadBigfileAsync()
        {

            StorageCredentials creds = new StorageCredentials(AcctName, AcctKey);
            CloudStorageAccount account = new CloudStorageAccount(creds, useHttps: true);
            CloudBlobClient client = account.CreateCloudBlobClient();
            CloudBlockBlob blob = new CloudBlockBlob(Uri, client);

            var blockList = new List<string>();

            using (FileStream fs = File.OpenRead(Filename))
            {
                ReturnFrom4mbSend r;
                do
                {

                    Console.Write(".");
                    r = await ReadAndPut4mbBlockAsync(fs, blob);
                    blockList.Add(r.BlockId);

                } while (!r.DoneSending);

                await blob.PutBlockListAsync(blockList);

            }
        }

        async Task<ReturnFrom4mbSend> ReadAndPut4mbBlockAsync(FileStream fs, CloudBlockBlob blob)
        {
            byte[] bytesRead = new byte[FOURMB];
            int numBytesRead = 0;

            NewBlockId blockId = new NewBlockId();

            Boolean LastBlock = false;

            using (MemoryStream ms = new MemoryStream())
            {

                numBytesRead = await fs.ReadAsync(bytesRead, 0, bytesRead.Length);
                ms.Capacity += numBytesRead;
                await ms.WriteAsync(bytesRead, 0, numBytesRead);
                ms.Position = 0;

                try
                {
                    await blob.PutBlockAsync(blockId: blockId.Base64BlockId,
                        blockData: ms,
                        contentMD5: null,
                        accessCondition: AccessCondition.GenerateEmptyCondition(),
                        options: new BlobRequestOptions
                        {
                            StoreBlobContentMD5 = true,
                            UseTransactionalMD5 = true
                        },
                        operationContext: new OperationContext());
                } catch (Exception ex)
                {
                    ;
                }

                LastBlock = ms.Length != FOURMB;
            }

            ReturnFrom4mbSend r = new ReturnFrom4mbSend(LastBlock, blockId.Base64BlockId);

            return (r);
        }

        public async Task UploadBigFileExtAsync()
        {
            StorageCredentials creds = new StorageCredentials(AcctName, AcctKey);
            CloudStorageAccount account = new CloudStorageAccount(creds, useHttps: true);
            CloudBlobClient client = account.CreateCloudBlobClient();
            CloudBlockBlob blob = new CloudBlockBlob(Uri, client);

            var blockList = new List<string>();
            byte[] bytesRead = new byte[FOURMB];
            int numBytesRead = 0;

            MemoryStream ms = new MemoryStream();
            ms.Capacity = FOURMB;
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
                        numBytesRead = await fs.ReadAsync(bytesRead, 0, FOURMB);

                        if (numBytesRead == FOURMB)
                        {
                            await ms.WriteAsync(bytesRead, 0, FOURMB);
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

                    } catch (Exception ex)
                    {
                        ;
                    }
                    
                    blockList.Add(blockId.Base64BlockId);

                } while (!DoneSending);

            }

            await blob.PutBlockListAsync(blockList);

        }

        public async Task UploadBigFileWithCommitsAsync()
        {
            StorageCredentials creds = new StorageCredentials(AcctName, AcctKey);
            CloudStorageAccount account = new CloudStorageAccount(creds, useHttps: true);
            CloudBlobClient client = account.CreateCloudBlobClient();
            CloudBlockBlob blob = new CloudBlockBlob(Uri, client);

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

        public void UploadBigFileWithCommits()
        {
            StorageCredentials creds = new StorageCredentials(AcctName, AcctKey);
            CloudStorageAccount account = new CloudStorageAccount(creds, useHttps: true);
            CloudBlobClient client = account.CreateCloudBlobClient();
            CloudBlockBlob blob = new CloudBlockBlob(Uri, client);

            var blockList = new List<string>();
            byte[] bytesRead = new byte[FOURMB];
            int numBytesRead = 0;

            MemoryStream ms = new MemoryStream();
            ms.Capacity = FOURMB;
            Boolean DoneSending = false;

            string acquiredLeaseId = null;
            string proposedLeaseId = Guid.NewGuid().ToString();

            using (FileStream fs = File.OpenRead(Filename))
            {
                AccessCondition accessCondition = AccessCondition.GenerateEmptyCondition();
                do
                {

                    Console.Write(".");
                    NewBlockId blockId = new NewBlockId();

                    try
                    {
                        if (blockList.Count > 0)
                        {
                            acquiredLeaseId = ((ICloudBlob)blob).AcquireLeaseAsync(TimeSpan.FromMinutes(1), proposedLeaseId).Result;
                            accessCondition = AccessCondition.GenerateLeaseCondition(acquiredLeaseId);
                        }

                        ms.Position = 0;
                        numBytesRead = fs.Read(bytesRead, 0, FOURMB);

                        if (numBytesRead == FOURMB)
                        {
                            ms.Write(bytesRead, 0, FOURMB);
                            Send4mbBlockAsyncWithAccessCondition(blob, ms, blockId, accessCondition).Wait();
                        }
                        else
                        {
                            MemoryStream msLast = new MemoryStream();
                            msLast.Capacity = numBytesRead;

                            msLast.Write(bytesRead, 0, numBytesRead);
                            Send4mbBlockAsyncWithAccessCondition(blob, msLast, blockId, accessCondition).Wait();

                            DoneSending = true;
                        }

                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine();
                        Console.WriteLine($"Error: {ex.Message} :sending block to storage.");
                        continue;
                    }

                    try
                    {
                        blockList.Add(blockId.Base64BlockId);
                        blob.PutBlockListAsync(
                            blockList,
                            accessCondition, 
                            options: new BlobRequestOptions
                            {
                                StoreBlobContentMD5 = true,
                                UseTransactionalMD5 = true
                            },
                            operationContext: new OperationContext()).Wait();
                    } catch (Exception ex)
                    {
                        Console.WriteLine();
                        Console.WriteLine($"Error: {ex.Message} :commiting block list.");
                    } finally
                    {
                        if (acquiredLeaseId != null)
                        {
                            ((ICloudBlob)blob).ReleaseLeaseAsync(accessCondition).Wait();
                            acquiredLeaseId = null;
                        }
                    }

                } while (!DoneSending);

            }


        }

        private async Task Send4mbBlockAsync(CloudBlockBlob blob, MemoryStream ms, NewBlockId blockId)
        {
            ms.Seek(0, SeekOrigin.Begin);
            await blob.PutBlockAsync(
                blockId: blockId.Base64BlockId,
                blockData: ms,
                contentMD5: null);
        }

        private async Task Send4mbBlockAsyncWithAccessCondition(CloudBlockBlob blob, MemoryStream ms, NewBlockId blockId, AccessCondition accessCondition)
        {
            ms.Seek(0, SeekOrigin.Begin);
            await blob.PutBlockAsync(
                blockId: blockId.Base64BlockId,
                blockData: ms,
                contentMD5: null,
                accessCondition: accessCondition,
                options: new BlobRequestOptions
                {
                    StoreBlobContentMD5 = true,
                    UseTransactionalMD5 = true
                },
                operationContext: new OperationContext());
        }

        public async Task CommitEmptyBlockList()
        {
            StorageCredentials creds = new StorageCredentials(AcctName, AcctKey);
            CloudStorageAccount account = new CloudStorageAccount(creds, useHttps: true);
            CloudBlobClient client = account.CreateCloudBlobClient();
            CloudBlockBlob blob = new CloudBlockBlob(Uri, client);

            var blockList = new List<string>();

            await blob.PutBlockListAsync(blockList);
        }
    }
}
