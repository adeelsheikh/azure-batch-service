using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Auth;
using Microsoft.Azure.Batch.Common;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace BatchServicePlayground
{
    internal class Program
    {
        private const string ApplicationId = "HelloWorldApp";

        public static void Main()
        {
            MainAsync().Wait();
        }

        protected static async Task MainAsync()
        {
            using (var client = GetBatchClient())
            {
                const string poolId = "Hello_World_Pool";
                const string jobId = "Hello_World_Job";
            
                await CreatePoolIfNotExistAsync(client, poolId);

                var pool = await client.PoolOperations.GetPoolAsync(poolId);

                if (pool.AllocationState != AllocationState.Steady)
                {
                    Console.ForegroundColor = ConsoleColor.Cyan;
                    Console.WriteLine("Nodes are not ready to create job / task. Please try again later.");

                    return;
                }
                else
                {
                    var validNodes = 0;

                    foreach (var node in pool.ListComputeNodes())
                    {
                        if (node.State == ComputeNodeState.Running 
                            || node.State == ComputeNodeState.Idle
                            || node.State == ComputeNodeState.WaitingForStartTask
                            || node.State == ComputeNodeState.StartTaskFailed)
                        {
                            validNodes++;
                        }
                    }

                    if (validNodes == 0)
                    {
                        Console.ForegroundColor = ConsoleColor.Cyan;
                        Console.WriteLine("Pool status is steady but nodes are still not ready to create job / task. Please try again later.");

                        return;
                    }
                }

                await CreateJobIfNotExistAsync(client, jobId, poolId);
                await AddTaskAsync(client, jobId);
            }
        }

        private static async Task AddTaskAsync(BatchClient client, string jobId)
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine("Adding task...");

            var packagePath = $"%AZ_BATCH_APP_PACKAGE_{ApplicationId.ToUpper()}%";
            var taskCommand = $"cmd /c {packagePath}\\HelloWorld.exe";
            var task = new CloudTask($"Hello_World_Task_{DateTime.Now:yyyyMMdd_hh_mm_ss}", taskCommand);

            await client.JobOperations.AddTaskAsync(jobId, task);

            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("Task successfully added!");
            Console.WriteLine();
        }

        private static async Task CreateJobIfNotExistAsync(BatchClient client, string jobId, string poolId)
        {
            CloudJob job;

            try
            {
                var pool = await client.PoolOperations.GetPoolAsync(poolId);
                
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine("Checking if job exists...");
                
                job = await client.JobOperations.GetJobAsync(jobId);

                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine("Job already exists!");
                Console.WriteLine();

                return;
            }
            catch (BatchException ex)
            {
                if (ex.Message.Equals("Operation returned an invalid status code 'NotFound'"))
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine("Creating job...");
                }
                else
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    throw;
                }
            }

            job = client.JobOperations.CreateJob(jobId, new PoolInformation { PoolId = poolId });
            await job.CommitAsync();

            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("Job successfully created!");
            Console.WriteLine();
        }

        private static async Task CreatePoolIfNotExistAsync(BatchClient client, string poolId)
        {
            CloudPool pool;

            try
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine("Checking if pool exists...");
                
                pool = await client.PoolOperations.GetPoolAsync(poolId);

                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine("Pool already exists!");
                Console.WriteLine();

                return;
            }
            catch (BatchException ex)
            {
                if (ex.Message.Equals("Operation returned an invalid status code 'NotFound'"))
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine("Creating instances pool...");
                }
                else
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    throw;
                }
            }

            var imageRef = new ImageReference(
                publisher: "MicrosoftWindowsServer",
                offer: "WindowsServer",
                sku: "2019-Datacenter",
                version: "latest");

            var vmConfiguration = new VirtualMachineConfiguration(imageRef, "batch.node.windows amd64");

            pool = client.PoolOperations.CreatePool(
                poolId: poolId,
                virtualMachineSize: "STANDARD_A1_v2",
                virtualMachineConfiguration: vmConfiguration,
                targetDedicatedComputeNodes: 1);

            pool.ApplicationPackageReferences = new List<ApplicationPackageReference>
            {
                new ApplicationPackageReference
                {
                    ApplicationId = ApplicationId,
                    Version = "1.0"
                }
            };

            await pool.CommitAsync();

            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("Pool successfully created!");
            Console.WriteLine();
        }

        private static BatchClient GetBatchClient()
        {
            var batchAccountName = "[batch account name]";
            var batchAccountLocation = "[location e.g., westeurope]";
            var batchAccountUrl = $"https://{batchAccountName}.{batchAccountLocation}.batch.azure.com";
            var batchAccountKey = "[batch account primary / secondary key]";

            var credentials = new BatchSharedKeyCredentials(batchAccountUrl, batchAccountName, batchAccountKey);

            return BatchClient.Open(credentials);
        }
    }
}
