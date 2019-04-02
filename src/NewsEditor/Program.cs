using System;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Threading.Tasks;

namespace NewsEditor
{
    public class Program
    {       
        private const string envVarName = "STORAGE_CONNECTION_STRING";

        private const string containerName = "mslearn-blob-concurrency-demo";
        private const string blobName = "newsStory.txt";

        private static string connectionString;

        public static async Task Main()
        {
            // This program is a demonstration of last-writer-wins concurrency behavior in Azure
            // Blob Storage. It simulates two news reporters using a text editing web application
            // that views and stores news articles in blobs. In this demo, the last reporter to
            // save their changes has their work preserved, and is never made aware that they have
            // overwritten someone else's work.
            //
            // In this scenario, the newsroom chief first uses the editing application to create
            // notes for new stories.When he is finished, he saves his changes, which uploads them to a new blob.
            // Then he requests that the story be assigned to a reporter. A reporter assigned to a story
            // loads the notes, overwrites them with his story, and saves them.
            // 
            // In this simulation, two reporters are assigned to a story at the same time by mistake.
            // Neither one knows about the other. Reporter A begins work quickly, retrieving the notes
            // and beginning to write a long story. Reporter B begins soon after, but works fast
            // and finishes first. Reporter B saves his changes, committing them to the blob.
            // Later, Reporter A finishes her story and saves it.
            //
            // The editing application used by the reporters does not actively manage
            // concurrency. As a result, Reporter A never realizes that someone else saved work to the blob
            // while she was writing, and Reporter B's work is lost.
            //
            // In this program, each user accessing the blob uses a separately-initialized CloudBlobClient,
            // to represent that they are all working separately.

            // Ensure that the storage account is ready
            CloudBlobContainer container;
            try
            {
                connectionString = Environment.GetEnvironmentVariable(envVarName);
                container = CloudStorageAccount.Parse(connectionString)
                    .CreateCloudBlobClient()
                    .GetContainerReference(containerName);

                await container.CreateIfNotExistsAsync();
            }
            catch (Exception)
            {
                var msg = $"Storage account not found. Ensure that the environment variable {envVarName}" +
                    " is set to a valid Azure Storage connection string and that the storage account exists.";
                Console.WriteLine(msg);
                return;
            }
                       
            // First, the newsroom chief writes the story notes to the blob
            await SimulateChief();
            Console.WriteLine();

            await Task.Delay(TimeSpan.FromSeconds(2));

            // Next, two reporters begin work on the story at the same time, one starting soon after the other
            var reporterA = SimulateReporter("Reporter A", writingTime: TimeSpan.FromSeconds(12));
            await Task.Delay(TimeSpan.FromSeconds(4));
            var reporterB = SimulateReporter("Reporter B", writingTime: TimeSpan.FromSeconds(4));

            await Task.WhenAll(reporterA, reporterB);
            await Task.Delay(TimeSpan.FromSeconds(2));

            Console.WriteLine();
            Console.WriteLine("=============================================");
            Console.WriteLine();
            Console.WriteLine("Reporters have finished, here's the story saved to the blob:");

            var story = await container.GetBlockBlobReference(blobName).DownloadTextAsync();

            Console.WriteLine(story);
        }

        // This method simulates the newsroom chief writing story notes to a new blob
        // prior to authors being assigned to the story
        private static async Task SimulateChief()
        {
            var blob = CloudStorageAccount.Parse(connectionString)
                .CreateCloudBlobClient()
                .GetContainerReference(containerName)
                .GetBlockBlobReference(blobName);

            var notes = "[[CHIEF'S STORY NOTES]]";
            await blob.UploadTextAsync(notes);
            Console.WriteLine($"The newsroom chief has saved story notes to the blob {containerName}/{blobName}");
        }

        // This method simulates what happens inside the news editing application when
        // a reporter loads a file in the editor, makes changes and saves it back
        private static async Task SimulateReporter(string authorName, TimeSpan writingTime)
        {
            // First, the reporter retrieves the current contents
            Console.WriteLine($"{authorName} begins work");
            var blob = CloudStorageAccount.Parse(connectionString)
                .CreateCloudBlobClient()
                .GetContainerReference(containerName)
                .GetBlockBlobReference(blobName);

            var contents = await blob.DownloadTextAsync();
            Console.WriteLine($"{authorName} loads the file and sees the following content: \"{contents}\"");

            // Next, the author writes their story. This takes some time.
            Console.WriteLine($"{authorName} begins writing their story...");
            await Task.Delay(writingTime);
            Console.WriteLine($"{authorName} has finished writing their story");

            // Finally, they save their story back to the blob.
            var story = $"[[{authorName.ToUpperInvariant()}'S STORY]]";
            await blob.UploadTextAsync(story);
            Console.WriteLine($"{authorName} has saved their story to Blob storage. New blob contents: \"{story}\"");
        }      
    }
}
