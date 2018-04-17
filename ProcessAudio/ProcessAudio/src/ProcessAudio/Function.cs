/*Copyright © 2018 Amazon Web Services

All rights reserved under the copyright laws of the United States and applicable 
international laws, treaties, and conventions.

You may freely redistribute and use this sample code, with or without modification, 
provided you include the original copyright notice and use restrictions.

Disclaimer: THE SAMPLE CODE IS PROVIDED "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, 
INCLUDING THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR 
PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL AWS OR CONTRIBUTORS BE LIABLE FOR ANY 
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, 
BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, 
OR PROFITS; OR BUSINESS INTERRUPTION) SUSTAINED BY YOU OR A THIRD PARTY, HOWEVER 
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR 
TORT ARISING IN ANY WAY OUT OF THE USE OF THIS SAMPLE CODE, EVEN IF ADVISED OF 
THE POSSIBILITY OF SUCH DAMAGE.
 */


using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;

using System.Net;
using System.IO;
using System.Text;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.S3;
using Amazon.S3.Util;
using Amazon.TranscribeService;
using Amazon.TranscribeService.Model;


// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace ProcessAudio
{

    public class Function
    {
        IAmazonS3 S3Client { get; set; }
        AmazonTranscribeServiceClient transClient { get; set; }


        public Function()
        {
            transClient = new AmazonTranscribeServiceClient();
            S3Client = new AmazonS3Client();
        }


        public Function(IAmazonS3 s3Client)
        {
            this.S3Client = s3Client;
        }


        public async Task<string> FunctionHandler(S3Event evnt, ILambdaContext context)
        {
            var s3Event = evnt.Records?[0].S3;
            if (s3Event == null)
            {
                return null;
            }

            try
            {
                var response = await this.S3Client.GetObjectMetadataAsync(s3Event.Bucket.Name, s3Event.Object.Key);
                Console.WriteLine(s3Event.Bucket.Name.ToString());
                Console.WriteLine(s3Event.Object.Key.ToString());
                await ProcessTranscribe(s3Event.Bucket.Name, s3Event.Object.Key);
                return response.Headers.ContentType;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Console.WriteLine(e.StackTrace);
                throw;
            }
        }
        private async Task ProcessTranscribe(string bucket, string key)
        {
            Settings settings = new Settings();
            settings.ShowSpeakerLabels = true;
            settings.MaxSpeakerLabels = 2;
            settings.VocabularyName = "Vocab";

            Media media = new Media();
            media.MediaFileUri = string.Format("https://s3.us-east-2.amazonaws.com/{0}/{1}", bucket, key);
            CancellationToken token = new CancellationToken();


            StartTranscriptionJobRequest startRequest = new StartTranscriptionJobRequest();
            startRequest.LanguageCode = LanguageCode.EnUS;
            startRequest.Settings = settings;
            startRequest.Media = media;
            startRequest.MediaFormat = MediaFormat.Mp3;
            startRequest.TranscriptionJobName = Guid.NewGuid().ToString();

            StartTranscriptionJobResponse response = await transClient.StartTranscriptionJobAsync(startRequest, token);


            GetTranscriptionJobRequest request = new GetTranscriptionJobRequest();
            request.TranscriptionJobName = startRequest.TranscriptionJobName;

            bool isComplete = false;
            while (!isComplete)
            {
                GetTranscriptionJobResponse response2 = await transClient.GetTranscriptionJobAsync(request);
                if (response2.TranscriptionJob.TranscriptionJobStatus == TranscriptionJobStatus.COMPLETED)
                {
                    //we need to DL the file to S3
                    isComplete = true;

                    WriteFileToS3(response2.TranscriptionJob.Transcript.TranscriptFileUri, startRequest.TranscriptionJobName);
                }
                else if (response2.TranscriptionJob.TranscriptionJobStatus == TranscriptionJobStatus.FAILED)
                {
                    isComplete = true;
                    //need to log the error.
                }
                else
                {
                    System.Threading.Thread.Sleep(5000);//wait 5 seconds and check again
                    //not done yet
                }
            }


        }
        private void WriteFileToS3(string transcriptURI, string jobName)
        {
            var webRequest = WebRequest.Create(transcriptURI);
            string strContent;
            using (var response = webRequest.GetResponse())
            using (var content = response.GetResponseStream())
            using (var reader = new StreamReader(content))
            {
                strContent = reader.ReadToEnd();
            }
            TransformFileToS3(strContent, jobName);
            //add the raw file here. 
            WriteRawFileToS3(strContent, jobName);

            
            
        }

        private bool WriteRawFileToS3(string strContent, string jobName)
        {
             byte[] memstring = Encoding.UTF8.GetBytes(strContent);
             

            using (Stream memStream = new MemoryStream(strContent.Length))
            {
                memStream.Write(memstring,0,memstring.Count());
                // upload to s3
                try
                {
                    AmazonS3Client s3 = new AmazonS3Client(Amazon.RegionEndpoint.USEast2);
                    using (Amazon.S3.Transfer.TransferUtility tranUtility =
                                new Amazon.S3.Transfer.TransferUtility(s3))
                    {

                        tranUtility.Upload(memStream, "raw-transcripts", jobName + ".json");

                        return true;
                    }
                }
                catch (Exception ex)
                {
                    throw ex;
                }

            }
        }

        private bool TransformFileToS3(string strContent, string jobName)
        {
            JObject transcriptJSON = JObject.Parse(strContent);
            JObject sendObject = new JObject(
                new JProperty("JobName",(string)transcriptJSON["jobName"]),
                new JProperty("Text", (string)transcriptJSON["results"]["transcripts"][0]["transcript"]));
            
            // Create file in memory
            byte[] memstring = Encoding.UTF8.GetBytes(sendObject.ToString());

            using (Stream memStream = new MemoryStream(strContent.Length))
            {
                memStream.Write(memstring,0,memstring.Count());
                // upload to s3
                try
                {
                    AmazonS3Client s3 = new AmazonS3Client(Amazon.RegionEndpoint.USEast2);
                    using (Amazon.S3.Transfer.TransferUtility tranUtility =
                                new Amazon.S3.Transfer.TransferUtility(s3))
                    {

                        tranUtility.Upload(memStream, "transcripts", jobName + ".json");

                        return true;
                    }
                }
                catch (Exception ex)
                {
                    throw ex;
                }

            }

        }
    }
}
