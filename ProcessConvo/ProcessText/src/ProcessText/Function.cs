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
using System.IO;
using System.Text;

using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.S3;
using Amazon.S3.Util;
using Amazon.S3.Model;
using Amazon.Comprehend;
using Amazon.Comprehend.Model;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using CsvHelper;




[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace ProcessText
{
    public class Function
    {
        IAmazonS3 S3Client { get; set; }
        
        
        public Function()
        {
            S3Client = new AmazonS3Client();
        }

        
        public Function(IAmazonS3 s3Client)
        {
            this.S3Client = s3Client;
        }
        
        
        public async Task<string> FunctionHandler(S3Event evnt, ILambdaContext context)
        {
            var s3Event = evnt.Records?[0].S3;
            if(s3Event == null)
            {
                return null;
            }

            try
            {
                var response = await this.S3Client.GetObjectMetadataAsync(s3Event.Bucket.Name, s3Event.Object.Key);
                Console.WriteLine(s3Event.Bucket.Name.ToString());
                Console.WriteLine(s3Event.Object.Key.ToString());

                var tmp = await GetComprehendData(await GetFileText(s3Event.Bucket.Name.ToString(), s3Event.Object.Key.ToString()),s3Event.Object.Key.ToString());


                return response.Headers.ContentType;
            }
            catch(Exception e)
            {
                Console.WriteLine(e.Message);
                Console.WriteLine(e.StackTrace);
                throw;
            }
        }
        private async Task<string> GetFileText(string bucketName, string key)
        {
            IAmazonS3 client;
            string strContent = default(string);
            using (client = new AmazonS3Client(Amazon.RegionEndpoint.USEast2))
            {
                GetObjectRequest request = new GetObjectRequest
                {
                    BucketName = bucketName,
                    Key = key
                };

                using (GetObjectResponse response = await client.GetObjectAsync(request))
                {
                    using (var reader = new StreamReader(response.ResponseStream))
                    {
                        strContent = reader.ReadToEnd();
                    }
                }
            }
            return strContent;
        }
       private async Task<string> GetComprehendData(string transcriptText, string key)
        {
            JObject transcriptJSON = JObject.Parse(transcriptText);
            string test = (string)transcriptJSON["Text"];
            AmazonComprehendClient client = new AmazonComprehendClient();


            DetectEntitiesRequest entitiesRequest = new DetectEntitiesRequest();
            entitiesRequest.LanguageCode = LanguageCode.En;
            entitiesRequest.Text = test;





            DetectSentimentRequest sentimentRequest = new DetectSentimentRequest();
            sentimentRequest.LanguageCode = LanguageCode.En;
            sentimentRequest.Text = test;

            DetectKeyPhrasesRequest keyPhrasesRequest = new DetectKeyPhrasesRequest();
            keyPhrasesRequest.LanguageCode = LanguageCode.En;
            keyPhrasesRequest.Text = test;

            DetectEntitiesResponse entitiesResponse = await client.DetectEntitiesAsync(entitiesRequest);
            DetectSentimentResponse setimentResponse = await client.DetectSentimentAsync(sentimentRequest);
            DetectKeyPhrasesResponse keyPhrasesResponse = await client.DetectKeyPhrasesAsync(keyPhrasesRequest);


            CreateKeyPhraseCSV(key, keyPhrasesResponse);
            CreateSetimentJSON(key, setimentResponse);



            //now send the file to s3

            //we need to write two different files, one for setiment and one for Key Phrases.

            return string.Empty;
        }

       
        private void CreateKeyPhraseCSV(string key, DetectKeyPhrasesResponse keyPhrasesResponse)
        {
            try
            {

                using (MemoryStream memStream = new MemoryStream())
                {
                    var csv = new CsvWriter(new StreamWriter(memStream, Encoding.UTF8));

                   
                    foreach (var a in keyPhrasesResponse.KeyPhrases)
                    {
                        dynamic keyPhrase = new System.Dynamic.ExpandoObject();
                        keyPhrase.Id = key.Replace(".json", "");
                        keyPhrase.Score = a.Score;
                        keyPhrase.Text = a.Text;
                        csv.WriteRecord<dynamic>(keyPhrase);
                        csv.NextRecord();
                        csv.Flush();

                    }

                    SendFileToS3(memStream, @"athena-data/key-phrases", key.Replace(".json",".csv"));

                }
            }
            catch (Exception ex)
            {
                var sdf = "test";
            }
        }

        private void CreateSetimentJSON(string key, DetectSentimentResponse setimentResponse)
        {
            try
            {

                using (MemoryStream memStream = new MemoryStream())
                {

                    dynamic setiment = new JObject();
                    setiment.Id = key.Replace(".json", "");
                    setiment.Setiment = setimentResponse.Sentiment.ToString();
                    setiment.MixedScore = setimentResponse.SentimentScore.Mixed;
                    setiment.NegativeScore = setimentResponse.SentimentScore.Negative;
                    setiment.NeutralScore = setimentResponse.SentimentScore.Neutral;
                    setiment.PostiveScore = setimentResponse.SentimentScore.Positive;

                    string json = ((JObject)setiment).ToString(Formatting.None);
                    byte[] jsonBytes = Encoding.UTF8.GetBytes(json);
                    memStream.Write(jsonBytes, 0, jsonBytes.Length);
                    SendFileToS3(memStream, @"athena-data/setiment", key);

                }
            }
            catch (Exception ex)
            {
                var sdf = "test";
            }
        }

        private static bool SendFileToS3(MemoryStream memStream, string bucket, string key)
        {
            try
            {
                AmazonS3Client s3 = new AmazonS3Client(Amazon.RegionEndpoint.USEast2);
                using (Amazon.S3.Transfer.TransferUtility tranUtility =
                              new Amazon.S3.Transfer.TransferUtility(s3))
                {
                    tranUtility.Upload(memStream, bucket, key);

                    return true;
                }
            }
            catch (Exception ex)
            {
                return false;
            }
        }
    }
}
