using System;
using System.Linq;
using System.Net;
using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;

namespace S3ToRedshift
{
    public class S3Operations : IS3Operations
    {
        private readonly BasicAWSCredentials _credentials;
        private readonly string _bucket;

        public S3Operations(BasicAWSCredentials credentials, string bucket)
        {
            _credentials = credentials;
            _bucket = bucket;
        }

        public string[] ListFiles(string prefix)
        {
            var response = GetListObjectsResponse(prefix);
            if (response != null && response.S3Objects != null)
            {
                return response.S3Objects.Select(x => x.Key).ToArray();
            }
            return null;
        }
        public ListObjectsResponse GetListObjectsResponse(string prefix, string marker=null)
        {
            var client = AWSClientFactory.CreateAmazonS3Client(_credentials,
                new AmazonS3Config {ServiceURL = "http://s3.amazonaws.com"});

            var request = new ListObjectsRequest
            {
                BucketName = _bucket,
                Prefix = prefix,
                Marker = marker
            };

            return client.ListObjects(request);
        }
        public void MoveFile(string sourcePath,  string destinationPath)
        {
            try
            {
                var request = new CopyObjectRequest
                {
                    DestinationBucket = _bucket,
                    DestinationKey = destinationPath,
                    SourceKey = sourcePath,
                    SourceBucket = _bucket
                };

                using (var s3 = new AmazonS3Client(_credentials.GetCredentials().AccessKey, _credentials.GetCredentials().SecretKey,
                    new AmazonS3Config { ServiceURL = "http://s3.amazonaws.com" }))
                {
                    var response = s3.CopyObject(request);
                    if (response.HttpStatusCode == HttpStatusCode.OK)
                    {
                        s3.DeleteObject(new DeleteObjectRequest() { BucketName = _bucket, Key = sourcePath });
                    }
                }
            }
            catch (Exception ex)
            {
                // maybe log this or something
                throw;
            }
        }
    }
}