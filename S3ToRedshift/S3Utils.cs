using System;
using System.Security.Cryptography.X509Certificates;
using Amazon.OpsWorks.Model;

namespace S3ToRedshift
{
    public class Redshift
    {
        private readonly IPattern _pattern;

        public Redshift(IPattern pattern)
        {
            _pattern = pattern;
        }

        public void CopyFromS3(RedshiftCopyStrategy strategy, RedshiftCopyConfig config)
        {
            _pattern.PreCopyOperation(config);

            
        }
    }

    public class RedshiftCopyConfig
    {
        public string Bucket { get; set; }
        public string Prefix { get; set; }
        public string EntityName { get; set; }
        public string TableName { get; set; }
        public string JsonPathsS3Path { get; set; }
    }

    public class AdkPattern : IPattern
    {
        private readonly IS3Operations _ops;

        public AdkPattern(IS3Operations ops)
        {
            _ops = ops;
        }

        public void Setup(RedshiftCopyConfig config)
        {
            var dirs = BuildDirectories(config.Bucket, config.Prefix, config.EntityName);
            MoveFilesToDirectory(_ops, dirs.Logdrop, dirs.Processing);
        }

        public void MoveFilesToDirectory(IS3Operations ops, string sourceDir, string destDir)
        {
            var files = ops.ListFiles(sourceDir);
            foreach (var file in files)
            {
                var dest = file.Replace(sourceDir, destDir);
                ops.MoveFile(file, dest);
            }
        }
        public AdkPatternDataDirectories BuildDirectories(string bucket, string prefix, string entityName)
        {
            return new AdkPatternDataDirectories
            {
                Logdrop = S3Utils.PathCombine(prefix, "logs/logdrop/", entityName),
                Processing = S3Utils.PathCombine(prefix, "logs/processing/", entityName),
                Completed = S3Utils.PathCombine(prefix, "logs/completed/", entityName),
                Error = S3Utils.PathCombine(prefix, "logs/error/", entityName)
            };
        }

        public void Copy()
        {
             
        }

        public void ErrorCopyOperation()
        {
            throw new NotImplementedException();
        }

   
    }

    public interface IPattern
    {
        void Setup(RedshiftCopyConfig config);
        void Copy();
        void Completed();
        void Error();

    }

    public class S3Utils
    {
        public static string PathCombine(params string[] paths)
        {
            var path = string.Empty;
            if (paths != null && paths.Length > 0)
            {
                path = paths[0];
                for (var i = 1; i < paths.Length; i++)
                {
                    if (!string.IsNullOrEmpty(paths[i]))
                    {
                        path = string.Format("{0}/{1}", path.TrimEnd('/'), paths[i]).TrimStart('/');
                    }
                }
            }
            return path;
        }
    }
}