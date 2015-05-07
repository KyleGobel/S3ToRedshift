namespace S3ToRedshift
{
    public interface IS3Operations
    {
        void MoveFile( string sourcePath,  string destinationPath);
        string[] ListFiles(string prefix);
    }
}