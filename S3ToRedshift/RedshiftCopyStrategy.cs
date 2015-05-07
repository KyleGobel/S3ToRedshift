namespace S3ToRedshift
{
    public enum RedshiftCopyStrategy
    {
        Copy = 0,
        Merge = 1,
        JsonPathsMerge = 2
    }
}