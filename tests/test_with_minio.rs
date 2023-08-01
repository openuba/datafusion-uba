#[cfg(test)]
mod tests {
    use datafusion::datasource::file_format::file_type::{FileType, GetExt};
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::listing::ListingOptions;
    use datafusion::error::Result;
    use datafusion::prelude::SessionContext;
    use object_store::aws::AmazonS3Builder;
    use std::sync::Arc;
    use url::Url;

    /// docker run \
    /// --detach \
    /// --rm \
    /// --publish 9000:9000 \
    /// --publish 9001:9001 \
    /// --name minio \
    /// --volume "$(pwd)/parquet-testing:/data" \
    /// --env "MINIO_ROOT_USER=AKIAIOSFODNN7EXAMPLE" \
    /// --env "MINIO_ROOT_PASSWORD=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
    /// minio/minio:RELEASE.2022-10-05T14-58-27Z server /data \
    /// --console-address ":9001"
    // #[allow(dead_code)]
    // #[tokio::test]
    async fn query_with_objectstore() -> Result<()> {
        // create local execution context
        let ctx = SessionContext::new();

        //enter region and bucket to which your credentials have GET and PUT access
        let region = "";
        let bucket_name = "uba";

        let s3 = AmazonS3Builder::new()
            .with_endpoint("http://localhost:9000")
            .with_bucket_name(bucket_name)
            .with_region(region)
            .with_access_key_id("AKIAIOSFODNN7EXAMPLE")
            .with_secret_access_key("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
            .with_allow_http(true)
            .build()?;

        let path = format!("s3://{bucket_name}");
        let s3_url = Url::parse(&path).unwrap();
        let arc_s3 = Arc::new(s3);
        ctx.runtime_env()
            .register_object_store(&s3_url, arc_s3.clone());

        let path = format!("s3://{bucket_name}/event/");
        let file_format = ParquetFormat::default().with_enable_pruning(Some(true));
        let listing_options = ListingOptions::new(Arc::new(file_format))
            .with_file_extension(FileType::PARQUET.get_ext());
        ctx.register_listing_table("event", &path, listing_options, None, None)
            .await?;

        // execute the query
        let _df = ctx.sql("SELECT * from event").await?.show_limit(10);
        Ok(())
    }
}
