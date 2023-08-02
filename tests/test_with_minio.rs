const ACCESS_KEY_ID: &str = "minioadmin";
const SECRET_ACCESS_KEY: &str = "minioadmin";
const TEST_BUCKET_NAME: &str = "uba";
#[cfg(test)]
#[ctor::ctor]
fn init() {
    let _ = env_logger::try_init();
}

#[cfg(test)]
mod tests {
    use crate::{ACCESS_KEY_ID, SECRET_ACCESS_KEY, TEST_BUCKET_NAME};
    use aws_config::SdkConfig;
    use aws_credential_types::provider::SharedCredentialsProvider;
    use aws_sdk_s3::config::{Credentials, Region};
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::Client;
    use datafusion::datasource::file_format::file_type::{FileType, GetExt};
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::listing::ListingOptions;
    use datafusion::error::Result;
    use datafusion::prelude::SessionContext;
    use datafusion_uba::{assert_batches_eq, test_util};
    use object_store::aws::AmazonS3Builder;
    use std::path::Path;
    use std::sync::Arc;
    use testcontainers::clients;
    use testcontainers::images::minio::MinIO;
    use url::Url;

    #[tokio::test]
    async fn test_with_minio() -> Result<()> {
        let docker = clients::Cli::default();
        let minio = docker.run(MinIO::default());
        let port = minio.get_host_port_ipv4(9000);

        let file_name = "event.parquet";
        let key = format!("event/{file_name}");
        let client = prepare_aws_client(port);

        prepare_data(
            format!("{}/{file_name}", test_util::parquet_test_data()).as_str(),
            key.as_str(),
            &client,
        )
        .await;

        let resp = client.list_objects_v2().bucket("uba").send().await.unwrap();

        let object = resp.contents().unwrap().get(0).unwrap();
        assert_eq!(key, object.key().unwrap());
        Ok(())
    }

    #[tokio::test]
    async fn query_with_objectstore() -> Result<()> {
        let docker = clients::Cli::default();
        let minio = docker.run(MinIO::default());
        let port = minio.get_host_port_ipv4(9000);

        let file_name = "event.parquet";
        let client = prepare_aws_client(port);

        prepare_data(
            format!("{}/{file_name}", test_util::parquet_test_data()).as_str(),
            format!("event/{file_name}").as_str(),
            &client,
        )
        .await;
        let ctx = SessionContext::new();

        let s3 = AmazonS3Builder::new()
            .with_endpoint(format!("http://localhost:{}", port))
            .with_bucket_name(TEST_BUCKET_NAME)
            .with_region("DUMMY")
            .with_access_key_id(ACCESS_KEY_ID)
            .with_secret_access_key(SECRET_ACCESS_KEY)
            .with_allow_http(true)
            .build()?;

        let path = format!("s3://{TEST_BUCKET_NAME}");
        let s3_url = Url::parse(&path).unwrap();
        let arc_s3 = Arc::new(s3);
        ctx.runtime_env()
            .register_object_store(&s3_url, arc_s3.clone());

        let path = format!("s3://{TEST_BUCKET_NAME}/event/");
        let file_format = ParquetFormat::default().with_enable_pruning(Some(true));
        let listing_options = ListingOptions::new(Arc::new(file_format))
            .with_file_extension(FileType::PARQUET.get_ext());
        ctx.register_listing_table("event", &path, listing_options, None, None)
            .await?;

        // execute the query
        let actual = ctx
            .sql("SELECT count(1) from event")
            .await?
            .collect()
            .await?;
        #[rustfmt::skip]
            let expected = vec![
            "+-----------------+",
            "| COUNT(Int64(1)) |",
            "+-----------------+",
            "| 841039          |",
            "+-----------------+",
        ];
        assert_batches_eq!(expected, &actual);
        Ok(())
    }

    fn prepare_aws_client(port: u16) -> Client {
        let credentials =
            Credentials::new(ACCESS_KEY_ID, SECRET_ACCESS_KEY, None, None, "example");
        let config = SdkConfig::builder()
            .endpoint_url(format!("http://127.0.0.1:{}", port))
            .region(Region::new("DUMMY"))
            .credentials_provider(SharedCredentialsProvider::new(credentials))
            .build();
        Client::new(&config)
    }

    async fn prepare_data(file_name: &str, key: &str, client: &Client) {
        client
            .create_bucket()
            .bucket(TEST_BUCKET_NAME)
            .send()
            .await
            .unwrap();

        let body = ByteStream::from_path(Path::new(file_name)).await.unwrap();
        client
            .put_object()
            .bucket(TEST_BUCKET_NAME)
            .key(key)
            .body(body)
            .send()
            .await
            .unwrap();
    }
}
