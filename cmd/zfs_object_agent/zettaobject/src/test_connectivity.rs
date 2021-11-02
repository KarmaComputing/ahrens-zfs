use crate::{ObjectAccess, ObjectAccessStatType};
use rand::Rng;
use std::time::Duration;

// Test by writing and deleting an object.
async fn do_test_connectivity(object_access: &ObjectAccess) -> anyhow::Result<()> {
    let num: u64 = rand::thread_rng().gen();
    let file = format!("test/test_connectivity_{}", num);
    let content = "test connectivity to S3".as_bytes().to_vec();

    object_access
        .put_object_timed(
            file.clone(),
            content.into(),
            ObjectAccessStatType::MetadataPut,
            Some(Duration::from_secs(30)),
        )
        .await?;

    object_access.delete_object(file).await;

    Ok(())
}

pub fn test_connectivity(
    endpoint: String,
    region: String,
    bucket: String,
    aws_access_key_id: Option<String>,
    aws_secret_access_key: Option<String>,
    aws_instance_profile: bool,
) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .thread_name("zoa_test_connectivity")
        .build()
        .unwrap()
        .block_on(async move {
            let client = if aws_instance_profile {
                ObjectAccess::get_client_with_instance_profile(&endpoint, &region)
            } else {
                // Both aws_access_key_id and aws_secret_access_key should also be specified.
                ObjectAccess::get_client_with_creds(
                    &endpoint,
                    &region,
                    aws_access_key_id.unwrap().as_str(),
                    aws_secret_access_key.unwrap().as_str(),
                )
            };
            let object_access =
                ObjectAccess::from_client(client, &bucket, false, &endpoint, &region);

            std::process::exit(match do_test_connectivity(&object_access).await {
                Err(_) => {
                    eprintln!("Connectivity test failed.");
                    1
                }
                Ok(_) => {
                    println!("Connectivity test succeeded.");
                    0
                }
            });
        });
}
