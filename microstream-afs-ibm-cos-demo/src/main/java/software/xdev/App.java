package software.xdev;

import java.util.ArrayList;
import java.util.List;

import com.ibm.cloud.objectstorage.ClientConfiguration;
import com.ibm.cloud.objectstorage.auth.AWSCredentials;
import com.ibm.cloud.objectstorage.auth.AWSStaticCredentialsProvider;
import com.ibm.cloud.objectstorage.client.builder.AwsClientBuilder;
import com.ibm.cloud.objectstorage.oauth.BasicIBMOAuthCredentials;
import com.ibm.cloud.objectstorage.services.s3.AmazonS3;
import com.ibm.cloud.objectstorage.services.s3.AmazonS3ClientBuilder;

import one.microstream.afs.blobstore.types.BlobStoreFileSystem;
import one.microstream.storage.embedded.types.EmbeddedStorage;
import one.microstream.storage.embedded.types.EmbeddedStorageManager;
import software.xdev.microstream.afs.ibm.cos.types.CosConnector;


@SuppressWarnings("checkstyle:MagicNumber")
public final class App
{
	private static final String COS_ENDPOINT = ""; // eg "https://s3.us.cloud-object-storage.appdomain.cloud"
	private static final String COS_API_KEY_ID = ""; // eg "0viPHOY7LbLNa9eLftrtHPpTjoGv6hbLD1QalRXikliJ"
	private static final String COS_SERVICE_CRN = "";
		// "crn:v1:bluemix:public:cloud-object-storage:global:a/<CREDENTIAL_ID_AS_GENERATED>:<SERVICE_ID_AS_GENERATED
		// >::"
	private static final String COS_BUCKET_LOCATION = "";  // eg "us"
	private static final String BUCKET_NAME = "";
	
	/**
	 * This function connects to the IBM COS and writes one million String-Entries on it.
	 */
	public static void main(final String[] args)
	{
		final List<String> stringList = new ArrayList<>();
		try(final EmbeddedStorageManager manager = getStorageManager(stringList))
		{
			for(int i = 0; i < 1_000_000; i++)
			{
				stringList.add("Test" + i);
			}
			manager.store(stringList);
		}
	}
	
	public static EmbeddedStorageManager getStorageManager(final Object root)
	{
		final AmazonS3 client = createClient(COS_API_KEY_ID, COS_SERVICE_CRN, COS_ENDPOINT, COS_BUCKET_LOCATION);
		final BlobStoreFileSystem cloudFileSystem = BlobStoreFileSystem.New(
			// use caching connector
			CosConnector.Caching(client)
		);
		
		return EmbeddedStorage.start(
			root,
			cloudFileSystem.ensureDirectoryPath(BUCKET_NAME));
	}
	
	public static AmazonS3 createClient(
		final String apiKey,
		final String serviceInstanceId,
		final String endpointUrl,
		final String location)
	{
		final AWSCredentials credentials = new BasicIBMOAuthCredentials(apiKey, serviceInstanceId);
		final ClientConfiguration clientConfig = new ClientConfiguration().withRequestTimeout(-1);
		clientConfig.setUseTcpKeepAlive(true);
		
		return AmazonS3ClientBuilder.standard()
			.withCredentials(new AWSStaticCredentialsProvider(credentials))
			.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpointUrl, location))
			.withPathStyleAccessEnabled(true)
			.withClientConfiguration(clientConfig)
			.build();
	}
	
	private App()
	{
	}
}
