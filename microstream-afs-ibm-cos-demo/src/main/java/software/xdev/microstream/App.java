package software.xdev.microstream;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import one.microstream.storage.types.Storage;
import one.microstream.storage.types.StorageLiveFileProvider;
import software.xdev.microstream.afs.ibm.cos.types.CosConnector;


public class App
{
	
	private static final Logger LOG = LoggerFactory.getLogger(App.class);
	
	public static void main(final String[] args)
	{
		final List<String> stringList = new ArrayList<>();
		LOG.info("List size before loading: {}", stringList.size());
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
		
		return EmbeddedStorage.Foundation(
				Storage.ConfigurationBuilder()
					.setChannelCountProvider(Storage.ChannelCountProvider(4))
					.setStorageFileProvider(
						StorageLiveFileProvider.Builder()
							.setDirectory(cloudFileSystem.ensureDirectoryPath(BUCKET_NAME))
							.createFileProvider()
					)
					.createConfiguration()
			)
			.setRoot(root)
			.start();
	}
	
	public static AmazonS3 createClient(
		final String api_key,
		final String service_instance_id,
		final String endpoint_url,
		final String location)
	{
		final AWSCredentials credentials = new BasicIBMOAuthCredentials(api_key, service_instance_id);
		final ClientConfiguration clientConfig = new ClientConfiguration().withRequestTimeout(-1);
		clientConfig.setUseTcpKeepAlive(true);
		
		final AmazonS3 cos = AmazonS3ClientBuilder.standard()
			.withCredentials(new AWSStaticCredentialsProvider(credentials))
			.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint_url, location))
			.withPathStyleAccessEnabled(true)
			.withClientConfiguration(clientConfig)
			.build();
		
		return cos;
	}
}
