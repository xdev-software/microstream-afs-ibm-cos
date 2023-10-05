/*
 * Copyright © 2023 XDEV Software (https://xdev.software)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package software.xdev.microstream.afs.ibm.cos.types;


import com.ibm.cloud.objectstorage.client.builder.AwsClientBuilder;
import com.ibm.cloud.objectstorage.services.s3.AmazonS3;
import com.ibm.cloud.objectstorage.services.s3.AmazonS3Client;
import com.ibm.cloud.objectstorage.services.s3.AmazonS3ClientBuilder;

import one.microstream.afs.blobstore.types.BlobStoreFileSystem;
import one.microstream.afs.types.AFileSystem;
import one.microstream.configuration.types.Configuration;
import software.xdev.microstream.afs.ibm.types.IbmFileSystemCreator;


public class CosFileSystemCreator extends IbmFileSystemCreator
{
	public CosFileSystemCreator()
	{
		super();
	}
	
	@Override
	public AFileSystem create(
		final Configuration configuration
	)
	{
		final Configuration s3Configuration = configuration.child("ibm.cos");
		if(s3Configuration == null)
		{
			return null;
		}
		
		final AwsClientBuilder<AmazonS3ClientBuilder, AmazonS3> clientBuilder = AmazonS3Client.builder();
		this.populateBuilder(clientBuilder, s3Configuration);
		
		final AmazonS3 client = clientBuilder.build();
		final boolean cache = configuration.optBoolean("cache").orElse(true);
		final CosConnector connector = cache
			? CosConnector.Caching(client)
			: CosConnector.New(client);
		return BlobStoreFileSystem.New(connector);
	}
}
