package org.cloudera.sa.s3connector;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.ProgressEvent;
import com.amazonaws.services.s3.model.ProgressListener;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.UploadPartRequest;

public class Main {

	public static void main(String[] args) throws IOException,
			InterruptedException {

		System.out.println("0.7");
		
		if (args.length == 0) {
			System.out.println("readFromS3ToDevNull {keyName} {bucketName} {bufferSize -1 for default}");
			System.out.println("readFromS3ToLocalDrive {keyName} {bucketName} {bufferSize -1 for default} {localOutputFile}");
			System.out.println("readFromS3ToHDFS {keyName} {bucketName} {bufferSize -1 for default} {hdfsOutputFile}");
		}
		
		int i = 0;
		String action = args[i++];
		
		if (action.equals("multipartLowLovel")) {
			String keyName = args[i++];
			// String uploadFileName = args[i++];
			String existingBucketName = args[i++];
			String filePath = args[i++];

			multipartLowLevel(keyName, existingBucketName, filePath);
		} else if (action.equals("readFromS3ToDevNull")) {
			String keyName = args[i++];
			String bucketName = args[i++];
			int bufferSize = Integer.parseInt(args[i++]);

			readFromS3ToDevNull(keyName, bucketName, bufferSize);
		} else if (action.equals("readFromS3ToLocalDrive")) {

			String keyName = args[i++];
			String bucketName = args[i++];
			int bufferSize = Integer.parseInt(args[i++]);
			String localFilePath = args[i++];
			
			readFromS3ToLocal(keyName, bucketName, bufferSize, localFilePath);
		} else if (action.equals("readFromS3ToHDFS")) {
			String keyName = args[i++];
			String bucketName = args[i++];
			int bufferSize = Integer.parseInt(args[i++]);
			String hdfsFilePath = args[i++];
			readFromS3ToHDFS(keyName, bucketName, bufferSize, hdfsFilePath);
		}

	}

	private static void readFromS3ToDevNull(String keyName, String bucketName, int bufferSize) throws IOException {
		AmazonS3 s3Client = new AmazonS3Client(new PropertiesCredentials(
				new FileInputStream( new File("./AwsCredentials.properties"))));
		
		S3Object s3Object = s3Client.getObject(bucketName, keyName);
		
		BufferedInputStream inputStream;
		if (bufferSize == -1) {
			inputStream = new BufferedInputStream(s3Object.getObjectContent());
		} else {
			inputStream = new BufferedInputStream(s3Object.getObjectContent(), bufferSize);
		}
		byte[] bytes = new byte[1000000];
		long counter = 0;
		int lastRead = 0;
		System.out.println("Start Reading: DevNull");
		long startTime = System.currentTimeMillis();
		long loopCounter = 0;
		
		while ((lastRead = inputStream.read(bytes)) >= 0) {
			//devnull
			counter += lastRead;
			if (loopCounter++ % 100 == 0) {
				long totalTime = (System.currentTimeMillis() - startTime);
				if ((totalTime/1000) > 0) {
					System.out.println(" Read=[" + (counter/1000000) + " MB] MB/s=[" + (counter/(totalTime/1000))/1000000  + "] Seconds=[" + (totalTime/1000) + "]");
				}		
			}
		}
		long totalTime = (System.currentTimeMillis() - startTime);
		
		System.out.println("Finished Reading: DevNull");
		System.out.println(" Time=[" + totalTime + "]");
		System.out.println(" Bytes=[" + counter + "]");
		if ((totalTime/1000) > 0) {
			System.out.println(" MB/s=[" + (counter/(totalTime/1000))/1000000  + "]");
		}
		
		inputStream.close();
	}
	
	private static void readFromS3ToLocal(String keyName, String bucketName, int bufferSize, String localFilePath) throws IOException {
		AmazonS3 s3Client = new AmazonS3Client(new PropertiesCredentials(
				new FileInputStream( new File("./AwsCredentials.properties"))));
		
		S3Object s3Object = s3Client.getObject(bucketName, keyName);
		
		BufferedInputStream inputStream;
		if (bufferSize == -1) {
			inputStream = new BufferedInputStream(s3Object.getObjectContent());
		} else {
			inputStream = new BufferedInputStream(s3Object.getObjectContent(), bufferSize);
		}
		
		BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(new File(localFilePath)));
		
		byte[] bytes = new byte[1000000];
		long counter = 0;
		int lastRead = 0;
		long loopCounter = 0;
		System.out.println("Start Reading: LocalFile");
		long startTime = System.currentTimeMillis();
		while ((lastRead = inputStream.read(bytes)) >= 0) {
			//devnull
			counter += lastRead;
			try {
				outputStream.write(bytes, 0, lastRead);	
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("lastRead:" + lastRead );
			}
			
			if (loopCounter++ % 100 == 0) {
				long totalTime = (System.currentTimeMillis() - startTime);
				if ((totalTime/1000) > 0) {
					System.out.println(" Read=[" + (counter/1000000) + " MB] MB/s=[" + (counter/(totalTime/1000))/1000000  + "] Seconds=[" + (totalTime/1000) + "]");
				}		
			}
		}
		outputStream.close();
		
		long totalTime = (System.currentTimeMillis() - startTime);
		
		System.out.println("Finished Reading: LocalFile");
		System.out.println(" Time=[" + totalTime + "]");
		System.out.println(" Bytes=[" + counter + "]");
		if ((totalTime/1000) > 0) {
			System.out.println(" Read=[" + counter + "bytes] MB/s=[" + (counter/(totalTime/1000))/1000000  + "] Seconds=[" + (totalTime/1000) + "]");
		}
		
		
		inputStream.close();
	}

	private static void readFromS3ToHDFS(String keyName, String bucketName, int bufferSize, String hdfsFilePath) throws IOException {
		AmazonS3 s3Client = new AmazonS3Client(new PropertiesCredentials(
				new FileInputStream( new File("./AwsCredentials.properties"))));
		
		S3Object s3Object = s3Client.getObject(bucketName, keyName);
		
		BufferedInputStream inputStream;
		if (bufferSize == -1) {
			inputStream = new BufferedInputStream(s3Object.getObjectContent());
		} else {
			inputStream = new BufferedInputStream(s3Object.getObjectContent(), bufferSize);
		}
		
		FileSystem fs = FileSystem.get(new Configuration());
		BufferedOutputStream outputStream = new BufferedOutputStream(fs.create(new Path(hdfsFilePath), true));
		
		byte[] bytes = new byte[1000000];
		long counter = 0;
		int lastRead = 0;
		long loopCounter = 0;
		System.out.println("Start Reading: HdfsFile");
		long startTime = System.currentTimeMillis();
		while ((lastRead = inputStream.read(bytes)) >= 0) {
			//devnull
			counter += lastRead;
			outputStream.write(bytes, 0, lastRead);
			if (loopCounter++ % 100 == 0) {
				long totalTime = (System.currentTimeMillis() - startTime);
				if ((totalTime/1000) > 0) {
					System.out.println(" Read=[" + (counter/1000000) + " MB] MB/s=[" + (counter/(totalTime/1000))/1000000  + "] Seconds=[" + (totalTime/1000) + "]");
				}		
			}
		}
		outputStream.close();
		
		long totalTime = (System.currentTimeMillis() - startTime);
		
		System.out.println("Finished Reading: HdfsFile");
		System.out.println(" Time=[" + totalTime + "]");
		System.out.println(" Bytes=[" + counter + "]");
		if ((totalTime/1000) > 0) {
			System.out.println(" MB/s=[" + (counter/(totalTime/1000))/1000000  + "]");
		}
		
		inputStream.close();
	}
	
	private static void multipartLowLevel(String keyName,
			String existingBucketName, String filePath) throws IOException {

		AmazonS3 s3Client = new AmazonS3Client(new PropertiesCredentials(
				Main.class.getResourceAsStream("AwsCredentials.properties")));

		// Create a list of UploadPartResponse objects. You get one of these
		// for each part upload.
		List<PartETag> partETags = new ArrayList<PartETag>();

		// Step 1: Initialize.
		InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(
				existingBucketName, keyName);

		ObjectMetadata objectMetadata = new ObjectMetadata();
		objectMetadata
				.setServerSideEncryption(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);

		initRequest.setObjectMetadata(objectMetadata);

		InitiateMultipartUploadResult initResponse = s3Client
				.initiateMultipartUpload(initRequest);

		File file = new File(filePath);
		long contentLength = file.length();
		long partSize = 5242880; // Set part size to 5 MB.

		try {
			// Step 2: Upload parts.
			long filePosition = 0;
			for (int i = 1; filePosition < contentLength; i++) {
				// Last part can be less than 5 MB. Adjust part size.
				partSize = Math.min(partSize, (contentLength - filePosition));

				// Create request to upload a part.
				UploadPartRequest uploadRequest = new UploadPartRequest()
						.withBucketName(existingBucketName).withKey(keyName)
						.withUploadId(initResponse.getUploadId())
						.withPartNumber(i).withFileOffset(filePosition)
						.withInputStream(new FileInputStream(file))
						.withPartSize(partSize);

				// Upload part and add response to our list.
				partETags.add(s3Client.uploadPart(uploadRequest).getPartETag());

				filePosition += partSize;
			}

			// Step 3: Complete.
			CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(
					existingBucketName, keyName, initResponse.getUploadId(),
					partETags);

			s3Client.completeMultipartUpload(compRequest);
		} catch (Exception e) {
			s3Client.abortMultipartUpload(new AbortMultipartUploadRequest(
					existingBucketName, keyName, initResponse.getUploadId()));
		}
	}

	private static void multipartUpload(String keyName,
			String existingBucketName, String filePath) throws IOException,
			InterruptedException {
		TransferManager tm = new TransferManager(new PropertiesCredentials(
				Main.class.getResourceAsStream("AwsCredentials.properties")));

		// For more advanced uploads, you can create a request object
		// and supply additional request parameters (ex: progress listeners,
		// canned ACLs, etc.)
		PutObjectRequest request = new PutObjectRequest(existingBucketName,
				keyName, new File(filePath));

		// You can ask the upload for its progress, or you can
		// add a ProgressListener to your request to receive notifications
		// when bytes are transferred.
		request.setProgressListener(new ProgressListener() {
			public void progressChanged(ProgressEvent event) {
				System.out.println("Transferred bytes: "
						+ event.getBytesTransfered());
			}
		});

		// TransferManager processes all transfers asynchronously,
		// so this call will return immediately.
		Upload upload = tm.upload(request);

		try {
			// You can block and wait for the upload to finish
			upload.waitForCompletion();
		} catch (AmazonClientException amazonClientException) {
			System.out.println("Unable to upload file, upload aborted.");
			amazonClientException.printStackTrace();
		}
	}

	private static void uploadSingleFile(String bucketName, String keyName,
			String uploadFileName) throws IOException {
		AmazonS3 s3client = new AmazonS3Client(new PropertiesCredentials(
				Main.class.getResourceAsStream("AwsCredentials.properties")));
		try {
			System.out.println("Uploading a new object to S3 from a file\n");
			File file = new File(uploadFileName);
			s3client.putObject(new PutObjectRequest(bucketName, keyName, file));

		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which "
					+ "means your request made it "
					+ "to Amazon S3, but was rejected with an error response"
					+ " for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which "
					+ "means the client encountered "
					+ "an internal error while trying to "
					+ "communicate with S3, "
					+ "such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
	}
}