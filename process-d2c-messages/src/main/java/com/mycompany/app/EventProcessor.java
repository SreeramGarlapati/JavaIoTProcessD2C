package com.mycompany.app;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.concurrent.ExecutionException;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventprocessorhost.*;
import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.*;
import com.microsoft.windowsazure.services.servicebus.*;
import com.microsoft.windowsazure.services.servicebus.models.BrokeredMessage;

public class EventProcessor implements IEventProcessor {
  public static CloudBlobContainer blobContainer;
  public static ServiceBusContract serviceBusContract;

  // final private int MAX_BLOCK_SIZE = 4 * 1024 * 1024;
  // Use smaller value to test.
  final private int MAX_BLOCK_SIZE = 4 * 1024;
  final private Duration MAX_CHECKPOINT_TIME = Duration.ofHours(1);
  private ByteArrayOutputStream toAppend =new ByteArrayOutputStream(MAX_BLOCK_SIZE);

  private Instant start = Instant.now();
  private EventData latestEventData;
   
  @Override
  public void onOpen(PartitionContext context) throws Exception {
	System.out.println("SAMPLE: Partition " + context.getPartitionId()
		+ " is opening");
 }

  @Override
  public void onClose(PartitionContext context, CloseReason reason)
	  throws Exception {
	System.out.println("SAMPLE: Partition " + context.getPartitionId()
		+ " is closing for reason " + reason.toString());
  }

  @Override
  public void onError(PartitionContext context, Throwable error) {
	System.out.println("SAMPLE: Partition " + context.getPartitionId()
		+ " onError: " + error.toString());
  }

  @Override
  public void onEvents(PartitionContext context, Iterable<EventData> messages)
	  throws Exception {
	
	for (EventData eventData : messages) {
	  byte[] data = eventData.getBody();

	  if (eventData.getProperties().containsKey("messageType")
		  && eventData.getProperties().get("messageType").equals("interactive")) {
		
		String messageId = (String) eventData.getSystemProperties().get("message-id");
		BrokeredMessage message = new BrokeredMessage(data);
		message.setMessageId(messageId);
		serviceBusContract.sendQueueMessage("d2ctutorial", message);
		//System.out.println("##### - Received and processed interactive message.");
		continue;
	  }
	  
	  if (toAppend.size() + data.length > MAX_BLOCK_SIZE || Duration.between(start, Instant.now()).compareTo(MAX_CHECKPOINT_TIME) > 0) {
		AppendAndCheckPoint(context);
	  }
	  toAppend.write(data);
	  
	  // Store the most recent message to enable access to the offset
	  // for checkpointing and generating a block id.
	  latestEventData = eventData;
	  
	}
  }

  private void AppendAndCheckPoint(PartitionContext context) throws URISyntaxException, StorageException, IOException, IllegalArgumentException, InterruptedException, ExecutionException {
	String currentOffset = latestEventData.getSystemProperties().getOffset();
	Long currentSequence = latestEventData.getSystemProperties().getSequenceNumber();
	System.out.printf("\nAppendAndCheckpoint using partition: %s, offset: %s, sequence: %s\n", context.getPartitionId(), currentOffset, currentSequence);
	
	Long blockId = Long.parseLong(currentOffset);
	String blockIdString = String.format("startSeq:%1$025d", blockId);
	String encodedBlockId = Base64.getEncoder().encodeToString(
		blockIdString.getBytes(StandardCharsets.US_ASCII));
	BlockEntry block = new BlockEntry(encodedBlockId);
	
	String blobName = String.format("iothubd2c_%s", context.getPartitionId());
	CloudBlockBlob currentBlob = blobContainer.getBlockBlobReference(blobName);
	
	// Look at efficiency of converting output to input buffer?
	currentBlob.uploadBlock(block.getId(),
	  new ByteArrayInputStream(toAppend.toByteArray()), toAppend.size());
	ArrayList<BlockEntry> blockList = currentBlob.downloadBlockList();

	if (currentBlob.exists()) { 
	  // Check if we should append new block or overwrite existing block
	  BlockEntry last = blockList.get(blockList.size()-1);
	  if (blockList.size() > 0 && !last.getId().equals(block.getId())) {
		System.out.printf("Appending block %s to blob %s\n", blockId, blobName);
		blockList.add(block);
	  }
	  else {
		System.out.printf("Overwriting block %s in blob %s\n", blockId, blobName);
	  }
	} else {
	  System.out.printf("Creating initial block %s in new blob: %s\n", blockId, blobName);
	  blockList.add(block);
	}
	currentBlob.commitBlockList(blockList);
	
	context.checkpoint(latestEventData);
	
	// Reset everything after the checkpoint
	toAppend.reset();
	start = Instant.now();
	System.out.printf("Checkpointed on partition id: %s\n", context.getPartitionId());
  }
}