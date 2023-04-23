# mini-kafka-go-broker

## Step 1

Start a TCP server. Send and receive sample data from the client.
The for loop ensures that the code can handle multiple TCP connections at once. Each TCP connection is handled by a goroutine.

## Step 2

handleRequest function takes in the incoming client request and depending on the command (commandCreateTopic, commandProduce, commandConsume), the switch case directs it to the respective function.

## Step 3

handleTopic function creates files topicMetaInformation.txt and consumer_offsets.txt if they do not exist already.
Then, it checks if topicMetaInformation.txt contains the specified topicName - if yes, then files for the topicName already exist. If not, then it creates files for the topicName as per the number of partitions and replica specified in the producer client command. And appends these file names corresponding to the topicName in topicMetaInformation.txt. It also writes the topicName and partition number in the consumer_offsets.txt file.

## Step 4

produceMessage function reads the relevant file name for the specified from topicMetaInformation.txt file. Then, it appends the producer message in a new line to the relevant topic partition file.

## Step 5

consumeMessage functions reads the line/offset till which a topic partition is read upto from consumer_offsets.txt file, and then reads the partition file data from the next line to the end of file, and appends the updated offset to the consumer_offsets.txt file.
