# mini-kafka-go-broker

## Step 1

Start a TCP server. Send and receive sample data from the client.
The for loop ensures that the code can handle multiple TCP connections at once. Each TCP connection is handled by a goroutine.

## Step 2

handleRequest function takes in the incoming client request and depending on the command (commandCreateTopic, commandProduce, commandConsume), the switch case directs it to the respective function.

##Step 3

handleTopic function creates files topicMetaInformation.txt and __consumer_offsets.txt if they do not exist already.
Then, it checks if topicMetaInformation.txt contains the specified topicName - if not, then it creates files for the topicName as per the number of partitions and replica specified in the producer client command. And it appends these file names corresponding to the topicName in topicMetaInformation.txt.
If yes, then the files for the topicName already exist.

##Step 4


