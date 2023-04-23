package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
)

const (
	connHost              = "localhost"
	connPort              = "3333"
	connType              = "tcp"
	topicInfoFileName    = "topicMetaInformation.txt"
	consumerInfoFileName = "__consumer_offsets.txt"
	commandCreateTopic = "createTopic"
	commandProduce = "produce"
	commandConsume = "consume"
)

func main() {
	// Listen for incoming connections.
	l, err1 := net.Listen(connType, connHost+":"+connPort)
	if err1 != nil {
		log.Fatal(err1)
		os.Exit(1)
	}
	// Close the listener when the application closes.
	defer l.Close()
	fmt.Println("Listening on " + connHost + ":" + connPort)
	for {
		// Listen for an incoming connection.
		conn, err2 := l.Accept()
		if err2 != nil {
			log.Fatal(err2)
			// fmt.Println("Error accepting: ", err2.Error())
			os.Exit(1)
		}
		// Handle connections in a new goroutine.
		go handleRequest(conn)
	}
}

// Handles incoming requests
func handleRequest(conn net.Conn) {

	buf, err1 := ioutil.ReadAll(conn)
	if err1 != nil {
		log.Fatal(err1)
	}

	// fmt.Printf("buf type %T\n", buf) //type is []uint8

	clientMessage := string(buf)
	res := strings.Split(clientMessage, ":")

	fmt.Println(res)
	command := res[0]
	topic := res[1]
	// fmt.Printf("res[0] type: %T\n", res[0]) //type is string
	switch command {
		case commandCreateTopic:
			partitions, _ := strconv.Atoi(res[3])
			replicas, _ := strconv.Atoi(res[5])
			handleTopic(topic, partitions, replicas)
			// write data to response
			responseStr := fmt.Sprintf("Acknowledgement for command : %v being executed", string(buf[:]))
			conn.Write([]byte(responseStr))
		case commandProduce:
			producerMessage := res[2]
			produceMessage(topic, producerMessage)
			responseStr := fmt.Sprintf("Acknowledgement for command : %v being executed", string(buf[:]))
			conn.Write([]byte(responseStr))
		case commandConsume:
			// fmt.Println("Inside case consume")
			messageForConsumer := consumeMessage(topic)
			messageForConsumerAsSingleString := strings.Join(messageForConsumer,";")
			fmt.Println(messageForConsumerAsSingleString)
			lengthMessage := len(messageForConsumerAsSingleString)
			consumerMessage := make([]byte, lengthMessage)
			for _, char := range messageForConsumerAsSingleString{
				consumerMessage = append(consumerMessage, byte(char))
			}
			// conn.Write(consumerMessage)

			responseStr := fmt.Sprintf("Message for command : %v is ::: %v", string(buf[:]), string(consumerMessage))
			conn.Write([]byte(responseStr))			
	}

	// Send a response back to person contacting us
	// conn.Write([]byte("Message received."))
	
	// Close the connection when you're done with it.
	conn.Close()
}

func produceMessage(topicName string, message string) {
	// fmt.Println(topicName, message)
	topicFile, err1 := os.Open(topicInfoFileName)
	if err1 != nil {
		log.Fatal(err1)
	}
	defer topicFile.Close()

	scanner := bufio.NewScanner(topicFile)
	for scanner.Scan() {
		lineContent := scanner.Text()
		// fmt.Printf("lineContent type : %T\n", lineContent) //type string
		isExist, err2 := regexp.Match(topicName, []byte(lineContent))
		if err2 != nil {
			panic(err2)
		}

		if isExist {
			output := strings.Split(lineContent, ":")
			fileName := output[1]
			topicFile, err3 := os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
			if err3 != nil {
				log.Fatal(err3)
			}
			defer topicFile.Close()
			_, err4 := topicFile.WriteString(message + "\n")
			if err4 != nil {
				log.Fatal(err4)
			}
			break
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func consumeMessage(topicName string) []string {
	consumerFile, err := os.Open(consumerInfoFileName)
	if err != nil {
		log.Fatal(err)
	}
	defer consumerFile.Close()

	consumeFrom := topicName+"-Partition-0"
	scanner := bufio.NewScanner(consumerFile)
	for scanner.Scan() {
		lineContent := scanner.Text()
		// fmt.Printf("lineContent type : %T\n", lineContent) //type string
		isExist, err2 := regexp.Match(consumeFrom, []byte(lineContent))
		if err2 != nil {
			panic(err2)
		}
		if isExist {
			output := strings.Split(lineContent, ":")
			fileName := output[0]
			lastReadOffset := output[1]
			var lineToReadFrom int
			if lastReadOffset == "" {
				fmt.Println("Not read any lines yet")
				lineToReadFrom = 1
			} else {
				lineToReadFrom, _ = strconv.Atoi(lastReadOffset)
				lineToReadFrom++
			}

			partitionFile, err := os.Open(fileName + "-Replica-0.txt")
			if err != nil {
				fmt.Println(err)
			}
			defer partitionFile.Close()

			fileScanner := bufio.NewScanner(partitionFile)
			fileScanner.Split(bufio.ScanLines)
			var fileLines []string
			for fileScanner.Scan() {
				fileLines = append(fileLines, fileScanner.Text())
			}

			messageForConsumer := fileLines[lineToReadFrom-1:]
			// fmt.Printf("fileLines type %T", fileLines) // type is []string

			totalReadLines := len(fileLines)
			// fmt.Println("Read to read from ", lineToReadFrom, " and line read till ", totalReadLines)

			//edit the offset in consumerInfoFileName to totalReadLines
			consumerFileContent, err := ioutil.ReadFile(consumerInfoFileName)
			if err != nil {
                log.Fatalln(err)
			}
			lines := strings.Split(string(consumerFileContent), "\n")
			for i, line := range lines {
				if strings.Contains(line, consumeFrom) {
                        lines[i] = consumeFrom+":"+strconv.Itoa(totalReadLines)
                }
			}
			op := strings.Join(lines, "\n")
			err = ioutil.WriteFile(consumerInfoFileName, []byte(op), 0644)
			if err != nil {
                log.Fatalln(err)
			}

			return messageForConsumer
		}
	}
	return []string{}
}

func handleTopic(topicName string, partitions int, replicas int) {
	// topicName = strings.TrimSpace(topicName)
	// fmt.Println(topicName, partitions, replicas)

	//check if topicInfoFileName exists
	_, err1 := os.Stat(topicInfoFileName)
	if os.IsNotExist(err1) { //File does not exist
		//create an empty file with name given in topicInfoFileName
		topicMetaFile, err2 := os.Create(topicInfoFileName)
		if err2 != nil {
			log.Fatal("ERROR! ", err2)
		}
		log.Println("A new empty file,"+topicInfoFileName+", created successfully. ", topicMetaFile)
		topicMetaFile.Close()
	}

	//check if consumerInfoFileName exists
	_, err3 := os.Stat(consumerInfoFileName)
	if os.IsNotExist(err3) { //File does not exist
		//create an empty file with name given in consumerInfoFileName
		consumerMetaFile, err4 := os.Create(consumerInfoFileName)
		if err4 != nil {
			log.Fatal("ERROR! ", err4)
		}
		log.Println("A new empty file,"+consumerInfoFileName+", created successfully. ", consumerMetaFile)
		consumerMetaFile.Close()
	}

	//Open file name mentioned in topicInfoFileName for appending data
	topicMetaFile, err5 := os.OpenFile(topicInfoFileName, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err5 != nil {
		log.Fatal(err5)
	}
	defer topicMetaFile.Close()

	consumerMetaFile, err6 := os.OpenFile(consumerInfoFileName, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err6 != nil {
		log.Fatal(err6)
	}
	defer consumerMetaFile.Close()

	content, err7 := ioutil.ReadFile(topicInfoFileName)
	if err7 != nil {
		log.Fatal(err7)
	}

	isExist, err8 := regexp.Match(topicName, content)
	if err8 != nil {
		panic(err8)
	}

	if isExist == false { //topic not present
		for i := 0; i < partitions; i++ {
			partitionFileName := topicName + "-Partition-" + strconv.Itoa(i)
			for j := 0; j < replicas; j++ {
				replicaFileName := partitionFileName + "-Replica-" + strconv.Itoa(j) + ".txt"
				replicaFile, err9 := os.Create(replicaFileName)
				if err9 != nil {
					log.Fatal("ERROR! ", err9)
				} else {
					log.Println("An empty file, ", replicaFileName, " created successfully")
					_, err10 := topicMetaFile.WriteString(topicName + ":" + replicaFileName + "\n")
					if err10 != nil {
						log.Fatal(err10)
					}
					if i == 0 && j == 0 { //assigning Partition-0 as the partition to be consumed from
						_, err11 := consumerMetaFile.WriteString(partitionFileName + ":")
						if err11 != nil {
							log.Fatal(err11)
						}
					}
				}
				replicaFile.Close()
			}
		}
	}
}
