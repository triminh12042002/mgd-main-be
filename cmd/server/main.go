package main

import (
	"FashionDesignWeb/internal/configs"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gorilla/websocket"
)

var imagePath string
var sketchPath string
var caption string
var segmnetationPath string
var posemapPath string
var posemapRenderedpath string
var finalImagePath string
var sketchCondRate float32

var test bool = false
var testSendBackAllData bool = true

type RequestMessage struct {
	ImageName      string  `json:"fileName1"`
	ImageData      string  `json:"fileData1"`
	SketchName     string  `json:"fileName2"`
	SketchData     string  `json:"fileData2"`
	Caption        string  `json:"caption"`
	SketchCondRate float32 `json:"sketchCondRate"`
}

type ResponeMessage struct {
	ImageName  string `json:"fileName1"`
	ImageData  string `json:"fileData1"`
	SketchName string `json:"fileName2"`
	SketchData string `json:"fileData2"`
	Caption    string `json:"caption"`
}

type InitMessage struct {
	Mess string `json:"mess"`
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		// Replace with your allowed origin check logic
		//always allow
		return true
	},
}

var ctx context.Context = context.Background()
var cfg *configs.Config
var errLoadConfig error

var connection *websocket.Conn

func handleGenerating(w http.ResponseWriter, r *http.Request) {

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Error upgrading to WebSocket:", err)
	}

	connection = conn
	log.Println("New WebSocket connection established")

	defer conn.Close()

	_, msg, err := conn.ReadMessage()
	if err != nil {
		log.Println("Error reading message from WebSocket:", err)
	}

	var message RequestMessage
	if err := json.Unmarshal(msg, &message); err != nil {
		log.Println("Error unmarshalling message:", err)
	}

	fmt.Println("Image name:", message.ImageName)

	topicSegmentation := "mgd.data.segmentation.input"

	// save image
	fileName := message.ImageName
	fileData, err := base64.StdEncoding.DecodeString(string(message.ImageData))
	if err != nil {
		log.Fatal("Error decoding original image value:", err)
	}
	imagePathTemp := "./mockData/MGD/image_" + fileName

	err = ioutil.WriteFile(imagePathTemp, fileData, 0644)
	if err != nil {
		log.Fatal("Error writing file:", err)
	}
	imagePath = imagePathTemp
	fmt.Printf("Saved original image %s to %s\n", fileName, imagePath)

	// save sketch
	sketchName := message.SketchName
	sketchData, err := base64.StdEncoding.DecodeString(string(message.SketchData))
	if err != nil {
		log.Fatal("Error decoding sketch value:", err)
	}
	sketchPathTemp := "./mockData/MGD/im_sketch/sketch_" + sketchName
	err = ioutil.WriteFile(sketchPathTemp, sketchData, 0644)
	if err != nil {
		log.Fatal("Error writing sketch file:", err)
	}
	fmt.Printf("Saved sketch image %s to %s\n", sketchName, sketchPath)
	sketchPath = sketchPathTemp

	caption = message.Caption
	log.Println("Save caption", caption)

	sketchCondRate = message.SketchCondRate
	log.Println("Save sketchCondRate", sketchCondRate)

	// produce to kafka
	err = produceToKafka(message.ImageName, message.ImageData, topicSegmentation)
	if err != nil {
		log.Fatal("Error produce message image to Kafka - segmentation topic and posemap topic", err)
	}

	// for reuse segmentation and pose-map
	// time.Sleep(1 * time.Second)
	// segmnetationPath = "/home/tri/Uni/Year4/Thesis/DemoApp/FashionDesign/FaDe/mockData/MGD/image-parse-v3/segmentation_00006_00.png"

	// time.Sleep(1 * time.Second)
	// posemapPath = "/home/tri/Uni/Year4/Thesis/DemoApp/FashionDesign/FaDe/mockData/MGD/openpose_json/posemap_00006_00_keypoints.json"
	// posemapRenderedpath = "/home/tri/Uni/Year4/Thesis/DemoApp/FashionDesign/FaDe/mockData/MGD/openpose_json/posemap_00006_00_rendered.png"

}

var run bool = true

var alphabet []rune = []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")

func randomString(n int, alphabet []rune) string {

	alphabetSize := len(alphabet)
	var sb strings.Builder

	for i := 0; i < n; i++ {
		ch := alphabet[rand.Intn(alphabetSize)]
		sb.WriteRune(ch)
	}

	s := sb.String()
	return s
}

func produceToKafka(fileName string, fileData string, topic string) error {

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		// User-specific properties that you must set
		"bootstrap.servers": cfg.Kafka.BootstrapServer,
		"sasl.username":     cfg.Kafka.SaslUsername,
		"sasl.password":     cfg.Kafka.SaslPassword,
		"ssl.ca.location":   cfg.Kafka.SslCaLocation,

		// Fixed properties
		"security.protocol": "SASL_SSL",
		"sasl.mechanisms":   "PLAIN",
		"acks":              "all",
		"group.id":          randomString(10, alphabet),

		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatal("Error create producer:", err)
		return err
	}

	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(fileName),
		Value:          []byte(fileData), // which already base64 encode, originally send from client through socket
	}, nil)

	poseMapTopic := "mgd.data.posemap.input"
	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &poseMapTopic, Partition: kafka.PartitionAny},
		Key:            []byte(fileName),
		Value:          []byte(fileData), // which already base64 encode, originally send from client through socket
	}, nil)

	if err != nil {
		log.Fatal("Error produce message:", err)
		return err
	}

	// wait for message to be delivered
	p.Flush(4 * 1000)
	p.Close()
	return nil
}

func consumeSegmentationFromKafka(topic string) {

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		// User-specific properties that you must set
		"bootstrap.servers": cfg.Kafka.BootstrapServer,
		"sasl.username":     cfg.Kafka.SaslUsername,
		"sasl.password":     cfg.Kafka.SaslPassword,
		"ssl.ca.location":   cfg.Kafka.SslCaLocation,

		// Fixed properties
		"security.protocol": "SASL_SSL",
		"sasl.mechanisms":   "PLAIN",
		"acks":              "all",
		"group.id":          randomString(10, alphabet),

		"auto.offset.reset": "latest",
	})
	if err != nil {
		log.Fatal("Error create consumer:", err)
	}

	defer c.Close()
	// segmentOutputTopic := "mgd.data.segmentation.output"

	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		log.Fatal("Error subcribe topic:", err)
	}

	fmt.Printf("Subscribed to segmentation topic %s\n", topic)

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
			return
		default:
			ev, err := c.ReadMessage(500 * time.Millisecond)
			if err != nil {
				// Errors are informational and automatically handled by the consumer
				continue
			}
			if ev != nil {
				if ev.Key != nil {

				}
				key := ev.Key
				segmentName, err := base64.StdEncoding.DecodeString(string(key))
				if err != nil {
					log.Fatal("error:", err)
				}

				// value := ev.Value
				fmt.Printf("Consumed event from segmentation topic %s: key = %-10s\n",
					*ev.TopicPartition.Topic, string(segmentName))
				// *ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))

				segmentNameStr := string(segmentName)
				fileData, err := base64.StdEncoding.DecodeString(string(ev.Value))
				if err != nil {
					log.Fatal("Error decoding value:", err)
				}

				finalImagePathTemp := "./mockData/MGD/image-parse-v3/segmentation_" + segmentNameStr

				err = ioutil.WriteFile(finalImagePathTemp, fileData, 0644)
				if err != nil {
					log.Fatal("Error writing file:", err)
				}
				segmnetationPath = finalImagePathTemp
				fmt.Printf("Saved %s to %s\n", segmentNameStr, segmnetationPath)

				// run = false
				// return
			}

		}
	}
}

type ListFile struct {
	FileName string `json:"fileName"`
	FileData string `json:"fileData"` // Base64 encoded data
}

func consumeFinalImageFromKafka(topic string) {

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		// User-specific properties that you must set
		"bootstrap.servers": cfg.Kafka.BootstrapServer,
		"sasl.username":     cfg.Kafka.SaslUsername,
		"sasl.password":     cfg.Kafka.SaslPassword,
		"ssl.ca.location":   cfg.Kafka.SslCaLocation,

		// Fixed properties
		"security.protocol": "SASL_SSL",
		"sasl.mechanisms":   "PLAIN",
		"acks":              "all",
		"group.id":          randomString(10, alphabet),

		"auto.offset.reset": "latest",
	})
	if err != nil {
		log.Fatal("Error create consumer:", err)
	}

	defer c.Close()

	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		log.Fatal("Error subcribe topic:", err)
	}

	fmt.Printf("Subscribed to final image topic %s\n", topic)

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
			return
		default:
			ev, err := c.ReadMessage(2000 * time.Millisecond)
			if err != nil {
				// Errors are informational and automatically handled by the consumer
				continue
			}
			if ev != nil {
				if ev.Key != nil {

				}
				key := ev.Key

				finalImageName, err := base64.StdEncoding.DecodeString(string(key))
				if err != nil {
					log.Fatal("error:", err)
				}

				finalImageNameStr := string(finalImageName)
				// value := ev.Value
				fmt.Printf("Consumed event from final mgd all data output topic %s: key = %-10s",
					*ev.TopicPartition.Topic, finalImageNameStr)

				fileData, err := base64.StdEncoding.DecodeString(string(ev.Value))
				if err != nil {
					log.Fatal("Error decoding value:", err)
				}

				finalImagePathTemp := "./mockData/MGD/final_image/finalimage_" + finalImageNameStr

				err = ioutil.WriteFile(finalImagePathTemp, fileData, 0644)
				if err != nil {
					log.Fatal("Error writing file:", err)
				}
				finalImagePath = finalImagePathTemp
				fmt.Printf("Saved %s to %s\n", finalImageNameStr, finalImagePath)

				// run = false
				// return
			}

		}
	}
}

func consumePoseMapFromKafka(topic string) {

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		// User-specific properties that you must set
		"bootstrap.servers": cfg.Kafka.BootstrapServer,
		"sasl.username":     cfg.Kafka.SaslUsername,
		"sasl.password":     cfg.Kafka.SaslPassword,
		"ssl.ca.location":   cfg.Kafka.SslCaLocation,

		// Fixed properties
		"security.protocol": "SASL_SSL",
		"sasl.mechanisms":   "PLAIN",
		"acks":              "all",
		"group.id":          randomString(9, alphabet),

		"auto.offset.reset": "latest",
	})
	if err != nil {
		log.Fatal("Error create consumer:", err)
	}

	defer c.Close()

	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		log.Fatal("Error subcribe topic:", err)
	}

	fmt.Printf("Subscribed to posemap topic %s\n", topic)

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
			return
		default:
			ev, err := c.ReadMessage(400 * time.Millisecond)
			if err != nil {
				// Errors are informational and automatically handled by the consumer
				continue
			}
			if ev != nil {
				var files []ListFile
				err = json.Unmarshal(ev.Value, &files)
				// key := ev.Key
				// value := ev.Value
				for _, file := range files {
					fileName := file.FileName
					// fileData := file.FileData
					fmt.Printf("Consumed event from posemap topic %s: key = %-10s	",
						*ev.TopicPartition.Topic, string(fileName))
					// store to posemap folder
					fileData, err := base64.StdEncoding.DecodeString(string(file.FileData))
					if err != nil {
						log.Fatal("Error decoding value:", err)
					}

					posemapPathTemp := "./mockData/MGD/openpose_json/posemap_" + fileName

					err = ioutil.WriteFile(posemapPathTemp, fileData, 0644)
					if err != nil {
						log.Fatal("Error writing file:", err)
					}
					if filepath.Ext(posemapPathTemp) == ".json" {
						posemapPath = posemapPathTemp
						log.Println("receive json,", posemapPath)
					} else if filepath.Ext(posemapPathTemp) == ".png" {
						posemapRenderedpath = posemapPathTemp
						log.Println("receive png,", posemapRenderedpath)
					} else {
						log.Fatal("Error, unknown file ext:", filepath.Ext(posemapPathTemp), " ;err:", err)
					}
					fmt.Printf("Saved %s to %s\n", fileName, posemapPathTemp)
				}

				// *ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))
				// run = false
				// return
			}

		}
	}
}

func produceAllDataToKafka(topic string) error {

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		// User-specific properties that you must set
		"bootstrap.servers": cfg.Kafka.BootstrapServer,
		"sasl.username":     cfg.Kafka.SaslUsername,
		"sasl.password":     cfg.Kafka.SaslPassword,
		"ssl.ca.location":   cfg.Kafka.SslCaLocation,

		// Fixed properties
		"security.protocol": "SASL_SSL",
		"sasl.mechanisms":   "PLAIN",
		"acks":              "all",
		"group.id":          randomString(10, alphabet),

		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatal("Error create producer:", err)
		return err
	}
	log.Println("Create produce all data")

	data, err := os.ReadFile(imagePath)
	if err != nil {
		log.Fatal(err)
	}
	imageData := base64.StdEncoding.EncodeToString(data)
	imageName := filepath.Base(imagePath)

	data, err = os.ReadFile(sketchPath)
	if err != nil {
		log.Fatal(err)
	}
	sketchData := base64.StdEncoding.EncodeToString(data)
	sketchName := filepath.Base(sketchPath)

	data, err = os.ReadFile(segmnetationPath)
	if err != nil {
		log.Fatal(err)
	}
	segmnetationData := base64.StdEncoding.EncodeToString(data)
	segmnetationName := filepath.Base(segmnetationPath)

	data, err = os.ReadFile(posemapPath)
	if err != nil {
		log.Fatal(err)
	}
	posemapData := base64.StdEncoding.EncodeToString(data)
	posemapName := filepath.Base(posemapPath)

	sketchCondRateStr := strconv.FormatFloat(float64(sketchCondRate), 'f', -1, 64)
	files := []ListFile{
		{FileName: imageName, FileData: imageData},
		{FileName: sketchName, FileData: sketchData},
		{FileName: segmnetationName, FileData: segmnetationData},
		{FileName: posemapName, FileData: posemapData},
		{FileName: "caption", FileData: base64.StdEncoding.EncodeToString([]byte(caption))},
		{FileName: "sketchCondRateStr", FileData: base64.StdEncoding.EncodeToString([]byte(sketchCondRateStr))},
	}

	log.Println("Produce Message files: imageName:", imageName, ", sketchName:", sketchName, ", segmnetationName:", segmnetationName, ", posemapName:", posemapName, ", caption:", caption, ", sketchCondRateStr:", sketchCondRateStr)

	filesData, err := json.Marshal(files)
	if err != nil {
		log.Fatal(err)
	}

	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte("All mgd data"),
		Value:          filesData,
	}, nil)

	if err != nil {
		log.Fatal("Error produce message:", err)
		return err
	}

	// wait for message to be delivered
	p.Flush(4 * 1000)
	p.Close()
	imagePath = ""
	sketchPath = ""
	segmnetationPath = ""
	posemapPath = ""
	posemapRenderedpath = ""
	finalImagePath = ""
	return nil
}

func sendBackData(fileType string, fileName string, filePath string) {
	// sen final output to Client thourgh socket
	// segmentPath := "/home/tri/Uni/Year4/Thesis/DemoApp/FashionDesign/FaDe/mockData/MGD/im_sketch/00006_00_sketch.png"
	// segmentData, err := ioutil.ReadFile(segmentPath)

	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		log.Println("Error reading file:", err)
		return
	}

	encodedData := base64.StdEncoding.EncodeToString(data)

	responseMessage := map[string]string{
		"fileType": fileType,
		"fileName": fileName,
		"fileData": encodedData,
	}

	// log.Println("responseMessage segmentation file:", responseMessage)

	if connection != nil {
		if err := connection.WriteJSON(responseMessage); err != nil {
			log.Println("Error sending file back to client:", err)
		}
	}
}

func sendBackAll(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Error upgrading to WebSocket:", err)
	}

	connection = conn
	log.Println("New WebSocket connection established for testing send data")

	defer conn.Close()
	if testSendBackAllData {
		time.Sleep(2 * time.Second)
		sendBackData("segmentation", "segmentation_00006_00.png", segmnetationPath)
		time.Sleep(2 * time.Second)
		sendBackData("posemap", "posemap_00006_00_rendered.png", posemapRenderedpath)
		time.Sleep(4 * time.Second)
		sendBackData("finalimage", "image_00006_00.jpg", finalImagePath)
	}
}

var sendBackSegmnetation bool = true
var sendBackPosemap bool = true
var sendBackFinal bool = true

func main() {
	test = false
	if test {
		log.Println("Test mode")
	}
	cfg, errLoadConfig = configs.LoadConfig(ctx, "")
	if errLoadConfig != nil {
		log.Fatal("cannot load config")
		panic(errLoadConfig)
	}

	// Channel to listen for termination signals
	var sigChan chan os.Signal = make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	if !test {
		// consumerDone := make(chan struct{})
		segmentOutputTopic := "mgd.data.segmentation.output"
		go func() {
			log.Println("consume segmentation")
			consumeSegmentationFromKafka(segmentOutputTopic)
			log.Println("stop consume segmentation")
			// close(consumerDone)
		}()

		posemapOutputTopic := "mgd.data.posemap.output"
		go func() {
			log.Println("consume pose map")
			consumePoseMapFromKafka(posemapOutputTopic)
			log.Println("stop consume pose map")
		}()

		finalImageOutputTopic := "mgd.data.all.output"
		go func() {
			log.Println("consume final image")
			consumeFinalImageFromKafka(finalImageOutputTopic)
			log.Println("stop consume final image")
		}()

		go func() {
			for true {
				if segmnetationPath != "" && sendBackSegmnetation {
					time.Sleep(1 * time.Second)
					sendBackData("segmentation", filepath.Base(segmnetationPath), segmnetationPath)
					sendBackSegmnetation = false
				}
				if posemapRenderedpath != "" && posemapPath != "" && sendBackPosemap {
					time.Sleep(1 * time.Second)
					pmName := filepath.Base(posemapRenderedpath)
					fmt.Println("pmName,", pmName)
					sendBackData("posemap", pmName, posemapRenderedpath)
					sendBackPosemap = false
				}
				if finalImagePath != "" && sendBackFinal {
					time.Sleep(1 * time.Second)
					sendBackData("finalimage", filepath.Base(finalImagePath), finalImagePath)
					sendBackFinal = false
				}
			}
		}()
	}

	// http.HandleFunc("/ws", handleWebsocket)
	if test {
		http.HandleFunc("/generating", sendBackAll)
	} else {
		http.HandleFunc("/generating", handleGenerating)
	}
	log.Println("Server start on port :8080")
	var server *http.Server = &http.Server{Addr: ":8080"}

	go func() {
		err := server.ListenAndServe()
		if err != nil {
			log.Fatal("ListenAndServe: ", err)
		}
	}()

	if test {
		imagePath = "/home/tri/Uni/Year4/Thesis/DemoApp/FashionDesign/FaDe/mockData/MGD/image/image_00006_00.jpg"
		sketchPath = "/home/tri/Uni/Year4/Thesis/DemoApp/FashionDesign/FaDe/mockData/MGD/im_sketch/sketch_00006_00.png"
		segmnetationPath = "/home/tri/Uni/Year4/Thesis/DemoApp/FashionDesign/FaDe/mockData/MGD/image-parse-v3/segmentation_00006_00.png"
		posemapPath = "/home/tri/Uni/Year4/Thesis/DemoApp/FashionDesign/FaDe/mockData/MGD/openpose_json/posemap_00006_00_keypoints.json"
		posemapRenderedpath = "/home/tri/Uni/Year4/Thesis/DemoApp/FashionDesign/FaDe/mockData/MGD/openpose_json/posemap_00006_00_rendered.png"
		finalImagePath = "/home/tri/Uni/Year4/Thesis/DemoApp/FashionDesign/FaDe/mockData/MGD/final_image/finalimage_00006_00.jpg"
	}

	go func() {
		for true {
			if !test {
				if imagePath != "" && sketchPath != "" && segmnetationPath != "" && posemapPath != "" {
					// produce all data to kaggel MGD service to gen final image
					mgdTopic := "mgd.data.all.input"
					produceAllDataToKafka(mgdTopic)
				}
			}
		}
	}()

	// Wait for a termination signal
	<-sigChan
	log.Println("Shutdown server")

	if err := server.Close(); err != nil {
		panic(err) // fail to shutdown server
	}

	// <-consumerDone
	log.Println("Program terminated gracefully")

}

// go run FaDe/cmd/server/main.go
