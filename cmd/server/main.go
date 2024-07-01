package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/gorilla/websocket"
)

type RequestMessage struct {
	ImageName  string `json:"fileName1"`
	ImageData  string `json:"fileData1"`
	SketchName string `json:"fileName2"`
	SketchData string `json:"fileData2"`
	Caption    string `json:"caption"`
}

type ResponeMessage struct {
	ImageName  string `json:"fileName1"`
	ImageData  string `json:"fileData1"`
	SketchName string `json:"fileName2"`
	SketchData string `json:"fileData2"`
	Caption    string `json:"caption"`
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		// Replace with your allowed origin check logic
		//always allow
		return true
	},
}

func handleWebsocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Error upgrading to WebSocket:", err)
		return
	}
	defer conn.Close()

	_, msg, err := conn.ReadMessage()
	if err != nil {
		log.Println("Error reading message from WebSocket:", err)
		return
	}

	// log.Printf("Raw message: %s", msg)
	// processTask(msg)

	// finalImage := consumeFromKafka()
	// conn.WriteJSON(finalImage)
	var message RequestMessage
	if err := json.Unmarshal(msg, &message); err != nil {
		log.Println("Error unmarshalling message:", err)
	}

	fmt.Println("receive")
	// fmt.Println(message.Caption)
	fmt.Println(message.ImageName)
	fmt.Println(message.SketchName)
	// fmt.Println(message)

	segmentPath := "/home/tri/Uni/Year4/Thesis/DemoApp/FashionDesign/FaDe/mockData/MGD/im_sketch/00006_00_sketch.png"
	segmentData, err := ioutil.ReadFile(segmentPath)
	if err != nil {
		log.Println("Error reading segmentation file:", err)
		return
	}

	encodedSegment := base64.StdEncoding.EncodeToString(segmentData)

	responseMessage := map[string]string{
		"segmentationName": "segmentation.jpg",
		"segmentationData": encodedSegment,
	}

	log.Println("responseMessage segmentation file:", responseMessage)

	if err := conn.WriteJSON(responseMessage); err != nil {
		log.Println("Error sending segment back to client:", err)
	}

}

func processTask(msg []byte) {

}

func consumeFromKafka() int {
	return 1
}

func main() {
	http.HandleFunc("/ws", handleWebsocket)
	log.Println("Server start on port :8080")
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
