package main

import (
	"log"
	"fmt"


	"opencv_golang/is/utils/vision"
	"google.golang.org/protobuf/proto"

	"gocv.io/x/gocv"

	"image"
	"image/jpeg"

	"os"
	"bytes"

	amqp "github.com/rabbitmq/amqp091-go"
)


func connect(broker_uri string) *amqp.Connection {
	conn, err := amqp.Dial(broker_uri)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	return conn
}

func createChannel(conn *amqp.Connection) *amqp.Channel {

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}
	return ch
}

func queueDeclare(ch *amqp.Channel) amqp.Queue {
	queue, err := ch.QueueDeclare(
		"",
		false,
		false,
		true,
		false,
		nil,
	)

	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	return queue
}



func serveFrames(imgByte []byte, i int) {
    img, _, err := image.Decode(bytes.NewReader(imgByte))
    if err != nil {
        log.Fatalln("Erro ao decodificar imagem:", err)
    }

    name_file := fmt.Sprintf("images/img_%d.jpeg", i)
    out, err := os.Create(name_file)
    if err != nil {
        log.Println("Erro ao criar arquivo:", err)
        return
    }
    defer out.Close()

    opts := jpeg.Options{Quality: 95} 
    err = jpeg.Encode(out, img, &opts)
    if err != nil {
        log.Println("Erro ao salvar imagem:", err)
    }
}


func main() {
    broker_uri := "amqp://guest:guest@10.10.2.211:30000/"
    exchange := "is"
    topic := "CameraGateway.1.Frame"

    conn := connect(broker_uri)
    defer conn.Close()
    log.Print("Conectado ao RabbitMQ")

    channel := createChannel(conn)
    defer channel.Close()

    channel.ExchangeDeclare(exchange, "topic", false, false, false, false, nil)
    queue := queueDeclare(channel)

    channel.QueueBind(queue.Name, topic, exchange, false, nil)

    msgs, err := channel.Consume(queue.Name, "", true, false, false, false, nil)
    if err != nil {
        log.Fatalf("Erro ao registrar consumidor: %v", err)
    }


	window := gocv.NewWindow("Imagem Recebida")
	defer window.Close()


    go func() {
        for d := range msgs {
            log.Printf("Mensagem recebida: %v", d.ContentType)

            objs := &is_vision.Image{}
            if err := proto.Unmarshal(d.Body, objs); err != nil {
                log.Fatalf("Erro ao desserializar a mensagem: %v", err)
            }


            imgData := objs.GetData() 

			img, _, err := image.Decode(bytes.NewReader(imgData))

			if err != nil {
				log.Fatalln("Erro ao decodificar imagem:", err)
			}

			mat, err := gocv.ImageToMatRGB(img)
			if err != nil {
				log.Fatalln("Erro ao converter a imagem para Mat:", err)
			}

			window.IMShow(mat)
			if window.WaitKey(1) >= 0 {
				break 
			}
            
        }
    }()

    <-make(chan bool) 
}
