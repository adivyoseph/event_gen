package main

import (
    "fmt"
    mqtt "github.com/eclipse/paho.mqtt.golang"
 //   "log"
    "time"
    "encoding/json"
)


type AdminMsg struct {
   Cmd string    `json:"cmd"`
   Rate    int         `json:"rate"`
//   Partner    string    `json:"partner"`
}


type StatusMsg struct {
    Name string    `json:"name"`
    Type    string    `json:"type"`
    State    string    `json:"state"`
    Rate    int           `json:"rate"`
    Partner    string    `json:"partner"`
     Lost  int    `json:"lost"`
     Latency  int    `json:"latency"`
     Sequence int   `json:"seq"`
}



  type Config struct {
      state int
      rate int
      totalSent int
      lastLatencey int
      lost int




  }

  var config = Config{}


func genEvents () {

    for {
        if config.state == 1 {
             fmt.Printf("send hhtps ger\n")
             config.totalSent++
             time.Sleep(10)
        }
    }
}


//Go routine to monitor brocker for start/stop events
func brokerSubscribe () {
    var broker = "localhost"
    var port = 1883

    opts := mqtt.NewClientOptions()
    opts.AddBroker(fmt.Sprintf("tcp://%s:%d", broker, port))
    opts.SetClientID("go_event_gen_adim")
    opts.SetUsername("martin")
    opts.SetPassword("martin")
    opts.SetDefaultPublishHandler( func(client mqtt.Client, msg mqtt.Message) {
          fmt.Printf("Received message: %s from topic: %s\n", msg.Payload(), msg.Topic())
          var adminMsg = AdminMsg{}
          err := json.Unmarshal(msg.Payload(),adminMsg )
          if err != nil {
              panic(err.Error())
          }
          if adminMsg.Cmd == "start"  {
              config.rate = adminMsg.Rate
              config.state = 1;
          }
          if adminMsg.Cmd == "stop"  {
              config.state =  0;
          }

    })

    //opts.OnConnect = connectHandler
    //opts.OnConnectionLost = connectLostHandler
    client := mqtt.NewClient(opts)
    if token := client.Connect(); token.Wait() && token.Error() != nil {
        panic(token.Error())
    }

    topic := "Admin"
    token := client.Subscribe(topic, 1, nil)
    token.Wait()
}




//Go routine to generate status broker events
func brokerPublish( ) {
    var broker = "localhost"
    var port = 1883

    opts := mqtt.NewClientOptions()
    opts.AddBroker(fmt.Sprintf("tcp://%s:%d", broker, port))
    opts.SetClientID("go_event_gen")
    opts.SetUsername("martin")
    opts.SetPassword("martin")
    client := mqtt.NewClient(opts)
    if token := client.Connect(); token.Wait() && token.Error() != nil {
        panic(token.Error())
    }
    sequence := 0
    for  {
            msg := StatusMsg{}
                msg.Name = "Gen"
                msg.Type = "status"
              if config.state == 1 {
                    msg.State = "run"
            } else {
                  msg.State = "idle"
              }
                msg.Rate = config.totalSent
                msg.Partner = "localnet.ngnix"
                msg.Lost = config.lost
                msg.Latency = config.lastLatencey
                sequence++
                msg.Sequence =  sequence 

                payload, _  := json.Marshal(msg)

                token := client.Publish("status", 0, false, payload)
                token.Wait()
                fmt.Printf("send msg %d  %s\n", sequence,  payload)
                time.Sleep(10 * time.Second)  //sleep for 10 seconds
        }
}

func main() {
    config.state = 0            //mark idle

   go brokerSubscribe ()
   go brokerPublish()
   go genEvents()

   for {
   }
}

