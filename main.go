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
//   Sequence int   `json:"seq"`              //add for debugging
}

// status/discovery message
type StatusMsg struct {
    Name string    `json:"name"`             //POD instance name
    Type    string    `json:"type"`               // always status
    State    string    `json:"state"`             //idle or run
    Rate    int           `json:"rate"`                // get requests per second to issue
    Partner    string    `json:"partner"` // place holder
     Lost  int    `json:"lost"`                          //sequence numbers not returned
     Latency  int    `json:"latency"`          //total round trip lantency 
     Sequence int   `json:"seq"`              //status sequency number, used to detect restarts
}

type Config struct {
      state int
      rate int
      totalSent int
      lastLatencey int
      lost int
  }

var config = Config{}            //global config and state info

//Go routine to generate https get requests
func genEvents () {

    currentTime := time.Now()
    for {
        if config.state == 1 {
            currentTime = time.Now()
             fmt.Printf("send hhtps gen %s\n", currentTime.Format("2006.01.02 15:04:05"))
             config.totalSent++
             time.Sleep(2* time.Second)
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
          err := json.Unmarshal(msg.Payload(),&adminMsg )
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

    topic := "admin"
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
                fmt.Printf("send status %d  %s\n", sequence,  payload)
                time.Sleep(10 * time.Second)  //sleep for 10 seconds
        }
}

func main() {
    config.state = 0            //mark idle from the start

   go brokerSubscribe ()  //look fro admin start/stop events
   go brokerPublish()         //discovery and status heart beat
   go genEvents()                 //actual https get engine

   for {
   }
}

