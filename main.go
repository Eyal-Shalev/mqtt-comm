package main

import (
	"context"
	"flag"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/dottedmag/must"
	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	clientId := flag.String("id", "mqtt-go-client", "Client ID")
	topic := flag.String("topic", "mqtt-go-topic", "Client ID")
	username := flag.String("username", "", "Username")
	password := flag.String("password", "", "Password")
	connectUriStr := flag.String("broker", "mqtt://localhost:1883", "mqtt URI to connect to")
	flag.Parse()

	connectUri := must.OK1(url.Parse(*connectUriStr))

	clientConf := autopaho.ClientConfig{
		ServerUrls:      []*url.URL{connectUri},
		ConnectUsername: *username,
		ConnectPassword: []byte(*password),
		KeepAlive:       20, // Keepalive message should be sent every 20 seconds
		// CleanStartOnInitialConnection defaults to false. Setting this to true will clear the session on the first connection.
		CleanStartOnInitialConnection: false,
		// SessionExpiryInterval - Seconds that a session will survive after disconnection.
		// It is important to set this because otherwise, any queued messages will be lost if the connection drops and
		// the server will not queue messages while it is down. The specific setting will depend upon your needs
		// (60 = 1 minute, 3600 = 1 hour, 86400 = one day, 0xFFFFFFFE = 136 years, 0xFFFFFFFF = don't expire)
		SessionExpiryInterval: 60,
		OnConnectionUp: func(cm *autopaho.ConnectionManager, connAck *paho.Connack) {
			fmt.Println("mqtt connection up")
			// Subscribing in the OnConnectionUp callback is recommended (ensures the subscription is reestablished if
			// the connection drops)
			if _, err := cm.Subscribe(context.Background(), &paho.Subscribe{
				Subscriptions: []paho.SubscribeOptions{
					{Topic: *topic, QoS: 1},
				},
			}); err != nil {
				fmt.Printf("failed to subscribe (%s). This is likely to mean no messages will be received.", err)
			}
			fmt.Println("mqtt subscription made")
		},
		OnConnectError: func(err error) { fmt.Printf("error whilst attempting connection: %s\n", err) },
		ClientConfig: paho.ClientConfig{
			ClientID: *clientId,
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){
				func(pr paho.PublishReceived) (bool, error) {
					fmt.Printf("received message on topic %s; body: %s (retain: %t)\n", pr.Packet.Topic, pr.Packet.Payload, pr.Packet.Retain)
					return true, nil
				}},
			OnClientError: func(err error) { fmt.Printf("client error: %s\n", err) },
			OnServerDisconnect: func(d *paho.Disconnect) {
				if d.Properties != nil {
					fmt.Printf("server requested disconnect: %s\n", d.Properties.ReasonString)
				} else {
					fmt.Printf("server requested disconnect; reason code: %d\n", d.ReasonCode)
				}
			},
		},
	}

	client := must.OK1(autopaho.NewConnection(ctx, clientConf))
	must.OK(client.AwaitConnection(ctx))

	ticker := time.NewTicker(time.Second)
	msgCount := 0
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			msgCount++
			_ = must.OK1(client.Publish(ctx, &paho.Publish{
				QoS:     1,
				Topic:   *topic,
				Payload: []byte("TestMessage: " + strconv.Itoa(msgCount)),
			}))
			continue
		case <-ctx.Done():
			fmt.Println("signal caught - exiting")
		}
		break
	}

	<-client.Done()
}
