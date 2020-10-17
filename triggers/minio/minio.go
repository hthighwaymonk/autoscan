package minio

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"github.com/cloudbox/autoscan"
	MQTT "github.com/eclipse/paho.mqtt.golang"
)

type Config struct {
	Broker     string             `yaml:"broker"`
	User       string             `yaml:"user"`
	Pass       string             `yaml:"pass"`
	Topic      string             `yaml:"topic"`
	Priority   int                `yaml:"priority"`
	TimeOffset time.Duration      `yaml:"time-offset"`
	Verbosity  string             `yaml:"verbosity"`
	Rewrite    []autoscan.Rewrite `yaml:"rewrite"`
	Include    []string           `yaml:"include"`
	Exclude    []string           `yaml:"exclude"`
}

func New(c Config) (autoscan.Trigger, error) {
	l := autoscan.GetLogger(c.Verbosity).With().
		Str("trigger", "minio").
		Str("broker", c.Broker).
		Logger()

	rewriter, err := autoscan.NewRewriter(append(c.Rewrite, c.Rewrite...))
	if err != nil {
		return nil, err
	}

	filterer, err := autoscan.NewFilterer(append(c.Include, c.Include...), append(c.Exclude, c.Exclude...))
	if err != nil {
		return nil, err
	}

	scanTime := func() time.Time {
		if c.TimeOffset.Seconds() > 0 {
			return time.Now().Add(c.TimeOffset)
		}
		return time.Now().Add(c.TimeOffset)
	}

	trigger := func(callback autoscan.ProcessorFunc) {
		d := daemon{
			log:      l,
			broker:   c.Broker,
			user:     c.User,
			pass:     c.Pass,
			topic:    c.Topic,
			callback: callback,
			priority: c.Priority,
			rewriter: rewriter,
			allowed:  filterer,
			scanTime: scanTime,
		}

		// start job(s)
		if err := d.startAutoSync(); err != nil {
			l.Error().
				Err(err).
				Msg("Failed initialising job")
			return
		}
	}

	return trigger, nil
}

type daemon struct {
	broker   string
	user     string
	pass     string
	topic    string
	callback autoscan.ProcessorFunc
	priority int
	rewriter autoscan.Rewriter
	allowed  autoscan.Filterer
	scanTime func() time.Time
	log      zerolog.Logger
}

func (d daemon) startAutoSync() error {
	// prepare client
	qos := 0
	opts := MQTT.NewClientOptions()
	opts.AddBroker(d.broker)
	opts.SetConnectTimeout(15 * time.Second)
	opts.SetAutoReconnect(true)
	opts.SetResumeSubs(true)
	opts.SetCleanSession(true)

	if d.user != "" && d.pass != "" {
		opts.SetUsername(d.user)
		opts.SetPassword(d.pass)

		opts.SetClientID(d.user)
		opts.SetCleanSession(false)
		qos = 2
	}

	opts.SetConnectionLostHandler(func(client MQTT.Client, err error) {
		d.log.Warn().
			Err(err).
			Msg("Disconnected")
	})

	opts.SetOnConnectHandler(func(client MQTT.Client) {
		d.log.Info().Msg("Connected")
	})

	opts.SetDefaultPublishHandler(func(client MQTT.Client, msg MQTT.Message) {
		// log event
		d.log.Trace().
			Str("topic", msg.Topic()).
			Bytes("payload", msg.Payload()).
			Msg("Received payload")

		// decode event
		event := new(minioEvent)
		if err := json.Unmarshal(msg.Payload(), event); err != nil {
			d.log.Error().
				Err(err).
				Msg("Failed decoding event payload")
			return
		}

		switch {
		case strings.HasPrefix(event.Type, "s3:ObjectCreated:"):
			event.Type = "create"
		case strings.HasPrefix(event.Type, "s3:ObjectRemoved:"):
			event.Type = "delete"
		default:
			// ignore non create/remove events
			return
		}

		// process event details
		eventDirectory := filepath.Dir(event.Path)
		rewritten := filepath.Clean(d.rewriter(eventDirectory))
		if !d.allowed(rewritten) {
			// path is not allowed
			return
		}

		// move to processor
		err := d.callback(autoscan.Scan{
			Folder:   rewritten,
			Priority: d.priority,
			Time:     d.scanTime(),
		})

		if err != nil {
			d.log.Error().
				Err(err).
				Interface("event", event).
				Msg("Failed moving scan to processor")
			return
		}

		d.log.Info().
			Str("path", rewritten).
			Str("type", event.Type).
			Msg("Scan moved to processor")

	})

	client := MQTT.NewClient(opts)

	// connect to broker
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return fmt.Errorf("connecting to broker: %v: %w", d.broker, token.Error())
	}

	// subscribe to topic(s)
	if token := client.Subscribe(d.topic, byte(qos), nil); token.Wait() && token.Error() != nil {
		return fmt.Errorf("subscribing to topic: %v: %w", d.topic, token.Error())
	}

	return nil
}

type minioEvent struct {
	Type string `json:"EventName"`
	Path string `json:"key"`
}
