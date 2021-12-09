package udp

import (
	"net"
	"testing"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/common/fmtstr"
	"github.com/elastic/beats/libbeat/outputs"
	"github.com/elastic/beats/libbeat/outputs/codec"
	"github.com/elastic/beats/libbeat/outputs/codec/format"
	"github.com/elastic/beats/libbeat/outputs/codec/json"
	"github.com/elastic/beats/libbeat/outputs/outest"
	"github.com/elastic/beats/libbeat/publisher"
	"github.com/stretchr/testify/assert"
)

func TestTcpOutput(t *testing.T) {
	config := Config{
		Host: "127.0.0.1",
		Port: "9000",
	}
	tests := []struct {
		title    string
		codec    codec.Codec
		events   []beat.Event
		expected string
	}{
		{
			"single json event (pretty=false)",
			json.New(false, true, "1.2.3"),
			[]beat.Event{
				{Fields: event("field", "value")},
			},
			"{\"@timestamp\":\"0001-01-01T00:00:00.000Z\",\"@metadata\":{\"beat\":\"test\",\"type\":\"doc\",\"version\":\"1.2.3\"},\"field\":\"value\"}",
		},
		{
			"single json event (pretty=true)",
			json.New(true, true, "1.2.3"),
			[]beat.Event{
				{Fields: event("field", "value")},
			},
			"{\n  \"@timestamp\": \"0001-01-01T00:00:00.000Z\",\n  \"@metadata\": {\n    \"beat\": \"test\",\n    \"type\": \"doc\",\n    \"version\": \"1.2.3\"\n  },\n  \"field\": \"value\"\n}",
		},
		{
			"event with custom format string",
			format.New(fmtstr.MustCompileEvent("%{[event]}")),
			[]beat.Event{
				{Fields: event("event", "myevent")},
			},
			"myevent",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.title, func(t *testing.T) {
			batch := outest.NewBatch(test.events...)
			lines, err := run(config, test.codec, batch)
			assert.Nil(t, err)
			assert.Equal(t, test.expected, lines)

			// check batch correctly signalled
			if !assert.Len(t, batch.Signals, 1) {
				return
			}
			assert.Equal(t, outest.BatchACK, batch.Signals[0].Tag)
		})
	}
}

func withUdpOut(address *net.UDPAddr, fn func()) (string, error) {
	outC, errC := make(chan string), make(chan error)
	go func() {
		conn, err := net.ListenUDP(networkUDP, address)
		if err != nil {
			errC <- err
			return
		}
		defer conn.Close()

		buf := make([]byte, 999)
		n, err := conn.Read(buf)
		if err != nil {
			errC <- err
			return
		}
		outC <- string(buf[:n])
	}()

	fn()

	select {
	case result := <- outC:
		return result, nil
	case err := <- errC:
		return "", err
	}
}

func run(config Config, codec codec.Codec, batches ...publisher.Batch) (string, error) {
	u, err := newUdpOut("test", config, outputs.NewNilObserver(), codec)
	if err != nil {
		return "", err
	}

	return withUdpOut(u.address, func() {
		for _, b := range batches {
			u.Publish(b)
		}
	})
}

func event(k, v string) common.MapStr {
	return common.MapStr{k: v}
}
