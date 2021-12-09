package udp

import (
	"net"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/outputs"
	"github.com/elastic/beats/libbeat/outputs/codec"
	"github.com/elastic/beats/libbeat/publisher"
	"github.com/pkg/errors"
)

const (
	networkUDP = "udp"
)

func init() {
	outputs.RegisterType("udp", makeUdp)
}

type udpOut struct {
	address       *net.UDPAddr

	observer      outputs.Observer
	index         string
	codec         codec.Codec
}

func makeUdp(
	beat beat.Info,
	observer outputs.Observer,
	cfg *common.Config,
) (outputs.Group, error) {
	config := Config{}
	if err := cfg.Unpack(&config); err != nil {
		return outputs.Fail(err)
	}

	// disable bulk support in publisher pipeline
	err := cfg.SetInt("bulk_max_size", -1, -1)
	if err != nil {
		logp.Warn("cfg.SetInt failed with: %v", err)
	}

	enc, err := codec.CreateEncoder(beat, config.Codec)
	if err != nil {
		return outputs.Fail(err)
	}
	u, err := newUdpOut(beat.Beat, config, observer, enc)
	if err != nil {
		return outputs.Fail(err)
	}
	return outputs.Success(-1, 0, u)
}

func newUdpOut(index string, c Config, observer outputs.Observer, codec codec.Codec) (*udpOut, error) {
	u := &udpOut{
		observer:      observer,
		index:         index,
		codec:         codec,
	}
	addr, err := net.ResolveUDPAddr(networkUDP, net.JoinHostPort(c.Host, c.Port))
	if err != nil {
		return nil, errors.Wrap(err, "resolve udp addr failed")
	}
	u.address = addr
	logp.Info("new udp output, address=%v", u.address)
	return u, nil
}

func (u *udpOut) Close() error {
	return nil
}

func (u *udpOut) Publish(
	batch publisher.Batch,
) error {
	events := batch.Events()
	u.observer.NewBatch(len(events))

	bulkSize := 0
	dropped := 0
	for i := range events {
		serializedEvent, err := u.codec.Encode(u.index, &events[i].Content)
		if err != nil {
			dropped++
			continue
		}
		conn, err := net.DialUDP(networkUDP, nil, u.address)
		if err != nil {
			u.observer.WriteError(err)
			dropped++
			continue
		}
		n, err := conn.Write(serializedEvent)
		if err != nil {
			u.observer.WriteError(err)
			dropped++
			continue
		}
		_ = conn.Close()
		bulkSize += n
	}

	u.observer.WriteBytes(bulkSize)
	u.observer.Dropped(dropped)
	u.observer.Acked(len(events) - dropped)
	batch.ACK()
	return nil
}

func (u *udpOut) String() string {
	return "udp(" + u.address.String() + ")"
}
