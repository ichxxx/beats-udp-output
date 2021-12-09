package udp

import (
	"github.com/elastic/beats/libbeat/outputs/codec"
)

type Config struct {
	Host            string       `config:"host"`
	Port            string       `config:"port"`

	Codec           codec.Config `config:"codec"`
}
