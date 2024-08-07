package configs

import (
	"FashionDesignWeb/pkg/kafka"
	"context"
	"log"

	"gopkg.in/validator.v2"
)

type (
	Config struct {
		ServerAddress string             `yaml:"server_address" validate:"nonzero"`
		Kafka         *kafka.KafkaConfig `yaml:"kafka" validate:"nonzero"`
	}
)

// Validate validate configuration
func (c *Config) Validate(ctx context.Context) error {
	if err := validator.Validate(c); err != nil {
		log.Fatal(ctx, "Fail to validate,", err)
		return err
	}

	return nil
}
