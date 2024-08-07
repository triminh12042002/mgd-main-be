package kafka

type KafkaConfig struct {
	BootstrapServer string `json:"bootstrap_server" yaml:"bootstrap_server" validate:"nonzero"` // Required
	SaslUsername    string `json:"sasl_username" yaml:"sasl_username" validate:"nonzero"`       // Required
	SaslPassword    string `json:"sasl_password" yaml:"sasl_password" validate:"nonzero"`       // Required
	SslCaLocation   string `json:"ssl_ca_location" yaml:"ssl_ca_location" validate:"nonzero"`   // Required
}
