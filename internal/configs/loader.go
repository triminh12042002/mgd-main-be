package configs

import (
	"context"
	"fmt"
	"log"
	"os"
	"path"

	"gopkg.in/yaml.v3"
)

// const (
// 	baseFile             = "config.yml"
// 	defaultConfigRootDir = "configs"
// )

// for local development
const (
	baseFile             = "config.yml"
	defaultConfigRootDir = "secrets"
)

// LoadConfig loads and validates configurations
// The hierachy is as follows from lowest to highest
//
// base.yaml
//		env.yaml -- environment is one of the input params ex: development
func LoadConfig(ctx context.Context, rootDir string) (*Config, error) {
	if len(rootDir) == 0 {
		rootDir = defaultConfigRootDir
	}

	log.Println("Load config from rootDir:", rootDir)
	files, err := getConfigFiles(ctx, rootDir)
	if err != nil {
		return nil, err
	}

	config := Config{}
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			log.Fatal(ctx, "cannot read file", err)
			return nil, err
		}

		if err := yaml.Unmarshal(data, &config); err != nil {
			log.Fatal(ctx, "cannot decode file", f)
			return nil, err
		}
	}

	return &config, config.Validate(ctx)
}

// getConfigFiles returns the list of config files to process in the hierarchy order
func getConfigFiles(ctx context.Context, configDir string) ([]string, error) {
	candidates := []string{
		path.Join(configDir, baseFile),
	}

	result := make([]string, 0, len(candidates))
	for _, c := range candidates {
		if _, err := os.Stat(c); err != nil {
			log.Fatal(ctx, "file not exist", c)
			continue
		}

		result = append(result, c)
	}

	if len(result) == 0 {
		err := fmt.Errorf("no config files found within %v", configDir)
		log.Fatal(ctx, err.Error())
		return nil, err
	}

	return result, nil
}
