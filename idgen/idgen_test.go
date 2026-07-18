package idgen_test

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/osiloke/gostore/idgen"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFactoryAndConfig(t *testing.T) {
	// Test Default configuration
	cfg := idgen.Default()
	assert.Equal(t, idgen.StrategyObjectId, cfg.Strategy)

	// Test New with default config (ObjectId)
	gen, err := idgen.New(cfg)
	require.NoError(t, err)
	assert.Equal(t, "objectid", gen.Name())

	// Test Must with default config
	assert.NotNil(t, idgen.Must(gen, err))

	// Test New with ULID strategy
	ulidGen, err := idgen.New(idgen.Config{Strategy: idgen.StrategyULID})
	require.NoError(t, err)
	assert.Equal(t, "ulid", ulidGen.Name())

	// Test New with Snowflake strategy
	snowflakeGen, err := idgen.New(idgen.Config{Strategy: idgen.StrategySnowflake})
	require.NoError(t, err)
	assert.Equal(t, "snowflake", snowflakeGen.Name())

	// Test New with unknown strategy
	_, err = idgen.New(idgen.Config{Strategy: "unknown"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown strategy")
}

func TestObjectIdStrategy(t *testing.T) {
	os.Setenv("GATEWAY_MACHINE_ID", "test-gateway-01")
	defer os.Unsetenv("GATEWAY_MACHINE_ID")

	gen, err := idgen.New(idgen.Default())
	require.NoError(t, err)

	ctx := context.Background()

	// 1. Test Generate
	id, err := gen.Generate(ctx)
	require.NoError(t, err)
	assert.Len(t, id.Raw, 12)
	assert.Len(t, id.String, 24)
	assert.True(t, id.Sortable)
	assert.WithinDuration(t, time.Now(), id.Time, 2*time.Second)

	// 2. Test Parser
	extractor, ok := gen.(idgen.Extractor)
	require.True(t, ok, "generator must implement Extractor")

	parsedID, err := extractor.Parse(id.String)
	require.NoError(t, err)
	assert.Equal(t, id.Raw, parsedID.Raw)
	assert.Equal(t, id.String, parsedID.String)
	assert.Equal(t, id.Time.Unix(), parsedID.Time.Unix())

	extractedTime, err := extractor.Time(id.String)
	require.NoError(t, err)
	assert.Equal(t, id.Time.Unix(), extractedTime.Unix())

	// 3. Test Invalid Inputs
	_, err = extractor.Parse("short")
	assert.Error(t, err)

	_, err = extractor.Parse("invalidhexcharactershere")
	assert.Error(t, err)

	_, err = extractor.Time("invalidhexcharactershere")
	assert.Error(t, err)
}

func TestObjectIdMachineIDResolution(t *testing.T) {
	// Test override
	genOverride, err := idgen.New(idgen.Config{
		Strategy: idgen.StrategyObjectId,
		ObjectId: idgen.ObjectIdOptions{MachineID: "explicit-override"},
	})
	require.NoError(t, err)
	id1, err := genOverride.Generate(context.Background())
	require.NoError(t, err)

	// Test env var
	os.Setenv("GATEWAY_MACHINE_ID", "env-var-machine")
	defer os.Unsetenv("GATEWAY_MACHINE_ID")

	genEnv, err := idgen.New(idgen.Default())
	require.NoError(t, err)
	id2, err := genEnv.Generate(context.Background())
	require.NoError(t, err)

	// IDs should be generated and have different machine ID bytes
	assert.NotEqual(t, id1.Raw[4:7], id2.Raw[4:7])
}

func TestULIDStrategy(t *testing.T) {
	gen, err := idgen.New(idgen.Config{Strategy: idgen.StrategyULID})
	require.NoError(t, err)

	ctx := context.Background()

	// 1. Test Generate
	id, err := gen.Generate(ctx)
	require.NoError(t, err)
	assert.Len(t, id.Raw, 16)
	assert.Len(t, id.String, 26)
	assert.True(t, id.Sortable)
	assert.WithinDuration(t, time.Now(), id.Time, 2*time.Second)

	// 2. Test Parser
	extractor, ok := gen.(idgen.Extractor)
	require.True(t, ok, "generator must implement Extractor")

	parsedID, err := extractor.Parse(id.String)
	require.NoError(t, err)
	assert.Equal(t, id.Raw, parsedID.Raw)
	assert.Equal(t, id.String, parsedID.String)
	assert.Equal(t, id.Time.UnixMilli(), parsedID.Time.UnixMilli())

	extractedTime, err := extractor.Time(id.String)
	require.NoError(t, err)
	assert.Equal(t, id.Time.UnixMilli(), extractedTime.UnixMilli())

	// 3. Test Invalid Inputs
	_, err = extractor.Parse("short")
	assert.Error(t, err)

	_, err = extractor.Time("short")
	assert.Error(t, err)
}

func TestULIDMonotonicity(t *testing.T) {
	gen, err := idgen.New(idgen.Config{Strategy: idgen.StrategyULID})
	require.NoError(t, err)

	ctx := context.Background()

	id1, err := gen.Generate(ctx)
	require.NoError(t, err)

	id2, err := gen.Generate(ctx)
	require.NoError(t, err)

	assert.True(t, id1.String < id2.String, "second ULID must be lexicographically greater than first")
}

func TestSnowflakeStrategy(t *testing.T) {
	// Test constructor validation
	_, err := idgen.New(idgen.Config{
		Strategy: idgen.StrategySnowflake,
		Snowflake: idgen.SnowflakeOptions{DatacenterID: 32},
	})
	assert.Error(t, err)

	_, err = idgen.New(idgen.Config{
		Strategy: idgen.StrategySnowflake,
		Snowflake: idgen.SnowflakeOptions{MachineID: -1},
	})
	assert.Error(t, err)

	gen, err := idgen.New(idgen.Config{
		Strategy: idgen.StrategySnowflake,
		Snowflake: idgen.SnowflakeOptions{
			DatacenterID: 12,
			MachineID:    24,
		},
	})
	require.NoError(t, err)

	ctx := context.Background()

	// 1. Test Generate
	id, err := gen.Generate(ctx)
	require.NoError(t, err)
	assert.Len(t, id.Raw, 8)
	assert.NotEmpty(t, id.String)
	assert.True(t, id.Sortable)
	assert.WithinDuration(t, time.Now(), id.Time, 2*time.Second)

	// 2. Test Parser
	extractor, ok := gen.(idgen.Extractor)
	require.True(t, ok, "generator must implement Extractor")

	parsedID, err := extractor.Parse(id.String)
	require.NoError(t, err)
	assert.Equal(t, id.Raw, parsedID.Raw)
	assert.Equal(t, id.String, parsedID.String)
	assert.Equal(t, id.Time.UnixMilli(), parsedID.Time.UnixMilli())

	extractedTime, err := extractor.Time(id.String)
	require.NoError(t, err)
	assert.Equal(t, id.Time.UnixMilli(), extractedTime.UnixMilli())

	// 3. Test Invalid Inputs
	_, err = extractor.Parse("invalid_snowflake")
	assert.Error(t, err)

	_, err = extractor.Time("invalid_snowflake")
	assert.Error(t, err)
}

func TestConcurrencySafety(t *testing.T) {
	// Test all three generators concurrently
	strategies := []string{idgen.StrategyObjectId, idgen.StrategyULID, idgen.StrategySnowflake}

	for _, strategy := range strategies {
		t.Run(strategy, func(t *testing.T) {
			gen, err := idgen.New(idgen.Config{Strategy: strategy})
			require.NoError(t, err)

			const goroutines = 10
			const iterations = 100

			var wg sync.WaitGroup
			wg.Add(goroutines)

			generatedMap := sync.Map{}

			for i := 0; i < goroutines; i++ {
				go func() {
					defer wg.Done()
					for j := 0; j < iterations; j++ {
						id, err := gen.Generate(context.Background())
						if err != nil {
							t.Errorf("Generate error: %v", err)
							return
						}
						// Ensure uniqueness
						if _, loaded := generatedMap.LoadOrStore(id.String, true); loaded {
							t.Errorf("Duplicate ID generated: %s", id.String)
						}
					}
				}()
			}

			wg.Wait()
		})
	}
}
