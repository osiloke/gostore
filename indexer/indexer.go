package indexer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/mgutz/logxi/v1"

	jsoniter "github.com/json-iterator/go"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/schollz/progressbar/v3"

	// "github.com/blevesearch/blevex/regexp"

	"golang.org/x/sync/errgroup"
)

const (
	defaultTablePrefix = "t$"
)

var jiter = jsoniter.ConfigCompatibleWithStandardLibrary

// ReIndexOptions specifies configuration for the re-indexing process.
type ReIndexOptions struct {
	// BatchSize is the number of documents to accumulate per batch before
	// flushing to the index. A value of 0 disables batching and indexes
	// documents one at a time.
	BatchSize int

	// Workers is the number of concurrent worker goroutines used to unmarshal
	// and index documents. Defaults to runtime.NumCPU() if <= 0.
	// Tune this down on memory-constrained instances.
	Workers int

	// UnsafeBatch, when true, tells the indexer to use Bleve's unsafe batch
	// mode (if supported by the underlying index store, e.g. Scorch). In this
	// mode, Batch() returns as soon as the data is searchable in memory,
	// without waiting for disk persistence. This provides maximum throughput
	// but risks data loss on crash. Default is false (safe mode).
	UnsafeBatch bool

	// PauseAfterDocs is the number of documents to process between cooldown
	// pauses. When > 0, the reindex pipeline drains and sleeps for
	// PauseDuration after every chunk of this size, giving Scorch's background
	// merger a window to merge accumulated segments without contention.
	// A value of 0 (default) disables periodic pausing.
	PauseAfterDocs int

	// PauseDuration is how long to sleep between chunks when PauseAfterDocs
	// is enabled. Defaults to 5 seconds if left at zero.
	PauseDuration time.Duration
}

// ReIndexOption is a function type that modifies ReIndexOptions.
type ReIndexOption func(*ReIndexOptions)

// WithBatchSize returns a ReIndexOption that sets the batch size for indexing.
func WithBatchSize(size int) ReIndexOption {
	return func(o *ReIndexOptions) {
		o.BatchSize = size
	}
}

// WithWorkers returns a ReIndexOption that sets the number of concurrent
// worker goroutines. Use a lower value on memory-constrained instances.
func WithWorkers(count int) ReIndexOption {
	return func(o *ReIndexOptions) {
		o.Workers = count
	}
}

// WithUnsafeBatch returns a ReIndexOption that enables or disables unsafe
// batch mode. When enabled, batches are committed without waiting for disk
// persistence, providing maximum throughput at the cost of crash safety.
func WithUnsafeBatch(unsafe bool) ReIndexOption {
	return func(o *ReIndexOptions) {
		o.UnsafeBatch = unsafe
	}
}

// WithPauseAfterDocs configures periodic cooldown pauses during reindexing.
// After every n documents are processed, the pipeline drains, workers go
// idle, and the function sleeps for PauseDuration to allow Scorch segment
// merges to complete. Set to 0 to disable (default).
func WithPauseAfterDocs(n int) ReIndexOption {
	return func(o *ReIndexOptions) {
		o.PauseAfterDocs = n
	}
}

// WithPauseDuration sets the cooldown duration used when PauseAfterDocs is
// enabled. If not set, defaults to 5 seconds.
func WithPauseDuration(d time.Duration) ReIndexOption {
	return func(o *ReIndexOptions) {
		o.PauseDuration = d
	}
}

// reindexJob is a unit of work sent from the producer to a worker goroutine.
type reindexJob struct {
	key *[]byte
	val *[]byte
}

// byteSlicePool reduces GC pressure during high-throughput reindexing by
// reusing byte slices for job payloads.
var byteSlicePool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 0, 4096)
		return &b
	},
}

// getPooledSlice returns a pointer to a byte slice from the pool, copying src into it.
func getPooledSlice(src []byte) *[]byte {
	bp := byteSlicePool.Get().(*[]byte)
	if cap(*bp) < len(src) {
		*bp = make([]byte, len(src))
	} else {
		*bp = (*bp)[:len(src)]
	}
	copy(*bp, src)
	return bp
}

// putPooledSlice returns a byte slice pointer to the pool for reuse.
func putPooledSlice(bp *[]byte) {
	*bp = (*bp)[:0]
	byteSlicePool.Put(bp)
}

// ErrorAwareIterator is an optional interface that iterators can implement to
// surface errors that occur during iteration (e.g. I/O failures in ValueCopy).
// If the iterator provided to ReIndex implements this interface, errors are
// checked after each call to Value().
type ErrorAwareIterator interface {
	LastError() error
}

// ReIndex rebuilds the search index from scratch by iterating over every record
// in the store and re-indexing each one. It is typically called when the index
// is missing, corrupt, or the backing index type has changed.
//
// Parameters:
//   - name: a label for the index type (e.g. "badger", "moss"). It is embedded in
//     the init file written on completion and used by callers to detect whether the
//     index type has changed since the last run.
//   - indexInitFilePath: path to a marker file written after a successful re-index.
//     The file contains "<name>|<UTC timestamp>|<document count>" and is used by
//     [NewWithIndex] to decide whether a re-index is needed on next startup.
//   - provider: the underlying key-value store. Its [ProviderStore.Cursor] method
//     is used to iterate all raw records. Keys are expected to be in the form
//     "<TablePrefix><table>|<id>" (e.g. "t$users|abc123").
//   - index: the target search index. If index is a [*GeoIndexer] the document is
//     wrapped with bucket and geo-field metadata before indexing; otherwise it is
//     wrapped in an [IndexedData] value containing the bucket name and the raw data
//     map. The full storage key is used as the document ID to prevent collisions
//     across tables.
//   - opts: optional re-indexing configuration (e.g. [WithBatchSize], [WithWorkers],
//     [WithUnsafeBatch]).
//
// # Key splitting
//
// Each raw key (e.g. "t$users|abc123") is split on the first "|" separator:
//
//	parts   = SplitN("t$users|abc123", "|", 2)
//	ID      = "users|abc123"     // TrimPrefix(full key, "t$") → Bleve document ID
//	store   = "users"            // parts[0] from stripped ID → bucket label inside the document
//
// Using the stripped key (e.g. "users|abc123") as the document ID ensures that
// documents are indexed correctly without the internal "t$" prefix, while
// still being unique across different tables.
// The stripped table name is stored as the "bucket" field so results can later be
// filtered or grouped by table.
//
// When index is a [*GeoIndexer], the document envelope also promotes the geo field
// from the record data to the top level (stripping the leading "_"):
//
//	v["_location"] → d["location"]
//
// This is required because Bleve's geopoint mapping expects the geo field at the
// root of the indexed document. For a plain [Indexer] the record is wrapped in an
// [IndexedData] struct instead.
//
// Records whose values cannot be unmarshalled as JSON objects are skipped with a
// warning log and do not contribute to the final count.
//
// # Concurrency
//
// When Workers > 1 (or defaults to NumCPU), the function uses a producer-worker
// pattern: a single producer goroutine reads from the cursor and fans out work
// to N worker goroutines via a bounded channel. Each worker maintains its own
// bleve.Batch for optimal Scorch concurrency. The bounded channel provides
// backpressure to prevent the producer from reading the entire database into
// memory, which is critical for low-memory instances.
//
// On success the function writes the init file and returns nil. Any error writing
// the init file is returned to the caller.
func ReIndex(name, indexInitFilePath string, provider ProviderStore, index Indexer, opts ...ReIndexOption) error {
	options := &ReIndexOptions{
		BatchSize:     0,
		Workers:       0,
		PauseDuration: 5 * time.Second,
	}
	for _, opt := range opts {
		opt(options)
	}

	// Default to NumCPU workers, minimum 1.
	workers := options.Workers
	if workers <= 0 {
		workers = runtime.NumCPU()
	}
	if workers < 1 {
		workers = 1
	}

	batchSize := options.BatchSize

	// For single worker or no batch size, fall back to sequential processing
	// to preserve exact backwards-compatible behaviour (including progress bar
	// semantics and init-file format).
	if workers == 1 || batchSize <= 0 {
		return reindexSequential(name, indexInitFilePath, provider, index, batchSize,
			options.PauseAfterDocs, options.PauseDuration)
	}

	return reindexParallel(name, indexInitFilePath, provider, index, workers, batchSize,
		options.PauseAfterDocs, options.PauseDuration)
}

// reindexSequential is the original single-goroutine reindex path, preserved
// for backwards compatibility and for cases where batching is disabled.
// When pauseAfterDocs > 0 the loop flushes the current batch and sleeps for
// pauseDuration every pauseAfterDocs rows, giving Scorch's merger a window
// to consolidate segments. A value of 0 disables pausing (default).
func reindexSequential(name, indexInitFilePath string, provider ProviderStore, index Indexer, batchSize, pauseAfterDocs int, pauseDuration time.Duration) error {
	iter, _ := provider.Cursor()
	defer iter.Close()

	count := 0
	totalRows := 0
	bar := progressbar.Default(-1, "reindexing")
	defer bar.Finish()
	batch := index.BatchIndex()

	processedInBatch := 0
	lastUpdate := time.Now()
	for iter.Valid() {
		totalRows++

		// Chunk boundary pause: flush and sleep every pauseAfterDocs rows.
		if pauseAfterDocs > 0 && totalRows%pauseAfterDocs == 0 {
			if batchSize > 0 && batch.Size() > 0 {
				if err := index.Batch(batch); err != nil {
					return err
				}
				bar.Add(processedInBatch)
				processedInBatch = 0
				lastUpdate = time.Now()
				batch = index.BatchIndex()
			}
			logger.Info("reindex chunk complete – pausing",
				"total_rows", totalRows, "pause", pauseDuration)
			time.Sleep(pauseDuration)
		}

		key := iter.Key()
		val := iter.Value()

		// Check for iterator errors (e.g. ValueCopy failures).
		if errIter, ok := iter.(ErrorAwareIterator); ok {
			if err := errIter.LastError(); err != nil {
				return fmt.Errorf("iterator error at key %s: %w", string(key), err)
			}
		}

		processedInBatch++

		var v map[string]interface{}
		if err := jiter.Unmarshal(val, &v); err != nil {
			logger.Warn("failed to unmarshal value", "key", string(key), "val_len", len(val))
			iter.Next()
			continue
		}

		k := string(key)
		// Use the full key as the document ID to prevent cross-store index overwriting
		indexID := strings.TrimPrefix(k, defaultTablePrefix)
		u := strings.SplitN(indexID, "|", 2)
		store := u[0]

		var d interface{}
		if ix, ok := index.(*GeoIndexer); ok {
			geoDoc := map[string]interface{}{"bucket": store, "data": v}
			if vv, ok := v["_"+ix.Field]; ok {
				geoDoc[ix.Field] = vv
			}
			d = geoDoc
		} else {
			d = IndexedData{store, v}
		}

		if batchSize > 0 {
			batch.Index(indexID, d)
		} else {
			if err := index.IndexDocument(indexID, d); err != nil {
				return err
			}
		}
		count++

		// Only check time periodically to save CPU cycles
		timeToUpdate := false
		if processedInBatch > 0 && processedInBatch%1000 == 0 {
			timeToUpdate = time.Since(lastUpdate) >= time.Minute
		}

		if batchSize > 0 && (count > 0 && count%batchSize == 0 || timeToUpdate) {
			if batch.Size() > 0 {
				if err := index.Batch(batch); err != nil {
					return err
				}
				bar.Add(processedInBatch)
				processedInBatch = 0
				lastUpdate = time.Now()
				batch = index.BatchIndex()
			}
		} else if batchSize <= 0 && (processedInBatch >= 1000 || timeToUpdate) {
			bar.Add(processedInBatch)
			processedInBatch = 0
			lastUpdate = time.Now()
		}

		iter.Next()
	}
	if batchSize > 0 && batch.Size() > 0 {
		if err := index.Batch(batch); err != nil {
			return err
		}
	}
	if processedInBatch > 0 {
		bar.Add(processedInBatch)
	}
	logger.Info("reindexed", "count", count, "total_rows", totalRows)
	logger.Info("writing index file", "path", indexInitFilePath)
	return os.WriteFile(indexInitFilePath, []byte(name+"|"+time.Now().UTC().String()+"|"+strconv.Itoa(count)), os.ModePerm)
}

// reindexParallel uses a producer-worker pattern for concurrent reindexing.
// Each worker maintains its own bleve.Batch to avoid lock contention and to
// align with Scorch's concurrent batch design. The bounded jobs channel
// provides backpressure so the producer cannot exhaust memory on low-memory
// instances.
//
// When pauseAfterDocs > 0 the work is split into chunks: the producer stops
// after sending pauseAfterDocs items, all workers drain and flush, then the
// function sleeps for pauseDuration before starting the next chunk. This
// gives Scorch's background merger an uncontested window to consolidate
// segments. When pauseAfterDocs == 0 the loop executes exactly once,
// preserving the original single-shot behaviour (100% backwards compatible).
func reindexParallel(name, indexInitFilePath string, provider ProviderStore, index Indexer, workers, batchSize, pauseAfterDocs int, pauseDuration time.Duration) error {
	iter, _ := provider.Cursor()
	defer iter.Close()

	var totalCount int64
	var totalRows int64

	bar := progressbar.Default(-1, "reindexing")
	defer bar.Finish()

	// Determine if this is a GeoIndexer to avoid the type assertion per-item
	// inside each worker.
	geoIx, isGeo := index.(*GeoIndexer)

	// --- chunk loop (collapses to a single iteration when pausing is disabled) ---
	for {
		if !iter.Valid() {
			break
		}

		// Bounded channel: capacity is workers * 2 to allow some buffering while
		// keeping memory usage proportional to the number of workers.
		jobsCap := workers * 2
		if jobsCap < 4 {
			jobsCap = 4
		}
		jobs := make(chan reindexJob, jobsCap)

		g, ctx := errgroup.WithContext(context.Background())
		var chunkCount int64 // items sent in this chunk

		// --- Producer goroutine ---
		g.Go(func() error {
			defer close(jobs)
			for iter.Valid() {
				// Stop the chunk once the per-chunk limit is reached.
				if pauseAfterDocs > 0 &&
					atomic.LoadInt64(&chunkCount) >= int64(pauseAfterDocs) {
					return nil // iterator NOT advanced; next outer loop resumes here
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				atomic.AddInt64(&totalRows, 1)
				key := iter.Key()
				val := iter.Value()

				// Check for iterator errors.
				if errIter, ok := iter.(ErrorAwareIterator); ok {
					if err := errIter.LastError(); err != nil {
						return fmt.Errorf("iterator error at key %s: %w", string(key), err)
					}
				}

				if val == nil {
					iter.Next()
					continue
				}

				// Copy key and val using the pool so they outlive the iterator step.
				keyCopy := getPooledSlice(key)
				valCopy := getPooledSlice(val)

				select {
				case <-ctx.Done():
					putPooledSlice(keyCopy)
					putPooledSlice(valCopy)
					return ctx.Err()
				case jobs <- reindexJob{key: keyCopy, val: valCopy}:
					atomic.AddInt64(&chunkCount, 1)
				}

				iter.Next()
			}
			return nil
		})

		// --- Worker goroutines ---
		for w := 0; w < workers; w++ {
			g.Go(func() error {
				batch := index.BatchIndex()
				batchCount := 0

				for job := range jobs {
					var v map[string]interface{}
					if err := jiter.Unmarshal(*job.val, &v); err != nil {
						logger.Warn("failed to unmarshal value", "key", string(*job.key), "val_len", len(*job.val))
						putPooledSlice(job.key)
						putPooledSlice(job.val)
						continue
					}

					k := string(*job.key)
					indexID := strings.TrimPrefix(k, defaultTablePrefix)
					u := strings.SplitN(indexID, "|", 2)
					store := u[0]

					var d interface{}
					if isGeo {
						geoDoc := map[string]interface{}{"bucket": store, "data": v}
						if vv, ok := v["_"+geoIx.Field]; ok {
							geoDoc[geoIx.Field] = vv
						}
						d = geoDoc
					} else {
						d = IndexedData{store, v}
					}

					batch.Index(indexID, d)
					batchCount++
					atomic.AddInt64(&totalCount, 1)

					// Return pooled slices now that we're done with them.
					putPooledSlice(job.key)
					putPooledSlice(job.val)

					if batchCount >= batchSize {
						if batch.Size() > 0 {
							if err := index.Batch(batch); err != nil {
								return err
							}
							bar.Add(batchCount)
							batch = index.BatchIndex()
							batchCount = 0
						}
					}
				}

				// Flush remaining items in the worker's batch.
				if batch.Size() > 0 {
					if err := index.Batch(batch); err != nil {
						return err
					}
					bar.Add(batchCount)
				}
				return nil
			})
		}

		if err := g.Wait(); err != nil {
			return err
		}

		// Chunk boundary: stop if exhausted or pausing is disabled.
		if !iter.Valid() || pauseAfterDocs <= 0 {
			break
		}

		logger.Info("reindex chunk complete – pausing for merge cooldown",
			"chunk_docs", atomic.LoadInt64(&chunkCount),
			"total", atomic.LoadInt64(&totalRows),
			"pause", pauseDuration,
		)
		time.Sleep(pauseDuration)
	}

	count := int(atomic.LoadInt64(&totalCount))
	rows := int(atomic.LoadInt64(&totalRows))
	logger.Info("reindexed", "count", count, "total_rows", rows, "workers", workers)
	logger.Info("writing index file", "path", indexInitFilePath)
	return os.WriteFile(indexInitFilePath, []byte(name+"|"+time.Now().UTC().String()+"|"+strconv.Itoa(count)), os.ModePerm)
}

// IndexedData represents a stored row
type IndexedData struct {
	Bucket string      `json:"bucket"`
	Data   interface{} `json:"data"`
}

var logger = log.New("gostore-contrib.indexer")

type RequestOpt func(*bleve.SearchRequest) error

var OrderRequest = func(orderBy []string) RequestOpt {
	return func(req *bleve.SearchRequest) error {
		req.SortBy(orderBy)
		return nil
	}
}

var ExplainRequest = func(v bool) RequestOpt {
	return func(req *bleve.SearchRequest) error {
		req.Explain = v
		return nil
	}
}

type DefaultIndexer struct {
	index bleve.Index
}

func (i *DefaultIndexer) Index() bleve.Index {
	return i.index
}
func (i *DefaultIndexer) BatchIndex() *bleve.Batch {
	return i.index.NewBatch()
}
func (i *DefaultIndexer) Batch(b *bleve.Batch) error {
	return i.index.Batch(b)
}
func (i *DefaultIndexer) AddDocumentMapping(name string, dm *mapping.DocumentMapping) {
	// i.index.AddDocumentMapping(name, dm)
}

func (i *DefaultIndexer) IndexDocument(id string, data interface{}) error {
	if i.index == nil {
		return errors.New("no index")
	}
	// logger.Debug("Indexing document", "id", id, "data", data)
	return i.index.Index(id, data)
}

func (i *DefaultIndexer) UnIndexDocument(id string) error {
	if i.index == nil {
		return errors.New("no index")
	}
	// logger.Debug("UnIndexing document", "id", id)
	return i.index.Delete(id)
}

func (i *DefaultIndexer) QueryMap(q map[string]interface{}, opts ...RequestOpt) (*bleve.SearchResult, error) {
	queryString := ""
	for k, v := range q {
		queryString = fmt.Sprintf("%s %s:%v", queryString, k, v)
	}
	return i.Query(queryString, opts...)
}
func (i *DefaultIndexer) Query(q string, opts ...RequestOpt) (*bleve.SearchResult, error) {
	if i.index == nil {
		return nil, errors.New("no index")
	}
	// println(q)
	query := bleve.NewQueryStringQuery(q)
	searchRequest := bleve.NewSearchRequest(query)
	for _, opt := range opts {
		if err := opt(searchRequest); err != nil {
			logger.Warn("failed option passed")
		}
	}
	return i.index.Search(searchRequest)
}

func (i *DefaultIndexer) QueryWithOptions(q string, size, from int, explain bool, fields []string, opts ...RequestOpt) (*bleve.SearchResult, error) {
	if i.index == nil {
		return nil, errors.New("no index")
	}
	query := bleve.NewQueryStringQuery(q)
	searchRequest := bleve.NewSearchRequestOptions(query, size, from, explain)
	if len(fields) > 0 {
		searchRequest.Fields = fields
	}
	for _, opt := range opts {
		if err := opt(searchRequest); err != nil {
			logger.Warn("failed option passed")
		}
	}
	return i.index.Search(searchRequest)
}

func (i *DefaultIndexer) FacetedQuery(q string, facets *Facets, size, from int, explain bool, fields []string, opts ...RequestOpt) (*bleve.SearchResult, error) {
	if i.index == nil {
		return nil, errors.New("no index")
	}
	query := bleve.NewQueryStringQuery(q)
	searchRequest := bleve.NewSearchRequestOptions(query, size, from, explain)
	if len(fields) > 0 {
		searchRequest.Fields = fields
	}
	for _, opt := range opts {
		if err := opt(searchRequest); err != nil {
			logger.Warn("failed option passed")
		}
	}
	AddFacets(searchRequest, facets)
	return i.index.Search(searchRequest)
}
func (i *DefaultIndexer) QueryWithOptionsHighlighted(q string, size, from int, explain bool, fields []string, opts ...RequestOpt) (*bleve.SearchResult, error) {

	if i.index == nil {
		return nil, errors.New("no index")
	}
	query := bleve.NewQueryStringQuery(q)
	searchRequest := bleve.NewSearchRequestOptions(query, size, from, explain)
	searchRequest.Highlight = bleve.NewHighlightWithStyle("ansi")
	for _, opt := range opts {
		if err := opt(searchRequest); err != nil {
			logger.Warn("failed option passed")
		}
	}
	return i.index.Search(searchRequest)
}

func (i *DefaultIndexer) MatchQuery(q, field string, opts ...RequestOpt) (*bleve.SearchResult, error) {

	if i.index == nil {
		return nil, errors.New("no index")
	}
	query := bleve.NewMatchQuery(q)
	query.SetField(field)
	query.SetFuzziness(0)
	searchRequest := bleve.NewSearchRequest(query)
	for _, opt := range opts {
		if err := opt(searchRequest); err != nil {
			logger.Warn("failed option passed")
		}
	}
	return i.index.Search(searchRequest)
}

func (i *DefaultIndexer) TermQuery(q string, opts ...RequestOpt) (*bleve.SearchResult, error) {

	if i.index == nil {
		return nil, errors.New("no index")
	}
	query := bleve.NewTermQuery(q)
	searchRequest := bleve.NewSearchRequest(query)
	for _, opt := range opts {
		if err := opt(searchRequest); err != nil {
			logger.Warn("failed option passed")
		}
	}
	return i.index.Search(searchRequest)
}

func (i *DefaultIndexer) MatchPhraseQuery(q string, opts ...RequestOpt) (*bleve.SearchResult, error) {

	if i.index == nil {
		return nil, errors.New("no index")
	}
	query := bleve.NewMatchPhraseQuery(q)
	searchRequest := bleve.NewSearchRequest(query)
	for _, opt := range opts {
		if err := opt(searchRequest); err != nil {
			logger.Warn("failed option passed")
		}
	}
	return i.index.Search(searchRequest)
}

func (i *DefaultIndexer) Close() {

	if i.index == nil {
		return
	}
	err := i.index.Close()
	if err != nil {
		logger.Warn("error while closing index")
	}
}

func GetIndex(indexPath string) (bleve.Index, bool) {
	index, err := bleve.Open(indexPath)
	if err == bleve.ErrorIndexPathDoesNotExist {
		logger.Debug("Index path does not exist", "path", "indexPath")
		return nil, false
	}
	return index, true
}
func NewIndexerFromIndex(index bleve.Index) Indexer {
	return &DefaultIndexer{index: index}
}

// NewIndexer creates a new indexer
func NewDefaultIndexer(indexPath string) Indexer {
	indexMapping := bleve.NewIndexMapping()
	indexMapping.StoreDynamic = true
	indexMapping.IndexDynamic = true
	return NewIndexer(indexPath, indexMapping)
}

// NewGeoEnabledIndexMapping creates a new mapping with geo support
func NewGeoEnabledIndexMapping(geoField, documentName, typeField string) mapping.IndexMapping {
	geoMapping := bleve.NewDocumentMapping()
	locationMapping := bleve.NewGeoPointFieldMapping()
	locationMapping.IncludeTermVectors = true
	locationMapping.IncludeInAll = true
	locationMapping.Index = true
	locationMapping.Store = true
	locationMapping.Type = "geopoint"
	geoMapping.AddFieldMappingsAt(geoField, locationMapping)
	indexMapping := bleve.NewIndexMapping()
	indexMapping.IndexDynamic = true
	indexMapping.StoreDynamic = true
	indexMapping.AddDocumentMapping(documentName, geoMapping)
	indexMapping.TypeField = typeField
	logger.Debug(fmt.Sprintf("NewGeoEnabledIndexMapping(geoField - %s %s %s)", geoField, documentName, typeField))
	return indexMapping
}

// NewIndexer creates a new indexer
func NewIndexer(indexPath string, indexMapping mapping.IndexMapping) *DefaultIndexer {
	index, err := bleve.Open(indexPath)
	if err != nil {
		logger.Debug("Error opening indexpath", "path", indexPath, "verbose", string(err.Error()))
		if err == bleve.ErrorIndexMetaMissing || err == bleve.ErrorIndexPathDoesNotExist {
			logger.Debug(fmt.Sprintf("Creating new index at %s ...", indexPath))
			// indexMapping.DefaultAnalyzer = "keyword"
			index, err = bleve.New(indexPath, indexMapping)
			if err != nil {
				logger.Warn("Index could not be created", "path", indexPath, "err", string(err.Error()))
				if err != bleve.ErrorIndexPathExists {
					panic(err)
				}
				return nil
			}
			return &DefaultIndexer{index: index}
		}
		panic(err)
	}
	return &DefaultIndexer{index: index}
}

// GeoIndexer an indexer that can handle geo queries
type GeoIndexer struct {
	Field string
	Indexer
}

func (g *GeoIndexer) SetField(field string) {
	g.Field = field
}

// GeoDistance get results within a distance from a lon lat
func (g *GeoIndexer) GeoDistance(lon, lat float64, distance string, opts ...RequestOpt) (*bleve.SearchResult, error) {

	//distance query
	distanceQuery := bleve.NewGeoDistanceQuery(lon, lat, distance)
	distanceQuery.SetField(g.Field)

	//execute request on index
	searchRequest := bleve.NewSearchRequest(distanceQuery)
	for _, opt := range opts {
		if err := opt(searchRequest); err != nil {
			logger.Warn("failed option passed")
		}
	}
	return g.Indexer.Index().Search(searchRequest)
}

// GeoDistanceQuery Geo
func (g *GeoIndexer) GeoDistanceQuery(q string, lon, lat float64, distance string, size, from int, explain bool, fields []string, opts ...RequestOpt) (*bleve.SearchResult, error) {
	//Search the index with GEO //https://github.com/blevesearch/bleve/issues/836
	//https://github.com/blevesearch/bleve/issues/599
	//term query
	if g.Index() == nil {
		return nil, errors.New("no index")
	}
	query := bleve.NewQueryStringQuery(q)
	//distance query
	distanceQuery := bleve.NewGeoDistanceQuery(lon, lat, distance)
	distanceQuery.SetField(g.Field)
	// fmt.Println("geofield", g.Field, "query", query, lon, lat, distance)

	//Conjonction of the term and distance queries
	conRequest := bleve.NewConjunctionQuery()
	conRequest.AddQuery(query)
	conRequest.AddQuery(distanceQuery)

	//execute request on index
	searchRequest := bleve.NewSearchRequestOptions(conRequest, size, from, explain)
	if len(fields) > 0 {
		searchRequest.Fields = fields
	}
	for _, opt := range opts {
		if err := opt(searchRequest); err != nil {
			logger.Warn("failed option passed")
		}
	}
	return g.Index().Search(searchRequest)
}
