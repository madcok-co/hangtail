package hangtailextension

import (
	"errors"
	"time"
)

// Config is the configuration for the hangtail extension.
type Config struct {
	// StorageDir is the directory where segment files are written.
	StorageDir string `mapstructure:"storage_dir"`

	// SegmentWindow is the duration covered by each segment file (default: 1m).
	SegmentWindow time.Duration `mapstructure:"segment_window"`

	// SegmentMaxAge is the maximum age a segment can reach before the reaper
	// processes it (default: 5m).
	SegmentMaxAge time.Duration `mapstructure:"segment_max_age"`

	// MaxStagingBytes is the maximum total size of all staging files.
	// When reached, the oldest segment is force-processed early (default: 20 GiB).
	MaxStagingBytes int64 `mapstructure:"max_staging_bytes"`

	// FsyncInterval controls how often segment data is fsynced to disk (default: 1s).
	FsyncInterval time.Duration `mapstructure:"fsync_interval"`

	// EndMarker configures how a flow completion is signalled.
	EndMarker EndMarkerConfig `mapstructure:"end_marker"`

	// KeepOnError forces forwarding of a trace that has flow.end but also
	// contains an ERROR log or an error-status span (default: true).
	KeepOnError bool `mapstructure:"keep_on_error"`

	// Untraced controls what happens to records that have no trace_id.
	// Valid values: "forward" (default) | "drop".
	Untraced string `mapstructure:"untraced"`

	// TailAddr is the host:port for the HTTP SSE tail server used by
	// `hangtail tail`. Leave empty to disable (default: disabled).
	TailAddr string `mapstructure:"tail_addr"`
}

// EndMarkerConfig describes the attribute that marks a flow as complete.
type EndMarkerConfig struct {
	// Attribute is the log/span attribute name (default: "flow.end").
	Attribute string `mapstructure:"attribute"`
	// Value is the expected string value (default: "true").
	Value string `mapstructure:"value"`
}

func (c *Config) Validate() error {
	if c.StorageDir == "" {
		return errors.New("hangtail: storage_dir is required")
	}
	if c.SegmentWindow <= 0 {
		return errors.New("hangtail: segment_window must be positive")
	}
	if c.SegmentMaxAge < c.SegmentWindow {
		return errors.New("hangtail: segment_max_age must be >= segment_window")
	}
	if c.MaxStagingBytes <= 0 {
		return errors.New("hangtail: max_staging_bytes must be positive")
	}
	if c.FsyncInterval <= 0 {
		return errors.New("hangtail: fsync_interval must be positive")
	}
	if c.EndMarker.Attribute == "" {
		return errors.New("hangtail: end_marker.attribute is required")
	}
	if c.Untraced != "forward" && c.Untraced != "drop" {
		return errors.New("hangtail: untraced must be \"forward\" or \"drop\"")
	}
	return nil
}
