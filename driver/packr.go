package driver

import (
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/gobuffalo/packr"
	"github.com/golang-migrate/migrate/v4/source"
)

func init() {
	source.Register("packr", &packrDriver{})
}

// ErrNoBox indicates that a source is not a Packr box instance.
var ErrNoBox = fmt.Errorf("not a box")

type packrDriver struct {
	box        packr.Box
	migrations *source.Migrations
}

// WithInstance returns a new driver from a box.
func WithInstance(box interface{}) (source.Driver, error) {
	b, ok := box.(packr.Box)
	if !ok {
		return nil, ErrNoBox
	}
	p := &packrDriver{box: b, migrations: source.NewMigrations()}
	if err := p.prepare(); err != nil {
		return nil, err
	}
	return p, nil
}

// Open returns a a new driver instance configured with parameters
// coming from the URL string.
func (d *packrDriver) Open(url string) (source.Driver, error) {
	if url == "" {
		return nil, fmt.Errorf("invalid URL '%s'", url)
	}
	box := packr.NewBox(url)
	p := &packrDriver{
		migrations: source.NewMigrations(),
		box:        box,
	}

	if err := p.prepare(); err != nil {
		return nil, err
	}

	return p, nil
}

// Close closes the underlying source instance managed by the driver.
// Since packr boxes don't close, this function doesn't do anything.
func (d *packrDriver) Close() error {
	// nothing to close
	return nil
}

// First returns the very first migration version available to the driver.
// If there is no version available, it returns os.ErrNotExist.
func (d *packrDriver) First() (version uint, err error) {
	v, ok := d.migrations.First()
	if ok {
		return v, nil
	}
	return 0, os.ErrNotExist
}

// Prev returns the previous version for a given version available to the driver.
// If there is no previous version available, it returns os.ErrNotExist.
func (d *packrDriver) Prev(version uint) (prevVersion uint, err error) {
	index, ok := d.migrations.Prev(version)
	if ok {
		return index, nil
	}
	return 0, os.ErrNotExist
}

// Next returns the next version for a given version available to the driver.
// If there is no next version available, it returns os.ErrNotExist.
func (d *packrDriver) Next(version uint) (nextVersion uint, err error) {
	index, ok := d.migrations.Next(version)
	if ok {
		return index, nil
	}
	return 0, os.ErrNotExist
}

// ReadUp returns the UP migration body and an identifier that helps
// finding this migration in the source for a given version.
// If there is no up migration available for this version,
// it returns os.ErrNotExist.
func (d *packrDriver) ReadUp(version uint) (r io.ReadCloser, identifier string, err error) {
	m, ok := d.migrations.Up(version)
	if !ok {
		return nil, "", os.ErrNotExist
	}

	data, err := d.box.Open(m.Raw)
	if err != nil {
		return nil, "", os.ErrExist
	}
	return data, m.Identifier, nil
}

// ReadDown returns the DOWN migration body and an identifier that helps
// finding this migration in the source for a given version.
// If there is no down migration available for this version,
// it returns os.ErrNotExist.
func (d *packrDriver) ReadDown(version uint) (r io.ReadCloser, identifier string, err error) {
	m, ok := d.migrations.Down(version)
	if !ok {
		return nil, "", os.ErrNotExist
	}
	data, err := d.box.Open(m.Raw)
	if err != nil {
		return nil, "", os.ErrExist
	}
	return data, m.Identifier, nil
}

func (d *packrDriver) prepare() error {
	files := d.box.List()
	sort.Strings(files)

	for _, file := range files {
		m, err := source.DefaultParse(file)
		if err != nil {
			continue
		}
		if !d.migrations.Append(m) {
			return fmt.Errorf("unable to parse migration: %s", file)
		}
	}
	return nil
}
